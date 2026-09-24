using RedisEvents.Config;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Producer;

/// <summary>One message a <see cref="TopicForwarder"/> publishes onto its target topic.</summary>
/// <param name="PartitionKey">The routing key on the target topic.</param>
/// <param name="Body">The message body. Passed to Redis by reference, so it must not be mutated until the forward completes.</param>
/// <param name="Type">The message type string consumers filter on.</param>
public readonly record struct ForwardedMessage(string PartitionKey, ReadOnlyMemory<byte> Body, string Type);

/// <summary>
/// Republishes events read from one topic onto another, <b>exactly once in effect</b>, although the
/// read that feeds it is at-least-once.
/// </summary>
/// <remarks>
/// <para>
/// <b>What it is for.</b> A service that consumes one topic and announces what it saw on another —
/// a ledger's events mapped onto a public <c>transactions</c> topic, a rail's status changes onto a
/// shared feed. The consume side is at-least-once: a redelivery, a restart before a position flush, or
/// a lost consumer position that falls back to <c>StartFromWhenMissing=Beginning</c> all hand the
/// forwarder events it has already forwarded. Publishing each again would duplicate them downstream —
/// and in the lost-position case, duplicate the whole retained source topic.
/// </para>
/// <para>
/// <b>How.</b> A cross-topic publish cannot join the source's own transaction, so two guards are
/// written in the <b>same</b> <c>MULTI</c>/<c>EXEC</c> as the publish, both hash-tagged on the target
/// topic so they share its slot (see <see cref="Outbox"/>):
/// </para>
/// <list type="bullet">
///   <item><description>
///   A durable <b>high-water mark</b> per <i>source</i>: the stream position of the last event forwarded
///   from it. This is the real guarantee. An event at or below the mark was forwarded already and is
///   skipped, which is what makes a replay of the whole source harmless. Positions are compared as
///   stream ids, not strings (<c>"10-0"</c> sorts before <c>"9-0"</c> as text).
///   </description></item>
///   <item><description>
///   An <see cref="Idempotency"/> marker per <i>dedupe id</i>, living <see cref="MarkerTtl"/>: an
///   optimisation for a redelivery inside that window, and a second line for callers whose dedupe id
///   is meaningful (one transaction id announced once). It is not a lock; the high-water mark is what
///   is relied on.
///   </description></item>
/// </list>
/// <para>
/// A concurrent forward from the same source loses the high-water <c>WATCH</c> and publishes nothing;
/// <see cref="ForwardAsync"/> returns <see langword="false"/>, the same as for an event forwarded before.
/// </para>
/// <para>
/// <b>A source must be delivered in order.</b> The mark only moves forward, so an event older than one
/// already forwarded from the same source is treated as forwarded. A source is therefore one ordered
/// stream of positions — a topic with one partition, or one partition of a larger topic (name the source
/// <c>"{topic}:{partition}"</c>). Several sources may forward onto one target; each keeps its own mark.
/// </para>
/// <code>
/// var forwarder = new TopicForwarder(sharedDb, "transactions", streamOptions);
///
/// // in a projection handler for the source topic:
/// await forwarder.ForwardAsync(
///     source: "customer_wallet",
///     dedupeId: $"{meta.PartitionKey}:{e.TransactionId}",
///     position: meta.Id,
///     [new ForwardedMessage(customerId, body, typeof(WalletTransaction).FullName!)]);
/// </code>
/// </remarks>
public sealed class TopicForwarder
{
    private readonly IDatabase database;
    private readonly RedisKey highWaterKey;
    private readonly TopicOptions? topicOptions;

    /// <summary>Creates a forwarder onto <paramref name="targetTopic"/>.</summary>
    /// <param name="database">A database on the <b>shared</b> streams multiplexer — the forward is an outbox transaction; see <see cref="Outbox"/>.</param>
    /// <param name="targetTopic">The topic every forward publishes to.</param>
    /// <param name="streamOptions">The configured streams, for the target topic's partitioning and trimming.</param>
    /// <param name="name">
    /// Names this forwarder's high-water state, <c>{targetTopic}:state:{name}:high-water</c>. Forwarders
    /// onto one target that share a name share marks, which is only correct when their sources do not
    /// overlap. Defaults to <c>"forward"</c>.
    /// </param>
    /// <param name="markerTtl">How long a dedupe marker lives. Defaults to <see cref="DefaultMarkerTtl"/>.</param>
    public TopicForwarder(IDatabase database, string targetTopic, StreamOptions streamOptions, string name = "forward", TimeSpan? markerTtl = null)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetTopic);
        ArgumentNullException.ThrowIfNull(streamOptions);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        this.database = database;
        this.TargetTopic = targetTopic;
        this.topicOptions = streamOptions.Topics.GetValueOrDefault(targetTopic);
        this.highWaterKey = Outbox.StateKey(targetTopic, $"{name}:high-water");
        this.MarkerTtl = markerTtl ?? DefaultMarkerTtl;
    }

    /// <summary>How long a dedupe marker lives when none is configured: a week.</summary>
    public static TimeSpan DefaultMarkerTtl { get; } = TimeSpan.FromDays(7);

    /// <summary>The topic every forward publishes to.</summary>
    public string TargetTopic { get; }

    /// <summary>How long a dedupe marker lives.</summary>
    public TimeSpan MarkerTtl { get; }

    /// <summary>
    /// Publishes <paramref name="messages"/> onto the target topic unless this source has already
    /// forwarded <paramref name="position"/> or anything after it, or <paramref name="dedupeId"/> is still
    /// marked.
    /// </summary>
    /// <param name="source">The ordered source the event came from — a topic, or <c>"{topic}:{partition}"</c>; names its high-water mark.</param>
    /// <param name="dedupeId">The event's dedupe id within the target, e.g. <c>"{entity}:{transactionId}"</c>.</param>
    /// <param name="position">The event's position in <paramref name="source"/>.</param>
    /// <param name="messages">What to publish; at least one. All land in one <c>MULTI</c>/<c>EXEC</c>.</param>
    /// <param name="correlationId">Stamped on every published message.</param>
    /// <param name="ct">Checked before the transaction is built.</param>
    /// <returns>
    /// <see langword="true"/> when published; <see langword="false"/> when it had already been forwarded,
    /// or a concurrent forward from the same source moved the mark first — either way nothing was written.
    /// </returns>
    public async Task<bool> ForwardAsync(
        string source,
        string dedupeId,
        StreamId position,
        IReadOnlyList<ForwardedMessage> messages,
        string? correlationId = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        ArgumentException.ThrowIfNullOrWhiteSpace(dedupeId);
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
        {
            throw new ArgumentException("A forward needs at least one message.", nameof(messages));
        }

        var stored = await this.database.HashGetAsync(this.highWaterKey, source).ConfigureAwait(false);
        if (!stored.IsNull && StreamId.TryParse(stored.ToString(), out var highWater) && highWater >= position)
        {
            return false;
        }

        var marker = Idempotency.Key(this.TargetTopic, dedupeId);

        var publishes = new OutboxPublish[messages.Count];
        for (var i = 0; i < publishes.Length; i++)
        {
            var message = messages[i];
            publishes[i] = new OutboxPublish(this.TargetTopic, message.PartitionKey, message.Body, message.Type, new PublishOptions(CorrelationId: correlationId), this.topicOptions);
        }

        var positionText = position.Format();
        var ids = await Outbox.WriteAndPublishManyAsync(
            this.database,
            tran =>
            {
                _ = tran.StringSetAsync(marker, string.Empty, this.MarkerTtl);
                _ = tran.HashSetAsync(this.highWaterKey, source, positionText);
            },
            publishes,
            conditions:
            [
                stored.IsNull ? Condition.HashNotExists(this.highWaterKey, source) : Condition.HashEqual(this.highWaterKey, source, stored),
                Condition.KeyNotExists(marker),
            ],
            stateKeys: [marker, this.highWaterKey],
            ct: ct).ConfigureAwait(false);

        return ids is not null;
    }
}
