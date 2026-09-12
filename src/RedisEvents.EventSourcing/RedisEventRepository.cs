using Microsoft.Extensions.Logging;

using RedisEvents.Producer;
using RedisEvents.Wire;

namespace RedisEvents.EventSourcing;

/// <summary>
/// The <see cref="IEventRepository"/> over <see cref="IStreamStore"/>: one Redis stream per
/// aggregate, its length as the version, and the topic publish that feeds projections riding in the
/// same transaction.
/// </summary>
/// <remarks>
/// <para>
/// <b>Everything interesting happens in one round trip.</b> A save is a single
/// <see cref="IStreamStore.AppendAndPublishAsync"/> call: no read to check the current version, no
/// second key holding it, no relay process moving events from the store to the topic. The version
/// check is <c>WATCH</c> plus an <c>XLEN</c> comparison inside the transaction, so the three outcomes
/// are committed, <see cref="ConcurrencyException"/> with nothing written, or a transport failure
/// that left both streams consistent with each other either way.
/// </para>
/// <para>
/// <b>Loading pages, and reads all of it.</b> History is read in pages of
/// <see cref="LoadPageSize"/> until a short page says the stream has ended. The reference
/// implementation this package replaces read history with a single <c>XRANGE … COUNT 4000</c>: an
/// aggregate that ever passed four thousand events silently loaded a prefix of itself and then
/// happily accepted commands against that wrong state. Paging until short has no such ceiling, and
/// costs exactly one extra round trip on the boundary case where the history is an exact multiple of
/// the page size.
/// </para>
/// <para>
/// <b>Headers.</b> Every event carries <c>es-version</c>, its 1-based position in the aggregate's
/// history, and <c>es-id</c>, a fresh <see cref="Guid"/> — the first lets a projection assert
/// per-aggregate monotonicity or drop a stale redelivery (core is at-least-once), the second gives
/// every event a stable identity independent of the stream id Redis assigns it. The caller's own
/// correlation id and headers flow through unchanged; the two of ours are added to them, never
/// instead of them.
/// </para>
/// <para>
/// <b>The logger is optional</b>, as it is everywhere in this library: a repository that cannot be
/// constructed without a logging stack is a repository that is awkward to use from a test or a
/// migration script. Nothing here logs on the success path.
/// </para>
/// <para>Instances are thread-safe and are intended to be registered as singletons. Aggregates are not.</para>
/// </remarks>
public sealed class RedisEventRepository : IEventRepository
{
    /// <summary>
    /// Events per <see cref="IStreamStore.ReadAsync"/> call while replaying history. Big enough that
    /// ordinary aggregates load in one round trip, small enough that a pathological one does not
    /// materialise its whole history in a single reply.
    /// </summary>
    internal const int LoadPageSize = 1000;

    /// <summary>The 1-based position of an event in its aggregate's history.</summary>
    internal const string VersionHeader = "es-version";

    /// <summary>An event's own identity, independent of the stream id Redis assigns it.</summary>
    internal const string IdHeader = "es-id";

    private readonly IStreamStore store;
    private readonly EventTypeRegistry registry;
    private readonly ILogger? log;

    /// <summary>
    /// Creates a repository over one topic's state store.
    /// </summary>
    /// <param name="store">
    /// The state store for the topic this aggregate family lives on, as registered by
    /// <c>AddStreamStore</c>. One topic per aggregate family: the aggregate id is the partition key,
    /// so every event of one save lands on one partition and stays ordered for the read side.
    /// </param>
    /// <param name="registry">
    /// The wire-type registry every event of this family is registered with. Both sides of the
    /// library share it — a save encodes through it and a load decodes through it — so a type
    /// registered once cannot be written under one string and read under another.
    /// </param>
    /// <param name="logger">Optional; used only to explain a concurrency loss at <c>Debug</c>.</param>
    public RedisEventRepository(IStreamStore store, EventTypeRegistry registry, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(registry);

        this.store = store;
        this.registry = registry;
        this.log = logger;
    }

    /// <summary>
    /// The state stream's name for one aggregate — the tail of the Redis key
    /// <c>{topic}:state:es:&lt;AggregateName&gt;:&lt;id&gt;</c> that <see cref="Outbox.StateKey"/>
    /// completes.
    /// </summary>
    /// <remarks>
    /// <see cref="AggregateRoot.AggregateName"/> and not <c>GetType().Name</c>, so renaming the
    /// aggregate class never moves its history to a key nothing reads.
    /// </remarks>
    /// <param name="aggregateName">The aggregate family.</param>
    /// <param name="id">The aggregate's id.</param>
    /// <returns>The state stream name.</returns>
    internal static string StreamName(string aggregateName, string id) => $"es:{aggregateName}:{id}";

    /// <inheritdoc />
    public async ValueTask<TAggregate?> LoadAsync<TAggregate>(string id, CancellationToken ct = default)
        where TAggregate : AggregateRoot, new()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        // Constructed before anything is read, because the aggregate is what knows which stream to
        // read: AggregateName is fixed per type and readable on an empty instance, while Id only
        // becomes meaningful once history has been applied. This is also the instance that is
        // replayed into, so nothing is built twice.
        var aggregate = new TAggregate();
        var name = StreamName(aggregate.AggregateName, id);

        List<object>? history = null;
        var after = StreamId.Min;

        while (true)
        {
            var page = await this.store.ReadAsync(name, after, LoadPageSize, ct).ConfigureAwait(false);
            if (page.Count == 0)
            {
                break;
            }

            history ??= new List<object>(page.Count);

            foreach (var message in page)
            {
                if (!this.registry.TryDecode(message.Type, message.Body, out var @event))
                {
                    throw new InvalidOperationException(
                        $"'{aggregate.AggregateName}' '{id}' has an event of wire type '{message.Type}' at " +
                        $"{message.Id.Format()} in its history, which is not registered with the " +
                        $"{nameof(EventTypeRegistry)}. An aggregate's own history must be fully understood; " +
                        "register the type (or a shim that reads it) rather than skipping the event.");
                }

                history.Add(@event);
            }

            after = page[^1].Id;

            // A short page is the end of the stream. The reference implementation's single capped
            // read is what this loop exists to replace.
            if (page.Count < LoadPageSize)
            {
                break;
            }
        }

        if (history is null)
        {
            return null;
        }

        aggregate.LoadFromHistory(history);
        return aggregate;
    }

    /// <inheritdoc />
    public async ValueTask<int> SaveAsync(
        AggregateRoot aggregate,
        int? expectedVersion = null,
        PublishOptions options = default,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(aggregate);

        var uncommitted = aggregate.GetUncommittedChanges();
        if (uncommitted.Count == 0)
        {
            // Saving an aggregate that decided to do nothing is not an error, and it is not a round
            // trip either: a transaction that appends no events is not an append.
            return aggregate.Version;
        }

        var expected = expectedVersion ?? aggregate.Version;
        var name = StreamName(aggregate.AggregateName, aggregate.Id);

        var events = new StateEvent[uncommitted.Count];
        for (var i = 0; i < events.Length; i++)
        {
            var (wireType, body) = this.registry.Encode(uncommitted[i]);
            events[i] = new StateEvent(body, wireType, WithEventHeaders(options, expected + i + 1));
        }

        var ids = await this.store
            .AppendAndPublishAsync(name, expected, aggregate.Id, events, ct)
            .ConfigureAwait(false);

        if (ids is null)
        {
            // Nothing was written — neither the history nor the topic — so the aggregate keeps its
            // uncommitted changes and the caller can reload, re-decide and try again.
            this.log?.LogDebug(
                "Event sourcing: save of '{AggregateName}' '{Id}' at expected version {ExpectedVersion} lost its version check; nothing was written.",
                aggregate.AggregateName,
                aggregate.Id,
                expected);

            throw new ConcurrencyException(aggregate.AggregateName, aggregate.Id, expected);
        }

        aggregate.MarkChangesAsCommitted();
        return expected + events.Length;
    }

    /// <summary>
    /// The caller's options with this event's <c>es-version</c> and <c>es-id</c> added to — never
    /// substituted for — whatever headers they passed.
    /// </summary>
    /// <param name="options">The caller's options for the save.</param>
    /// <param name="version">The 1-based version this event becomes.</param>
    /// <returns>The options to stamp on this one event.</returns>
    private static PublishOptions WithEventHeaders(PublishOptions options, int version)
    {
        var supplied = options.Headers;
        var headers = new List<KeyValuePair<string, string>>((supplied?.Count ?? 0) + 2);

        if (supplied is not null)
        {
            headers.AddRange(supplied);
        }

        headers.Add(new KeyValuePair<string, string>(VersionHeader, version.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        headers.Add(new KeyValuePair<string, string>(IdHeader, Guid.NewGuid().ToString("N")));

        return options with { Headers = headers };
    }
}
