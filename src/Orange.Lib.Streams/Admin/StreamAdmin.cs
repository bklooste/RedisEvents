using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Admin;

/// <summary>
/// Operator-facing helpers over a topic's Redis keys: topology reconciliation and entry dumping.
/// </summary>
/// <remarks>
/// Redis streams spring into existence on the first <c>XADD</c>, so "create a topic" really means
/// "record what shape it is and refuse to change that shape unsafely". That is what
/// <see cref="EnsureTopicAsync(IConnectionMultiplexer, string, TopicOptions, ILogger?, CancellationToken)"/>
/// does, and it is called from both the consumer host and the publisher at startup.
/// </remarks>
public static partial class StreamAdmin
{
    /// <summary>Field of <c>m:{topic}</c> holding the codec version the topic was created under.</summary>
    public const string MetaVersionField = "v";

    /// <summary>Field of <c>m:{topic}</c> holding the current partition count.</summary>
    public const string MetaPartitionsField = "partitions";

    /// <summary>Field of <c>m:{topic}</c> holding the creation timestamp, ISO-8601 round-trip.</summary>
    public const string MetaCreatedUtcField = "createdUtc";

    /// <summary>
    /// Field of <c>m:{topic}</c> holding the <see cref="TopicOptions.MaxLen"/> in force when the
    /// topic was first created. It is a breadcrumb for whoever is reading the hash in
    /// <c>redis-cli</c>, not the live value: trimming is always driven by the caller's configuration,
    /// which is free to change per environment without rewriting topic metadata.
    /// </summary>
    public const string MetaMaxLenField = "maxLen";

    /// <summary>Bytes of the body rendered in a dump before it is elided.</summary>
    private const int BodyPreviewBytes = 48;

    private const string HexDigits = "0123456789abcdef";

    /// <summary>
    /// Topics this process has already reconciled. The reconcile is idempotent, but it is also a
    /// handful of round trips, and every publisher and consumer calls it on the same topic at
    /// startup. The partition count is part of the key so a genuinely different configuration is
    /// still checked rather than swallowed by the guard.
    /// </summary>
    private static readonly ConcurrentDictionary<string, byte> EnsuredTopics = new(StringComparer.Ordinal);

    /// <summary>
    /// Records the topic's metadata, reconciles its partition count and makes sure every partition
    /// stream exists. Safe to call from every process on every start; it runs once per process.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The partition count is the only field that can move, and it may only move <em>up</em>.
    /// </para>
    /// <para>
    /// <b>Increasing is supported and lossless.</b> Streams <c>0..oldN-1</c> keep their contents and
    /// stay in range, so any backlog on them still drains; streams <c>oldN..newN-1</c> are created
    /// empty. What it costs is a one-off ordering break — a key that used to hash to partition 2 may
    /// now hash to partition 9, so its old backlog and its new messages are consumed concurrently
    /// until the backlog drains. That is logged at Warning, and the safe procedure is to let
    /// consumers drain to near-zero lag first.
    /// </para>
    /// <para>
    /// <b>Decreasing is refused.</b> Streams <c>newN..oldN-1</c> would fall out of the read range and
    /// anything unprocessed on them would be orphaned — silent, unrecoverable loss. The guard is
    /// "never below the recorded count", which is also what makes a rolling deploy of an increase
    /// safe in either pod order: the pod still on the old, smaller count refuses to start rather
    /// than quietly writing the count back down and stranding the higher partitions.
    /// </para>
    /// <para>
    /// Partition streams are created empty via the standard <c>XGROUP CREATE … MKSTREAM</c> +
    /// <c>XGROUP DESTROY</c> trick, because <c>XADD … NOMKSTREAM</c> cannot create one. Without it a
    /// never-published topic makes <c>XLEN</c> and lag reads error instead of returning zero.
    /// </para>
    /// </remarks>
    /// <param name="redis">The shared multiplexer. Admin work never runs on a reader connection.</param>
    /// <param name="topic">The topic name.</param>
    /// <param name="options">The topic's configured options.</param>
    /// <param name="logger">Optional logger; the partition-increase warning is the only thing written.</param>
    /// <param name="ct">Cancellation token, observed between round trips.</param>
    /// <exception cref="StreamConfigurationException">
    /// The configured partition count is below the recorded one, the configured count is less than
    /// one, or the topic was created under a codec version this build cannot read.
    /// </exception>
    public static async Task EnsureTopicAsync(
        IConnectionMultiplexer redis,
        string topic,
        TopicOptions options,
        ILogger? logger = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(redis);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(options);

        if (options.Partitions < 1)
        {
            throw new StreamConfigurationException(
                $"Topic '{topic}' is configured with {options.Partitions} partitions; at least one is required.");
        }

        var guard = string.Create(CultureInfo.InvariantCulture, $"{topic}|{options.Partitions}");
        if (EnsuredTopics.ContainsKey(guard))
        {
            return;
        }

        ct.ThrowIfCancellationRequested();

        var db = redis.GetDatabase();
        var metaKey = StreamKeys.TopicMeta(topic);

        // Seed with HSETNX so two processes starting together cannot half-overwrite each other, and
        // so a re-run never rewrites the creation breadcrumbs.
        var seeds = new[]
        {
            db.HashSetAsync(metaKey, MetaPartitionsField, options.Partitions, When.NotExists),
            db.HashSetAsync(metaKey, MetaVersionField, EntryCodec.CodecVersion, When.NotExists),
            db.HashSetAsync(metaKey, MetaCreatedUtcField, DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture), When.NotExists),
            db.HashSetAsync(metaKey, MetaMaxLenField, options.MaxLen, When.NotExists),
        };

        await Task.WhenAll(seeds).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();

        var meta = await db.HashGetAsync(metaKey, [MetaPartitionsField, MetaVersionField]).ConfigureAwait(false);
        var recorded = ReadRecordedPartitions(meta[0], options.Partitions);
        EnsureRecordedVersion(topic, meta[1]);

        if (options.Partitions < recorded)
        {
            throw new StreamConfigurationException(
                $"Topic '{topic}' is recorded with {recorded} partitions but this process is configured for {options.Partitions}. " +
                "Decreasing the partition count would orphan every unprocessed message on the partitions that fall out of range, " +
                "so it is refused. Restore the configured count, or migrate deliberately to a new topic name.");
        }

        await EnsureStreamsExistAsync(db, topic, options, ct).ConfigureAwait(false);

        if (options.Partitions > recorded)
        {
            await db.HashSetAsync(metaKey, MetaPartitionsField, options.Partitions).ConfigureAwait(false);

            logger?.LogWarning(
                "Stream topic {Topic} partition count increased from {OldPartitions} to {NewPartitions}. " +
                "No messages are lost, but per-key ordering breaks for as long as a backlog remains on the original partitions: " +
                "a key may now hash to a new partition while its old backlog is still being drained. " +
                "Ordering is restored once that backlog clears; the safe procedure is to drain to near-zero lag before increasing.",
                topic,
                recorded,
                options.Partitions);
        }

        EnsuredTopics[guard] = 0;
    }

    /// <summary>
    /// Reads one entry back and renders it, for when a packed entry needs eyeballing from a console.
    /// </summary>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">The topic name.</param>
    /// <param name="partition">The partition to read from.</param>
    /// <param name="id">The entry id.</param>
    /// <param name="options">Topic options, for the partition key layout. Defaults are assumed when null.</param>
    /// <param name="ct">Cancellation token, observed before the round trip.</param>
    /// <returns>The rendered entry, or a note saying nothing is stored at that id.</returns>
    public static async Task<string> DumpEntryAsync(
        IConnectionMultiplexer redis,
        string topic,
        int partition,
        StreamId id,
        TopicOptions? options = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(redis);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ct.ThrowIfCancellationRequested();

        var coLocate = options?.CoLocatePartitions ?? true;
        var key = StreamKeys.Stream(topic, partition, coLocate);
        var formatted = id.Format();

        var entries = await redis.GetDatabase()
            .StreamRangeAsync(key, formatted, formatted, count: 1)
            .ConfigureAwait(false);

        return entries.Length == 0
            ? $"{topic}[{partition.ToString(CultureInfo.InvariantCulture)}] has no entry at {formatted}."
            : DumpEntry(entries[0], partition);
    }

    /// <summary>
    /// Renders one raw stream entry, decoding it where it can and falling back to a field-by-field
    /// dump where it cannot — the packed <c>h</c> field is opaque in <c>redis-cli</c>, and an entry
    /// that will not decode is exactly the one worth looking at.
    /// </summary>
    /// <param name="entry">The entry, as returned by <c>XRANGE</c> or <c>XREAD</c>.</param>
    /// <param name="partition">The partition it came from; the entry itself does not know.</param>
    /// <param name="codecVersion">The version recorded in <c>m:{topic}</c>.</param>
    public static string DumpEntry(in StreamEntry entry, int partition = 0, int codecVersion = EntryCodec.CodecVersion)
    {
        try
        {
            return DumpEntry(EntryCodec.Decode(entry, partition, codecVersion));
        }
        catch (StreamTransportException ex)
        {
            return DumpRaw(entry, partition, ex);
        }
    }

    /// <summary>Renders one decoded message in a form meant for a terminal, not a log aggregator.</summary>
    public static string DumpEntry(in StreamMsg message)
    {
        var builder = new StringBuilder(256);

        builder.Append("entry ").Append(message.Id.Format())
            .Append("  partition ").Append(message.Partition.ToString(CultureInfo.InvariantCulture))
            .Append("  ").Append(message.EnqueuedTime.ToString("O", CultureInfo.InvariantCulture))
            .AppendLine();

        AppendField(builder, "type", message.Type);
        AppendField(builder, "key", message.PartitionKey);
        AppendField(builder, "corr", message.CorrelationId);
        AppendField(builder, "trace", message.TraceParent);
        AppendHeaders(builder, message.Headers);
        AppendBody(builder, message.Body.Span);

        return builder.ToString();
    }

    /// <summary>
    /// Resolves the recorded partition count, tolerating a meta hash that was written by hand or
    /// truncated: an unreadable count is treated as "whatever this process is configured for", which
    /// is repaired by the reconcile that follows.
    /// </summary>
    private static int ReadRecordedPartitions(RedisValue value, int configured)
        => value.TryParse(out int recorded) && recorded >= 1 ? recorded : configured;

    /// <summary>
    /// Fails at startup rather than at the first read when the topic's entries were written by a
    /// codec this build cannot parse. An unreadable meta value is not treated as a mismatch — the
    /// codec version is authoritative only when it is actually there.
    /// </summary>
    private static void EnsureRecordedVersion(string topic, RedisValue value)
    {
        if (value.TryParse(out int version) && !EntryCodec.IsSupportedVersion(version))
        {
            throw new StreamConfigurationException(
                $"Topic '{topic}' was created under stream codec version {version}, but this build writes and reads version {EntryCodec.CodecVersion}. " +
                "Refusing to start rather than mis-parse entries; upgrade the service or check the 'v' field of the topic's m:{topic} hash.");
        }
    }

    /// <summary>
    /// Creates any partition stream that does not exist yet, so <c>XREAD</c> and <c>XLEN</c> behave
    /// and lag reads zero on a topic nobody has published to. Existence is probed in one pipelined
    /// round trip; only the missing streams cost the create/destroy pair.
    /// </summary>
    private static async Task EnsureStreamsExistAsync(IDatabase db, string topic, TopicOptions options, CancellationToken ct)
    {
        var probes = new Task<bool>[options.Partitions];
        var keys = new RedisKey[options.Partitions];

        for (var partition = 0; partition < options.Partitions; partition++)
        {
            keys[partition] = StreamKeys.Stream(topic, partition, options.CoLocatePartitions);
            probes[partition] = db.KeyExistsAsync(keys[partition]);
        }

        var exists = await Task.WhenAll(probes).ConfigureAwait(false);

        // A per-call group name: two processes racing here simply both create and both destroy, and
        // it can never collide with a real consumer group somebody else owns.
        var scratchGroup = (RedisValue)("__orange-ensure-" + Guid.NewGuid().ToString("N"));

        for (var partition = 0; partition < options.Partitions; partition++)
        {
            if (exists[partition])
            {
                continue;
            }

            ct.ThrowIfCancellationRequested();

            try
            {
                await db.StreamCreateConsumerGroupAsync(keys[partition], scratchGroup, StreamPosition.NewMessages, createStream: true)
                    .ConfigureAwait(false);
            }
            catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.Ordinal))
            {
                // Another process created the same scratch group first; the stream now exists, which
                // is all we wanted.
            }

            await db.StreamDeleteConsumerGroupAsync(keys[partition], scratchGroup).ConfigureAwait(false);
        }
    }

    private static string DumpRaw(in StreamEntry entry, int partition, StreamTransportException error)
    {
        var builder = new StringBuilder(256);

        builder.Append("entry ").Append(entry.Id.ToString())
            .Append("  partition ").Append(partition.ToString(CultureInfo.InvariantCulture))
            .AppendLine("  UNDECODABLE");

        AppendField(builder, "error", error.Message);

        var values = entry.Values;
        for (var i = 0; i < values.Length; i++)
        {
            var name = values[i].Name.ToString() ?? "?";
            if (string.Equals(name, EntryCodec.BodyField, StringComparison.Ordinal) ||
                string.Equals(name, EntryCodec.HeadersField, StringComparison.Ordinal))
            {
                AppendBody(builder, ((ReadOnlyMemory<byte>)values[i].Value).Span, name);
            }
            else
            {
                AppendField(builder, name, values[i].Value.ToString());
            }
        }

        return builder.ToString();
    }

    private static void AppendHeaders(StringBuilder builder, HeaderBlock headers)
    {
        if (headers.IsEmpty)
        {
            return;
        }

        try
        {
            AppendField(builder, "headers", $"{headers.CountHeaders().ToString(CultureInfo.InvariantCulture)}  {headers}");
        }
        catch (StreamTransportException ex)
        {
            AppendField(builder, "headers", $"MALFORMED — {ex.Message}");
            AppendBody(builder, headers.Packed.Span, "h.raw");
        }
    }

    private static void AppendField(StringBuilder builder, string label, string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return;
        }

        builder.Append("  ").Append(label.PadRight(9)).Append(value).AppendLine();
    }

    /// <summary>
    /// Renders a byte run as hex over printable ASCII, which is what makes the packed header format
    /// readable: the length prefixes show up as digits and the payload stays legible underneath.
    /// </summary>
    private static void AppendBody(StringBuilder builder, ReadOnlySpan<byte> bytes, string label = "body")
    {
        builder.Append("  ").Append(label.PadRight(9))
            .Append(bytes.Length.ToString(CultureInfo.InvariantCulture)).Append(" bytes");

        if (bytes.IsEmpty)
        {
            builder.AppendLine();
            return;
        }

        var shown = Math.Min(bytes.Length, BodyPreviewBytes);
        builder.AppendLine();

        builder.Append("            ");
        for (var i = 0; i < shown; i++)
        {
            builder.Append(HexDigits[bytes[i] >> 4]).Append(HexDigits[bytes[i] & 0x0F]).Append(' ');
        }

        builder.AppendLine(shown < bytes.Length ? "…" : string.Empty);

        builder.Append("            ");
        for (var i = 0; i < shown; i++)
        {
            builder.Append(bytes[i] is >= 0x20 and < 0x7F ? (char)bytes[i] : '.');
        }

        builder.AppendLine(shown < bytes.Length ? "…" : string.Empty);
    }
}
