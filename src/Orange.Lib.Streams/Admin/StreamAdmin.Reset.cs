using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Positions;
using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Admin;

/// <summary>Which way a reset moves a partition's position.</summary>
public enum ResetDirection
{
    /// <summary>The target equals the current position: nothing is reprocessed and nothing skipped.</summary>
    None = 0,

    /// <summary>The target is behind the current position: entries are reprocessed.</summary>
    Rewind = 1,

    /// <summary>The target is ahead of the current position: entries are skipped, never processed.</summary>
    Skip = 2,
}

/// <summary>
/// What a reset would do to one partition, computed without changing anything.
/// </summary>
/// <param name="Partition">The partition.</param>
/// <param name="Current">The consumer's current recorded position, or <see langword="null"/> when it
/// has none and would resolve from <c>StartFromWhenMissing</c> instead.</param>
/// <param name="Target">The position that would be written. The next read starts strictly after it.</param>
/// <param name="Entries">
/// How many entries lie between the two positions — the number that would be reprocessed
/// (<see cref="ResetDirection.Rewind"/>) or skipped (<see cref="ResetDirection.Skip"/>).
/// </param>
/// <param name="EntriesExact">
/// <see langword="false"/> when the count hit the scan cap and <see cref="Entries"/> is therefore a
/// floor ("at least this many") rather than the exact number.
/// </param>
/// <param name="Oldest">The oldest id still in the partition's stream, or <see langword="null"/> when
/// the stream is empty.</param>
/// <param name="Newest">The newest id in the partition's stream, or <see langword="null"/> when empty.</param>
/// <param name="Length">The partition's current <c>XLEN</c>.</param>
/// <param name="Direction">Whether the reset rewinds, skips, or does nothing.</param>
/// <param name="TrimmedAway">
/// <see langword="true"/> when the requested start predates the oldest surviving entry — the data
/// asked for has been trimmed and the consumer would start from whatever remains.
/// </param>
/// <param name="AssumedStart">
/// Where the count was measured <em>from</em> when <paramref name="Current"/> is <see langword="null"/>,
/// and <see langword="null"/> when there is a real stored position. A consumer with no stored position
/// starts wherever <c>StartFromWhenMissing</c> resolves to, which the positions hash does not record:
/// under <c>Beginning</c> that is <c>0-0</c>, under <c>Now</c> it is the partition's tail. The two give
/// wildly different answers to "how many entries does this reset skip", so the assumption is reported
/// rather than buried — see the <c>startFromWhenMissing</c> parameter on the preview overloads.
/// </param>
public sealed record PartitionResetPreview(
    int Partition,
    StreamId? Current,
    StreamId Target,
    long Entries,
    bool EntriesExact,
    StreamId? Oldest,
    StreamId? Newest,
    long Length,
    ResetDirection Direction,
    bool TrimmedAway,
    StreamId? AssumedStart = null);

/// <summary>
/// The read-only answer to "what would this reset actually do?", per partition.
/// </summary>
/// <remarks>
/// Two things it is there to prevent. A mistyped date that quietly reprocesses 412,000 messages —
/// the count is on the record before anything is written. And the quieter failure: a date older than
/// the retention window, where the naive behaviour is to start from whatever survived and report
/// success. <see cref="TrimmedAway"/> says so instead. At the default <c>MaxLen</c> of 10,000 that is
/// the common case on a busy topic, not an edge case, and replaying further back is a different
/// mechanism entirely — a backfill from the archive, not a position reset.
/// </remarks>
/// <param name="Topic">Topic name.</param>
/// <param name="Consumer">Consumer name.</param>
/// <param name="RequestedFrom">The requested date, when the reset was expressed as one.</param>
/// <param name="Partitions">Per-partition detail, in partition order.</param>
public sealed record ResetPreview(
    string Topic,
    string Consumer,
    DateTimeOffset? RequestedFrom,
    IReadOnlyList<PartitionResetPreview> Partitions)
{
    /// <summary>Total entries that would be reprocessed across every previewed partition.</summary>
    public long TotalToReprocess
    {
        get
        {
            long total = 0;
            for (var i = 0; i < this.Partitions.Count; i++)
            {
                var partition = this.Partitions[i];
                if (partition.Direction == ResetDirection.Rewind)
                {
                    total += partition.Entries;
                }
            }

            return total;
        }
    }

    /// <summary>Total entries that would be skipped — never processed — across every partition.</summary>
    public long TotalToSkip
    {
        get
        {
            long total = 0;
            for (var i = 0; i < this.Partitions.Count; i++)
            {
                var partition = this.Partitions[i];
                if (partition.Direction == ResetDirection.Skip)
                {
                    total += partition.Entries;
                }
            }

            return total;
        }
    }

    /// <summary>
    /// <see langword="true"/> when at least one partition's requested start predates its oldest
    /// surviving entry. The reset will still run; it just cannot deliver what is no longer there.
    /// </summary>
    public bool AnyTrimmedAway
    {
        get
        {
            for (var i = 0; i < this.Partitions.Count; i++)
            {
                if (this.Partitions[i].TrimmedAway)
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>Renders the preview for a console or an admin response body.</summary>
    /// <returns>A multi-line, human-readable summary.</returns>
    public string Describe()
    {
        var text = new StringBuilder(256);

        text.Append("reset ").Append(this.Topic).Append(" / ").Append(this.Consumer);
        if (this.RequestedFrom is { } from)
        {
            text.Append("  from ").Append(from.ToString("O", CultureInfo.InvariantCulture));
        }

        text.AppendLine();

        foreach (var partition in this.Partitions)
        {
            text.Append("  p").Append(partition.Partition.ToString(CultureInfo.InvariantCulture))
                .Append("  current ").Append(DescribeCurrent(partition))
                .Append(" -> ").Append(partition.Target.Format())
                .Append("  ").Append(partition.Direction.ToString().ToLowerInvariant())
                .Append(' ').Append(partition.EntriesExact ? string.Empty : "≥")
                .Append(partition.Entries.ToString(CultureInfo.InvariantCulture))
                .Append(" of ").Append(partition.Length.ToString(CultureInfo.InvariantCulture))
                .Append("  oldest ").Append(partition.Oldest?.Format() ?? "(empty)");

            if (partition.TrimmedAway)
            {
                text.Append("  ** requested start is older than the oldest surviving entry: that data is gone **");
            }

            text.AppendLine();
        }

        return text.ToString();
    }

    /// <summary>
    /// Renders the position the count was measured from: the stored one, or — when there is none —
    /// the fallback the preview had to assume, spelled out so nobody reads the count as a fact.
    /// </summary>
    private static string DescribeCurrent(PartitionResetPreview partition)
        => partition.Current?.Format()
            ?? (partition.AssumedStart is { } assumed
                ? string.Concat("(none, assuming ", assumed.Format(), ")")
                : "(none)");
}

/// <summary>
/// Replay: moving a consumer's recorded position backwards to reprocess, or forwards to abandon a
/// backlog.
/// </summary>
/// <remarks>
/// <para>
/// Redis stream ids carry the millisecond they were written in, so a reset by date is arithmetic
/// rather than a search — <see cref="StartPosition.FromDate"/> turns an instant straight into the
/// position that sits immediately before the first entry of that millisecond, and no index or probe
/// read is involved.
/// </para>
/// <para>
/// Every reset writes two things per partition: the position itself, and a reset marker
/// (<see cref="ResetMarkerPrefix"/>) that a <i>running</i> worker notices on its next flush tick and
/// acts on. Without the marker a live reset would simply be overwritten by the flusher a second
/// later. Operators are still told the safe path is scale to zero, reset, scale up.
/// </para>
/// </remarks>
public static partial class StreamAdmin
{
    /// <summary>
    /// Field prefix of a reset marker in the positions hash. The full field is
    /// <c>__reset:&lt;partition&gt;</c> so a worker checks only the partitions it owns, and the value is
    /// <c>&lt;targetId&gt;|&lt;issuedUtc&gt;</c>.
    /// </summary>
    /// <remarks>
    /// The prefix is not a number, so <c>RedisPositionStore</c>'s hash parse skips these fields and a
    /// marker can never be mistaken for a position.
    /// </remarks>
    public const string ResetMarkerPrefix = "__reset";

    /// <summary>Separator between the target id and the issue time in a reset marker value.</summary>
    public const char ResetMarkerSeparator = '|';

    /// <summary>Entries fetched per round trip while counting a range.</summary>
    private const int CountPageSize = 1000;

    /// <summary>
    /// Default ceiling on how many entries <see cref="PreviewResetAsync(IConnectionMultiplexer, string, string, DateTimeOffset, int?, TopicOptions, long, CancellationToken)"/>
    /// will count before it reports a floor instead of an exact number.
    /// </summary>
    /// <remarks>
    /// Counting a range is a paged <c>XRANGE</c> — Redis has no "count ids between" command — so an
    /// unbounded preview on a huge stream would be a long, pointless read. Past this many entries the
    /// operator already knows the answer is "far too many to reprocess casually".
    /// </remarks>
    public const long DefaultMaxCountScan = 250_000;

    /// <summary>The reset-marker field for one partition.</summary>
    /// <param name="partition">The partition.</param>
    /// <returns><c>__reset:&lt;partition&gt;</c>.</returns>
    public static string ResetMarkerField(int partition)
        => string.Concat(ResetMarkerPrefix, ":", partition.ToString(CultureInfo.InvariantCulture));

    /// <summary>Renders a reset-marker value.</summary>
    /// <param name="target">The position the worker must restart from.</param>
    /// <param name="issuedUtc">When the reset was issued.</param>
    /// <returns><c>&lt;targetId&gt;|&lt;issuedUtc&gt;</c>.</returns>
    public static string FormatResetMarker(StreamId target, DateTimeOffset issuedUtc)
        => string.Concat(
            target.Format(),
            ResetMarkerSeparator.ToString(),
            issuedUtc.ToString("O", CultureInfo.InvariantCulture));

    /// <summary>Parses a reset-marker value.</summary>
    /// <param name="value">The raw hash value.</param>
    /// <param name="target">The target position, when this returns <see langword="true"/>.</param>
    /// <param name="issuedUtc">The issue time, or <see cref="DateTimeOffset.MinValue"/> when the
    /// marker carried none.</param>
    /// <returns><see langword="true"/> when the value held a well-formed stream id.</returns>
    public static bool TryParseResetMarker(ReadOnlySpan<char> value, out StreamId target, out DateTimeOffset issuedUtc)
    {
        target = default;
        issuedUtc = DateTimeOffset.MinValue;

        var at = value.IndexOf(ResetMarkerSeparator);
        var idText = at < 0 ? value : value[..at];

        if (!StreamId.TryParse(idText, out target))
        {
            return false;
        }

        if (at >= 0 &&
            DateTimeOffset.TryParse(
                value[(at + 1)..],
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind,
                out var parsed))
        {
            issuedUtc = parsed;
        }

        return true;
    }

    /// <summary>
    /// Rewinds (or fast-forwards) a consumer to the first entry written at or after
    /// <paramref name="from"/>.
    /// </summary>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name — no wildcards, deliberately.</param>
    /// <param name="from">The instant to resume from. An entry written exactly on that millisecond is
    /// delivered, not skipped.</param>
    /// <param name="partition">One partition, or <see langword="null"/> for every partition.</param>
    /// <param name="options">Topic options, for the partition count and key layout.</param>
    /// <param name="logger">Optional logger; the reset is recorded at Warning.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The positions actually written, per partition.</returns>
    /// <remarks>
    /// A date older than the retention window cannot resurrect trimmed entries — call
    /// <see cref="PreviewResetAsync(IConnectionMultiplexer, string, string, DateTimeOffset, int?, TopicOptions, long, CancellationToken)"/>
    /// first, which says so explicitly.
    /// </remarks>
    public static Task<IReadOnlyList<(int Partition, StreamId Target)>> ResetPositionAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        DateTimeOffset from,
        int? partition = null,
        TopicOptions? options = null,
        ILogger? logger = null,
        CancellationToken ct = default)
        => ResetPositionAsync(
            redis,
            topic,
            consumer,
            StartPosition.FromDate(from).After,
            partition,
            options,
            logger,
            ct);

    /// <summary>
    /// Moves a consumer to an exact position. Reading resumes at the entry immediately
    /// <em>after</em> <paramref name="exactId"/>.
    /// </summary>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="exactId">The last id to treat as already processed.</param>
    /// <param name="partition">One partition, or <see langword="null"/> for every partition.</param>
    /// <param name="options">Topic options, for the partition count and key layout.</param>
    /// <param name="logger">Optional logger; the reset is recorded at Warning.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The positions actually written, per partition.</returns>
    public static async Task<IReadOnlyList<(int Partition, StreamId Target)>> ResetPositionAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        StreamId exactId,
        int? partition = null,
        TopicOptions? options = null,
        ILogger? logger = null,
        CancellationToken ct = default)
    {
        var partitions = await ResolvePartitionsAsync(redis, topic, consumer, partition, options, ct)
            .ConfigureAwait(false);

        var targets = new (int Partition, StreamId Target)[partitions.Length];
        for (var i = 0; i < partitions.Length; i++)
        {
            targets[i] = (partitions[i], exactId);
        }

        await ApplyAsync(redis, topic, consumer, targets, logger, ct).ConfigureAwait(false);
        return targets;
    }

    /// <summary>Rewinds a consumer to the very beginning of each partition (<c>0-0</c>).</summary>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="partition">One partition, or <see langword="null"/> for every partition.</param>
    /// <param name="options">Topic options, for the partition count and key layout.</param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The positions actually written, per partition.</returns>
    /// <remarks>
    /// "The beginning" is the oldest entry that still <i>exists</i>, not the first ever published:
    /// trimming has removed the rest, and no position can bring it back.
    /// </remarks>
    public static Task<IReadOnlyList<(int Partition, StreamId Target)>> ResetPositionToStartAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        int? partition = null,
        TopicOptions? options = null,
        ILogger? logger = null,
        CancellationToken ct = default)
        => ResetPositionAsync(redis, topic, consumer, StreamId.Min, partition, options, logger, ct);

    /// <summary>
    /// Skips each partition's backlog: the position is moved to that partition's newest entry, so
    /// only what arrives afterwards is processed.
    /// </summary>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="partition">One partition, or <see langword="null"/> for every partition.</param>
    /// <param name="options">Topic options, for the partition count and key layout.</param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The positions actually written, per partition.</returns>
    /// <remarks>
    /// The target differs per partition — each one's own tail — so this is not the same as resetting
    /// every partition to one id. Everything skipped is skipped permanently; there is no dead-letter
    /// consolation prize.
    /// </remarks>
    public static async Task<IReadOnlyList<(int Partition, StreamId Target)>> ResetPositionToEndAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        int? partition = null,
        TopicOptions? options = null,
        ILogger? logger = null,
        CancellationToken ct = default)
    {
        var partitions = await ResolvePartitionsAsync(redis, topic, consumer, partition, options, ct)
            .ConfigureAwait(false);

        var db = redis.GetDatabase();
        var coLocate = await ResolveCoLocateAsync(db, topic, options).ConfigureAwait(false);
        var targets = new (int Partition, StreamId Target)[partitions.Length];

        for (var i = 0; i < partitions.Length; i++)
        {
            ct.ThrowIfCancellationRequested();

            var key = StreamKeys.Stream(topic, partitions[i], coLocate);
            var bounds = await ReadBoundsAsync(db, key).ConfigureAwait(false);

            if (bounds.Newest is null && !await db.KeyExistsAsync(key).ConfigureAwait(false))
            {
                // Reading no tail from a stream that does not exist is not "the stream is empty", it
                // is "this is the wrong key" — wrong topic name, or the wrong CoLocatePartitions
                // layout. Writing 0-0 there is a rewind to the start, the exact opposite of skip.
                throw new StreamConfigurationException(
                    $"Partition stream '{key}' does not exist, so a reset to the end has no tail to move to and would " +
                    "write 0-0 — a rewind to the beginning, the opposite of what was asked. Check the topic name and " +
                    "whether the topic is co-located (TopicOptions.CoLocatePartitions).");
            }

            // An empty partition has no tail to skip to; 0-0 is already "everything there is".
            targets[i] = (partitions[i], bounds.Newest ?? StreamId.Min);
        }

        await ApplyAsync(redis, topic, consumer, targets, logger, ct).ConfigureAwait(false);
        return targets;
    }

    /// <summary>
    /// Reports what a reset to <paramref name="from"/> would do, without writing anything.
    /// </summary>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="from">The instant the reset would resume from.</param>
    /// <param name="partition">One partition, or <see langword="null"/> for every partition.</param>
    /// <param name="options">Topic options, for the partition count and key layout.</param>
    /// <param name="maxCountScan">Ceiling on entries counted per partition; see
    /// <see cref="DefaultMaxCountScan"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The preview, per partition.</returns>
    public static Task<ResetPreview> PreviewResetAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        DateTimeOffset from,
        int? partition = null,
        TopicOptions? options = null,
        long maxCountScan = DefaultMaxCountScan,
        CancellationToken ct = default)
        => PreviewCoreAsync(
            redis,
            topic,
            consumer,
            StartPosition.FromDate(from).After,
            from,
            partition,
            options,
            maxCountScan,
            StartFrom.Beginning,
            ct);

    /// <summary>
    /// Reports what a reset to an exact id would do, without writing anything.
    /// </summary>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="exactId">The position the reset would write.</param>
    /// <param name="partition">One partition, or <see langword="null"/> for every partition.</param>
    /// <param name="options">Topic options, for the partition count and key layout.</param>
    /// <param name="maxCountScan">Ceiling on entries counted per partition.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The preview, per partition.</returns>
    public static Task<ResetPreview> PreviewResetAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        StreamId exactId,
        int? partition = null,
        TopicOptions? options = null,
        long maxCountScan = DefaultMaxCountScan,
        CancellationToken ct = default)
        => PreviewCoreAsync(
            redis,
            topic,
            consumer,
            exactId,
            requestedFrom: null,
            partition,
            options,
            maxCountScan,
            StartFrom.Beginning,
            ct);

    /// <summary>
    /// Reports what a reset to <paramref name="from"/> would do, without writing anything, telling the
    /// preview where a consumer with <em>no</em> stored position would otherwise have started.
    /// </summary>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="from">The instant the reset would resume from.</param>
    /// <param name="startFromWhenMissing">
    /// The consumer's <c>StartFromWhenMissing</c>. It is only consulted for a partition with no stored
    /// position, and it decides what "how many entries does this move past" even means there:
    /// <see cref="StartFrom.Now"/> measures from the partition's tail, anything else from <c>0-0</c>.
    /// </param>
    /// <param name="partition">One partition, or <see langword="null"/> for every partition.</param>
    /// <param name="options">Topic options, for the partition count and key layout.</param>
    /// <param name="maxCountScan">Ceiling on entries counted per partition.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The preview, per partition.</returns>
    public static Task<ResetPreview> PreviewResetAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        DateTimeOffset from,
        StartFrom startFromWhenMissing,
        int? partition = null,
        TopicOptions? options = null,
        long maxCountScan = DefaultMaxCountScan,
        CancellationToken ct = default)
        => PreviewCoreAsync(
            redis,
            topic,
            consumer,
            StartPosition.FromDate(from).After,
            from,
            partition,
            options,
            maxCountScan,
            startFromWhenMissing,
            ct);

    /// <summary>
    /// Reports what a reset to an exact id would do, without writing anything, telling the preview
    /// where a consumer with <em>no</em> stored position would otherwise have started.
    /// </summary>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="exactId">The position the reset would write.</param>
    /// <param name="startFromWhenMissing">The consumer's <c>StartFromWhenMissing</c>; see the by-date overload.</param>
    /// <param name="partition">One partition, or <see langword="null"/> for every partition.</param>
    /// <param name="options">Topic options, for the partition count and key layout.</param>
    /// <param name="maxCountScan">Ceiling on entries counted per partition.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The preview, per partition.</returns>
    public static Task<ResetPreview> PreviewResetAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        StreamId exactId,
        StartFrom startFromWhenMissing,
        int? partition = null,
        TopicOptions? options = null,
        long maxCountScan = DefaultMaxCountScan,
        CancellationToken ct = default)
        => PreviewCoreAsync(
            redis,
            topic,
            consumer,
            exactId,
            requestedFrom: null,
            partition,
            options,
            maxCountScan,
            startFromWhenMissing,
            ct);

    /// <summary>
    /// Reports what <see cref="ResetPositionToEndAsync"/> would do, without writing anything. The
    /// target is each partition's own tail, so this cannot be expressed as a single-id preview.
    /// </summary>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="partition">One partition, or <see langword="null"/> for every partition.</param>
    /// <param name="options">Topic options, for the partition count and key layout.</param>
    /// <param name="maxCountScan">Ceiling on entries counted per partition.</param>
    /// <param name="startFromWhenMissing">The consumer's <c>StartFromWhenMissing</c>; see the by-date overload.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The preview, per partition. Every partition's <c>Target</c> is its own newest entry.</returns>
    public static Task<ResetPreview> PreviewResetToEndAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        int? partition = null,
        TopicOptions? options = null,
        long maxCountScan = DefaultMaxCountScan,
        StartFrom startFromWhenMissing = StartFrom.Beginning,
        CancellationToken ct = default)
        => PreviewCoreAsync(
            redis,
            topic,
            consumer,
            target: null,
            requestedFrom: null,
            partition,
            options,
            maxCountScan,
            startFromWhenMissing,
            ct);

    /// <summary>
    /// Reads back what Redis says the topic's shape is: the recorded partition count, and which key
    /// layout its partition streams actually use.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every reset and preview needs <see cref="TopicOptions.CoLocatePartitions"/> to build the right
    /// stream key, and a caller that does not have the service's configuration to hand — an admin API,
    /// a CLI — used to have no way to supply it. Guessing is not harmless: guessing <c>true</c> against
    /// a spread topic reads <c>s:{topic}:0</c>, which does not exist, so a preview reports
    /// <c>Length 0 / Entries 0</c> and a reset to the end writes <c>0-0</c>. Both look like success.
    /// </para>
    /// <para>
    /// The layout is probed rather than recorded, because <c>m:{topic}</c> has never carried it:
    /// partition 0's stream exists under exactly one of the two shapes once the topic has been
    /// ensured or published to. When neither exists — a topic nobody has touched — the library
    /// default (<see langword="true"/>) is returned, and <see cref="TopicOptions.Partitions"/> is 0
    /// so the caller can tell "no such topic" from "topic with one partition".
    /// </para>
    /// </remarks>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Options carrying the recorded partition count (0 when unrecorded) and the probed layout.</returns>
    public static async Task<TopicOptions> ReadTopicLayoutAsync(
        IConnectionMultiplexer redis,
        string topic,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(redis);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ct.ThrowIfCancellationRequested();

        var db = redis.GetDatabase();
        var partitions = await ReadPartitionCountAsync(db, topic).ConfigureAwait(false);
        var coLocate = await DetectCoLocationAsync(db, topic).ConfigureAwait(false);

        return new TopicOptions { Partitions = partitions, CoLocatePartitions = coLocate };
    }

    /// <summary>
    /// The key layout to read with: what the caller configured, or — when the caller has no
    /// configuration — what Redis actually holds.
    /// </summary>
    private static async Task<bool> ResolveCoLocateAsync(IDatabase db, string topic, TopicOptions? options)
        => options?.CoLocatePartitions ?? await DetectCoLocationAsync(db, topic).ConfigureAwait(false);

    /// <summary>
    /// Probes partition 0 under both key shapes. Co-located wins a tie (it is the library default and
    /// the shape a mixed leftover is most likely to be), and "neither" also returns the default.
    /// </summary>
    private static async Task<bool> DetectCoLocationAsync(IDatabase db, string topic)
    {
        var tagged = db.KeyExistsAsync(StreamKeys.Stream(topic, 0, coLocate: true));
        var spread = db.KeyExistsAsync(StreamKeys.Stream(topic, 0, coLocate: false));

        return await tagged.ConfigureAwait(false) || !await spread.ConfigureAwait(false);
    }

    private static async Task<ResetPreview> PreviewCoreAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        StreamId? target,
        DateTimeOffset? requestedFrom,
        int? partition,
        TopicOptions? options,
        long maxCountScan,
        StartFrom startFromWhenMissing,
        CancellationToken ct)
    {
        var partitions = await ResolvePartitionsAsync(redis, topic, consumer, partition, options, ct)
            .ConfigureAwait(false);

        var db = redis.GetDatabase();
        var coLocate = await ResolveCoLocateAsync(db, topic, options).ConfigureAwait(false);
        var stored = await new RedisPositionStore(redis).LoadRecordsAsync(topic, consumer, ct).ConfigureAwait(false);

        var previews = new PartitionResetPreview[partitions.Length];
        for (var i = 0; i < partitions.Length; i++)
        {
            ct.ThrowIfCancellationRequested();

            var p = partitions[i];
            var key = StreamKeys.Stream(topic, p, coLocate);
            var current = stored.TryGetValue(p, out var record) ? record.Id : (StreamId?)null;

            var length = await db.StreamLengthAsync(key).ConfigureAwait(false);
            var bounds = await ReadBoundsAsync(db, key).ConfigureAwait(false);

            // A null target means "each partition's own tail" — the reset-to-end shape, which has no
            // single id to preview against.
            var partitionTarget = target ?? bounds.Newest ?? StreamId.Min;

            // With no stored position there is nothing to measure from, so the fallback the consumer
            // would have used has to stand in for it. Assuming 0-0 under StartFromWhenMissing = Now
            // reports a skip of the entire stream for a move that skips nothing at all.
            var assumed = current is null
                ? startFromWhenMissing == StartFrom.Now ? bounds.Newest ?? StreamId.Min : StreamId.Min
                : (StreamId?)null;

            var baseline = current ?? assumed!.Value;

            var direction = Compare(baseline, partitionTarget);
            var (from, to) = direction == ResetDirection.Rewind
                ? (partitionTarget, baseline)                    // entries that would be reprocessed
                : (baseline, partitionTarget);                   // entries that would be skipped

            var (entries, exact) = direction == ResetDirection.None
                ? (0L, true)
                : await CountBetweenAsync(db, key, from, to, maxCountScan, ct).ConfigureAwait(false);

            // The first entry the consumer would actually see. If that predates what survives, the
            // data asked for is gone — say so rather than starting from whatever is left.
            var wanted = Next(partitionTarget);
            var trimmed = bounds.Oldest is { } oldest && oldest > wanted;

            previews[i] = new PartitionResetPreview(
                p,
                current,
                partitionTarget,
                entries,
                exact,
                bounds.Oldest,
                bounds.Newest,
                length,
                direction,
                trimmed,
                assumed);
        }

        return new ResetPreview(topic, consumer, requestedFrom, previews);
    }

    /// <summary>
    /// Which way the reset moves, against the position the consumer is actually at — its stored one,
    /// or the fallback <see cref="PreviewCoreAsync"/> resolved for a consumer that has never run.
    /// </summary>
    private static ResetDirection Compare(StreamId current, StreamId target)
    {
        var order = target.CompareTo(current);
        return order == 0 ? ResetDirection.None : order < 0 ? ResetDirection.Rewind : ResetDirection.Skip;
    }

    /// <summary>
    /// Counts entries in <c>(from, to]</c>. Redis has no "count between two ids" command, so this is
    /// a paged <c>XRANGE</c> capped at <paramref name="maxScan"/>; hitting the cap returns a floor.
    /// </summary>
    private static async Task<(long Count, bool Exact)> CountBetweenAsync(
        IDatabase db,
        RedisKey key,
        StreamId from,
        StreamId to,
        long maxScan,
        CancellationToken ct)
    {
        if (to <= from)
        {
            return (0, true);
        }

        var cap = maxScan > 0 ? maxScan : DefaultMaxCountScan;
        var upper = to.Format();
        var cursor = Next(from);
        long count = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var page = await db.StreamRangeAsync(key, cursor.Format(), upper, CountPageSize).ConfigureAwait(false);
            if (page.Length == 0)
            {
                return (count, true);
            }

            count += page.Length;

            if (count >= cap)
            {
                return (count, false);
            }

            if (page.Length < CountPageSize)
            {
                return (count, true);
            }

            var last = StreamId.Parse(page[^1].Id.ToString());
            if (last >= to)
            {
                return (count, true);
            }

            cursor = Next(last);
        }
    }

    /// <summary>The smallest id strictly greater than <paramref name="id"/>.</summary>
    private static StreamId Next(StreamId id)
        => id.Seq == long.MaxValue ? new StreamId(id.Ms + 1, 0) : new StreamId(id.Ms, id.Seq + 1);

    private static async Task<(StreamId? Oldest, StreamId? Newest)> ReadBoundsAsync(IDatabase db, RedisKey key)
    {
        var first = db.StreamRangeAsync(key, count: 1, messageOrder: Order.Ascending);
        var last = db.StreamRangeAsync(key, count: 1, messageOrder: Order.Descending);

        var oldest = await first.ConfigureAwait(false);
        var newest = await last.ConfigureAwait(false);

        return (Bound(oldest), Bound(newest));
    }

    private static StreamId? Bound(StreamEntry[] entries)
        => entries.Length > 0 && StreamId.TryParse(entries[0].Id.ToString(), out var id) ? id : null;

    /// <summary>
    /// Resolves which partitions a reset touches: the one named, or every partition of the topic.
    /// </summary>
    private static async Task<int[]> ResolvePartitionsAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        int? partition,
        TopicOptions? options,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(redis);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        ct.ThrowIfCancellationRequested();

        var db = redis.GetDatabase();

        if (partition is { } one)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(one);

            // A partition index past the end is not a no-op: it writes a position field and a reset
            // marker that no worker ever reads, and the caller is told the reset succeeded. Refuse it
            // whenever the count is knowable; when it is not, there is nothing to check against.
            var known = options?.Partitions ?? 0;
            if (known < 1)
            {
                known = await ReadPartitionCountAsync(db, topic).ConfigureAwait(false);
            }

            if (known >= 1 && one >= known)
            {
                throw new StreamConfigurationException(
                    $"Topic '{topic}' has {known.ToString(CultureInfo.InvariantCulture)} partition(s), " +
                    $"numbered 0..{(known - 1).ToString(CultureInfo.InvariantCulture)}, so partition " +
                    $"{one.ToString(CultureInfo.InvariantCulture)} does not exist. Resetting it would leave an " +
                    $"orphan position and reset marker in p:{{{topic}}}:{consumer} that no worker reads.");
            }

            return [one];
        }

        var total = options?.Partitions ?? 0;
        if (total < 1)
        {
            total = await ReadPartitionCountAsync(db, topic).ConfigureAwait(false);
        }

        if (total < 1)
        {
            throw new StreamConfigurationException(
                $"Topic '{topic}' has no recorded partition count in m:{{{topic}}}, so a whole-topic reset cannot know how many " +
                "partitions to move. Pass TopicOptions with the configured partition count, or reset one partition at a time.");
        }

        var partitions = new int[total];
        for (var i = 0; i < total; i++)
        {
            partitions[i] = i;
        }

        return partitions;
    }

    /// <summary>
    /// Writes the positions and their reset markers. The marker is what makes a reset stick against a
    /// running consumer: the flusher would otherwise overwrite the position on its next tick.
    /// </summary>
    private static async Task ApplyAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        (int Partition, StreamId Target)[] targets,
        ILogger? logger,
        CancellationToken ct)
    {
        if (targets.Length == 0)
        {
            return;
        }

        var issued = DateTimeOffset.UtcNow;
        var markers = new HashEntry[targets.Length];
        for (var i = 0; i < targets.Length; i++)
        {
            markers[i] = new HashEntry(
                ResetMarkerField(targets[i].Partition),
                FormatResetMarker(targets[i].Target, issued));
        }

        // Markers first: a worker that sees a marker before the position lands still restarts at the
        // right place, whereas the reverse order leaves a window where the position moves with nothing
        // telling a live worker to act on it.
        await redis.GetDatabase()
            .HashSetAsync(StreamKeys.Positions(topic, consumer), markers)
            .ConfigureAwait(false);

        await new RedisPositionStore(redis)
            .SaveAsync(topic, consumer, targets.AsSpan(), ct)
            .ConfigureAwait(false);

        logger?.LogWarning(
            "Stream positions reset for topic {Topic}, consumer {Consumer}: {PartitionCount} partition(s) moved, " +
            "first target {Target}. Any running worker rewinds on its next flush tick; entries after the target are " +
            "delivered again, so handlers must be idempotent.",
            topic,
            consumer,
            targets.Length,
            targets[0].Target.Format());
    }
}
