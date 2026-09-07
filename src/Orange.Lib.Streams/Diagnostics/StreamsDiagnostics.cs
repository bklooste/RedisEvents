using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Numerics;

namespace Orange.Lib.Streams.Diagnostics;

/// <summary>
/// The single <see cref="ActivitySource"/> and <see cref="Meter"/> for Orange.Lib.Streams, plus
/// every instrument the library publishes.
///
/// <para>
/// <b>Counters and histograms</b> are recorded inline at the point the thing happens. <b>Observable
/// gauges</b> — lag, block state, ownership, buffer depth, stream length — cannot be: nothing is
/// "happening" when a partition is quietly falling behind, and the value is a level rather than an
/// event. Each of those is backed by a small map keyed by the series it describes; components call
/// the matching <c>Set…</c> method when the level changes, and the gauge callback walks the map
/// when the exporter scrapes. Removing a key (passing 0 where noted) is how a series stops being
/// reported once it no longer exists — a pod that stops owning a partition should stop publishing
/// that partition's lag, not pin it at its last value forever.
/// </para>
/// <para>
/// Keys are <c>":"</c>-joined and are parsed into tags once, at <c>Set</c> time, so the scrape path
/// allocates only the <see cref="Measurement{T}"/> list it must.
/// </para>
/// </summary>
internal static class StreamsDiagnostics
{
    /// <summary>
    /// Source name for OpenTelemetry traces and metrics. Register it with
    /// <c>AddSource</c>/<c>AddMeter</c>, or use <c>AddOrangeStreams()</c>.
    /// </summary>
    internal const string SourceName = "Orange.Lib.Streams";

    /// <summary>
    /// Assembly version for diagnostics.
    /// </summary>
    private const string Version = "1.0.0";

    /// <summary>
    /// ActivitySource for OpenTelemetry tracing.
    /// </summary>
    internal static readonly ActivitySource Source = new(SourceName, Version);

    /// <summary>
    /// Meter for OpenTelemetry metrics.
    /// </summary>
    internal static readonly Meter Meter = new(SourceName, Version);

    // ---------------------------------------------------------------- gauge backing stores
    // Written by the components, read by the gauge callbacks on the exporter's thread.

    private static readonly ConcurrentDictionary<string, Sample<long>> BlockedValues = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, Sample<double>> BlockDurationValues = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, Sample<long>> UnownedPartitionsValues = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, Sample<long>> ContestedPartitionsValues = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, Sample<long>> LagEntriesValues = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, Sample<double>> LagMsValues = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, Sample<long>> BufferQueuedValues = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, Sample<long>> StreamLengthValues = new(StringComparer.Ordinal);

    // ---------------------------------------------------------------- counters

    /// <summary>
    /// Counter: number of messages published. Tags: topic, partition.
    /// </summary>
    internal static readonly Counter<long> StreamsPublished =
        Meter.CreateCounter<long>("streams.published", "messages", "Number of messages published");

    /// <summary>
    /// Counter: number of messages consumed. Tags: topic, partition, consumer.
    /// </summary>
    internal static readonly Counter<long> StreamsConsumed =
        Meter.CreateCounter<long>("streams.consumed", "messages", "Number of messages consumed");

    /// <summary>
    /// Counter: messages dropped before the handler saw them, because the consumer's type filter
    /// did not match. Tags: topic, consumer.
    /// </summary>
    internal static readonly Counter<long> StreamsFiltered =
        Meter.CreateCounter<long>("streams.filtered", "messages", "Number of messages filtered");

    /// <summary>
    /// Counter: number of messages failed during processing. Tags: topic, consumer, policy, exception type.
    /// </summary>
    internal static readonly Counter<long> StreamsErrors =
        Meter.CreateCounter<long>("streams.errors", "messages", "Number of processing errors");

    /// <summary>
    /// Counter: the background trimmer overrode a stuck consumer's position clamp to protect memory.
    /// Every increment means a consumer lost part of its backlog. Tags: topic, partition, consumer.
    /// </summary>
    internal static readonly Counter<long> StreamsClampReleased =
        Meter.CreateCounter<long>("streams.clamp.released", "messages", "Number of clamp releases");

    /// <summary>
    /// Counter: number of position flush failures. Tags: topic, consumer.
    /// </summary>
    internal static readonly Counter<long> StreamsPositionsFlushFailures =
        Meter.CreateCounter<long>("streams.positions.flush_failures", "failures", "Number of position flush failures");

    /// <summary>
    /// Counter: entries shed by a buffered publisher because its queue was full and
    /// <c>DropOldest</c> was set. Tags: topic. Always zero under the default wait-on-full mode.
    /// </summary>
    internal static readonly Counter<long> StreamsBufferDropped = Meter.CreateCounter<long>(
        "streams.buffer.dropped",
        "messages",
        "Entries dropped by a buffered publisher because its queue was full");

    // ---------------------------------------------------------------- histograms

    /// <summary>
    /// Histogram: duration of batch processing in milliseconds. Tags: topic, consumer.
    /// </summary>
    internal static readonly Histogram<double> StreamsBatchDuration =
        Meter.CreateHistogram<double>("streams.batch.duration", "ms", "Duration of batch processing");

    /// <summary>
    /// Histogram: size of batches. Tags: topic, consumer.
    /// </summary>
    internal static readonly Histogram<long> StreamsBatchSize =
        Meter.CreateHistogram<long>("streams.batch.size", "messages", "Number of messages in a batch");

    // ---------------------------------------------------------------- observable gauges

    /// <summary>
    /// Observable gauge: 1 while a partition is blocked retrying a <c>DontIgnoreException</c>,
    /// 0 once it is not. Tags: topic, partition, consumer.
    /// </summary>
    internal static readonly ObservableGauge<long> StreamsBlocked = Meter.CreateObservableGauge(
        "streams.blocked", () => Observe(BlockedValues), "status", "Whether a partition is blocked");

    /// <summary>
    /// Observable gauge: how long the currently blocked partition has been blocked. Tags: topic, partition, consumer.
    /// </summary>
    internal static readonly ObservableGauge<double> StreamsBlockDurationMs = Meter.CreateObservableGauge(
        "streams.block.duration_ms", () => Observe(BlockDurationValues), "ms", "How long a partition has been blocked");

    /// <summary>
    /// Observable gauge: partitions this consumer group believes nobody owns. Tags: topic, consumer.
    /// </summary>
    internal static readonly ObservableGauge<long> StreamsPartitionsUnowned = Meter.CreateObservableGauge(
        "streams.partitions.unowned", () => Observe(UnownedPartitionsValues), "partitions", "Number of unowned partitions");

    /// <summary>
    /// Observable gauge: partitions claimed by more than one instance. Tags: topic, consumer.
    /// </summary>
    internal static readonly ObservableGauge<long> StreamsPartitionsContested = Meter.CreateObservableGauge(
        "streams.partitions.contested", () => Observe(ContestedPartitionsValues), "partitions", "Number of contested partitions");

    /// <summary>
    /// Observable gauge: entries between the consumer's position and the stream tail. Tags: topic, partition, consumer.
    /// </summary>
    internal static readonly ObservableGauge<long> StreamsLagEntries = Meter.CreateObservableGauge(
        "streams.lag.entries", () => Observe(LagEntriesValues), "entries", "Entries between the consumer position and the stream tail");

    /// <summary>
    /// Observable gauge: age of the oldest unprocessed entry. Tags: topic, partition, consumer.
    /// </summary>
    internal static readonly ObservableGauge<double> StreamsLagMs = Meter.CreateObservableGauge(
        "streams.lag.ms", () => Observe(LagMsValues), "ms", "Age of the oldest unprocessed entry");

    /// <summary>
    /// Observable gauge: entries waiting in a buffered publisher's queue. Tags: topic.
    /// A depth pinned at the bound means the producer is outrunning Redis.
    /// </summary>
    internal static readonly ObservableGauge<long> StreamsBufferQueued = Meter.CreateObservableGauge(
        "streams.buffer.queued", () => Observe(BufferQueuedValues), "messages", "Entries waiting in a buffered publisher queue");

    /// <summary>
    /// Observable gauge: entries currently held in a partition's stream (<c>XLEN</c>). Tags: topic, partition.
    /// Compared against <c>MaxLen</c> to see how close trim is to eating unprocessed data.
    /// </summary>
    internal static readonly ObservableGauge<long> StreamsStreamLength = Meter.CreateObservableGauge(
        "streams.stream.length", () => Observe(StreamLengthValues), "entries", "Entries currently held in a partition stream");

    // ---------------------------------------------------------------- setters

    /// <summary>
    /// Sets whether a partition is blocked: 1 while it is retrying a <c>DontIgnoreException</c>,
    /// 0 once it recovers. Both values are reported; <see cref="ClearPartition"/> is what stops the
    /// series, when the partition is no longer consumed here.
    /// </summary>
    /// <param name="key">"topic:partition:consumer".</param>
    /// <param name="value">0 or 1.</param>
    /// <remarks>
    /// <b>Why 0 is reported rather than removed (R-16).</b> The plan specifies a 0/1 gauge and this
    /// removed the series at 0, which is a different signal: a gauge that disappears is indistinguishable
    /// from a pod that died, so <c>streams.blocked == 1</c> alerts resolved on a crash exactly as they
    /// did on a recovery, and a recovery left no datapoint to show it had happened. Reporting 0 makes
    /// the recovery visible and lets <c>max_over_time</c> and "no data" mean what they say.
    /// </remarks>
    internal static void SetBlockedGauge(string key, long value) =>
        Set(BlockedValues, key, value, TagShape.TopicPartitionConsumer, removeAtZero: false);

    /// <summary>
    /// Sets how long a partition has been blocked, 0 once it is not blocked. Reported at 0 for the
    /// same reason as <see cref="SetBlockedGauge"/> — the pair has to rise and fall together, or a
    /// dashboard showing both tells two different stories about the same partition.
    /// </summary>
    /// <param name="key">"topic:partition:consumer".</param>
    /// <param name="milliseconds">Block duration.</param>
    internal static void SetBlockDurationMs(string key, double milliseconds) =>
        Set(BlockDurationValues, key, milliseconds, TagShape.TopicPartitionConsumer, removeAtZero: false);

    /// <summary>
    /// Sets the number of partitions nobody owns. Zero removes the series.
    /// </summary>
    /// <param name="key">"topic:consumer".</param>
    /// <param name="count">Unowned partition count.</param>
    internal static void SetUnownedPartitions(string key, long count) =>
        Set(UnownedPartitionsValues, key, count, TagShape.TopicConsumer, removeAtZero: true);

    /// <summary>
    /// Sets the number of partitions claimed by more than one instance. Zero removes the series.
    /// </summary>
    /// <param name="key">"topic:consumer".</param>
    /// <param name="count">Contested partition count.</param>
    internal static void SetContestedPartitions(string key, long count) =>
        Set(ContestedPartitionsValues, key, count, TagShape.TopicConsumer, removeAtZero: true);

    /// <summary>
    /// Sets the entry lag for one partition. Zero is kept: "caught up" is a value worth graphing.
    /// Use <see cref="ClearPartition"/> when the partition stops being consumed here.
    /// </summary>
    /// <param name="key">"topic:partition:consumer".</param>
    /// <param name="entries">Entries behind the tail.</param>
    internal static void SetLagEntries(string key, long entries) =>
        Set(LagEntriesValues, key, entries, TagShape.TopicPartitionConsumer, removeAtZero: false);

    /// <summary>
    /// Sets the time lag for one partition. Zero is kept, as for <see cref="SetLagEntries"/>.
    /// </summary>
    /// <param name="key">"topic:partition:consumer".</param>
    /// <param name="milliseconds">Age of the oldest unprocessed entry.</param>
    internal static void SetLagMs(string key, double milliseconds) =>
        Set(LagMsValues, key, milliseconds, TagShape.TopicPartitionConsumer, removeAtZero: false);

    /// <summary>
    /// Sets a buffered publisher's queue depth. Zero is kept — an idle producer reporting 0 is how
    /// you tell it apart from one that has gone away.
    /// </summary>
    /// <param name="key">The topic.</param>
    /// <param name="queued">Entries waiting in the queue.</param>
    internal static void SetBufferQueued(string key, long queued) =>
        Set(BufferQueuedValues, key, queued, TagShape.Topic, removeAtZero: false);

    /// <summary>
    /// Sets the length of one partition's stream. Zero is kept.
    /// </summary>
    /// <param name="key">"topic:partition".</param>
    /// <param name="length">Entries in the stream.</param>
    internal static void SetStreamLength(string key, long length) =>
        Set(StreamLengthValues, key, length, TagShape.TopicPartition, removeAtZero: false);

    /// <summary>
    /// Stops reporting every per-partition consumer gauge for one key. Called when a partition is
    /// released, so a pod that no longer owns a partition stops publishing its lag.
    /// </summary>
    /// <param name="key">"topic:partition:consumer".</param>
    internal static void ClearPartition(string key)
    {
        ArgumentNullException.ThrowIfNull(key);

        BlockedValues.TryRemove(key, out _);
        BlockDurationValues.TryRemove(key, out _);
        LagEntriesValues.TryRemove(key, out _);
        LagMsValues.TryRemove(key, out _);
    }

    /// <summary>
    /// Stops reporting a buffered publisher's queue depth, for a publisher that has shut down.
    /// </summary>
    /// <param name="key">The topic.</param>
    internal static void ClearBufferQueued(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        BufferQueuedValues.TryRemove(key, out _);
    }

    /// <summary>
    /// Stops reporting a stream's length, for a stream this process no longer watches.
    /// </summary>
    /// <param name="key">"topic:partition".</param>
    internal static void ClearStreamLength(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        StreamLengthValues.TryRemove(key, out _);
    }

    /// <summary>
    /// Touches the static instruments so the meter and its gauges exist before the first
    /// measurement. Calling it is optional — every instrument is created by the static
    /// constructor — but it makes the intent explicit at startup.
    /// </summary>
    internal static void EnsureInitialized() => _ = Meter.Name;

    // ---------------------------------------------------------------- plumbing

    private static void Set<T>(
        ConcurrentDictionary<string, Sample<T>> store,
        string key,
        T value,
        TagShape shape,
        bool removeAtZero)
        where T : struct, INumber<T>
    {
        ArgumentNullException.ThrowIfNull(key);

        if (removeAtZero && value <= T.Zero)
        {
            store.TryRemove(key, out _);
            return;
        }

        // The tags are derived from the key, so an existing entry can be updated in place and the
        // parse cost is paid once per series rather than once per update.
        if (store.TryGetValue(key, out var existing))
        {
            store[key] = new Sample<T>(value, existing.Tags);
            return;
        }

        store[key] = new Sample<T>(value, Tags(key, shape));
    }

    private static List<Measurement<T>> Observe<T>(ConcurrentDictionary<string, Sample<T>> store)
        where T : struct
    {
        var measurements = new List<Measurement<T>>(store.Count);

        foreach (var entry in store)
            measurements.Add(new Measurement<T>(entry.Value.Value, entry.Value.Tags));

        return measurements;
    }

    /// <summary>
    /// Splits a <c>":"</c>-joined series key into its tags. Segments beyond the shape are ignored,
    /// so a caller may append a discriminator (a "positions" suffix, say) without changing the tags.
    /// </summary>
    private static KeyValuePair<string, object?>[] Tags(string key, TagShape shape)
    {
        var parts = key.Split(':');

        string Part(int index) => index < parts.Length ? parts[index] : "unknown";

        static object Partition(string text) =>
            int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) ? value : text;

        return shape switch
        {
            TagShape.Topic =>
            [
                new KeyValuePair<string, object?>("topic", Part(0)),
            ],
            TagShape.TopicPartition =>
            [
                new KeyValuePair<string, object?>("topic", Part(0)),
                new KeyValuePair<string, object?>("partition", Partition(Part(1))),
            ],
            TagShape.TopicConsumer =>
            [
                new KeyValuePair<string, object?>("topic", Part(0)),
                new KeyValuePair<string, object?>("consumer", Part(1)),
            ],
            _ =>
            [
                new KeyValuePair<string, object?>("topic", Part(0)),
                new KeyValuePair<string, object?>("partition", Partition(Part(1))),
                new KeyValuePair<string, object?>("consumer", Part(2)),
            ],
        };
    }

    /// <summary>How a series key maps onto metric tags.</summary>
    private enum TagShape
    {
        /// <summary>"topic".</summary>
        Topic = 0,

        /// <summary>"topic:partition".</summary>
        TopicPartition = 1,

        /// <summary>"topic:consumer".</summary>
        TopicConsumer = 2,

        /// <summary>"topic:partition:consumer".</summary>
        TopicPartitionConsumer = 3,
    }

    /// <summary>A gauge value and the tags of the series it belongs to.</summary>
    private readonly struct Sample<T>(T value, KeyValuePair<string, object?>[] tags)
        where T : struct
    {
        public T Value { get; } = value;

        public KeyValuePair<string, object?>[] Tags { get; } = tags;
    }
}
