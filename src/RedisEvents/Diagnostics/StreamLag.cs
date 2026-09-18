using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Logging;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Diagnostics;

/// <summary>
/// What a partition's worker is doing, as the gauges and the health check see it.
/// </summary>
internal enum PartitionRunState
{
    /// <summary>Registered, not yet reading — the window between host start and the first fetch.</summary>
    Starting = 0,

    /// <summary>Reading and processing normally.</summary>
    Running = 1,

    /// <summary>Retrying a <c>DontIgnoreException</c>; the position is not advancing.</summary>
    Blocked = 2,

    /// <summary>Stood down — <c>ErrorPolicy.StopPartition</c>, or an ordinary shutdown.</summary>
    Stopped = 3,
}

/// <summary>
/// The per-partition health signals: how far behind it is, and whether its worker is running,
/// blocked or stopped.
/// </summary>
/// <remarks>
/// <para>
/// <b>One object per partition, held by the worker.</b> The alternative — a dictionary keyed by
/// <c>topic:partition:consumer</c> that the processing loop writes to per batch — would put a string
/// concatenation and a hash lookup on the hot path for a value that is read once every scrape
/// interval. The worker takes its monitor once at startup and afterwards
/// <see cref="Observe(StreamId)"/> is two <see cref="Volatile"/> writes.
/// </para>
/// <para>
/// <b>Lag and run state live together</b> because they are the same question asked twice — "is this
/// partition keeping up?" — and the health check needs them at the same instant for the same
/// partition. Splitting them would mean two registries keyed identically and two chances for them to
/// disagree.
/// </para>
/// <para>
/// <b>Idle is not lag.</b> <c>lag.ms</c> is <em>now − the timestamp of the last processed id</em>,
/// which on a quiet topic climbs forever even though the consumer is sitting at the head of the
/// stream with nothing to do. <see cref="MarkCaughtUp"/> is how a reader says "that fetch returned
/// nothing, I am at the tail": lag then reads zero until the next entry arrives. Without it every
/// low-traffic topic would page overnight.
/// </para>
/// </remarks>
internal sealed class StreamPartitionMonitor
{
    private long lastProcessedMs = -1;
    private long lastProcessedSeq;
    private long caughtUp;
    private long lagEntries = -1;
    private int state;
    private long blockedSince;
    private long stoppedSince;
    private int escalates;
    private string? stopReason;

    internal StreamPartitionMonitor(
        string topic,
        string consumer,
        int partition,
        RedisKey streamKey,
        int unhealthyLagMs,
        int unhealthyBlockSeconds,
        int unhealthyStoppedSeconds = 300)
    {
        this.Topic = topic;
        this.Consumer = consumer;
        this.Partition = partition;
        this.StreamKey = streamKey;
        this.UnhealthyLagMs = unhealthyLagMs;
        this.UnhealthyBlockSeconds = unhealthyBlockSeconds;
        this.UnhealthyStoppedSeconds = unhealthyStoppedSeconds;
        this.MetricKey = StreamLag.Key(topic, consumer, partition);
        this.StreamMetricKey = string.Concat(topic, ":", partition.ToString(CultureInfo.InvariantCulture));
    }

    internal string Topic { get; }

    internal string Consumer { get; }

    internal int Partition { get; }

    /// <summary>The Redis key of the partition's stream, for the <c>XINFO STREAM</c> sample.</summary>
    internal RedisKey StreamKey { get; }

    /// <summary><c>topic:partition:consumer</c> — the gauge series key.</summary>
    internal string MetricKey { get; }

    /// <summary><c>topic:partition</c> — the stream-length series key, which has no consumer.</summary>
    internal string StreamMetricKey { get; }

    /// <summary>Lag in milliseconds above which the health check reports Degraded.</summary>
    internal int UnhealthyLagMs { get; }

    /// <summary>Seconds a partition may stay blocked before the health check reports Unhealthy.</summary>
    internal int UnhealthyBlockSeconds { get; }

    /// <summary>
    /// Seconds a stood-down partition that is behind its stream may stay stopped before the health
    /// check reports Unhealthy — only for stops that <see cref="Escalates"/>.
    /// </summary>
    internal int UnhealthyStoppedSeconds { get; }

    /// <summary>
    /// Whether this stop should turn Unhealthy once it has lasted <see cref="UnhealthyStoppedSeconds"/>
    /// with the stream still moving. A contested or co-located stand-down is nobody's decision — the
    /// partition is simply not being read and only a restart brings it back. An
    /// <c>ErrorPolicy.StopPartition</c> stop is an operator's decision and stays Degraded.
    /// </summary>
    internal bool Escalates => Volatile.Read(ref this.escalates) != 0;

    /// <summary>Milliseconds this partition has been stopped, or 0 when it is not stopped.</summary>
    internal double StoppedMs
    {
        get
        {
            var since = Volatile.Read(ref this.stoppedSince);

            return since == 0 ? 0 : Stopwatch.GetElapsedTime(since).TotalMilliseconds;
        }
    }

    internal PartitionRunState State => (PartitionRunState)Volatile.Read(ref this.state);

    /// <summary>Why the partition stopped, when it has; <see langword="null"/> otherwise.</summary>
    internal string? StopReason => Volatile.Read(ref this.stopReason);

    /// <summary>The last id this partition processed, or <see cref="StreamId.Min"/> before the first batch.</summary>
    internal StreamId LastProcessed
    {
        get
        {
            var ms = Volatile.Read(ref this.lastProcessedMs);

            return ms < 0 ? StreamId.Min : new StreamId(ms, Volatile.Read(ref this.lastProcessedSeq));
        }
    }

    /// <summary>
    /// Milliseconds between the last processed entry and now — the local, Redis-free lag.
    /// </summary>
    /// <remarks>
    /// Zero while the partition has processed nothing (there is no measurement yet, and reporting a
    /// number would be inventing one) and zero while it is caught up at the tail.
    /// </remarks>
    internal double LagMs
    {
        get
        {
            if (Volatile.Read(ref this.caughtUp) != 0)
            {
                return 0;
            }

            var ms = Volatile.Read(ref this.lastProcessedMs);
            if (ms < 0)
            {
                return 0;
            }

            var lag = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - ms;

            // The id's timestamp is Redis's clock and "now" is ours; a small negative reading is
            // skew, not negative lag.
            return lag < 0 ? 0 : lag;
        }
    }

    /// <summary>
    /// Entries behind the tail as of the last <c>XINFO STREAM</c> sample, or <c>-1</c> when the
    /// sampler has not run (or is not wired) and the number is genuinely unknown.
    /// </summary>
    internal long LagEntries => Volatile.Read(ref this.lagEntries);

    /// <summary>Milliseconds this partition has been blocked, or 0 when it is not blocked.</summary>
    internal double BlockedMs
    {
        get
        {
            var since = Volatile.Read(ref this.blockedSince);

            return since == 0 ? 0 : Stopwatch.GetElapsedTime(since).TotalMilliseconds;
        }
    }

    /// <summary>
    /// Records the last id this partition processed. Called once per batch from the processing loop.
    /// </summary>
    /// <param name="id">The last id in the batch that was handled successfully.</param>
    /// <remarks>Two volatile writes and nothing else: no allocation, no lookup, no Redis.</remarks>
    internal void Observe(StreamId id)
    {
        Volatile.Write(ref this.lastProcessedSeq, id.Seq);
        Volatile.Write(ref this.lastProcessedMs, id.Ms);
        Volatile.Write(ref this.caughtUp, 0);
    }

    /// <summary>
    /// Records that a fetch came back empty, so this partition is at the tail of its stream and its
    /// lag is zero however old the last entry is.
    /// </summary>
    internal void MarkCaughtUp() => Volatile.Write(ref this.caughtUp, 1);

    /// <summary>Records the entry count from a sample.</summary>
    /// <param name="entries">Entries behind the tail, or <c>-1</c> for unknown.</param>
    internal void SetLagEntries(long entries) => Volatile.Write(ref this.lagEntries, entries);

    /// <summary>The worker is reading. Clears any block and any stop reason.</summary>
    internal void MarkRunning()
    {
        Volatile.Write(ref this.stopReason, null);
        Volatile.Write(ref this.state, (int)PartitionRunState.Running);
        Volatile.Write(ref this.stoppedSince, 0);
        Volatile.Write(ref this.escalates, 0);
        this.ClearBlock();
    }

    /// <summary>
    /// The worker is retrying a <see cref="Errors.DontIgnoreException"/>. Idempotent — the block
    /// clock starts on the first call and keeps running across every retry, because what matters for
    /// health is how long the partition has been stuck, not how many attempts it has made.
    /// </summary>
    internal void MarkBlocked()
    {
        Volatile.Write(ref this.state, (int)PartitionRunState.Blocked);

        if (Interlocked.CompareExchange(ref this.blockedSince, Stopwatch.GetTimestamp(), 0) == 0)
        {
            StreamsDiagnostics.SetBlockedGauge(this.MetricKey, 1);
        }

        StreamsDiagnostics.SetBlockDurationMs(this.MetricKey, this.BlockedMs);
    }

    /// <summary>The blocking batch finally succeeded.</summary>
    internal void MarkUnblocked()
    {
        this.ClearBlock();
        Volatile.Write(ref this.state, (int)PartitionRunState.Running);
    }

    /// <summary>The partition has stood down and will not read again without a restart.</summary>
    /// <param name="reason">Why — surfaced verbatim in the health check description.</param>
    /// <param name="escalate">
    /// Whether this stop turns Unhealthy after <see cref="UnhealthyStoppedSeconds"/> if the stream
    /// keeps moving. The first mark decides: a later re-mark of an already stopped partition (the
    /// co-located reader retiring a slot its policy already stopped) keeps the original verdict.
    /// </param>
    internal void MarkStopped(string reason, bool escalate = false)
    {
        Volatile.Write(ref this.stopReason, reason);

        if (Interlocked.CompareExchange(ref this.stoppedSince, Stopwatch.GetTimestamp(), 0) == 0)
        {
            Volatile.Write(ref this.escalates, escalate ? 1 : 0);
        }

        Volatile.Write(ref this.state, (int)PartitionRunState.Stopped);
        this.ClearBlock();
    }

    /// <summary>Pushes the locally computed lag onto its gauge. Called by the sampler's timer.</summary>
    internal void PublishLagMs()
    {
        StreamsDiagnostics.SetLagMs(this.MetricKey, this.LagMs);

        if (Volatile.Read(ref this.blockedSince) != 0)
        {
            StreamsDiagnostics.SetBlockDurationMs(this.MetricKey, this.BlockedMs);
        }
    }

    private void ClearBlock()
    {
        if (Interlocked.Exchange(ref this.blockedSince, 0) != 0)
        {
            StreamsDiagnostics.SetBlockedGauge(this.MetricKey, 0);
            StreamsDiagnostics.SetBlockDurationMs(this.MetricKey, 0);
        }
    }
}

/// <summary>
/// The process-wide set of <see cref="StreamPartitionMonitor"/>s, and the lag arithmetic they share.
/// </summary>
/// <remarks>
/// Static, like <see cref="StreamsDiagnostics"/> and the ownership registry's snapshot map, because
/// the health check and the gauge callbacks have no way to reach a worker's private state otherwise:
/// health checks are constructed by the framework's own container plumbing, not by the consumer host.
/// </remarks>
internal static class StreamLag
{
    private static readonly ConcurrentDictionary<string, StreamPartitionMonitor> Monitors =
        new(StringComparer.Ordinal);

    /// <summary>How often <c>XINFO STREAM</c> is sampled for <c>streams.lag.entries</c>.</summary>
    internal static TimeSpan DefaultSampleInterval => TimeSpan.FromSeconds(15);

    /// <summary>How often the locally computed <c>streams.lag.ms</c> is pushed onto its gauge.</summary>
    /// <remarks>
    /// Faster than the entry sample because it costs nothing but arithmetic, and because a gauge that
    /// is only pushed when a batch is processed would freeze at its last value exactly when a
    /// consumer stalls — the one moment the number matters.
    /// </remarks>
    internal static TimeSpan DefaultPushInterval => TimeSpan.FromSeconds(1);

    /// <summary>Every monitor registered in this process.</summary>
    internal static ICollection<StreamPartitionMonitor> All => Monitors.Values;

    /// <summary>Whether any partition is being monitored at all.</summary>
    internal static bool Any => !Monitors.IsEmpty;

    /// <summary>The gauge series key for one partition: <c>topic:partition:consumer</c>.</summary>
    internal static string Key(string topic, string consumer, int partition)
        => string.Concat(topic, ":", partition.ToString(CultureInfo.InvariantCulture), ":", consumer);

    /// <summary>
    /// Registers (or re-registers) one partition and hands back its monitor. The worker holds the
    /// result for the rest of its life.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="partition">Partition index.</param>
    /// <param name="streamKey">The partition's stream key, for the entry sample.</param>
    /// <param name="unhealthyLagMs">Lag threshold from <c>ConsumerOptions.UnhealthyLagMs</c>.</param>
    /// <param name="unhealthyBlockSeconds">Block threshold from <c>ConsumerOptions.UnhealthyBlockSeconds</c>.</param>
    /// <param name="unhealthyStoppedSeconds">Stood-down threshold from <c>ConsumerOptions.UnhealthyStoppedSeconds</c>.</param>
    /// <returns>The monitor for that partition.</returns>
    internal static StreamPartitionMonitor Track(
        string topic,
        string consumer,
        int partition,
        RedisKey streamKey,
        int unhealthyLagMs = 120_000,
        int unhealthyBlockSeconds = 300,
        int unhealthyStoppedSeconds = 300)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);

        var monitor = new StreamPartitionMonitor(
            topic, consumer, partition, streamKey, unhealthyLagMs, unhealthyBlockSeconds, unhealthyStoppedSeconds);

        return Monitors.AddOrUpdate(monitor.MetricKey, monitor, (_, _) => monitor);
    }

    /// <summary>
    /// Drops a partition and stops reporting its gauges — a pod that no longer owns a partition must
    /// stop publishing its lag rather than pinning it at the last value it saw.
    /// </summary>
    /// <param name="monitor">The monitor to retire.</param>
    internal static void Forget(StreamPartitionMonitor monitor)
    {
        ArgumentNullException.ThrowIfNull(monitor);

        Monitors.TryRemove(monitor.MetricKey, out _);
        StreamsDiagnostics.ClearPartition(monitor.MetricKey);
        StreamsDiagnostics.ClearStreamLength(monitor.StreamMetricKey);
    }

    /// <summary>Drops every monitor. Test hook — nothing in the library calls it.</summary>
    internal static void Clear()
    {
        foreach (var monitor in Monitors.Values)
        {
            Forget(monitor);
        }
    }

    /// <summary>Pushes the local lag of every monitored partition onto its gauge.</summary>
    internal static void PublishLagMs()
    {
        // Enumerated directly rather than through Values, which snapshots into a new list on every
        // access — this runs once a second for the life of the process.
        foreach (var pair in Monitors)
        {
            pair.Value.PublishLagMs();
        }
    }

    /// <summary>
    /// Estimates how many entries a partition is behind, from one <c>XINFO STREAM</c> reply.
    /// </summary>
    /// <param name="length">The stream's entry count.</param>
    /// <param name="first">Id of the oldest surviving entry.</param>
    /// <param name="last">The stream's last-generated id.</param>
    /// <param name="processed">The last id this consumer processed.</param>
    /// <returns>Entries behind the tail, clamped to <c>[0, length]</c>.</returns>
    /// <remarks>
    /// <para>
    /// Redis stream ids are <c>ms-seq</c> stamps, not a monotonic entry counter, so "how many entries
    /// lie between my position and the tail" has no exact answer short of walking the range — which
    /// on a lagging stream is precisely the scan we cannot afford to run every 15 seconds. So this
    /// interpolates on time, and is <b>exact at both ends</b>, which is where the decisions are made:
    /// zero when the position is at or past the last generated id (caught up), and the full length
    /// when the position is older than the oldest surviving entry — the case where trimming has
    /// already eaten unprocessed data and the <c>lag.entries vs MaxLen</c> alert must page.
    /// </para>
    /// <para>
    /// In between it is an estimate of the right order of magnitude, which is all a 50 %-of-MaxLen
    /// threshold needs.
    /// </para>
    /// </remarks>
    internal static long EstimateEntriesBehind(long length, StreamId first, StreamId last, StreamId processed)
    {
        if (length <= 0 || processed >= last)
        {
            return 0;
        }

        if (processed < first)
        {
            // Our position predates the oldest entry still in the stream: everything present is
            // unread, and whatever was trimmed between the two is gone.
            return length;
        }

        var span = last.Ms - first.Ms;
        if (span <= 0)
        {
            // Every entry landed in the same millisecond; there is nothing to interpolate over, and
            // we already know we are behind.
            return length;
        }

        var behind = (long)((double)length * (last.Ms - processed.Ms) / span);

        // processed < last, so the answer is at least one entry however the arithmetic rounds.
        return Math.Clamp(behind, 1, length);
    }
}

/// <summary>
/// The slow timer behind <c>streams.lag.entries</c> and <c>streams.stream.length</c>, and the fast
/// one behind <c>streams.lag.ms</c>.
/// </summary>
/// <remarks>
/// <para>
/// <c>lag.ms</c> is local arithmetic over an id the worker already recorded, so it is pushed every
/// second and costs nothing. <c>lag.entries</c> needs an <c>XINFO STREAM</c> per partition, so it is
/// sampled every 15 seconds and cached on the monitor in between — one command per partition per 15
/// seconds is negligible next to the read traffic, and going any faster buys an alert threshold
/// nothing.
/// </para>
/// <para>
/// Sampling failures are logged at Debug and swallowed. A metric that cannot be collected must never
/// take down the consumer that is otherwise working.
/// </para>
/// </remarks>
internal sealed class StreamLagSampler : IAsyncDisposable
{
    private readonly IConnectionMultiplexer redis;
    private readonly ILogger? log;
    private readonly TimeSpan push;
    private readonly TimeSpan sample;

    private CancellationTokenSource? cts;
    private Task? loop;

    /// <summary>
    /// Creates a sampler over the <b>shared</b> multiplexer — never a consumer's dedicated reader
    /// connection, which may be parked in a blocking <c>XREAD</c>.
    /// </summary>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="sampleInterval">Entry-sample interval; defaults to 15 s.</param>
    /// <param name="pushInterval">Local-lag push interval; defaults to 1 s.</param>
    internal StreamLagSampler(
        IConnectionMultiplexer redis,
        ILogger? logger = null,
        TimeSpan? sampleInterval = null,
        TimeSpan? pushInterval = null)
    {
        ArgumentNullException.ThrowIfNull(redis);

        this.redis = redis;
        this.log = logger;
        this.sample = sampleInterval ?? StreamLag.DefaultSampleInterval;
        this.push = pushInterval ?? StreamLag.DefaultPushInterval;

        if (this.push <= TimeSpan.Zero || this.sample <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pushInterval),
                "Streams: lag sampler intervals must be positive.");
        }
    }

    /// <summary>Starts both timers. Idempotent.</summary>
    internal void Start()
    {
        if (this.cts is not null)
        {
            return;
        }

        this.cts = new CancellationTokenSource();
        this.loop = Task.Run(() => this.RunAsync(this.cts.Token), CancellationToken.None);
    }

    /// <summary>Stops the timers and waits for the loop to unwind.</summary>
    /// <returns>A task that completes when the loop has stopped.</returns>
    internal async Task StopAsync()
    {
        if (this.cts is null)
        {
            return;
        }

        await this.cts.CancelAsync().ConfigureAwait(false);

        if (this.loop is not null)
        {
            try
            {
                await this.loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }

            this.loop = null;
        }

        this.cts.Dispose();
        this.cts = null;
    }

    /// <summary>
    /// Runs one <c>XINFO STREAM</c> pass over every monitored partition. Exposed separately from the
    /// timer so it can be driven directly.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when every partition has been sampled or skipped.</returns>
    internal async Task SampleOnceAsync(CancellationToken ct)
    {
        var db = this.redis.GetDatabase();

        foreach (var monitor in StreamLag.All)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                var info = await db.StreamInfoAsync(monitor.StreamKey).ConfigureAwait(false);

                StreamsDiagnostics.SetStreamLength(monitor.StreamMetricKey, info.Length);

                var last = ParseId(info.LastGeneratedId);
                var first = info.FirstEntry.IsNull ? StreamId.Min : ParseId(info.FirstEntry.Id);

                monitor.SetLagEntries(
                    StreamLag.EstimateEntriesBehind(info.Length, first, last, monitor.LastProcessed));

                StreamsDiagnostics.SetLagEntries(monitor.MetricKey, monitor.LagEntries);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (RedisServerException)
            {
                // No such key: the stream has not been written to yet. Nothing behind us, nothing in
                // it — that is a real answer, not a failure.
                monitor.SetLagEntries(0);
                StreamsDiagnostics.SetLagEntries(monitor.MetricKey, 0);
                StreamsDiagnostics.SetStreamLength(monitor.StreamMetricKey, 0);
            }
            catch (Exception ex)
            {
                this.log?.LogDebug(
                    ex,
                    "Streams: lag sample failed for topic={Topic} partition={Partition} consumer={Consumer}; streams.lag.entries keeps its previous value until the next sample.",
                    monitor.Topic,
                    monitor.Partition,
                    monitor.Consumer);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync() => await this.StopAsync().ConfigureAwait(false);

    private static StreamId ParseId(RedisValue value)
        => StreamId.TryParse(((string?)value).AsSpan(), out var id) ? id : StreamId.Min;

    private async Task RunAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(this.push);
        var nextSample = Stopwatch.GetTimestamp();

        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                StreamLag.PublishLagMs();

                if (Stopwatch.GetElapsedTime(nextSample) < this.sample)
                {
                    continue;
                }

                nextSample = Stopwatch.GetTimestamp();
                await this.SampleOnceAsync(ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
    }
}
