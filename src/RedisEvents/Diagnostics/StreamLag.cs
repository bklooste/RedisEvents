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

/// <summary>One reading of a partition's progress against the tail of its stream.</summary>
/// <param name="Position">Where the partition has read up to.</param>
/// <param name="PositionUnchangedMs">Milliseconds since that position last changed.</param>
/// <param name="Tail">The last sampled tail id; <see langword="null"/> before the first sample.</param>
/// <param name="TailSampleAgeMs">Milliseconds since the tail was sampled, or <c>-1</c>.</param>
/// <param name="TailUnchangedMs">Milliseconds since a sample last found a new last entry, or <c>-1</c>.</param>
/// <param name="BehindMs">Milliseconds continuously behind the tail with a frozen position; 0 when not behind.</param>
/// <param name="IsBehind">Whether a fresh tail sample is strictly ahead of the position.</param>
internal readonly record struct TailProgress(
    StreamId Position,
    double PositionUnchangedMs,
    StreamId? Tail,
    double TailSampleAgeMs,
    double TailUnchangedMs,
    double BehindMs,
    bool IsBehind);

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

    // The stall clock: the last time this partition showed it can make progress at all — a read
    // that came back (empty or not) or a batch that completed. Independent of the tail sample, so it
    // keeps working when this process cannot reach Redis, which is exactly when the tail sample
    // stops arriving.
    private long progressAt;
    private long busySince;
    private int transportBlocked;
    private int stalledOut;
    private long persistFailingMs;

    // Progress against the stream's tail. Guarded by progressGate rather than Volatile: these are
    // read and written together, once a second at most, and never from the processing loop.
    private readonly Lock progressGate = new();
    private readonly TimeProvider clock;
    private readonly StreamId startPosition;
    private StreamId notedPosition;
    private long positionChangedAt;
    private bool tailSampled;
    private StreamId tail;
    private StreamId lastEntrySeen;
    private long tailSampledAt;
    private long tailChangedAt;
    private bool behind;
    private long behindSince;

    internal StreamPartitionMonitor(
        string topic,
        string consumer,
        int partition,
        RedisKey streamKey,
        int unhealthyLagMs,
        int unhealthyBlockSeconds,
        int unhealthyStoppedSeconds = 300,
        int unhealthyBehindSeconds = 0,
        StreamId startPosition = default,
        TimeProvider? clock = null,
        int degradedSilenceSeconds = 0,
        int unhealthyStalledSeconds = 0)
    {
        this.Topic = topic;
        this.Consumer = consumer;
        this.Partition = partition;
        this.StreamKey = streamKey;
        this.UnhealthyLagMs = unhealthyLagMs;
        this.UnhealthyBlockSeconds = unhealthyBlockSeconds;
        this.UnhealthyStoppedSeconds = unhealthyStoppedSeconds;
        this.UnhealthyBehindSeconds = unhealthyBehindSeconds;
        this.DegradedSilenceSeconds = degradedSilenceSeconds;
        this.UnhealthyStalledSeconds = unhealthyStalledSeconds;
        this.clock = clock ?? TimeProvider.System;
        this.startPosition = startPosition;
        this.notedPosition = startPosition;

        // The frozen-position clock starts when this instance starts reading the partition — at
        // acquisition under Lease, not at process start: a monitor is created per acquisition.
        this.positionChangedAt = this.clock.GetTimestamp();

        // So does the stall clock: a partition that never manages its first read is stalled from
        // the moment it was acquired, not from process start.
        this.progressAt = this.positionChangedAt;
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
    /// Seconds a partition may sit behind the tail of its stream with a position that does not move
    /// before the health check reports Unhealthy; 0 turns the rule off.
    /// </summary>
    internal int UnhealthyBehindSeconds { get; }

    /// <summary>
    /// Seconds this partition's consumer may be caught up and receive nothing before the health
    /// check reports Degraded; 0 turns the rule off.
    /// </summary>
    internal int DegradedSilenceSeconds { get; }

    /// <summary>
    /// Seconds this partition may go without any progress — no read returning, no batch completing,
    /// no position write landing — before it is stalled: the health check reports Unhealthy and, in
    /// <c>Lease</c> mode, the host gives its lease up. 0 turns the rule off.
    /// </summary>
    internal int UnhealthyStalledSeconds { get; }

    /// <summary>
    /// Milliseconds this partition has gone without progress, or 0 when the stall rule does not
    /// apply to its current state.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Progress is anything that proves the partition's pipeline still turns over: a fetch that
    /// returned, empty or not (an idle consumer in <c>ReadMode.Block</c> completes one every
    /// <c>BlockMs</c>, so a quiet topic is never stalled), a batch the handler finished, or the end
    /// of a block. A batch the handler is still holding counts against it from the moment it was
    /// handed over (<see cref="MarkBusy"/>), because the reader runs ahead of the processor and its
    /// empty reads would otherwise hide a handler that never returns. Positions that cannot be
    /// written count against it too: a consumer that reads and handles but whose every position
    /// write fails is not making durable progress, and the host feeds that duration in through
    /// <see cref="SetPersistFailing"/>.
    /// </para>
    /// <para>
    /// It deliberately ignores the sampled tail, the lag and <see cref="IsCaughtUp"/>. The tail is
    /// sampled over the same Redis connection a wedged consumer cannot use, and when that sample
    /// goes stale the behind-the-tail rule stands down — "unknown is not Unhealthy". A stall is
    /// measured on this process's own clock, so it still says something when Redis says nothing.
    /// </para>
    /// <para>
    /// A partition blocked retrying a <c>DontIgnoreException</c> is not stalled: it is doing what
    /// its handler asked, and <see cref="UnhealthyBlockSeconds"/> grades it. A partition blocked
    /// because its reads fail is: that is the outage this rule exists for. A stop is not either,
    /// unless it was this rule that stood the partition down (<see cref="IsStalledOut"/>).
    /// </para>
    /// </remarks>
    internal double StalledMs
    {
        get
        {
            switch (this.State)
            {
                case PartitionRunState.Stopped when Volatile.Read(ref this.stalledOut) == 0:
                    return 0;

                case PartitionRunState.Blocked when Volatile.Read(ref this.transportBlocked) == 0:
                    return 0;
            }

            var idle = this.clock.GetElapsedTime(Volatile.Read(ref this.progressAt)).TotalMilliseconds;

            // A batch the handler has not finished. Separate from the idle clock because the read
            // side runs ahead of the processor: on a quiet topic the reader keeps completing empty
            // reads — progress, by the rule above — while the handler sits on one batch forever.
            var since = Volatile.Read(ref this.busySince);
            var busy = since == 0 ? 0 : this.clock.GetElapsedTime(since).TotalMilliseconds;
            var unpersisted = (double)Volatile.Read(ref this.persistFailingMs);

            return Math.Max(Math.Max(idle, busy), unpersisted);
        }
    }

    /// <summary>
    /// Whether this partition was stood down because it stalled. Such a partition keeps reporting
    /// Unhealthy until this instance reads it again or restarts, because the stall is evidence that
    /// this process cannot do its job, whoever reads the partition now.
    /// </summary>
    internal bool IsStalledOut => Volatile.Read(ref this.stalledOut) != 0;

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

            return since == 0 ? 0 : this.clock.GetElapsedTime(since).TotalMilliseconds;
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
    /// Where this partition has read up to: <see cref="LastProcessed"/> once it has processed a
    /// batch, and before that the position it started from.
    /// </summary>
    /// <remarks>
    /// <see cref="LastProcessed"/> alone reads <c>0-0</c> between a restart and the first batch, which
    /// on a quiet topic is indefinitely — and <c>0-0</c> is behind every non-empty stream. Comparing
    /// that against the tail would call every restarted, caught-up consumer behind.
    /// </remarks>
    internal StreamId Position
    {
        get
        {
            var ms = Volatile.Read(ref this.lastProcessedMs);

            return ms < 0 ? this.startPosition : new StreamId(ms, Volatile.Read(ref this.lastProcessedSeq));
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

            var lag = this.clock.GetUtcNow().ToUnixTimeMilliseconds() - ms;

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

            return since == 0 ? 0 : this.clock.GetElapsedTime(since).TotalMilliseconds;
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
        this.NoteProgress();
    }

    /// <summary>A batch has been handed to the handler. Starts the in-flight clock <see cref="StalledMs"/> reads.</summary>
    internal void MarkBusy() => Volatile.Write(ref this.busySince, this.clock.GetTimestamp());

    /// <summary>
    /// The batch is finished with — handled, skipped under <c>BestEffort</c>, or abandoned on a stop.
    /// Progress, and the end of the in-flight clock.
    /// </summary>
    internal void MarkIdle()
    {
        Volatile.Write(ref this.busySince, 0);
        this.NoteProgress();
    }

    /// <summary>
    /// Records that a fetch returned entries. Progress for the stall clock: the read side is alive
    /// and Redis answered. A fetch that returned nothing reports through <see cref="MarkCaughtUp"/>.
    /// </summary>
    internal void MarkFetched() => this.NoteProgress();

    /// <summary>
    /// Records that a fetch came back empty, so this partition is at the tail of its stream and its
    /// lag is zero however old the last entry is.
    /// </summary>
    internal void MarkCaughtUp()
    {
        Volatile.Write(ref this.caughtUp, 1);
        this.NoteProgress();
    }

    /// <summary>
    /// True when the last fetch returned nothing and this partition is sitting at the tail.
    /// </summary>
    /// <remarks>
    /// Read by <see cref="StreamStatus"/> so an external watchdog can tell a quiet topic from a
    /// stalled one: a position that stands still while caught up is idle, not stuck.
    /// </remarks>
    internal bool IsCaughtUp => Volatile.Read(ref this.caughtUp) != 0;

    /// <summary>Records the entry count from a sample.</summary>
    /// <param name="entries">Entries behind the tail, or <c>-1</c> for unknown.</param>
    internal void SetLagEntries(long entries) => Volatile.Write(ref this.lagEntries, entries);

    /// <summary>
    /// Records one sample of the stream's tail and re-judges whether this partition is behind it.
    /// </summary>
    /// <param name="lastEntry">
    /// The id of the last entry in the stream, or <see cref="StreamId.Min"/> when the stream is empty
    /// or does not exist — in which case there is nothing unread, whatever the position.
    /// </param>
    /// <remarks>
    /// Called by the sampler from the <c>XINFO STREAM</c> reply it already fetches for
    /// <c>streams.lag.entries</c>, so the tail costs no extra round trip.
    /// </remarks>
    internal void SetTail(StreamId lastEntry)
    {
        lock (this.progressGate)
        {
            var now = this.clock.GetTimestamp();

            // A trim that empties the stream is not the tail moving: only a new last entry is. So
            // "the tail last changed" keeps meaning "an entry was last written", which is what a host
            // judging silence wants to know.
            if (!this.tailSampled || (lastEntry != StreamId.Min && lastEntry != this.lastEntrySeen))
            {
                this.tailChangedAt = now;
            }

            if (lastEntry != StreamId.Min)
            {
                this.lastEntrySeen = lastEntry;
            }

            this.tail = lastEntry;
            this.tailSampled = true;
            this.tailSampledAt = now;

            this.NoteProgress(now);
        }
    }

    /// <summary>
    /// Notices a position change and restarts the frozen-position clock. Called once a second by the
    /// sampler's push timer and again whenever progress is read, so the processing loop never pays
    /// for it and a reader never grades a position that moved since the last tick.
    /// </summary>
    internal void RefreshProgress()
    {
        lock (this.progressGate)
        {
            this.NoteProgress(this.clock.GetTimestamp());
        }
    }

    /// <summary>Reads this partition's progress against its tail, as of now.</summary>
    internal TailProgress ReadProgress()
    {
        lock (this.progressGate)
        {
            var now = this.clock.GetTimestamp();
            this.NoteProgress(now);

            var sampleAge = this.tailSampled ? this.ElapsedMs(this.tailSampledAt, now) : -1;

            // A sample that has stopped arriving says nothing about now. The behind clock is not
            // cleared — if sampling resumes and the partition is still behind, it was behind all
            // along — but while the tail is unknown the partition is not reported behind.
            var fresh = this.tailSampled && sampleAge <= StreamLag.TailStaleAfter.TotalMilliseconds;

            return new TailProgress(
                this.notedPosition,
                this.ElapsedMs(this.positionChangedAt, now),
                this.tailSampled ? this.tail : null,
                sampleAge,
                this.tailSampled ? this.ElapsedMs(this.tailChangedAt, now) : -1,
                this.behind && fresh ? this.ElapsedMs(this.behindSince, now) : 0,
                this.behind && fresh);
        }
    }

    /// <summary>The worker is reading. Clears any block and any stop reason.</summary>
    internal void MarkRunning()
    {
        Volatile.Write(ref this.stopReason, null);
        Volatile.Write(ref this.state, (int)PartitionRunState.Running);
        Volatile.Write(ref this.stoppedSince, 0);
        Volatile.Write(ref this.escalates, 0);
        Volatile.Write(ref this.stalledOut, 0);
        this.ClearBlock();
        this.NoteProgress();
    }

    /// <summary>
    /// The worker is retrying a <see cref="Errors.DontIgnoreException"/>. Idempotent — the block
    /// clock starts on the first call and keeps running across every retry, because what matters for
    /// health is how long the partition has been stuck, not how many attempts it has made.
    /// </summary>
    internal void MarkBlocked()
    {
        Volatile.Write(ref this.transportBlocked, 0);
        Volatile.Write(ref this.state, (int)PartitionRunState.Blocked);

        if (Interlocked.CompareExchange(ref this.blockedSince, this.clock.GetTimestamp(), 0) == 0)
        {
            StreamsDiagnostics.SetBlockedGauge(this.MetricKey, 1);
        }

        StreamsDiagnostics.SetBlockDurationMs(this.MetricKey, this.BlockedMs);
    }

    /// <summary>
    /// The read is failing on the transport — Redis unreachable, a timeout, a reply that could not
    /// be parsed — and the read loop is retrying it. Reported as Blocked like a handler block, but
    /// unlike one it does not suspend the stall clock: a partition that cannot read is not making
    /// progress, and nothing its handler asked for explains it.
    /// </summary>
    internal void MarkTransportBlocked()
    {
        this.MarkBlocked();
        Volatile.Write(ref this.transportBlocked, 1);
    }

    /// <summary>The blocking batch finally succeeded.</summary>
    internal void MarkUnblocked()
    {
        this.ClearBlock();
        Volatile.Write(ref this.state, (int)PartitionRunState.Running);
        this.NoteProgress();
    }

    /// <summary>
    /// The host stood this partition down because it stalled, and in <c>Lease</c> mode gave its
    /// lease up. The monitor stays registered after the read side lets the partition go, so the
    /// health check keeps reporting the stall until this instance reads the partition again (a
    /// fresh monitor replaces this one) or restarts.
    /// </summary>
    /// <param name="reason">Why — surfaced verbatim in the health check description.</param>
    internal void MarkStalledOut(string reason)
    {
        Volatile.Write(ref this.stalledOut, 1);
        this.MarkStopped(reason, escalate: true);
    }

    /// <summary>
    /// How long this partition's recorded positions have been failing to reach Redis, or zero when
    /// the last write landed. Pushed by the host, which reads it off the position flusher.
    /// </summary>
    /// <param name="failing">The duration of the current run of failed position writes.</param>
    internal void SetPersistFailing(TimeSpan failing)
        => Volatile.Write(ref this.persistFailingMs, (long)Math.Max(0, failing.TotalMilliseconds));

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

        if (Interlocked.CompareExchange(ref this.stoppedSince, this.clock.GetTimestamp(), 0) == 0)
        {
            Volatile.Write(ref this.escalates, escalate ? 1 : 0);
        }

        Volatile.Write(ref this.state, (int)PartitionRunState.Stopped);
        this.ClearBlock();
    }

    /// <summary>Pushes the locally computed lag onto its gauge. Called by the sampler's timer.</summary>
    internal void PublishLagMs()
    {
        this.RefreshProgress();
        StreamsDiagnostics.SetLagMs(this.MetricKey, this.LagMs);

        if (Volatile.Read(ref this.blockedSince) != 0)
        {
            StreamsDiagnostics.SetBlockDurationMs(this.MetricKey, this.BlockedMs);
        }
    }

    /// <summary>
    /// The one place the behind clock moves. Caller holds <see cref="progressGate"/>.
    /// </summary>
    /// <remarks>
    /// The clock measures "behind the tail AND not moving", so it restarts on either half failing:
    /// any position change (a consumer that is behind but advancing never accumulates time, however
    /// large its backlog), and any sample that finds the position at or past the tail. It starts when
    /// the partition is first <em>seen</em> behind, not when the position last changed — otherwise a
    /// topic that was idle for a day would be a day "behind" the instant its next entry landed.
    /// It deliberately ignores <see cref="LagEntries"/> and <see cref="IsCaughtUp"/>: those are the
    /// consumer's own account of itself, which is what a wedged consumer gets wrong.
    /// </remarks>
    private void NoteProgress(long now)
    {
        var position = this.Position;

        if (position != this.notedPosition)
        {
            this.notedPosition = position;
            this.positionChangedAt = now;
            this.behind = false;
        }

        if (this.tailSampled && this.tail > position)
        {
            if (!this.behind)
            {
                this.behind = true;
                this.behindSince = now;
            }
        }
        else
        {
            this.behind = false;
        }
    }

    private double ElapsedMs(long from, long to) => this.clock.GetElapsedTime(from, to).TotalMilliseconds;

    private void NoteProgress() => Volatile.Write(ref this.progressAt, this.clock.GetTimestamp());

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

    /// <summary>
    /// How old a tail sample may be before it stops counting as knowledge of the tail: four missed
    /// samples. Past this a partition is not graded behind — unknown is not Unhealthy.
    /// </summary>
    internal static TimeSpan TailStaleAfter => DefaultSampleInterval * 4;

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
    /// <param name="unhealthyBehindSeconds">Behind-the-tail threshold from <c>ConsumerOptions.UnhealthyBehindSeconds</c>; 0 is off.</param>
    /// <param name="startPosition">The position this instance starts reading after.</param>
    /// <param name="clock">Clock for every duration the monitor reports; the system clock when null.</param>
    /// <param name="degradedSilenceSeconds">Silence threshold from <c>ConsumerOptions.DegradedSilenceSeconds</c>; 0 is off.</param>
    /// <param name="unhealthyStalledSeconds">Stall threshold from <c>ConsumerOptions.UnhealthyStalledSeconds</c>; 0 is off.</param>
    /// <returns>The monitor for that partition.</returns>
    internal static StreamPartitionMonitor Track(
        string topic,
        string consumer,
        int partition,
        RedisKey streamKey,
        int unhealthyLagMs = 120_000,
        int unhealthyBlockSeconds = 300,
        int unhealthyStoppedSeconds = 300,
        int unhealthyBehindSeconds = 0,
        StreamId startPosition = default,
        TimeProvider? clock = null,
        int degradedSilenceSeconds = 0,
        int unhealthyStalledSeconds = 0)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);

        var monitor = new StreamPartitionMonitor(
            topic,
            consumer,
            partition,
            streamKey,
            unhealthyLagMs,
            unhealthyBlockSeconds,
            unhealthyStoppedSeconds,
            unhealthyBehindSeconds,
            startPosition,
            clock,
            degradedSilenceSeconds,
            unhealthyStalledSeconds);

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

        // Only this monitor, never whatever is registered under its key now. A stalled-out monitor
        // outlives its read side (see MarkStalledOut), and the same partition can be re-acquired
        // and re-registered while it is still held; forgetting the old one later must not take the
        // live one, or its gauges, with it.
        if (!Monitors.TryRemove(new KeyValuePair<string, StreamPartitionMonitor>(monitor.MetricKey, monitor)))
        {
            return;
        }

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

                // The tail is the last entry that is actually there, not the last id ever generated:
                // those differ once the newest entry has been deleted or the stream trimmed empty,
                // and "behind the tail" has to mean there is an entry left to read.
                monitor.SetTail(info.LastEntry.IsNull ? StreamId.Min : ParseId(info.LastEntry.Id));

                monitor.SetLagEntries(
                    StreamLag.EstimateEntriesBehind(info.Length, first, last, monitor.Position));

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
                monitor.SetTail(StreamId.Min);
                monitor.SetLagEntries(0);
                StreamsDiagnostics.SetLagEntries(monitor.MetricKey, 0);
                StreamsDiagnostics.SetStreamLength(monitor.StreamMetricKey, 0);
            }
            catch (Exception ex)
            {
                this.log?.LogDebug(
                    ex,
                    "Streams: lag sample failed for topic={Topic} partition={Partition} consumer={Consumer}; streams.lag.entries and the sampled tail keep their previous values until the next sample.",
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
