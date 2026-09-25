using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Logging;
using RedisEvents.Consumer;
using RedisEvents.Diagnostics;
using RedisEvents.Ownership;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Positions;

/// <summary>
/// Raised when a flush finds that another instance has been writing one of our partitions.
/// </summary>
/// <remarks>
/// The host binds this to "stop that partition's worker". Two instances writing one position hash
/// can move the position backwards, which replays already-processed messages — tolerable, because
/// at-least-once handlers must already tolerate replay — but pointless. Rather than fight, we stand
/// down and let whoever else is running keep going, so the outcome is one owner rather than two.
/// <para>
/// Raised only for a <em>live</em> rival, and only by the loser of the tiebreak: the lower instance
/// id keeps the partition. Both contenders standing down would leave it consumed by nobody, which
/// is what happened before R-01.
/// </para>
/// </remarks>
/// <param name="partition">The contested partition.</param>
/// <param name="mine">This instance's id.</param>
/// <param name="theirs">The instance id found in the hash before our write.</param>
internal delegate void PartitionContestedCallback(int partition, Guid mine, Guid theirs);

/// <summary>
/// Raised when a partition stood down under <see cref="PartitionContestedCallback"/> can be picked
/// up again: the instance it stood down for is no longer present in the ownership hash.
/// </summary>
/// <remarks>
/// The host binds this to "bring the read side back up". The evidence is the same evidence the
/// stand-down itself used — the rival's presence field — so resuming cannot admit a second writer
/// the original decision would have tolerated: a contender that is gone from the hash is exactly
/// the contender <see cref="PositionFlusher.NoteStale"/> already declines to stand down for.
/// </remarks>
/// <param name="partition">The partition that may resume.</param>
/// <param name="theirs">The contender this instance had stood down for.</param>
internal delegate void PartitionRecoveredCallback(int partition, Guid theirs);

/// <summary>
/// Coalesces per-partition positions for one consumer and writes the dirty ones in a single
/// <c>HSET</c> on a timer.
/// </summary>
/// <remarks>
/// <para>
/// One flusher per consumer, not per partition — that is what lets a tick with twelve busy
/// partitions cost one round trip instead of twelve. <see cref="Record"/> runs on the processor
/// loop and does no work beyond three stores and at most two interlocked operations: it never
/// allocates, never awaits, and never touches Redis. Everything expensive happens on the timer.
/// </para>
/// <para>
/// When nothing has been recorded since the last tick the flusher writes nothing at all, so an idle
/// consumer generates no Redis traffic — this matters because a service with twenty low-rate topics
/// would otherwise heartbeat Redis twenty times a second for no reason.
/// </para>
/// <para>
/// A flush failure is logged at Warning, counted on <c>streams.positions.flush_failures</c>, and
/// retried on the next tick: the affected partitions are simply marked dirty again. It never
/// propagates into the processing path. Positions are an optimisation against redelivery, not a
/// delivery guarantee, so losing a flush costs replay and nothing else.
/// </para>
/// <para>
/// <b>The reset tick.</b> When a <see cref="ResetSignal"/> is supplied, the same timer also polls the
/// consumer's reset markers (<see cref="PollResetsAsync"/>). It belongs here rather than in the read
/// loop because this is the loop a live reset races: without the marker, the admin call's target is
/// overwritten by the very next flush. Polling costs one small <c>HMGET</c> per reset interval per
/// consumer — the one thing an otherwise idle consumer does spend, and the reason the interval is a
/// knob.
/// </para>
/// </remarks>
internal sealed class PositionFlusher : IAsyncDisposable
{
    /// <summary>Sentinel in <see cref="pendingSeq"/> meaning "no value, or a write in progress".</summary>
    /// <remarks>A real Redis sequence number is never negative, so this cannot collide with one.</remarks>
    private const long Unwritten = long.MinValue;

    /// <summary>Nothing has been said about this partition's foreign writer yet.</summary>
    private const int ReportedNothing = 0;

    /// <summary>A foreign id was seen, but nobody was behind it.</summary>
    private const int ReportedStale = 1;

    /// <summary>A live second writer was seen. Outranks <see cref="ReportedStale"/>.</summary>
    private const int ReportedContention = 2;

    /// <summary>A live peer's late write landed on a partition this instance holds the claim for.</summary>
    private const int ReportedLateWrite = 3;

    private readonly IPositionStore store;
    private readonly IConnectionMultiplexer? redis;
    private readonly RedisKey key;
    private readonly string topic;
    private readonly string consumer;
    private readonly string metricKey;
    private readonly Guid instanceId;
    private readonly TimeSpan interval;
    private readonly ILogger? log;
    private readonly PartitionContestedCallback? onContested;

    /// <summary>
    /// Whether a partition's claim field names exactly one holder. True under
    /// <c>InstanceMode.Lease</c>, where claims are taken with <c>HSETNX</c>; false under Static, where
    /// every configured instance rewrites its claims each cycle and two misconfigured replicas of one
    /// ordinal flip the field between them — there the claim proves nothing and only the id tiebreak
    /// can settle it.
    /// </summary>
    private readonly bool exclusiveClaims;

    /// <summary>
    /// How long an overlap must persist before a Static-mode instance acts on it. A rolling deploy
    /// surges to two pods of one ordinal for a few seconds; a misconfigured replica count does not
    /// go away. <see cref="TimeSpan.Zero"/> restores the act-on-first-sight behaviour.
    /// </summary>
    private readonly TimeSpan contestedGrace;

    /// <summary>Flush ticks between re-arbitrations of a stood-down partition; 0 disables them.</summary>
    private readonly int recheckTicks;

    private readonly PartitionRecoveredCallback? onRecovered;

    private readonly KeyValuePair<string, object?> topicTag;
    private readonly KeyValuePair<string, object?> consumerTag;

    // Reset-marker protocol. All four arrays are index-aligned with resetPartitions and are built
    // once, so a tick that finds no marker allocates nothing at all.
    private readonly ResetSignal? resets;
    private readonly int[] resetPartitions;
    private readonly RedisValue[] resetFields;
    private readonly RedisValue[] resetClearFields;
    private readonly ResetMarker[] resetBuffer;
    private readonly int resetPollTicks;

    // Indexed by partition. pendingMs/pendingSeq hold the last recorded id; dirty is 0/1; stopped is
    // 0/1 and latches on once a second writer has been seen. reported and counted latch the
    // once-per-partition log line and the contested gauge, so a persistent overlap does not log or
    // count on every flush.
    private readonly long[] pendingMs;
    private readonly long[] pendingSeq;
    private readonly int[] dirty;
    private readonly int[] stopped;
    private readonly int[] reported;
    private readonly int[] counted;

    // contestedSince is the Stopwatch timestamp of the first overlap seen on a partition that the
    // claim could not exonerate, and 0 when the partition is clean; contenders is the id a
    // stood-down partition is waiting on, so the re-arbitration knows whose presence to re-probe.
    private readonly long[] contestedSince;
    private readonly Guid[] contenders;

    // Scratch for the re-arbitration probe. Its own array rather than foreignPartitions: that one
    // belongs to the flush path and is only safe under flushGate, which this does not take.
    private readonly int[] recheckPartitions;

    // Scratch for the second-writer check, sized to the partition count and reused: a flush that
    // finds no foreign id touches none of it. Only ever read on the flush path, which flushGate
    // serialises.
    private readonly int[] foreignPartitions;
    private readonly PositionRecord[] foreignRecords;

    // o:{topic}:<consumer>, read only when a foreign id has to be checked for liveness.
    private readonly RedisKey ownershipKey;

    // Set when a reset rewound a partition, cleared by the first record that comes back behind the
    // pending value. Read only by the DEBUG-only monotonicity assert, which must not fire on the
    // replay a reset deliberately caused.
    private readonly int[] rewound;

    // Reused across flushes; safe because flushGate serialises every flush.
    private readonly (int Partition, StreamId Id)[] buffer;
    private readonly SemaphoreSlim flushGate = new(1, 1);

    private int dirtyCount;
    private int contestedCount;
    private int contentionCount;
    private long flushes;
    private long failures;
    private long resetsApplied;
    private long recoveries;
    private int ticks;
    private int recheckCountdown;

    private CancellationTokenSource? cts;
    private Task? loop;

    /// <summary>
    /// Creates a flusher for one (topic, consumer) pair.
    /// </summary>
    /// <param name="store">Where positions are written. <see cref="NullPositionStore"/> for
    /// <c>Persist = None</c>; the flusher then coalesces into a no-op and costs nothing.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name — the owner of the position hash.</param>
    /// <param name="partitionCount">Total partitions on the topic. Slots are allocated for all of
    /// them, including ones this instance does not own; an unowned slot is simply never recorded.</param>
    /// <param name="interval">Flush period, from <c>ConsumerOptions.PersistIntervalMs</c>. This is
    /// also the width of the duplicate window after a hard kill.</param>
    /// <param name="log">Logger for flush failures and contention. Optional.</param>
    /// <param name="redis">The shared multiplexer, used only to pipeline the second-writer
    /// <c>HMGET</c> alongside the store's <c>HSET</c>. Pass <see langword="null"/> to skip the check
    /// — which is the right thing for a non-Redis store, where the field layout is not ours.</param>
    /// <param name="instanceId">This instance's id, stamped into every written value. Defaults to
    /// the store's id when it is a <see cref="RedisPositionStore"/>, else to the per-process id.</param>
    /// <param name="onContested">Invoked once per partition when another instance's id is found in
    /// a field we were about to write. Expected to stop that partition's worker.</param>
    /// <param name="resets">
    /// The hand-off to this instance's read loops for the reset-marker protocol, covering exactly the
    /// partitions this instance owns. Pass <see langword="null"/> — as the tests and any non-Redis
    /// store do — to skip marker polling entirely; a reset then applies at the next start instead of
    /// live.
    /// </param>
    /// <param name="resetPollInterval">
    /// How often reset markers are polled, rounded to whole flush ticks. Defaults to
    /// <paramref name="interval"/>, which is the plan's "noticed on the next flush tick" and costs one
    /// small <c>HMGET</c> per second per consumer. A service with many idle topics can widen it; the
    /// only thing it delays is how quickly a <em>running</em> consumer rewinds.
    /// </param>
    /// <param name="exclusiveClaims">
    /// True when the ownership registry runs in <c>InstanceMode.Lease</c>, so a partition's claim
    /// field names its one holder and a live writer that is not that holder is a late flush, not a
    /// rival. False (Static) keeps the id tiebreak as the only judge.
    /// </param>
    /// <param name="contestedGrace">
    /// How long an overlap the claim could not exonerate must persist before this instance acts on
    /// it, from <c>ConsumerOptions.ContestedGraceSeconds</c>. Only consulted when
    /// <paramref name="exclusiveClaims"/> is false: under Lease the claim settles it outright and no
    /// timer is needed. <see cref="TimeSpan.Zero"/> (the default here, so no existing caller changes
    /// behaviour) acts on the first sight of the overlap.
    /// </param>
    /// <param name="onRecovered">
    /// Invoked once per partition when a stood-down partition's contender has left the ownership
    /// hash, so the partition can be read again. Expected to bring the read side back up. Pass
    /// <see langword="null"/> — as the default does — and a stand-down stays permanent.
    /// </param>
    /// <param name="recheckInterval">
    /// How often a stood-down partition re-probes its contender, rounded to whole flush ticks. Only
    /// a flusher with a stood-down partition pays anything for it. <see langword="null"/> or
    /// non-positive disables re-arbitration entirely.
    /// </param>
    internal PositionFlusher(
        IPositionStore store,
        string topic,
        string consumer,
        int partitionCount,
        TimeSpan interval,
        ILogger? log = null,
        IConnectionMultiplexer? redis = null,
        Guid instanceId = default,
        PartitionContestedCallback? onContested = null,
        ResetSignal? resets = null,
        TimeSpan? resetPollInterval = null,
        bool exclusiveClaims = false,
        TimeSpan contestedGrace = default,
        PartitionRecoveredCallback? onRecovered = null,
        TimeSpan? recheckInterval = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partitionCount);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);

        this.store = store;
        this.redis = redis;
        this.topic = topic;
        this.consumer = consumer;
        this.interval = interval;
        this.log = log;
        this.onContested = onContested;
        this.exclusiveClaims = exclusiveClaims;
        this.contestedGrace = contestedGrace > TimeSpan.Zero ? contestedGrace : TimeSpan.Zero;
        this.onRecovered = onRecovered;

        this.instanceId = instanceId != Guid.Empty
            ? instanceId
            : (store as RedisPositionStore)?.InstanceId ?? OwnershipRegistry.StableInstanceId;

        this.key = StreamKeys.Positions(topic, consumer);

        // Deliberately distinct from the OwnershipRegistry's "<topic>:<consumer>" gauge key: the two
        // detect contention by different means and must not overwrite each other's count.
        this.metricKey = string.Concat(topic, ":", consumer, ":positions");

        this.topicTag = new KeyValuePair<string, object?>("topic", topic);
        this.consumerTag = new KeyValuePair<string, object?>("consumer", consumer);

        this.ownershipKey = StreamKeys.Ownership(topic, consumer);

        this.pendingMs = new long[partitionCount];
        this.pendingSeq = new long[partitionCount];
        this.dirty = new int[partitionCount];
        this.stopped = new int[partitionCount];
        this.reported = new int[partitionCount];
        this.counted = new int[partitionCount];
        this.foreignPartitions = new int[partitionCount];
        this.foreignRecords = new PositionRecord[partitionCount];
        this.rewound = new int[partitionCount];
        this.contestedSince = new long[partitionCount];
        this.contenders = new Guid[partitionCount];
        this.recheckPartitions = new int[partitionCount];
        this.buffer = new (int, StreamId)[partitionCount];

        Array.Fill(this.pendingSeq, Unwritten);

        // Marker polling needs both a signal (which partitions to poll, and who to hand a reset to)
        // and the multiplexer (the hash is ours to read only when the store is the Redis one).
        this.resets = redis is null ? null : resets;
        this.resetPartitions = this.resets is null ? [] : this.resets.Partitions.ToArray();
        this.resetFields = this.resets is null ? [] : ResetMarkers.Fields(this.resetPartitions);
        this.resetClearFields = new RedisValue[this.resetPartitions.Length];
        this.resetBuffer = new ResetMarker[this.resetPartitions.Length];

        // Re-arbitration rides the same timer as everything else: a whole tick is the smallest unit
        // this loop has, and a probe more often than a flush would only ask Redis the same question
        // twice with nothing having moved between.
        this.recheckTicks = recheckInterval is { } recheck && recheck > TimeSpan.Zero && onRecovered is not null
            ? (int)Math.Clamp(Math.Round(recheck.TotalMilliseconds / interval.TotalMilliseconds), 1, int.MaxValue)
            : 0;
        this.recheckCountdown = this.recheckTicks;

        var pollEvery = resetPollInterval ?? interval;
        this.resetPollTicks = pollEvery <= interval
            ? 1
            : (int)Math.Clamp(Math.Round(pollEvery.TotalMilliseconds / interval.TotalMilliseconds), 1, int.MaxValue);

        this.Recorder = this.Record;
    }

    /// <summary>
    /// <see cref="Record"/> as a delegate, allocated once here rather than at every wiring site, so
    /// the partition workers can take it without a closure.
    /// </summary>
    internal PositionRecorder Recorder { get; }

    /// <summary>Partitions with a recorded position not yet written to Redis.</summary>
    internal int DirtyCount => Volatile.Read(ref this.dirtyCount);

    /// <summary>Flushes that reached Redis with at least one partition. Diagnostics and tests.</summary>
    internal long Flushes => Interlocked.Read(ref this.flushes);

    /// <summary>Flushes that threw and will be retried. Mirrors <c>streams.positions.flush_failures</c>.</summary>
    internal long Failures => Interlocked.Read(ref this.failures);

    /// <summary>Partitions stood down because a live second instance was writing them.</summary>
    /// <remarks>
    /// Strictly the partitions this instance <em>gave up</em>. A partition it kept after winning the
    /// tiebreak is contention too and shows in <see cref="ContentionCount"/> and on
    /// <c>streams.positions.contested</c>, but it is still being consumed, so it is not counted here.
    /// </remarks>
    internal int ContestedCount => Volatile.Read(ref this.contestedCount);

    /// <summary>
    /// Partitions in contention with a live second writer, whether or not this instance gave them
    /// up. This is what the gauge and the health check see: two pods writing one partition is a
    /// misconfiguration from both sides, and only one of them stands down.
    /// </summary>
    internal int ContentionCount => Volatile.Read(ref this.contentionCount);

    /// <summary>Stood-down partitions this flusher has taken back after their contender left.</summary>
    internal long Recoveries => Interlocked.Read(ref this.recoveries);

    /// <summary>Reset markers this flusher has handed to a read loop and then cleared.</summary>
    internal long ResetsApplied => Interlocked.Read(ref this.resetsApplied);

    /// <summary>
    /// Records a processed position. Called on the processor loop, after a batch has been handled.
    /// </summary>
    /// <param name="partition">The partition the position belongs to.</param>
    /// <param name="id">The last entry id processed in that partition.</param>
    /// <remarks>
    /// <para>
    /// No allocation, no await, no lock, no CAS loop. There is exactly one writer per partition and
    /// positions only ever move forward, so the value is a plain store; the interlocked pair is only
    /// the dirty bookkeeping the timer reads.
    /// </para>
    /// <para>
    /// The three stores are ordered <c>seq := Unwritten</c>, <c>ms</c>, <c>seq</c> so that the timer
    /// can tell a half-written pair from a complete one (see <see cref="TryReadPending"/>). Without
    /// that, a flush landing between the two halves could publish <c>(newMs, oldSeq)</c> — a
    /// position slightly <em>ahead</em> of what was processed, which would skip messages rather than
    /// replay them. On x86 these are plain stores; the cost is a compiler barrier.
    /// </para>
    /// </remarks>
    internal void Record(int partition, StreamId id)
    {
        if ((uint)partition >= (uint)this.dirty.Length)
        {
            // A position for a partition we have no slot for is a wiring bug, not a message the
            // caller can do anything about. Losing it costs redelivery, so we do not throw into the
            // processing path over it.
            Debug.Fail("Streams: position recorded for a partition outside the topic's range.");
            return;
        }

        if (Volatile.Read(ref this.stopped[partition]) != 0)
        {
            // We stood this partition down after seeing a second writer; stop writing its field.
            return;
        }

        this.AssertMovesForward(partition, id);

        Volatile.Write(ref this.pendingSeq[partition], Unwritten);
        Volatile.Write(ref this.pendingMs[partition], id.Ms);
        Volatile.Write(ref this.pendingSeq[partition], id.Seq);

        if (Interlocked.Exchange(ref this.dirty[partition], 1) == 0)
        {
            Interlocked.Increment(ref this.dirtyCount);
        }
    }

    /// <summary>Starts the timer loop. Idempotent.</summary>
    internal void Start()
    {
        if (this.cts is not null)
        {
            return;
        }

        this.cts = new CancellationTokenSource();
        this.loop = Task.Run(() => this.LoopAsync(this.cts.Token), CancellationToken.None);
    }

    /// <summary>
    /// Stops the timer loop and does one final, synchronous flush so a clean shutdown does not
    /// redeliver the last interval's worth of messages.
    /// </summary>
    /// <param name="ct">Cancellation for the final flush.</param>
    /// <returns>A task that completes once the loop has stopped and the final flush was attempted.</returns>
    internal async Task StopAsync(CancellationToken ct = default)
    {
        if (this.cts is not null)
        {
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

        await this.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes every dirty partition in one <c>HSET</c>, pipelined with the second-writer
    /// <c>HMGET</c>. Never throws.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when the write has landed or has failed and been re-queued.</returns>
    internal async ValueTask FlushAsync(CancellationToken ct = default)
    {
        if (Volatile.Read(ref this.dirtyCount) == 0)
        {
            return;
        }

        await this.flushGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        var count = 0;

        try
        {
            count = this.Drain();
            if (count == 0)
            {
                return;
            }

            await this.WriteAsync(count, ct).ConfigureAwait(false);
            Interlocked.Increment(ref this.flushes);
        }
        catch (Exception ex)
        {
            // Never fails a batch: re-queue what we drained and let the next tick try again.
            this.Requeue(count);
            Interlocked.Increment(ref this.failures);
            StreamsDiagnostics.StreamsPositionsFlushFailures.Add(1, this.topicTag, this.consumerTag);

            if (ex is not OperationCanceledException)
            {
                this.log?.LogWarning(
                    ex,
                    "Streams: position flush failed for topic {Topic} consumer {Consumer} ({Partitions} partitions); retrying in {IntervalMs}ms. Messages already processed may be redelivered if the process stops before a flush succeeds.",
                    this.topic,
                    this.consumer,
                    count,
                    (long)this.interval.TotalMilliseconds);
            }
        }
        finally
        {
            this.flushGate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await this.StopAsync(CancellationToken.None).ConfigureAwait(false);
        StreamsDiagnostics.SetContestedPartitions(this.metricKey, 0);
        this.flushGate.Dispose();
    }

    /// <summary>
    /// Moves every dirty partition into <see cref="buffer"/>, clearing its flag first so a
    /// <see cref="Record"/> that lands during the drain re-marks itself and is picked up next tick
    /// rather than being lost.
    /// </summary>
    /// <returns>How many leading entries of <see cref="buffer"/> are live.</returns>
    private int Drain()
    {
        var count = 0;

        for (var partition = 0; partition < this.dirty.Length; partition++)
        {
            if (Volatile.Read(ref this.dirty[partition]) == 0)
            {
                continue;
            }

            if (Interlocked.Exchange(ref this.dirty[partition], 0) == 1)
            {
                Interlocked.Decrement(ref this.dirtyCount);
            }

            if (Volatile.Read(ref this.stopped[partition]) != 0)
            {
                continue;
            }

            if (this.TryReadPending(partition, out var id))
            {
                this.buffer[count++] = (partition, id);
            }
            else
            {
                // Caught mid-write. Costs one tick, never a wrong position.
                this.MarkDirty(partition);
            }
        }

        return count;
    }

    /// <summary>
    /// Reads a partition's pending id, rejecting a pair caught mid-write.
    /// </summary>
    /// <param name="partition">The partition.</param>
    /// <param name="id">The pending id when this returns <see langword="true"/>.</param>
    /// <returns><see langword="false"/> when nothing has been recorded yet, or when the read raced a
    /// concurrent <see cref="Record"/>.</returns>
    /// <remarks>
    /// Reads <c>seq</c>, then <c>ms</c>, then <c>seq</c> again and requires the two <c>seq</c> reads
    /// to match. Given the store order in <see cref="Record"/>, that leaves only pairs that a real
    /// record produced, or a pair whose <c>ms</c> is older than the matching <c>seq</c> — i.e. a
    /// position at or behind the truth, which replays rather than skips.
    /// </remarks>
    private bool TryReadPending(int partition, out StreamId id)
    {
        var first = Volatile.Read(ref this.pendingSeq[partition]);
        if (first == Unwritten)
        {
            id = default;
            return false;
        }

        var ms = Volatile.Read(ref this.pendingMs[partition]);

        if (Volatile.Read(ref this.pendingSeq[partition]) != first)
        {
            id = default;
            return false;
        }

        id = new StreamId(ms, first);
        return true;
    }

    /// <summary>Marks a partition dirty without touching its value.</summary>
    /// <param name="partition">The partition.</param>
    private void MarkDirty(int partition)
    {
        if (Interlocked.Exchange(ref this.dirty[partition], 1) == 0)
        {
            Interlocked.Increment(ref this.dirtyCount);
        }
    }

    /// <summary>Re-queues a failed flush's partitions so the next tick retries them.</summary>
    /// <param name="count">How many leading entries of <see cref="buffer"/> were in the failed write.</param>
    private void Requeue(int count)
    {
        for (var i = 0; i < count; i++)
        {
            this.MarkDirty(this.buffer[i].Partition);
        }
    }

    /// <summary>
    /// Issues the write, and — when a multiplexer was supplied — the second-writer read alongside it.
    /// </summary>
    /// <param name="count">How many leading entries of <see cref="buffer"/> to write.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when both commands have returned.</returns>
    /// <remarks>
    /// The <c>HMGET</c> is dispatched but not awaited before the store's <c>HSET</c> is dispatched,
    /// so the multiplexer pipelines the pair into one round trip and the values we read back are the
    /// ones that were there <em>before</em> our write. This is detection, not fencing: there is no
    /// atomicity here and none is wanted. Under an unlucky interleaving it misses a flush, and it
    /// converges on the next one.
    /// </remarks>
    private async ValueTask WriteAsync(int count, CancellationToken ct)
    {
        if (this.redis is null)
        {
            await this.SaveAsync(count, ct).ConfigureAwait(false);
            return;
        }

        var fields = new RedisValue[count];
        for (var i = 0; i < count; i++)
        {
            fields[i] = RedisPositionStore.Field(this.buffer[i].Partition);
        }

        var previous = this.redis.GetDatabase().HashGetAsync(this.key, fields);
        var save = this.SaveAsync(count, ct);

        await save.ConfigureAwait(false);
        var before = await previous.ConfigureAwait(false);

        await this.InspectAsync(before, count, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Hands the drained buffer to the store. A separate method because a <c>Span</c> cannot live
    /// across an <c>await</c> in an async method.
    /// </summary>
    /// <param name="count">How many leading entries of <see cref="buffer"/> to write.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The store's write.</returns>
    private ValueTask SaveAsync(int count, CancellationToken ct)
        => this.store.SaveAsync(this.topic, this.consumer, this.buffer.AsSpan(0, count), ct);

    /// <summary>
    /// Compares the pre-write values against our own instance id and stands down any partition a
    /// <em>live</em> second instance is writing.
    /// </summary>
    /// <param name="before">The <c>HMGET</c> result, in <see cref="buffer"/> order.</param>
    /// <param name="count">How many leading entries of <see cref="buffer"/> were written.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes once every mismatch has been judged.</returns>
    /// <remarks>
    /// <para>
    /// <b>R-01.</b> A foreign id in the field used to be enough to stand the partition down, which
    /// made three ordinary events look like a rival: a pod restart (a fresh per-process GUID —
    /// fixed by <see cref="OwnershipRegistry.StableInstanceId"/>), a reset issued from another
    /// process, and a genuine overlap, where <em>both</em> sides stood down and the partition was
    /// then consumed by nobody.
    /// </para>
    /// <para>
    /// So a mismatch is now a question rather than a verdict, answered in one <c>HMGET</c> against
    /// the ownership hash — and only on the mismatch path, so a healthy consumer pays nothing.
    /// The foreign id must hold a live, unexpired claim (<c>i:&lt;instanceId&gt;</c>, which no other
    /// instance overwrites and which lapses with the pod) to count as a contender at all; and of two
    /// genuine contenders the lower id keeps the partition, so exactly one stands down.
    /// </para>
    /// <para>
    /// When the probe itself fails, nothing stands down. Redis being unreachable is not evidence of
    /// a second writer, and the next flush asks again.
    /// </para>
    /// </remarks>
    private async ValueTask InspectAsync(RedisValue[] before, int count, CancellationToken ct)
    {
        var limit = Math.Min(count, before.Length);
        var candidates = 0;

        for (var i = 0; i < limit; i++)
        {
            var raw = (string?)before[i];
            if (raw is null)
            {
                // First write for this partition: nobody to contend with.
                continue;
            }

            if (!PositionRecord.TryParse(raw, out var record))
            {
                continue;
            }

            // Guid.Empty means the value carried no attribution — a legacy or hand-written position.
            // Unattributed is not evidence of a second writer, so it is left alone.
            if (record.InstanceId == Guid.Empty || record.InstanceId == this.instanceId)
            {
                if (record.InstanceId == this.instanceId)
                {
                    // Our own id came back out of the field: whatever overlap the grace clock was
                    // timing has stopped. Reset it, or a brand new overlap months later would be
                    // judged against a timestamp from this one and lose its grace.
                    Volatile.Write(ref this.contestedSince[this.buffer[i].Partition], 0L);
                }

                continue;
            }

            if (record.InstanceId == RedisPositionStore.AdminInstanceId)
            {
                // An operator rewinding us, not a rival. The marker protocol on the same tick is
                // what actually applies it; all this has to do is not fight it.
                continue;
            }

            this.foreignPartitions[candidates] = this.buffer[i].Partition;
            this.foreignRecords[candidates] = record;
            candidates++;
        }

        if (candidates == 0)
        {
            return;
        }

        await this.JudgeAsync(candidates, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Asks the ownership hash which of the foreign writers are actually still alive, then contests
    /// or holds each partition accordingly.
    /// </summary>
    /// <param name="candidates">How many leading entries of the foreign arrays are live.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes once every candidate has been judged.</returns>
    private async ValueTask JudgeAsync(int candidates, CancellationToken ct)
    {
        var db = this.redis?.GetDatabase();
        if (db is null)
        {
            return;
        }

        RedisValue[] live;

        try
        {
            ct.ThrowIfCancellationRequested();

            // Two questions per candidate in one HMGET: is the foreign writer alive (its presence
            // field), and who holds the partition's claim right now (the partition field).
            var fields = new RedisValue[candidates * 2];
            for (var i = 0; i < candidates; i++)
            {
                fields[i] = OwnershipRegistry.PresenceField(this.foreignRecords[i].InstanceId);
                fields[candidates + i] = this.foreignPartitions[i].ToString(CultureInfo.InvariantCulture);
            }

            live = await db.HashGetAsync(this.ownershipKey, fields).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Fail safe: an unanswerable question is not a second writer.
            this.log?.LogWarning(
                ex,
                "Streams: could not check whether another instance writing topic {Topic} consumer {Consumer} is still live; " +
                "leaving the partition running and re-checking on the next flush.",
                this.topic,
                this.consumer);
            return;
        }

        for (var i = 0; i < candidates && candidates + i < live.Length; i++)
        {
            var partition = this.foreignPartitions[i];
            var other = this.foreignRecords[i];

            if (live[i].IsNull)
            {
                // The id is in the field but its owner is gone: a previous life of this consumer, an
                // out-of-process reset, or a pod that has since died. Nothing to stand down for.
                this.NoteStale(partition, other);
                continue;
            }

            // R-01 follow-up. The writer is alive, but "alive" is not "owns this partition". On a
            // Lease-mode handoff the old owner keeps its other partitions — and its presence field —
            // while its last async flush for the one it gave up lands after the new owner has claimed
            // it. That write is late, not rival: the claim says so, and the claim is the only thing
            // that can. Without this check the new owner stood down, kept the lease, and the
            // partition was read by nobody until a restart.
            if (this.exclusiveClaims &&
                !live[candidates + i].IsNull &&
                PartitionOwner.Parse(partition, (string)live[candidates + i]!).InstanceId == this.instanceId)
            {
                this.NoteLateWrite(partition, other);
                continue;
            }

            // Static mode, where the claim cannot exonerate anyone. Two same-ordinal replicas both
            // HSET the partition's claim every cycle, so the field names whichever wrote last and
            // proves nothing about who should be reading — which is why the check above is
            // Lease-only. What does separate the two cases is time: a rolling deploy's overlap is
            // the surge window of one Deployment and ends when the predecessor exits, while a
            // misconfigured replica count does not end at all. So an overlap the claim could not
            // settle has to persist before this instance acts on it.
            if (!this.exclusiveClaims && this.WithinGrace(partition, other))
            {
                continue;
            }

            if (this.instanceId.CompareTo(other.InstanceId) < 0)
            {
                this.Hold(partition, other);
                continue;
            }

            this.Contest(partition, other);
        }
    }

    /// <summary>
    /// Whether a Static-mode overlap is still inside its grace window, and so must not be acted on
    /// yet.
    /// </summary>
    /// <param name="partition">The partition.</param>
    /// <param name="other">The record found in the field before our write.</param>
    /// <returns><see langword="true"/> to defer the verdict to a later flush.</returns>
    /// <remarks>
    /// <para>
    /// This delays a stand-down; it never cancels one. An overlap that outlives the window is judged
    /// exactly as it was before, by the same tiebreak, so a genuine second writer still ends with
    /// one side stopped and the pair cannot both keep the partition.
    /// </para>
    /// <para>
    /// What it costs is a bounded extension of a window that already exists: from the first flush
    /// that sees the overlap to the window's end, both instances write the position and messages are
    /// processed twice. That is the same at-least-once exposure the overlap itself creates, capped
    /// by the option, and it buys the far worse failure — a partition read by nobody — not happening
    /// on every rolling deploy.
    /// </para>
    /// </remarks>
    private bool WithinGrace(int partition, PositionRecord other)
    {
        if (this.contestedGrace <= TimeSpan.Zero)
        {
            return false;
        }

        var now = Stopwatch.GetTimestamp();
        var since = Interlocked.CompareExchange(ref this.contestedSince[partition], now, 0L);

        if (since == 0L)
        {
            this.log?.LogWarning(
                "Streams: partition {Partition} of topic {Topic} consumer {Consumer} carried a position written by live " +
                "instance {Other}. On a rolling deploy that is the outgoing pod and it ends when the pod does, so this " +
                "instance keeps the partition for up to {GraceSeconds}s before deciding. If it is still there after that, " +
                "the instance count really is wrong.",
                partition,
                this.topic,
                this.consumer,
                other.InstanceId.ToString("D", CultureInfo.InvariantCulture),
                (long)this.contestedGrace.TotalSeconds);

            return true;
        }

        return Stopwatch.GetElapsedTime(since, now) < this.contestedGrace;
    }

    /// <summary>
    /// Re-probes the contenders that stood partitions down, and resumes any partition whose
    /// contender has left the ownership hash.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes once every stood-down partition has been re-arbitrated.</returns>
    /// <remarks>
    /// <para>
    /// <b>The safety property.</b> The stand-down exists so that two instances never advance one
    /// stored position. The only evidence it ever had for "there are two of us" is the contender's
    /// presence field, and this asks precisely that field again. A contender whose presence field is
    /// gone has either released it on shutdown or let it lapse past its TTL without a renewal — the
    /// same condition under which <see cref="NoteStale"/> already declines to stand down in the
    /// first place. Resuming on it therefore admits no writer that a first flush arriving at this
    /// moment would not have admitted; it makes the rule "stopped while a second writer is live"
    /// instead of "stopped forever because one once was".
    /// </para>
    /// <para>
    /// Everything else fails closed. A probe that throws resumes nothing. A contender still present
    /// resumes nothing and is logged again, so an overlap that really is a misconfiguration keeps
    /// saying so instead of falling silent after its one line.
    /// </para>
    /// </remarks>
    internal async ValueTask RecheckAsync(CancellationToken ct)
    {
        var db = this.redis?.GetDatabase();
        if (db is null || this.onRecovered is null)
        {
            return;
        }

        var waiting = 0;
        for (var partition = 0; partition < this.stopped.Length; partition++)
        {
            if (Volatile.Read(ref this.stopped[partition]) != 0 && this.contenders[partition] != Guid.Empty)
            {
                this.recheckPartitions[waiting++] = partition;
            }
        }

        if (waiting == 0)
        {
            return;
        }

        RedisValue[] live;

        try
        {
            ct.ThrowIfCancellationRequested();

            var fields = new RedisValue[waiting];
            for (var i = 0; i < waiting; i++)
            {
                fields[i] = OwnershipRegistry.PresenceField(this.contenders[this.recheckPartitions[i]]);
            }

            live = await db.HashGetAsync(this.ownershipKey, fields).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Fail closed: an unanswerable question is not evidence that the contender has gone.
            this.log?.LogWarning(
                ex,
                "Streams: could not re-check whether the instance that stood a partition of topic {Topic} consumer " +
                "{Consumer} down is still live; the partition stays stopped and the next tick asks again.",
                this.topic,
                this.consumer);
            return;
        }

        for (var i = 0; i < waiting && i < live.Length; i++)
        {
            var partition = this.recheckPartitions[i];
            var contender = this.contenders[partition];

            if (!live[i].IsNull)
            {
                this.log?.LogWarning(
                    "Streams: partition {Partition} of topic {Topic} consumer {Consumer} is still stood down — instance " +
                    "{Other} holds a live presence claim and is presumed to be writing it. This instance resumes the " +
                    "partition as soon as that instance is gone.",
                    partition,
                    this.topic,
                    this.consumer,
                    contender.ToString("D", CultureInfo.InvariantCulture));
                continue;
            }

            this.Resume(partition, contender);
        }
    }

    /// <summary>Clears a stand-down whose contender has gone, and asks the host to read again.</summary>
    /// <param name="partition">The partition to resume.</param>
    /// <param name="contender">The instance it had stood down for.</param>
    private void Resume(int partition, Guid contender)
    {
        if (Interlocked.Exchange(ref this.stopped[partition], 0) == 0)
        {
            return;
        }

        this.contenders[partition] = Guid.Empty;
        Volatile.Write(ref this.contestedSince[partition], 0L);
        _ = Interlocked.Exchange(ref this.reported[partition], ReportedNothing);
        _ = Interlocked.Decrement(ref this.contestedCount);
        Interlocked.Increment(ref this.recoveries);

        if (Interlocked.Exchange(ref this.counted[partition], 0) == 1)
        {
            StreamsDiagnostics.SetContestedPartitions(this.metricKey, Interlocked.Decrement(ref this.contentionCount));
        }

        this.log?.LogWarning(
            "Streams: partition {Partition} of topic {Topic} consumer {Consumer} is resuming — instance {Other}, which it " +
            "stood down for, no longer holds a presence claim, so there is no second writer left to fight. This is the " +
            "normal end of a rolling deploy that overlapped two pods of one ordinal.",
            partition,
            this.topic,
            this.consumer,
            contender.ToString("D", CultureInfo.InvariantCulture));

        this.onRecovered?.Invoke(partition, contender);
    }

    /// <summary>
    /// Reports, once per partition, that a foreign id was found but its writer is no longer live.
    /// </summary>
    /// <param name="partition">The partition.</param>
    /// <param name="other">The record found in the field before our write.</param>
    private void NoteStale(int partition, PositionRecord other)
    {
        // Only from "nothing reported": a partition already reported as genuine contention must not
        // be downgraded to this, or the Error would be replaced by an Information the next time the
        // rival happens to look dead.
        if (Interlocked.CompareExchange(ref this.reported[partition], ReportedStale, ReportedNothing) != ReportedNothing)
        {
            return;
        }

        this.log?.LogInformation(
            "Streams: partition {Partition} of topic {Topic} consumer {Consumer} carried a position written by instance " +
            "{Other}, which holds no live ownership claim — a previous life of this consumer, or a position written by an " +
            "admin tool. Continuing to write it.",
            partition,
            this.topic,
            this.consumer,
            other.InstanceId.ToString("D", CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Reports, once per partition, that a live peer wrote a position for a partition whose
    /// ownership claim this instance holds — a late flush from before the handoff, not a rival.
    /// </summary>
    /// <param name="partition">The partition.</param>
    /// <param name="other">The record found in the field before our write.</param>
    private void NoteLateWrite(int partition, PositionRecord other)
    {
        if (Interlocked.CompareExchange(ref this.reported[partition], ReportedLateWrite, ReportedNothing) != ReportedNothing)
        {
            return;
        }

        this.log?.LogInformation(
            "Streams: partition {Partition} of topic {Topic} consumer {Consumer} carried a position written by live instance " +
            "{Other}, which no longer holds that partition's claim — this instance does. A late flush from before the " +
            "handoff, not a second writer. Continuing to write it.",
            partition,
            this.topic,
            this.consumer,
            other.InstanceId.ToString("D", CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Keeps a genuinely contested partition because this instance won the tiebreak, and says so.
    /// </summary>
    /// <param name="partition">The contested partition.</param>
    /// <param name="other">The record found in the field before our write.</param>
    /// <remarks>
    /// The overlap is still an Error — it is a real misconfiguration and the messages really are
    /// being processed twice — but somebody has to keep the partition, or the stand-down that fixes
    /// the duplication becomes an outage. Lowest id wins, which both sides compute identically, so
    /// the loser is the one that stops.
    /// </remarks>
    private void Hold(int partition, PositionRecord other)
    {
        this.CountContention(partition);

        // Upgrades a stale note to real contention, then stays quiet: a persistent overlap must not
        // log on every flush.
        if (Interlocked.Exchange(ref this.reported[partition], ReportedContention) == ReportedContention)
        {
            return;
        }

        this.log?.LogError(
            "Streams: partition {Partition} of topic {Topic} consumer {Consumer} is also being written by live instance " +
            "{Other}, so its messages are being processed twice. This instance ({Instance}) has the lower id and keeps the " +
            "partition; the other stands down. A partitioned consumer must run as a StatefulSet, not a Deployment; check " +
            "STREAMS_INSTANCE_COUNT and the pod ordinals.",
            partition,
            this.topic,
            this.consumer,
            other.InstanceId.ToString("D", CultureInfo.InvariantCulture),
            this.instanceId.ToString("D", CultureInfo.InvariantCulture));
    }

    /// <summary>Counts a partition as in contention once, for the gauge and the health check.</summary>
    /// <param name="partition">The partition.</param>
    private void CountContention(int partition)
    {
        if (Interlocked.Exchange(ref this.counted[partition], 1) != 0)
        {
            return;
        }

        StreamsDiagnostics.SetContestedPartitions(this.metricKey, Interlocked.Increment(ref this.contentionCount));
    }

    /// <summary>Latches a partition as contested, reports it, and asks the host to stop its worker.</summary>
    /// <param name="partition">The contested partition.</param>
    /// <param name="other">The record found in the field before our write.</param>
    private void Contest(int partition, PositionRecord other)
    {
        if (Interlocked.Exchange(ref this.stopped[partition], 1) != 0)
        {
            // Already stood down; do not log or count it twice.
            return;
        }

        if (Interlocked.Exchange(ref this.dirty[partition], 0) == 1)
        {
            Interlocked.Decrement(ref this.dirtyCount);
        }

        // Recorded before anything is logged: the re-arbitration reads it to know whose presence
        // field to re-probe, and a stand-down with no contender recorded can never be undone.
        this.contenders[partition] = other.InstanceId;

        this.CountContention(partition);
        _ = Interlocked.Increment(ref this.contestedCount);
        _ = Interlocked.Exchange(ref this.reported[partition], ReportedContention);

        this.log?.LogError(
            "Streams: partition {Partition} of topic {Topic} consumer {Consumer} is being written by instance {Other} as well as this instance {Instance} (their last position {OtherPosition}). Two writers move the stored position backwards and replay messages, so this instance is standing that partition down rather than fighting for it. A partitioned consumer must run as a StatefulSet, not a Deployment; check STREAMS_INSTANCE_COUNT and the pod ordinals.",
            partition,
            this.topic,
            this.consumer,
            other.InstanceId.ToString("D", CultureInfo.InvariantCulture),
            this.instanceId.ToString("D", CultureInfo.InvariantCulture),
            other.Id.Format());

        this.onContested?.Invoke(partition, this.instanceId, other.InstanceId);
    }

    /// <summary>The timer loop. Flushes only when something is dirty, so idle costs nothing.</summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when the loop stops.</returns>
    private async Task LoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(this.interval);

        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                if (Volatile.Read(ref this.dirtyCount) != 0)
                {
                    await this.FlushAsync(ct).ConfigureAwait(false);
                }

                // Deliberately outside the dirty check: an idle consumer is exactly the one an
                // operator is most likely to reset, and it would otherwise never look.
                if (this.resets is not null && ++this.ticks >= this.resetPollTicks)
                {
                    this.ticks = 0;
                    await this.PollResetsAsync(ct).ConfigureAwait(false);
                }

                // Also outside the dirty check, and for the same reason turned around: a stood-down
                // partition records nothing, so it is never dirty and would never be looked at again.
                if (this.recheckTicks > 0 && Volatile.Read(ref this.contestedCount) != 0 && --this.recheckCountdown <= 0)
                {
                    this.recheckCountdown = this.recheckTicks;
                    await this.RecheckAsync(ct).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
    }

    /// <summary>
    /// The reset-marker tick: notice markers, hand them to the read loops, and delete the ones a read
    /// loop has already acted on. Never throws.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when the poll — and any acknowledging delete — has landed.</returns>
    /// <remarks>
    /// <para>
    /// <b>Why the flusher does this and not the reader.</b> The flusher is the thing a live reset
    /// races: it is what would otherwise overwrite the target a second after the admin call wrote it.
    /// Putting the check on the same tick means the reset is noticed by the very loop that would have
    /// clobbered it, and the read path keeps a single volatile read as its whole cost.
    /// </para>
    /// <para>
    /// <b>Delete last, and only after a rewind.</b> A marker is cleared only once
    /// <see cref="ResetSignal.TryTake"/> has handed it to a read loop, so a reader parked in a
    /// blocking <c>XREAD</c>, or not running at all, cannot lose a reset — the marker simply waits.
    /// The delete is not atomic with the poll that read it: a marker written by an operator inside
    /// that round trip can be deleted along with the one it replaced. That is the same "prefer the
    /// simple thing, replay is cheap" trade the rest of this file makes, and it is why the runbook
    /// still tells operators to scale to zero for anything they care about.
    /// </para>
    /// <para>
    /// A failure leaves every slot exactly as it was, so the next tick retries: the markers are still
    /// in Redis, and re-publishing an identical marker is a no-op rather than a second rewind.
    /// </para>
    /// </remarks>
    private async ValueTask PollResetsAsync(CancellationToken ct)
    {
        var signal = this.resets;
        var db = this.redis?.GetDatabase();

        if (signal is null || db is null)
        {
            return;
        }

        try
        {
            var found = await ResetMarkers
                .PollAsync(db, this.key, this.resetPartitions, this.resetFields, this.resetBuffer, ct)
                .ConfigureAwait(false);

            for (var i = 0; i < found; i++)
            {
                ref readonly var marker = ref this.resetBuffer[i];

                if (!signal.Request(in marker))
                {
                    // Already pending, or already taken and waiting for its field to be deleted.
                    continue;
                }

                this.log?.LogWarning(
                    "Streams: reset marker found for topic {Topic} consumer {Consumer} partition {Partition}; the running worker will restart its reads after {Target} (reset issued {IssuedUtc}). Entries after that id are delivered again, so handlers must be idempotent. The safe path for a planned replay is still scale to zero, reset, scale up.",
                    this.topic,
                    this.consumer,
                    marker.Partition,
                    marker.Target.Format(),
                    marker.IssuedUtc.ToString("O", CultureInfo.InvariantCulture));
            }

            var clear = 0;

            for (var i = 0; i < this.resetPartitions.Length; i++)
            {
                if (signal.IsTaken(this.resetPartitions[i]))
                {
                    this.resetClearFields[clear++] = this.resetFields[i];
                }
            }

            if (clear == 0)
            {
                return;
            }

            await ResetMarkers.ClearAsync(db, this.key, this.resetClearFields, clear).ConfigureAwait(false);

            // Only now that the field is gone: clearing the slot first would let the next tick see the
            // same marker again and rewind the reader a second time.
            for (var i = 0; i < this.resetPartitions.Length; i++)
            {
                var partition = this.resetPartitions[i];

                if (signal.TryClearTaken(partition, out var applied))
                {
                    // Rewind() already ran on the read loop's thread when it took this reset (see
                    // Rewind's remarks) — this tick only deletes the now-safe-to-clear marker field
                    // and counts the reset as applied.
                    Interlocked.Increment(ref this.resetsApplied);

                    // Information, not Warning: the reset itself was already warned about, twice —
                    // here when it was noticed, and in the read loop when it actually rewound.
                    this.log?.LogInformation(
                        "Streams: reset marker cleared for topic {Topic} consumer {Consumer} partition {Partition}; reads restarted after {Target}.",
                        this.topic,
                        this.consumer,
                        partition,
                        applied.Target.Format());
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            this.log?.LogWarning(
                ex,
                "Streams: reset-marker poll failed for topic {Topic} consumer {Consumer}; retrying in {IntervalMs}ms. A pending reset is delayed, not lost — the marker stays in Redis until a worker acts on it.",
                this.topic,
                this.consumer,
                (long)this.interval.TotalMilliseconds * this.resetPollTicks);
        }
        catch (OperationCanceledException)
        {
            // Shutdown.
        }
    }

    /// <summary>
    /// Drops a partition's un-flushed position after its reader was rewound by a reset.
    /// </summary>
    /// <param name="partition">The partition that rewound.</param>
    /// <remarks>
    /// <para>
    /// Nothing is written here. Redis already holds the target — the admin call wrote the position and
    /// the marker together — so the only useful thing to do with a pending position recorded
    /// <em>before</em> the rewind is to forget it, rather than flush a position the operator has just
    /// overruled.
    /// </para>
    /// <para>
    /// <b>Called by the read loop, synchronously with taking the reset — not by the flusher's own
    /// poll tick.</b> The read loop applies the seek and can read and record a brand-new, correct
    /// post-reset position before the flusher's tick gets back around to noticing the take (it first
    /// awaits a Redis round trip to delete the marker field). Clearing the pending value from that
    /// later, independently-timed tick would discard a position that already reflects the replay
    /// rather than one left over from before it — silently freezing the stored position at the reset
    /// target forever, since nothing sets it dirty again once the replay has drained. Calling this at
    /// take-time, on the same thread that is about to issue the seeked read, guarantees the clear
    /// always happens before any post-seek <see cref="Record"/> rather than racing it.
    /// </para>
    /// <para>
    /// Batches already in flight when the reset landed are still processed and still recorded, so the
    /// stored position can move forward again before the replay catches up. Every entry after the
    /// target is delivered either way; what a crash inside that window costs is the remainder of the
    /// replay, which is the honest reason the runbook prefers scale-to-zero.
    /// </para>
    /// </remarks>
    internal void Rewind(int partition)
    {
        if ((uint)partition >= (uint)this.dirty.Length)
        {
            return;
        }

        if (Interlocked.Exchange(ref this.dirty[partition], 0) == 1)
        {
            Interlocked.Decrement(ref this.dirtyCount);
        }

        Volatile.Write(ref this.pendingSeq[partition], Unwritten);

        // The replay will record ids behind what this partition last recorded. That is the point of a
        // reset, not the wiring bug the monotonicity assert hunts for, so arm it to be forgiven once.
        Volatile.Write(ref this.rewound[partition], 1);
    }

    /// <summary>
    /// Debug-only guard that positions never go backwards for a partition. There is one writer per
    /// partition, so the current pending value is that writer's own last record.
    /// </summary>
    /// <param name="partition">The partition.</param>
    /// <param name="id">The id being recorded.</param>
    [Conditional("DEBUG")]
    private void AssertMovesForward(int partition, StreamId id)
    {
        var seq = Volatile.Read(ref this.pendingSeq[partition]);
        if (seq == Unwritten)
        {
            return;
        }

        if (id >= new StreamId(Volatile.Read(ref this.pendingMs[partition]), seq))
        {
            return;
        }

        // A reset rewound this partition, so the first record that lands behind the pending value is
        // the replay it asked for. Consume the token: anything backwards after that is a real bug.
        if (Interlocked.Exchange(ref this.rewound[partition], 0) != 0)
        {
            return;
        }

        Debug.Fail(
            "Streams: a partition's recorded position moved backwards; positions are monotonic per partition.");
    }
}
