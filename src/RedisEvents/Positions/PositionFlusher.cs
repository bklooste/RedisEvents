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
    private int ticks;

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
        TimeSpan? resetPollInterval = null)
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
        this.buffer = new (int, StreamId)[partitionCount];

        Array.Fill(this.pendingSeq, Unwritten);

        // Marker polling needs both a signal (which partitions to poll, and who to hand a reset to)
        // and the multiplexer (the hash is ours to read only when the store is the Redis one).
        this.resets = redis is null ? null : resets;
        this.resetPartitions = this.resets is null ? [] : this.resets.Partitions.ToArray();
        this.resetFields = this.resets is null ? [] : ResetMarkers.Fields(this.resetPartitions);
        this.resetClearFields = new RedisValue[this.resetPartitions.Length];
        this.resetBuffer = new ResetMarker[this.resetPartitions.Length];

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

            var presence = new RedisValue[candidates];
            for (var i = 0; i < candidates; i++)
            {
                presence[i] = OwnershipRegistry.PresenceField(this.foreignRecords[i].InstanceId);
            }

            live = await db.HashGetAsync(this.ownershipKey, presence).ConfigureAwait(false);
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

        for (var i = 0; i < candidates && i < live.Length; i++)
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

            if (this.instanceId.CompareTo(other.InstanceId) < 0)
            {
                this.Hold(partition, other);
                continue;
            }

            this.Contest(partition, other);
        }
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
                    this.Rewind(partition);
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
    /// Batches already in flight when the reset landed are still processed and still recorded, so the
    /// stored position can move forward again before the replay catches up. Every entry after the
    /// target is delivered either way; what a crash inside that window costs is the remainder of the
    /// replay, which is the honest reason the runbook prefers scale-to-zero.
    /// </para>
    /// </remarks>
    private void Rewind(int partition)
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
