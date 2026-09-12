using Microsoft.Extensions.Logging;
using RedisEvents.Errors;
using RedisEvents.Positions;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Consumer;

/// <summary>
/// The <c>UseConsumerGroup = true</c> fetch delegate: <c>XREADGROUP GROUP &lt;consumer&gt;
/// &lt;instanceId&gt; COUNT n STREAMS &lt;key&gt; &gt;</c>, with an <c>XACK</c> after each batch the
/// handler completes and <c>XAUTOCLAIM</c> recovery of entries another instance left pending.
/// </summary>
/// <remarks>
/// <para>
/// <b>This mode is off by default, deliberately.</b> It costs an extra <c>XACK</c> round trip per
/// batch and Redis-side pending-entries-list (PEL) bookkeeping per entry, and it gives up the
/// cheapest thing we have: static partition ownership plus our own coalesced position writes, where
/// a thousand processed messages cost one <c>HSET</c> per flush interval and Redis tracks nothing
/// per consumer at all. Take consumer groups only where competing consumers with claim/recovery
/// semantics are genuinely wanted — a work queue whose items must survive an instance dying
/// mid-batch and be picked up by a sibling — and not merely because Redis offers them.
/// </para>
/// <para>
/// <b>The position store is bypassed entirely in this mode.</b> Redis owns the read cursor (the
/// group's last-delivered id) and the PEL, so there is no <c>p:{topic}:{consumer}</c> hash to write,
/// no flusher tick and no start-position resolution after the first run — which also means the reset
/// family in <see cref="Positions"/> cannot move this consumer: the hash it rewrites is not read
/// here, so <see cref="SetGroupPositionAsync"/> (<c>XGROUP SETID</c>) is the reset in this mode and
/// <c>StreamAdmin</c> has to route to it. Bind the processor's
/// <see cref="PositionRecorder"/> to <see cref="NoPositions"/> and await
/// <see cref="AckAsync"/> where a non-group consumer would record a position: acknowledgement is
/// this mode's position advance, and like a position it happens on <em>processing</em> success only.
/// </para>
/// <para>
/// <b>Shape matches <see cref="PollFetch"/> line for line.</b> <see cref="FetchAsync"/> is a
/// <c>Func&lt;CancellationToken, ValueTask&lt;StreamEntryBatch&gt;&gt;</c>, so
/// <see cref="PartitionWorker"/>, the decode, the filter, the pooled batch and the channel are
/// identical; the group is a different way to get entries, not a different pipeline. Reads run on
/// the <b>shared</b> multiplexer with the same <see cref="Backoff"/> ladder as
/// <c>ReadMode.Poll</c> — the blocking form of <c>XREADGROUP</c> would need a dedicated connection
/// per the read-only reader invariant, which group mode does not carry.
/// </para>
/// <para>
/// <b>Thread safety.</b> One instance per partition, but not one thread: <see cref="FetchAsync"/>
/// runs on that partition's reader loop and <see cref="AckAsync"/> on its processor loop, and with
/// backpressure enabled (the default) those are different threads. The class doc used to claim the
/// opposite and the pending queue was an unsynchronised <see cref="Queue{T}"/> — see
/// <see cref="PendingAcks"/>, which now owns that state and its lock. Everything else here
/// (<see cref="IdleDelayMs"/>, the claim cursor) is touched by the reader loop alone.
/// </para>
/// </remarks>
internal sealed class ConsumerGroupFetch
{
    /// <summary>
    /// How long an entry must sit unacknowledged in another consumer's PEL before this instance may
    /// claim it — long enough that a slow handler is never stolen from, short enough that a pod that
    /// died mid-batch is recovered within a deploy window.
    /// </summary>
    private const int DefaultClaimMinIdleMs = 30_000;

    /// <summary>How many pending entries one <c>XAUTOCLAIM</c> sweep will take at most.</summary>
    private const int ClaimBatchLimit = 100;

    private readonly IDatabase db;
    private readonly RedisKey key;
    private readonly string groupName;
    private readonly RedisValue group;
    private readonly RedisValue instanceId;
    private readonly StartPosition start;
    private readonly int batchSize;
    private readonly int maxIdleDelayMs;
    private readonly long claimMinIdleMs;
    private readonly int claimIntervalMs;
    private readonly ILogger log;

    /// <summary>
    /// Fetched-but-unacknowledged batches, oldest first, in fetch order. Written by the reader loop
    /// and drained by the processor loop, so the synchronisation and the ack-matching rule both
    /// live in <see cref="PendingAcks"/> rather than here.
    /// </summary>
    private readonly PendingAcks pending = new();

    /// <summary>Milliseconds to wait after the next consecutive empty read; zero after any entries.</summary>
    private int idleDelayMs;

    /// <summary>Where the next <c>XAUTOCLAIM</c> sweep resumes scanning the PEL.</summary>
    private RedisValue claimCursor = ClaimStart;

    /// <summary><see cref="Environment.TickCount64"/> at the last sweep; <see cref="long.MinValue"/> until the first.</summary>
    private long lastClaimTicks = long.MinValue;

    /// <summary>The PEL scan start — <c>XAUTOCLAIM</c>'s cursor, not a stream read position.</summary>
    private static RedisValue ClaimStart => "0-0";

    /// <summary>
    /// Creates the group fetch for one partition.
    /// </summary>
    /// <param name="db">
    /// The shared multiplexer's database. Group reads are non-blocking, so unlike
    /// <c>ReadMode.Block</c> this mode needs no dedicated <see cref="StreamReaderConnection"/>.
    /// </param>
    /// <param name="key">This partition's stream key, <c>s:{topic}:&lt;partition&gt;</c>.</param>
    /// <param name="group">
    /// The consumer group name — the consumer name, so the group is the logical subscriber and its
    /// members are that subscriber's instances.
    /// </param>
    /// <param name="instanceId">
    /// This instance's name within the group. Must be stable across a restart (the pod name, per
    /// <c>InstanceResolver</c>) or a bounced pod's pending entries are orphaned until the claim
    /// sweep finds them rather than resumed directly.
    /// </param>
    /// <param name="start">
    /// The resolved start position, used to create the group and to recreate it if it is deleted
    /// underneath a running consumer.
    /// </param>
    /// <param name="batchSize">Entries requested per read (<c>COUNT</c>).</param>
    /// <param name="maxIdleDelayMs">The idle backoff cap in milliseconds (<c>MaxIdleDelayMs</c>, default 50).</param>
    /// <param name="log">Logger; claim sweeps and group recreation are reported here.</param>
    /// <param name="claimMinIdleMs">
    /// Minimum idle time before another instance's pending entry may be claimed
    /// (<see cref="DefaultClaimMinIdleMs"/>).
    /// </param>
    /// <param name="claimIntervalMs">
    /// How often the sweep may run, in milliseconds. Zero (the default) means the idle backoff cap,
    /// so recovery keeps pace with the idle read cadence and stops entirely under load.
    /// </param>
    internal ConsumerGroupFetch(
        IDatabase db,
        RedisKey key,
        string group,
        string instanceId,
        StartPosition start,
        int batchSize,
        int maxIdleDelayMs,
        ILogger log,
        int claimMinIdleMs = DefaultClaimMinIdleMs,
        int claimIntervalMs = 0)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(claimMinIdleMs);
        ArgumentOutOfRangeException.ThrowIfNegative(claimIntervalMs);

        this.db = db;
        this.key = key;
        this.groupName = group;
        this.group = group;
        this.instanceId = instanceId;
        this.start = start;
        this.batchSize = batchSize;
        this.maxIdleDelayMs = maxIdleDelayMs;
        this.claimMinIdleMs = claimMinIdleMs;
        this.claimIntervalMs = claimIntervalMs > 0 ? claimIntervalMs : Math.Max(maxIdleDelayMs, 1);
        this.log = log;
    }

    /// <summary>The delay, in milliseconds, that the next consecutive empty read will wait.</summary>
    internal int IdleDelayMs => this.idleDelayMs;

    /// <summary>
    /// Batches this instance has read but not yet acknowledged. Bounded in practice by the channel
    /// capacity, since the reader stops fetching once the channel is full.
    /// </summary>
    internal int PendingBatches => this.pending.Count;

    /// <summary>The pending queue, for the tests that assert on ack ordering directly.</summary>
    internal PendingAcks Pending => this.pending;

    /// <summary>
    /// The <see cref="PositionRecorder"/> to bind in group mode: positions are Redis's job here, so
    /// recording one would write a hash nothing ever reads and, worse, imply a second source of
    /// truth for the read cursor.
    /// </summary>
    /// <param name="partition">Ignored.</param>
    /// <param name="id">Ignored.</param>
    internal static void NoPositions(int partition, StreamId id)
    {
        _ = partition;
        _ = id;
    }

    /// <summary>
    /// Creates the consumer group at the resolved start position, tolerating a group that already
    /// exists.
    /// </summary>
    /// <remarks>
    /// <c>MKSTREAM</c> so a consumer may start before anything has ever been published to the
    /// partition — without it, <c>XGROUP CREATE</c> fails on a missing key and a cold topic could
    /// never be subscribed to. <c>BUSYGROUP</c> is the normal case on every run after the first, and
    /// on every instance but whichever one wins the race at startup; the position the group was
    /// created at is <em>not</em> reapplied, because the group's cursor is authoritative once it
    /// exists and rewinding it under a live sibling would redeliver its in-flight work.
    /// </remarks>
    /// <param name="db">The shared multiplexer's database.</param>
    /// <param name="key">The partition's stream key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="start">The resolved start position; <c>$</c> only for a <c>StartFrom.Now</c> consumer.</param>
    /// <param name="log">Logger.</param>
    /// <param name="ct">Observed before the call.</param>
    /// <returns><see langword="true"/> when this call created the group, <see langword="false"/> when it already existed.</returns>
    /// <exception cref="StreamTransportException">Redis refused the create for any reason other than <c>BUSYGROUP</c>.</exception>
    private static async ValueTask<bool> CreateGroupAsync(
        IDatabase db,
        RedisKey key,
        string group,
        StartPosition start,
        ILogger log,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        ArgumentNullException.ThrowIfNull(log);
        ct.ThrowIfCancellationRequested();

        // ReadFrom(null) is the first-read form: "$" for StartFrom.Now, a concrete id otherwise —
        // exactly the "last delivered id" XGROUP CREATE wants, since both are exclusive.
        var position = start.ReadFrom(lastRead: null);

        try
        {
            await db.StreamCreateConsumerGroupAsync(key, group, position, createStream: true)
                .ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.Ordinal))
        {
            log.LogDebug(
                "Streams: consumer group '{Group}' already exists on {Key}; keeping its current position.",
                group,
                (string?)key);
            return false;
        }
        catch (RedisException ex)
        {
            throw new StreamTransportException(
                $"Failed to create consumer group '{group}' on stream '{(string?)key}' at position '{position}'.",
                ex);
        }

        log.LogInformation(
            "Streams: created consumer group '{Group}' on {Key} at {Position}.",
            group,
            (string?)key,
            (string?)position);

        return true;
    }

    /// <summary>
    /// Ensures this instance's group exists, at the start position it was constructed with.
    /// </summary>
    /// <param name="ct">Observed before the call.</param>
    /// <returns><see langword="true"/> when this call created the group.</returns>
    internal ValueTask<bool> EnsureGroupAsync(CancellationToken ct)
        => CreateGroupAsync(this.db, this.key, this.groupName, this.start, this.log, ct);

    /// <summary>
    /// Group mode's reset: <c>XGROUP SETID</c> moves the group's last-delivered id, and — unless
    /// <paramref name="purgePending"/> says otherwise — every consumer of the group is deleted so
    /// its pending-entries list goes with it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why this exists.</b> R-07(c): in group mode Redis owns the read cursor, so there is no
    /// <c>p:{topic}:{consumer}</c> hash for <c>StreamAdmin</c>'s reset family to rewrite and no
    /// flusher to hand a marker to. A reset issued against a group consumer therefore wrote a hash
    /// nobody reads and reported success while changing nothing. <c>XGROUP SETID</c> is the only
    /// thing that actually moves a group.
    /// </para>
    /// <para>
    /// <b>Run it with the consumers stopped</b> — the same scale-to-zero rule the README gives for
    /// the non-group reset, and here it is not advice. A live member holds entries in its PEL that
    /// <c>SETID</c> does not touch, so a "skip to the end" reset would still see them redelivered
    /// through the claim sweep; and a member that fetched before the reset lands acknowledges
    /// against the old cursor afterwards. Deleting the consumers (<paramref name="purgePending"/>)
    /// removes the PEL half of that, but it cannot stop a running member from re-registering
    /// itself on its next read.
    /// </para>
    /// <para>
    /// The group is created at <paramref name="lastDelivered"/> when it does not exist, so a reset
    /// issued before the consumer has ever run behaves like one issued after it.
    /// </para>
    /// </remarks>
    /// <param name="db">The shared multiplexer's database.</param>
    /// <param name="key">The partition's stream key.</param>
    /// <param name="group">The consumer group name.</param>
    /// <param name="lastDelivered">
    /// The id to treat as already delivered; reading resumes at the entry immediately after it.
    /// <see cref="StreamId.Min"/> (<c>0-0</c>) replays the whole retained stream.
    /// </param>
    /// <param name="log">Logger; the reset is recorded at Warning, as the non-group reset is.</param>
    /// <param name="purgePending">
    /// Whether to delete the group's consumers, and with them the entries pending in their PELs.
    /// </param>
    /// <param name="ct">Observed before the call.</param>
    /// <exception cref="StreamTransportException">Redis refused the reset.</exception>
    internal static async ValueTask SetGroupPositionAsync(
        IDatabase db,
        RedisKey key,
        string group,
        StreamId lastDelivered,
        ILogger log,
        bool purgePending,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        ArgumentNullException.ThrowIfNull(log);
        ct.ThrowIfCancellationRequested();

        var position = (RedisValue)lastDelivered.Format();

        try
        {
            await db.StreamConsumerGroupSetPositionAsync(key, group, position, CommandFlags.DemandMaster)
                .ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("NOGROUP", StringComparison.Ordinal))
        {
            // Nothing to move yet. Creating it at the target is the same end state, and it makes a
            // reset issued against a consumer that has not started yet mean what the operator meant.
            await db.StreamCreateConsumerGroupAsync(key, group, position, createStream: true)
                .ConfigureAwait(false);
        }
        catch (RedisException ex)
        {
            throw new StreamTransportException(
                $"Failed to move consumer group '{group}' on stream '{(string?)key}' to position '{position}'.",
                ex);
        }

        var purged = purgePending
            ? await PurgeConsumersAsync(db, key, group, ct).ConfigureAwait(false)
            : 0;

        log.LogWarning(
            "Streams: consumer group '{Group}' on {Key} was reset to {Position}; {Purged} consumer(s) deleted with " +
            "their pending entries. Entries after that position will be redelivered.",
            group,
            (string?)key,
            (string?)position,
            purged);
    }

    /// <summary>
    /// Deletes every consumer of a group, dropping the entries pending in their lists.
    /// </summary>
    /// <remarks>
    /// A failure here is logged and swallowed: the cursor has already moved, which is the part the
    /// operator asked for, and leftover pending entries are redelivered rather than lost.
    /// </remarks>
    /// <returns>How many consumers were deleted.</returns>
    private static async ValueTask<int> PurgeConsumersAsync(
        IDatabase db,
        RedisKey key,
        string group,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        try
        {
            var pending = await db.StreamPendingAsync(key, group, CommandFlags.DemandMaster)
                .ConfigureAwait(false);

            var consumers = pending.Consumers;
            var purged = 0;

            for (var i = 0; i < consumers.Length; i++)
            {
                await db.StreamDeleteConsumerAsync(key, group, consumers[i].Name, CommandFlags.DemandMaster)
                    .ConfigureAwait(false);
                purged++;
            }

            return purged;
        }
        catch (RedisException)
        {
            return 0;
        }
    }

    /// <summary>
    /// Moves this instance's group to <paramref name="lastDelivered"/> and forgets whatever it was
    /// holding unacknowledged, since those ids belong to the cursor that was just replaced.
    /// </summary>
    /// <param name="lastDelivered">The id to treat as already delivered.</param>
    /// <param name="purgePending">Whether to delete the group's consumers and their pending entries.</param>
    /// <param name="ct">Observed before the call.</param>
    internal async ValueTask ResetToAsync(StreamId lastDelivered, bool purgePending, CancellationToken ct)
    {
        await SetGroupPositionAsync(this.db, this.key, this.groupName, lastDelivered, this.log, purgePending, ct)
            .ConfigureAwait(false);

        this.pending.Clear();
    }

    /// <summary>
    /// One group read: <c>XREADGROUP GROUP &lt;group&gt; &lt;instanceId&gt; COUNT &lt;batchSize&gt;
    /// STREAMS &lt;key&gt; &gt;</c>. When nothing new is delivered, a claim sweep runs (at most once
    /// per claim interval, and only once the backoff has reached its cap) and its entries are
    /// returned as an ordinary batch; otherwise the current backoff step is waited out and an empty
    /// batch is returned.
    /// </summary>
    /// <param name="ct">The linked host token. Observed before the read and during any idle wait.</param>
    /// <returns>The fetched entries, or <see cref="StreamEntryBatch.Empty"/>.</returns>
    /// <exception cref="OperationCanceledException">The token was cancelled.</exception>
    internal async ValueTask<StreamEntryBatch> FetchAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        StreamEntry[]? entries;

        try
        {
            // NewMessages is ">" for XREADGROUP: undelivered entries only, which is what puts them
            // in this consumer's PEL. DemandMaster because a replica's view of the group is stale.
            entries = await this.db
                .StreamReadGroupAsync(
                    this.key,
                    this.group,
                    this.instanceId,
                    StreamPosition.NewMessages,
                    this.batchSize,
                    noAck: false,
                    CommandFlags.DemandMaster)
                .ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("NOGROUP", StringComparison.Ordinal))
        {
            // The group (or the whole key) went away underneath us — a manual XGROUP DESTROY, a
            // flushed database, or a key that expired. Recreate and let the next read proceed
            // rather than tearing the worker down over something self-healing.
            this.log.LogWarning(
                "Streams: consumer group '{Group}' is missing on {Key}; recreating it at the configured start position.",
                this.groupName,
                (string?)this.key);

            // Whatever we were holding referred to the PEL of a group that no longer exists, so
            // acknowledging it later would be a no-op against ids Redis has never heard of — and if
            // the group came back at "$" nothing would ever match those batches and they would sit
            // in the queue for good. Drop them: the entries themselves are either redelivered by
            // the recreated group or gone with it, and an in-flight batch's ack simply finds no
            // match and does nothing.
            await this.EnsureGroupAsync(ct).ConfigureAwait(false);
            this.pending.Clear();
            entries = null;
        }

        if (entries is { Length: > 0 })
        {
            this.Track(entries);
            this.idleDelayMs = 0;
            return new StreamEntryBatch(entries);
        }

        // Recovery runs only on the idle path and only once the ladder is at its cap, so a consumer
        // that is keeping up never pays for it — under load there is nothing to recover anyway,
        // because a live sibling's entries have not been idle long enough to be claimable.
        if (this.idleDelayMs >= this.maxIdleDelayMs && this.DueForClaim())
        {
            var claimed = await this.ClaimAsync(ct).ConfigureAwait(false);

            if (claimed is { Length: > 0 })
            {
                this.Track(claimed);
                this.idleDelayMs = 0;
                return new StreamEntryBatch(claimed);
            }
        }

        var delay = this.idleDelayMs;
        this.idleDelayMs = Backoff.Next(delay, this.maxIdleDelayMs);

        if (delay <= 0)
        {
            // Delay(0) semantics, spelled as a yield so it is guaranteed to give the scheduler a
            // turn rather than completing synchronously and spinning the loop on this thread.
            await Task.Yield();
        }
        else
        {
            await Task.Delay(delay, ct).ConfigureAwait(false);
        }

        return StreamEntryBatch.Empty;
    }

    /// <summary>
    /// Acknowledges the batch the handler just completed — the one whose id range contains
    /// <paramref name="processedThrough"/> — together with any older, still-unacknowledged batch
    /// ahead of it in fetch order: one <c>XACK</c>, issued after the handler succeeded.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the group-mode equivalent of recording a position, and carries the same rule: call it
    /// on handler success only. An entry that is never acknowledged stays in this consumer's PEL and
    /// is redelivered — to this instance on restart, or to a sibling through
    /// <c>XAUTOCLAIM</c> — which is the at-least-once guarantee the mode exists for.
    /// </para>
    /// <para>
    /// Filtered-out entries are acknowledged along with the rest of their batch: the ids come from
    /// the fetch, not from the surviving messages, so a topic where the consumer cares about 1&#160;%
    /// of traffic does not leave 99&#160;% of it pending forever.
    /// </para>
    /// <para>
    /// <b>Matching is by fetch order, never by id order</b> — see <see cref="PendingAcks"/>. An id
    /// that belongs to no pending batch acknowledges nothing at all rather than guessing.
    /// </para>
    /// <para>
    /// A failed <c>XACK</c> takes its ids with it: they are already out of the queue, so this
    /// instance will not retry them. That is the safe direction — the entries are still in the PEL
    /// precisely because the acknowledgement did not land, so they are redelivered on restart or
    /// recovered by a sibling's claim sweep, which is the at-least-once guarantee this mode exists
    /// for. Re-queuing them would only add a second delivery.
    /// </para>
    /// </remarks>
    /// <param name="processedThrough">The last id the handler completed, i.e. the batch's last id.</param>
    /// <param name="ct">Observed before the call.</param>
    /// <exception cref="OperationCanceledException">The token was cancelled.</exception>
    /// <exception cref="StreamTransportException">Redis refused the acknowledgement.</exception>
    internal async ValueTask AckAsync(StreamId processedThrough, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var ids = this.pending.Take(processedThrough);

        if (ids is null)
        {
            return;
        }

        try
        {
            await this.db.StreamAcknowledgeAsync(this.key, this.group, ids, CommandFlags.DemandMaster)
                .ConfigureAwait(false);
        }
        catch (RedisException ex)
        {
            throw new StreamTransportException(
                $"Failed to acknowledge {ids.Length} entries for consumer group '{this.groupName}' on stream '{(string?)this.key}'.",
                ex);
        }
    }

    /// <summary>
    /// One <c>XAUTOCLAIM</c> sweep: takes entries pending past <c>claimMinIdleMs</c> from whichever
    /// consumer holds them and moves them into this instance's PEL.
    /// </summary>
    /// <remarks>
    /// A failed sweep is logged and swallowed. Recovery is opportunistic — the entries stay pending
    /// and the next sweep tries again — and tearing down a healthy reader because a recovery scan
    /// failed would turn a transient error into an outage.
    /// </remarks>
    private async ValueTask<StreamEntry[]?> ClaimAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        this.lastClaimTicks = Environment.TickCount64;

        StreamAutoClaimResult result;

        try
        {
            result = await this.db
                .StreamAutoClaimAsync(
                    this.key,
                    this.group,
                    this.instanceId,
                    this.claimMinIdleMs,
                    this.claimCursor,
                    ClaimBatchLimit,
                    CommandFlags.DemandMaster)
                .ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("NOGROUP", StringComparison.Ordinal))
        {
            // Same self-healing path as the read: recreate and try again on the next sweep.
            await this.EnsureGroupAsync(ct).ConfigureAwait(false);
            return null;
        }
        catch (RedisException ex)
        {
            this.log.LogWarning(
                ex,
                "Streams: XAUTOCLAIM sweep failed for consumer group '{Group}' on {Key}; entries stay pending and the next sweep retries.",
                this.groupName,
                (string?)this.key);
            return null;
        }

        if (result.IsNull)
        {
            this.claimCursor = ClaimStart;
            return null;
        }

        // NextStartId walks the PEL; "0-0" means the scan wrapped, so the next sweep starts over.
        this.claimCursor = result.NextStartId.IsNullOrEmpty ? ClaimStart : result.NextStartId;

        var claimed = result.ClaimedEntries;

        if (claimed is not { Length: > 0 })
        {
            return null;
        }

        this.log.LogInformation(
            "Streams: claimed {Count} entries idle over {IdleMs}ms for consumer group '{Group}' on {Key}.",
            claimed.Length,
            this.claimMinIdleMs,
            this.groupName,
            (string?)this.key);

        return claimed;
    }

    /// <summary>Whether a claim sweep is due, by the wall clock rather than by loop iterations.</summary>
    private bool DueForClaim()
    {
        var now = Environment.TickCount64;

        return this.lastClaimTicks == long.MinValue || now - this.lastClaimTicks >= this.claimIntervalMs;
    }

    /// <summary>
    /// Records a fetch's ids so the batch can be acknowledged once the handler has processed it.
    /// </summary>
    /// <remarks>
    /// One exactly-sized <see cref="RedisValue"/> array per fetch, because that is the shape
    /// <c>XACK</c> takes — a pooled array cannot be handed to it without a copy, so pooling would
    /// only move the allocation. It is per batch, never per message, and it is part of the price of
    /// the mode.
    /// </remarks>
    private void Track(StreamEntry[] entries)
    {
        var ids = new RedisValue[entries.Length];

        for (var i = 0; i < entries.Length; i++)
        {
            ids[i] = entries[i].Id;
        }

        // Two parses per batch, never per message: the first and last ids bound the range the
        // processor's reported position must fall inside for this batch to be the one it just
        // handled. An id we cannot parse yields an EMPTY range (First > Last), which matches
        // nothing — the batch is then never acknowledged and its entries are redelivered, which is
        // the safe direction. (The read loop throws on such an id anyway, so this is belt and
        // braces rather than a live path.)
        var parsedFirst = StreamId.TryParse(((string?)entries[0].Id).AsSpan(), out var first);
        var parsedLast = StreamId.TryParse(((string?)entries[^1].Id).AsSpan(), out var last);

        if (!parsedFirst || !parsedLast)
        {
            first = StreamId.Max;
            last = StreamId.Min;
        }

        var dropped = this.pending.Add(ids, first, last);

        if (dropped > 0)
        {
            this.log.LogWarning(
                "Streams: consumer group '{Group}' on {Key} is holding more than {Limit} unacknowledged batches; " +
                "dropped the oldest {Dropped} from the ack queue. Those entries stay in this consumer's PEL and are " +
                "recovered by the claim sweep or on restart, but a queue this deep means acknowledgements are not " +
                "reaching this instance — check that the processing loop is running.",
                this.groupName,
                (string?)this.key,
                PendingAcks.MaxPendingBatches,
                dropped);
        }
    }

}


/// <summary>
/// The fetched-but-unacknowledged batches of one <see cref="ConsumerGroupFetch"/>, in fetch order,
/// with the rule that decides which of them one reported position acknowledges.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why it owns a lock (R-07a).</b> Batches are enqueued by the partition's reader loop, inside
/// <see cref="ConsumerGroupFetch.FetchAsync"/>, and dequeued by its processor loop, inside
/// <see cref="ConsumerGroupFetch.AckAsync"/>. With backpressure enabled — the default, and the only
/// shape the channel pipeline has — those are two different threads, whatever the class doc used to
/// say. A bare <see cref="Queue{T}"/> torn between them loses ids, hands the same ids to two
/// <c>XACK</c>s, or throws out of the reader loop and kills the partition. The lock is taken once
/// per <em>batch</em> and never per message; in the steady state it is uncontended, so a lock-free
/// scheme would buy nothing here that is worth the difficulty of arguing it correct.
/// </para>
/// <para>
/// <b>Why matching is by range and not by "at or below" (R-07b).</b> Pending ids are <em>not</em>
/// monotone across batches: an <c>XAUTOCLAIM</c> sweep returns entries older than the live batch
/// fetched before it, so the queue routinely holds <c>[live 500-0..510-0]</c> followed by
/// <c>[claimed 5-0..8-0]</c>. The old rule — pop every batch whose last id is at or below the id
/// the handler reported — acknowledged the claimed batch as soon as the live one was reported, i.e.
/// <b>before the handler had ever seen it</b>: the entries left the PEL, and dying before they were
/// processed lost them silently. Fetch order is the order that actually holds (one fetch is one
/// channel item is one handler call, and the channel preserves order), so the batch just handled is
/// found by scanning from the head for the one whose id range <em>contains</em> the reported id.
/// Only that batch and any still-unacknowledged batches ahead of it — which the processor handled
/// earlier, by construction — are acknowledged. An id inside no pending batch acknowledges nothing.
/// </para>
/// </remarks>
internal sealed class PendingAcks
{
    /// <summary>
    /// The most batches one partition will hold unacknowledged before the oldest are dropped.
    /// </summary>
    /// <remarks>
    /// In a healthy pipeline the depth is bounded by the channel capacity, because the reader stops
    /// fetching once the channel is full. This is the guard for the unhealthy case — acknowledgements
    /// that never match — where the queue would otherwise grow without limit. Dropping an entry
    /// loses no message: the ids stay in this consumer's PEL and come back through the claim sweep
    /// or on restart.
    /// </remarks>
    internal const int MaxPendingBatches = 1024;

    private readonly Lock gate = new();
    private readonly Queue<PendingAck> queue = new();

    /// <summary>How many fetched batches are waiting to be acknowledged.</summary>
    internal int Count
    {
        get
        {
            lock (this.gate)
            {
                return this.queue.Count;
            }
        }
    }

    /// <summary>Records one fetch's ids at the tail of the queue.</summary>
    /// <param name="ids">The entry ids, exactly as Redis returned them — ready for <c>XACK</c>.</param>
    /// <param name="first">The batch's first id.</param>
    /// <param name="last">The batch's last id. <paramref name="first"/> above it means "matches nothing".</param>
    /// <returns>How many batches were dropped to stay under <see cref="MaxPendingBatches"/>; normally zero.</returns>
    internal int Add(RedisValue[] ids, StreamId first, StreamId last)
    {
        ArgumentNullException.ThrowIfNull(ids);

        lock (this.gate)
        {
            this.queue.Enqueue(new PendingAck(ids, first, last));

            var dropped = 0;

            while (this.queue.Count > MaxPendingBatches)
            {
                this.queue.Dequeue();
                dropped++;
            }

            return dropped;
        }
    }

    /// <summary>
    /// Removes and returns the ids to acknowledge for a handler that reported
    /// <paramref name="processedThrough"/>, or <see langword="null"/> when that id belongs to no
    /// pending batch or to one the handler has not finished.
    /// </summary>
    /// <param name="processedThrough">The last id the handler completed.</param>
    internal RedisValue[]? Take(StreamId processedThrough)
    {
        lock (this.gate)
        {
            var take = this.CountAcknowledgeable(processedThrough);

            if (take == 0)
            {
                return null;
            }

            var head = this.queue.Dequeue();

            if (take == 1)
            {
                // The steady-state case — one fetch, one handler call, one ack — and it hands
                // XACK the fetch's own array with no copy.
                return head.Ids;
            }

            var total = head.Ids.Length;
            var extra = new RedisValue[take - 1][];

            for (var i = 0; i < take - 1; i++)
            {
                var segment = this.queue.Dequeue();
                extra[i] = segment.Ids;
                total += segment.Ids.Length;
            }

            var combined = new RedisValue[total];
            head.Ids.CopyTo(combined, 0);
            var offset = head.Ids.Length;

            for (var i = 0; i < extra.Length; i++)
            {
                extra[i].CopyTo(combined, offset);
                offset += extra[i].Length;
            }

            return combined;
        }
    }

    /// <summary>Forgets every pending batch — the group they belong to is gone or has been reset.</summary>
    internal void Clear()
    {
        lock (this.gate)
        {
            this.queue.Clear();
        }
    }

    /// <summary>
    /// How many batches from the head are acknowledged by <paramref name="processedThrough"/>.
    /// </summary>
    /// <remarks>
    /// Zero has two meanings and both are "do nothing": the id belongs to no pending batch (a
    /// duplicate flush after the batch was already acknowledged, or an ack whose <c>XACK</c> threw
    /// and took its ids with it), or it falls inside the head batch without reaching its end, which
    /// is per-message progress through a batch that is not finished yet.
    /// </remarks>
    private int CountAcknowledgeable(StreamId processedThrough)
    {
        var index = 0;

        // Queue&lt;T&gt;'s enumerator is a struct, so this walk allocates nothing; it is per batch,
        // and in the steady state it stops on the first item.
        foreach (var batch in this.queue)
        {
            if (processedThrough >= batch.First && processedThrough <= batch.Last)
            {
                return processedThrough == batch.Last ? index + 1 : index;
            }

            index++;
        }

        return 0;
    }

    /// <summary>One fetch's worth of ids, waiting for its batch to be processed.</summary>
    /// <param name="Ids">The entry ids, exactly as Redis returned them — ready for <c>XACK</c>.</param>
    /// <param name="First">The first id in the batch.</param>
    /// <param name="Last">The last id in the batch, the value the processor reports on success.</param>
    private readonly record struct PendingAck(RedisValue[] Ids, StreamId First, StreamId Last);
}
