using Microsoft.Extensions.Logging;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Positions;
using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Consumer;

/// <content>
/// Inline (no-backpressure) consumption: <c>Backpressure.Enabled = false</c>.
/// <para>
/// The channel between the reader and the processor is removed entirely and the read loop calls the
/// handler itself, in line, before issuing the next fetch. Simpler and lower memory — no channel, no
/// second long-running task, at most one batch of messages resident per partition — at the cost of
/// overlap: while the handler runs, nothing is being read, so throughput falls to
/// <c>read + process</c> rather than <c>max(read, process)</c>. The default is the channel, per the
/// requirement; this is for consumers whose handler is trivially fast, or where the extra 4 ×
/// <c>BatchSize</c> messages in flight are not wanted.
/// </para>
/// <para>
/// This is deliberately <em>not</em> a second pipeline. The only difference from the channel path is
/// whether a channel sits between the fetch and the handle: everything downstream of the fetch —
/// <see cref="BuildBatch"/> (decode, filter, pooled array, position-past-filtered-entries), the
/// activity and correlation scope, the handler invocation, the position advance, the error policy
/// and the pool return — is the same <c>ProcessBatchAsync</c> the channel processor calls. A change
/// to error handling or position semantics lands in both modes at once because there is only one
/// copy of it.
/// </para>
/// </content>
internal static partial class PartitionWorker
{
    /// <summary>
    /// Runs one partition with no channel: fetch, decode, filter, handle, advance — repeat.
    /// </summary>
    /// <param name="ctx">
    /// The partition's context. Its <see cref="PartitionContext.Writer"/> must be
    /// <see langword="null"/>: a writer here means the caller built a channel and then chose the
    /// mode that does not read from it, which would look healthy while consuming nothing.
    /// </param>
    /// <param name="from">The resolved start position, for logging; the fetch owns the read cursor.</param>
    /// <param name="fetch">The fetch seam, exactly as in the channel path.</param>
    /// <param name="handler">The batch handler. The memory it receives is valid only for the call.</param>
    /// <param name="positions">Called after a batch is handled successfully. Never at read time.</param>
    /// <param name="ct">The linked host token.</param>
    /// <param name="hopToThreadPool">
    /// Forces the handler onto the <see cref="ThreadPool"/> with a <see cref="Task.Run(Func{Task})"/>
    /// per batch instead of running it on the calling thread. Set this whenever the fetch is
    /// <c>ReadMode.Block</c>, whose dedicated reader thread must never run application code: a
    /// handler executing there stalls every read for that consumer and defeats the point of
    /// dedicating the thread. Cheaper to hop than to explain the stall. The consumer host owns the
    /// decision and logs it at Information; this parameter is how it is expressed.
    /// </param>
    /// <param name="persist">Position-persistence mode, as in the channel path.</param>
    /// <param name="flush">The synchronous flush required by the <c>Sync*</c> persist modes.</param>
    /// <param name="resets">Live reset hand-off, or <see langword="null"/>.</param>
    /// <param name="seek">Moves the fetch's read cursor for a live reset, or <see langword="null"/>.</param>
    internal static Task RunInlineAsync(
        in PartitionContext ctx,
        StreamId from,
        Func<CancellationToken, ValueTask<StreamEntryBatch>> fetch,
        Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> handler,
        PositionRecorder positions,
        CancellationToken ct,
        bool hopToThreadPool = false,
        PersistMode persist = PersistMode.AsyncBatch,
        PositionFlush? flush = null,
        ResetSignal? resets = null,
        ResetSeek? seek = null)
    {
        // Copied out of the `in` parameter so the loop's state machine can hold it.
        var local = ctx;

        return ReadInlineLoopAsync(
            local, from, fetch, handler, positions, ct, hopToThreadPool, persist, flush, resets, seek);
    }

    /// <summary>
    /// The single-partition inline loop. Mirrors <c>ReadLoopAsync</c> line for line, with
    /// <c>ProcessBatchAsync</c> where the channel write would be.
    /// </summary>
    internal static async Task ReadInlineLoopAsync(
        PartitionContext ctx,
        StreamId from,
        Func<CancellationToken, ValueTask<StreamEntryBatch>> fetch,
        Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> handler,
        PositionRecorder positions,
        CancellationToken ct,
        bool hopToThreadPool = false,
        PersistMode persist = PersistMode.AsyncBatch,
        PositionFlush? flush = null,
        ResetSignal? resets = null,
        ResetSeek? seek = null)
    {
        ArgumentNullException.ThrowIfNull(fetch);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(positions);

        GuardInline(in ctx, persist, flush);

        // Startup work, not per batch — the same call the channel reader makes.
        var filter = TypeFilter.Create(ctx.Filter);

        ctx.Log.LogDebug(
            "Streams: inline read loop started for topic={Topic} partition={Partition} consumer={Consumer} from={From} hop={Hop}. " +
            "Backpressure is disabled, so reads and handler execution do not overlap.",
            ctx.Topic,
            ctx.Partition,
            ctx.Consumer,
            from.Format(),
            hopToThreadPool);

        ctx.Monitor?.MarkRunning();

        // The backoff of the current transport outage, or zero when the connection is healthy.
        var outage = 0;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (resets is not null && seek is not null && resets.HasPending)
                {
                    ApplyReset(in ctx, resets, seek);
                }

                StreamEntryBatch raw;

                try
                {
                    raw = await fetch(ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (IsTransientTransportFailure(ex) && !ct.IsCancellationRequested)
                {
                    outage = await WaitOutTransportFailureAsync(ctx, ex, outage, ct).ConfigureAwait(false);
                    continue;
                }

                outage = ResumeAfterOutage(in ctx, outage);

                if (raw.IsEmpty)
                {
                    ctx.Monitor?.MarkCaughtUp();
                    continue;
                }

                var batch = BuildBatch(in ctx, in filter, raw.Span);

                // Where the channel write would be. The next fetch is not issued until the handler
                // has finished, which is the whole of what "no backpressure" costs: there is no
                // queue to back up, because there is no queue.
                var outcome = await HandleInlineAsync(
                    ctx, batch, handler, positions, persist, flush, hopToThreadPool, ct).ConfigureAwait(false);

                if (outcome == BatchOutcome.Stop)
                {
                    return;
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown, not a fault. Nothing to complete: there is no channel and no processor task
            // waiting on one, so the loop returning is the whole of the drain.
        }
        finally
        {
            ctx.Log.LogDebug(
                "Streams: inline read loop stopped for topic={Topic} partition={Partition} consumer={Consumer}.",
                ctx.Topic,
                ctx.Partition,
                ctx.Consumer);
        }
    }

    /// <summary>
    /// The co-located inline loop: one multi-stream fetch across every partition the worker owns,
    /// each reply slot decoded and handled in place. The channel-mode counterpart is
    /// <c>ReadGroupLoopAsync</c> plus one processor task per partition; here there are none.
    /// </summary>
    /// <remarks>
    /// Ordering within a partition is preserved, as ever. What inline mode gives up here is the
    /// independence between co-located partitions: a slow handler on partition 3 delays the next
    /// fetch for partitions 0-2 as well, because there is one loop. With the channel, each partition
    /// has its own processor and only its own channel fills. That is the trade being made, and it is
    /// why the default stays enabled.
    /// </remarks>
    internal static async Task ReadGroupInlineLoopAsync(
        PartitionContext[] partitions,
        StreamId[] from,
        MultiStreamFetch fetch,
        Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> handler,
        PositionRecorder positions,
        CancellationToken ct,
        bool hopToThreadPool = false,
        PersistMode persist = PersistMode.AsyncBatch,
        PositionFlush? flush = null,
        ResetSignal? resets = null)
    {
        ArgumentNullException.ThrowIfNull(partitions);
        ArgumentNullException.ThrowIfNull(from);
        ArgumentNullException.ThrowIfNull(fetch);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(positions);

        if (partitions.Length == 0)
        {
            throw new ArgumentException("A co-located read loop needs at least one partition.", nameof(partitions));
        }

        if (from.Length != partitions.Length)
        {
            throw new ArgumentException(
                $"The start-position array holds {from.Length} ids for {partitions.Length} partitions; they must be index-aligned.",
                nameof(from));
        }

        var count = partitions.Length;

        // Allocated once; nothing below allocates per round except the id string of a partition that
        // actually advanced.
        var filters = new TypeFilter[count];
        var cursors = new StreamPosition[count];

        // Which partitions the round's reply carried entries for; the rest are at their tail and must
        // be marked caught up, or lag.ms on a quiet partition climbs off the age of its last entry
        // forever. Allocated once and cleared per round, never per message.
        var delivered = new bool[count];

        for (var i = 0; i < count; i++)
        {
            GuardInline(in partitions[i], persist, flush);
            filters[i] = TypeFilter.Create(partitions[i].Filter);
            cursors[i] = new StreamPosition(partitions[i].StreamKey, from[i].Format());
        }

        var countPerStream = partitions[0].BatchSize;

        partitions[0].Log.LogDebug(
            "Streams: co-located inline read loop started for topic={Topic} consumer={Consumer} partitions={PartitionCount} countPerStream={CountPerStream} hop={Hop}.",
            partitions[0].Topic,
            partitions[0].Consumer,
            count,
            countPerStream,
            hopToThreadPool);

        // R-06. The backoff of the current transport outage, or zero when the connection is healthy.
        // One read serves the whole group, so an outage blocks the group and recovery clears it.
        var outage = 0;

        // R-06. Every partition is live from here on. Without this the partitions stayed in
        // PartitionRunState.Starting for the life of the process — the health check reads Starting as
        // "not yet reading", so a perfectly healthy co-located inline consumer looked wedged.
        for (var i = 0; i < count; i++)
        {
            partitions[i].Monitor?.MarkRunning();
        }

        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (resets is not null && resets.HasPending)
                {
                    ApplyResets(partitions, resets, cursors);
                }

                StreamSlice[] reply;

                try
                {
                    reply = await fetch(cursors, countPerStream, ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (IsTransientTransportFailure(ex) && !ct.IsCancellationRequested)
                {
                    // R-06. This await was bare, so one RedisConnectionException — a failover, a
                    // rolling Redis restart, a dropped connection — faulted this loop permanently and
                    // with it EVERY partition the worker owned, until the pod was restarted by hand.
                    // The other three loops have always treated a transport failure as an outage to be
                    // waited out; this one is now the fourth.
                    if (outage == 0)
                    {
                        // Slot 0 is marked by WaitOutTransportFailureAsync itself, along with the one
                        // Error line the whole outage gets.
                        for (var i = 1; i < count; i++)
                        {
                            partitions[i].Monitor?.MarkBlocked();
                        }
                    }

                    outage = await WaitOutTransportFailureAsync(partitions[0], ex, outage, ct).ConfigureAwait(false);
                    continue;
                }

                if (outage != 0)
                {
                    for (var i = 1; i < count; i++)
                    {
                        partitions[i].Monitor?.MarkRunning();
                    }

                    outage = ResumeAfterOutage(in partitions[0], outage);
                }

                if (reply.Length == 0)
                {
                    // At the tail on every partition. Without this, lag.ms on a quiet topic climbs
                    // forever off the age of the last entry and every idle consumer looks behind.
                    for (var i = 0; i < count; i++)
                    {
                        partitions[i].Monitor?.MarkCaughtUp();
                    }

                    continue;
                }

                Array.Clear(delivered);

                // Redis answers in request order and omits streams with nothing new.
                var hint = 0;

                for (var r = 0; r < reply.Length; r++)
                {
                    if (reply[r].IsEmpty)
                    {
                        continue;
                    }

                    var slot = SlotOf(partitions, reply[r].Key, hint);

                    if (slot < 0)
                    {
                        partitions[0].Log.LogWarning(
                            "Streams: XREAD returned stream key {Key}, which this worker does not own. Ignoring {EntryCount} entries.",
                            reply[r].Key.ToString(),
                            reply[r].Count);
                        continue;
                    }

                    hint = slot + 1 == count ? 0 : slot + 1;
                    delivered[slot] = true;

                    var ctx = partitions[slot];
                    var batch = BuildBatch(in ctx, in filters[slot], reply[r].Span);

                    // Advance this partition's read cursor past everything READ, filtered-out
                    // entries included, before the handler runs: a handler failure under
                    // BestEffort must not re-read the same entries next round.
                    cursors[slot] = new StreamPosition(ctx.StreamKey, batch.Last.Format());

                    var outcome = await HandleInlineAsync(
                        ctx, batch, handler, positions, persist, flush, hopToThreadPool, ct).ConfigureAwait(false);

                    if (outcome == BatchOutcome.Stop)
                    {
                        // ErrorPolicy.StopPartition with co-location stops the loop, and with it
                        // every partition this worker owns — there is only one loop to stop. The
                        // channel mode can stop a single processor instead; another reason the
                        // default stays enabled.
                        ctx.Log.LogWarning(
                            "Streams: inline co-located loop stopping for consumer={Consumer} because partition={Partition} stopped. " +
                            "Partitions {PartitionCount} share this loop, so all of them stop together.",
                            ctx.Consumer,
                            ctx.Partition,
                            count);

                        // The stopping partition marks itself; its siblings would otherwise keep
                        // reporting Running with no loop left to read them, which is exactly the
                        // "healthy but consuming nothing" state the monitor exists to expose.
                        for (var i = 0; i < count; i++)
                        {
                            if (i != slot)
                            {
                                partitions[i].Monitor?.MarkStopped(
                                    "a co-located sibling stopped and inline co-located partitions share one read loop");
                            }
                        }

                        return;
                    }
                }

                for (var i = 0; i < count; i++)
                {
                    if (!delivered[i])
                    {
                        // At the tail with nothing to do — see the note on the empty reply above.
                        partitions[i].Monitor?.MarkCaughtUp();
                    }
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown.
        }
        finally
        {
            partitions[0].Log.LogDebug(
                "Streams: co-located inline read loop stopped for topic={Topic} consumer={Consumer}.",
                partitions[0].Topic,
                partitions[0].Consumer);
        }
    }

    /// <summary>
    /// Hands one batch to the shared processing step, optionally after a <see cref="ThreadPool"/> hop.
    /// </summary>
    /// <remarks>
    /// The hop is a per-batch <see cref="Task.Run(Func{Task})"/> — one closure and one task per
    /// batch, not per message, and only in the <c>Block</c> + inline combination that needs it. The
    /// non-hopping path awaits directly and allocates nothing beyond what the channel path does.
    /// The pooled array is returned inside <c>ProcessBatchAsync</c>'s <c>finally</c> either way, so
    /// neither path can leak it.
    /// </remarks>
    private static ValueTask<BatchOutcome> HandleInlineAsync(
        PartitionContext ctx,
        StreamBatch batch,
        Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> handler,
        PositionRecorder positions,
        PersistMode persist,
        PositionFlush? flush,
        bool hopToThreadPool,
        CancellationToken ct)
    {
        if (!hopToThreadPool)
        {
            return ProcessBatchAsync(ctx, batch, handler, positions, persist, flush, ct);
        }

        // CancellationToken.None on Task.Run: cancellation is the batch's business, not the hop's.
        // Cancelling the hop itself would abandon a rented array without returning it.
        var hopped = Task.Run(
            () => ProcessBatchAsync(ctx, batch, handler, positions, persist, flush, ct).AsTask(),
            CancellationToken.None);

        return new ValueTask<BatchOutcome>(hopped);
    }

    /// <summary>
    /// The two ways an inline worker can be misconfigured into looking healthy while doing the wrong
    /// thing: a channel it will never read, and a <c>Sync*</c> persist mode with nothing to flush with.
    /// </summary>
    private static void GuardInline(in PartitionContext ctx, PersistMode persist, PositionFlush? flush)
    {
        if (ctx.Writer is not null)
        {
            throw new StreamConfigurationException(
                $"Streams: the inline (Backpressure.Enabled = false) worker for topic '{ctx.Topic}' partition {ctx.Partition} " +
                "was given a channel writer. Inline mode calls the handler itself and never reads that channel, so every " +
                "batch written to it would be dropped. Build the PartitionContext with a null Writer for inline mode, or " +
                "leave Backpressure.Enabled at its default of true and use the channel path.");
        }

        if (persist is PersistMode.SyncBatch or PersistMode.SyncMessage && flush is null)
        {
            throw new StreamConfigurationException(
                $"Streams: PersistMode.{persist} was configured for topic '{ctx.Topic}' partition {ctx.Partition} " +
                "but no synchronous position flush was supplied, so the mode would silently behave like AsyncBatch. " +
                "Pass PositionFlusher.FlushAsync, or use PersistMode.AsyncBatch.");
        }
    }
}
