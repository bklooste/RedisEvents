using System.Collections.Frozen;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RedisEvents.Diagnostics;
using RedisEvents.Errors;
using RedisEvents.Positions;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Consumer;

/// <summary>
/// Issues one multi-stream <c>XREAD</c> across every co-located partition a worker owns and hands
/// back the reply.
/// </summary>
/// <remarks>
/// A delegate rather than an interface, for the same reason the rest of the path is static: it is
/// the seam that lets a unit test script a read sequence with no Redis, and it lets
/// <c>ReadMode.Poll</c> (shared multiplexer) and <c>ReadMode.Block</c> (dedicated
/// <see cref="StreamReaderConnection"/>) share every line downstream of the fetch.
/// </remarks>
/// <param name="positions">
/// One entry per owned partition — stream key plus the exclusive id to read after. The read loop
/// owns this array and mutates it in place between rounds, so a steady read allocates nothing.
/// </param>
/// <param name="countPerStream">The <c>COUNT</c> applied to each stream in the read.</param>
/// <param name="ct">The host token.</param>
/// <returns>The streams that had entries, in request order; streams with nothing new are omitted.</returns>
internal delegate ValueTask<StreamSlice[]> MultiStreamFetch(
    StreamPosition[] positions,
    int countPerStream,
    CancellationToken ct);

/// <content>
/// The reader half of the pipeline: fetch, decode, filter, write to the channel — in that order,
/// with the channel write happening <em>before</em> the next fetch is issued so that a full channel
/// stops the reader by construction rather than by policy.
/// </content>
internal static partial class PartitionWorker
{
    /// <summary>
    /// Fetch → decode → filter → write to the channel, repeating until cancelled. One partition,
    /// driven by the fetch seam supplied by the read mode.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The loop never waits on its own: an empty fetch simply loops round, because both read modes
    /// own their idling — <c>Block</c> parks server-side inside the fetch for up to <c>BlockMs</c>,
    /// <c>Poll</c> applies its backoff inside the fetch. That is what keeps one loop body correct
    /// for both modes.
    /// </para>
    /// <para>
    /// A worker owning several co-located partitions uses <see cref="ReadGroupLoopAsync"/> instead,
    /// which is one <c>XREAD</c> for the whole group rather than one per partition.
    /// </para>
    /// <para>
    /// <b>Resets.</b> A pending reset is applied at the top of the loop, before the next fetch is
    /// issued, so the rewind takes effect on the very next read rather than after the batch in
    /// flight. The check is one volatile read per round; the cursor itself belongs to the fetch, so
    /// moving it is <paramref name="seek"/>'s job — <c>PollFetch.SeekTo</c> in the poll mode.
    /// </para>
    /// </remarks>
    /// <param name="ctx">The partition's context.</param>
    /// <param name="from">The resolved start position; the first read returns entries after it.</param>
    /// <param name="fetch">The fetch seam supplied by the read mode.</param>
    /// <param name="ct">The linked host token.</param>
    /// <param name="resets">The reset hand-off from the flusher tick, or <see langword="null"/> when
    /// live resets are not wired (they then apply at the next start instead).</param>
    /// <param name="seek">Moves the fetch's cursor. Required for a live reset to be taken at all: with
    /// no way to move the cursor there is nothing this loop could do with one, so the marker is left
    /// in Redis for a restart to apply.</param>
    /// <param name="rewind">
    /// Notifies the position flusher that this partition just took a reset, so it drops any pending
    /// position recorded before the seek. Called synchronously, before the next fetch, so a record
    /// made after this call — for a batch actually read from the seeked position — is never clobbered
    /// by it. See <c>PositionFlusher.Rewind</c>.
    /// </param>
    internal static async Task ReadLoopAsync(
        PartitionContext ctx,
        StreamId from,
        Func<CancellationToken, ValueTask<StreamEntryBatch>> fetch,
        CancellationToken ct,
        ResetSignal? resets = null,
        ResetSeek? seek = null,
        Action<int>? rewind = null)
    {
        ArgumentNullException.ThrowIfNull(fetch);

        var writer = ctx.Writer ?? throw MissingChannelWriter();

        // Built once, here, and not per batch: turning string[] into a comparison structure is
        // startup work, and the loop below runs it against every entry that is read.
        var filter = TypeFilter.Create(ctx.Filter);

        ctx.Log.LogDebug(
            "Streams: read loop started for topic={Topic} partition={Partition} consumer={Consumer} from={From}.",
            ctx.Topic,
            ctx.Partition,
            ctx.Consumer,
            from.Format());

        Exception? failure = null;

        // The backoff of the current transport outage, or zero when the connection is healthy.
        var outage = 0;

        // The partition is live from here on: the health check reports Starting as "not yet reading",
        // and a worker that never announced itself would look wedged the moment it began.
        ctx.Monitor?.MarkRunning();

        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (resets is not null && seek is not null && resets.HasPending)
                {
                    ApplyReset(in ctx, resets, seek, rewind);
                }

                StreamEntryBatch raw;

                try
                {
                    raw = await fetch(ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (IsTransientTransportFailure(ex) && !ct.IsCancellationRequested)
                {
                    // Redis is unreachable or the read timed out. Per 06-errors-and-observability
                    // this is a block-and-retry failure, not a partition-ending one: tearing the
                    // worker down would turn a failover into a dead consumer that only a pod
                    // restart brings back.
                    outage = await WaitOutTransportFailureAsync(ctx, ex, outage, ct).ConfigureAwait(false);
                    continue;
                }

                outage = ResumeAfterOutage(in ctx, outage);

                if (raw.IsEmpty)
                {
                    // At the tail with nothing to do. Without this, lag.ms on a quiet topic climbs
                    // forever off the age of the last entry and every idle consumer looks behind.
                    ctx.Monitor?.MarkCaughtUp();
                    continue;
                }

                var batch = BuildBatch(in ctx, in filter, raw.Span);

                // The channel write happens BEFORE the next fetch. When the processor falls behind,
                // this await is where the reader stops, so the next XREAD is never issued and the
                // lag accumulates in Redis rather than in this process's heap.
                await WriteAsync(writer, batch, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown, not a fault: complete the writer normally so the processor drains.
        }
        catch (ChannelClosedException)
        {
            // R-09. Somebody else completed this partition's writer while this loop was parked in
            // WriteAsync. There are exactly two of them and neither is a failure: the host completing
            // every writer on shutdown, and the continuation that completes this one when the
            // processor loop ends (ErrorPolicy.StopPartition, or a contested stand-down). Either way
            // there is nobody left to hand a batch to, so the loop is finished — and letting the
            // exception out instead faulted the worker and logged a spurious "worker fail during
            // shutdown" on an ordinary stop. The batch in flight was already returned to the pool by
            // WriteAsync.
            ctx.Log.LogDebug(
                "Streams: the channel for topic={Topic} partition={Partition} consumer={Consumer} was completed while the reader " +
                "was writing to it, so the read loop is stopping. Nothing was lost: the position is only ever recorded by the " +
                "processor, so the un-drained tail is redelivered.",
                ctx.Topic,
                ctx.Partition,
                ctx.Consumer);
        }
        catch (Exception ex)
        {
            failure = ex;
            throw;
        }
        finally
        {
            // Completing the writer is what lets the processor loop finish its drain and stop.
            writer.TryComplete(failure);
        }
    }

    /// <summary>
    /// The co-located read loop: <b>one</b> <c>XREAD</c> across every partition this worker owns,
    /// split by stream key, then decoded, filtered and written to each partition's own channel.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One round trip regardless of how many partitions the worker owns — that is the point of
    /// co-location (D2 in the overview). Sixteen partitions on one instance cost the same number of
    /// Redis round trips as one.
    /// </para>
    /// <para>
    /// Ordering is still per partition: entries from one stream key are decoded in reply order and
    /// written to that partition's channel in that order, and each partition has its own processor.
    /// </para>
    /// <para>
    /// Backpressure is shared across the group, deliberately. Each partition's batch is written
    /// before the next fetch is issued, so if any one partition's channel fills, the whole group's
    /// next <c>XREAD</c> waits. That is the co-location contract: one reader, one read, and the
    /// group moves at the speed of its slowest consumer instead of buffering the others without
    /// bound.
    /// </para>
    /// </remarks>
    /// <param name="partitions">The partitions this worker owns; each must have a channel writer.</param>
    /// <param name="from">The exclusive start id per partition, index-aligned with <paramref name="partitions"/>.</param>
    /// <param name="fetch">The multi-stream fetch seam.</param>
    /// <param name="ct">The linked host token.</param>
    /// <param name="resets">
    /// The reset hand-off from the flusher tick, or <see langword="null"/>. This loop owns its own
    /// cursor array, so it needs no seek delegate: a taken reset is a store into
    /// <c>positions[slot]</c> and the next <c>XREAD</c> asks for the target.
    /// </param>
    /// <param name="rewind">
    /// Notifies the position flusher that a partition just took a reset, so it drops any pending
    /// position recorded before the seek. See <c>PositionFlusher.Rewind</c> and the single-partition
    /// overload's matching parameter.
    /// </param>
    internal static async Task ReadGroupLoopAsync(
        PartitionContext[] partitions,
        StreamId[] from,
        MultiStreamFetch fetch,
        CancellationToken ct,
        ResetSignal? resets = null,
        Action<int>? rewind = null)
    {
        ArgumentNullException.ThrowIfNull(partitions);
        ArgumentNullException.ThrowIfNull(from);
        ArgumentNullException.ThrowIfNull(fetch);

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

        // Everything the loop needs, allocated once. Nothing below allocates per round except the
        // id string of a partition that actually advanced.
        var filters = new TypeFilter[count];
        var writers = new ChannelWriter<StreamBatch>[count];
        var positions = new StreamPosition[count];

        // Which partitions the round's reply carried entries for; the rest are at their tail and
        // must be marked caught up, or lag.ms on a quiet partition climbs off the age of its last
        // entry forever. Allocated once and cleared per round, never per message.
        var delivered = new bool[count];

        // Partitions whose processor has gone (ErrorPolicy.StopPartition, or a stood-down
        // partition): their channel is completed, so the group must stop trying to write to it. See
        // the retirement note where this is set.
        var retired = new bool[count];

        for (var i = 0; i < count; i++)
        {
            filters[i] = TypeFilter.Create(partitions[i].Filter);
            writers[i] = partitions[i].Writer ?? throw MissingChannelWriter();
            positions[i] = new StreamPosition(partitions[i].StreamKey, from[i].Format());
        }

        // Co-located partitions belong to one consumer, so they share its batch size.
        var countPerStream = partitions[0].BatchSize;

        partitions[0].Log.LogDebug(
            "Streams: co-located read loop started for topic={Topic} consumer={Consumer} partitions={PartitionCount} countPerStream={CountPerStream}.",
            partitions[0].Topic,
            partitions[0].Consumer,
            count,
            countPerStream);

        Exception? failure = null;

        // The backoff of the current transport outage, or zero when the connection is healthy. One
        // read serves the whole group, so an outage blocks the group and recovery clears it.
        var outage = 0;

        // Every partition is live from here on: a worker that never announced itself would look
        // wedged to the health check the moment it began reading.
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
                    ApplyResets(partitions, resets, positions, rewind);
                }

                StreamSlice[] reply;

                try
                {
                    reply = await fetch(positions, countPerStream, ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (IsTransientTransportFailure(ex) && !ct.IsCancellationRequested)
                {
                    // Block-and-retry, exactly as the single-partition loop does — tearing the group
                    // down would turn a failover into a dead consumer only a pod restart brings back.
                    // The whole group is blocked, because the whole group shares the one read.
                    if (outage == 0)
                    {
                        // Slot 0 is marked by WaitOutTransportFailureAsync itself, along with the
                        // one Error line the whole outage gets.
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

                Array.Clear(delivered);

                // Redis answers in request order and omits streams with nothing new, so the slot of
                // the previous reply entry is the right place to start looking for the next.
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
                        // A key we did not ask for. Never expected; skipping is safer than decoding
                        // entries into a partition context that does not describe them.
                        partitions[0].Log.LogWarning(
                            "Streams: XREAD returned stream key {Key}, which this worker does not own. Ignoring {EntryCount} entries.",
                            reply[r].Key.ToString(),
                            reply[r].Count);
                        continue;
                    }

                    hint = slot + 1 == count ? 0 : slot + 1;
                    delivered[slot] = true;

                    if (retired[slot])
                    {
                        // Nobody is draining this partition any more. Its cursor still advances so
                        // the read cannot spin on the same entries, but nothing is decoded, nothing
                        // is handed on, and — because only the processor records positions — its
                        // stored position stays exactly where it stopped, so the backlog is replayed
                        // when the partition is picked up again.
                        var span = reply[r].Span;
                        positions[slot] = new StreamPosition(
                            partitions[slot].StreamKey,
                            ParseEntryId(span[span.Length - 1].Id).Format());
                        continue;
                    }

                    StreamBatch batch;

                    {
                        ref readonly var ctx = ref partitions[slot];
                        batch = BuildBatch(in ctx, in filters[slot], reply[r].Span);

                        // Advance this partition's read cursor past everything read, including
                        // entries the filter dropped, and re-render its id only for the slot that
                        // moved.
                        positions[slot] = new StreamPosition(ctx.StreamKey, batch.Last.Format());
                    }

                    try
                    {
                        // Before the next fetch — see the remarks on shared backpressure.
                        await WriteAsync(writers[slot], batch, ct).ConfigureAwait(false);
                    }
                    catch (ChannelClosedException) when (!ct.IsCancellationRequested)
                    {
                        // The processor for this partition ended while its siblings kept running —
                        // ErrorPolicy.StopPartition, or a contested position. The group cannot stop
                        // reading the stream (it is one command covering every key), so the slot is
                        // retired instead: read on, discard, record nothing.
                        retired[slot] = true;
                        partitions[slot].Monitor?.MarkStopped("the partition was stood down while its co-located siblings kept reading");

                        partitions[slot].Log.LogWarning(
                            "Streams: partition {Partition} of topic {Topic} consumer {Consumer} was stood down, so the " +
                            "co-located reader is discarding what it reads for it. Its stored position is unchanged, so the " +
                            "backlog is replayed when the partition is picked up again; its co-located siblings keep running.",
                            partitions[slot].Partition,
                            partitions[slot].Topic,
                            partitions[slot].Consumer);
                    }
                }

                for (var i = 0; i < count; i++)
                {
                    if (!delivered[i] && !retired[i])
                    {
                        // At the tail with nothing to do. A retired partition is deliberately left
                        // reporting Stopped rather than being flipped back to caught-up.
                        partitions[i].Monitor?.MarkCaughtUp();
                    }
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown.
        }
        catch (ChannelClosedException)
        {
            // R-09, the group's half of it. The per-slot catch above retires a partition whose
            // processor has gone, but only while the host is running; on shutdown the host completes
            // every writer itself and a reader parked in WriteAsync sees this instead of a
            // cancellation. It is an ordinary stop, not a fault.
            partitions[0].Log.LogDebug(
                "Streams: a co-located channel for topic={Topic} consumer={Consumer} was completed while the shared reader was " +
                "writing to it, so the read loop is stopping.",
                partitions[0].Topic,
                partitions[0].Consumer);
        }
        catch (Exception ex)
        {
            failure = ex;
            throw;
        }
        finally
        {
            for (var i = 0; i < count; i++)
            {
                writers[i].TryComplete(failure);
            }
        }
    }

    /// <summary>
    /// Maps StackExchange.Redis's own multi-stream reply onto <see cref="StreamSlice"/>.
    /// </summary>
    /// <remarks>
    /// One array per fetch and nothing per message: <c>RedisStream</c> has no accessible
    /// constructor, so the hand-parsed blocking reply cannot produce one, and the two read modes
    /// meet on this shape instead.
    /// </remarks>
    internal static StreamSlice[] ToSlices(RedisStream[]? reply)
    {
        if (reply is null || reply.Length == 0)
        {
            return [];
        }

        var slices = new StreamSlice[reply.Length];

        for (var i = 0; i < reply.Length; i++)
        {
            slices[i] = new StreamSlice(reply[i].Key, reply[i].Entries ?? []);
        }

        return slices;
    }

    /// <summary>
    /// Decodes and filters one stream's worth of raw entries into a pooled batch.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One rental, one <c>for</c> loop, no LINQ and no per-message allocation — this replaces the
    /// <c>Select</c>/<c>Where</c>/<c>ToArray</c> chain the Kafka consumer ran per batch.
    /// </para>
    /// <para>
    /// The filter is applied on the <c>t</c> field <em>before</em> the entry is decoded, so an entry
    /// this consumer does not want costs one comparison and nothing else: no body memory, no type,
    /// key or correlation strings, no header view.
    /// </para>
    /// </remarks>
    private static StreamBatch BuildBatch(in PartitionContext ctx, in TypeFilter filter, ReadOnlySpan<StreamEntry> entries)
    {
        var partition = ctx.Partition;
        var items = StreamBatch.Rent(entries.Length);
        var count = 0;
        var lastKept = -1;
        var last = default(StreamId);

        try
        {
            for (var i = 0; i < entries.Length; i++)
            {
                ref readonly var entry = ref entries[i];

                if (!filter.Matches(entry.Values))
                {
                    continue;
                }

                var msg = EntryCodec.Decode(in entry, partition);
                items[count++] = msg;
                last = msg.Id;
                lastKept = i;
            }
        }
        catch
        {
            new StreamBatch(items, 0, default).Return();
            throw;
        }

        // R-16. streams.filtered was declared in StreamsDiagnostics and never recorded anywhere, so
        // a consumer with a type filter reported streams.consumed far below the topic's
        // streams.published and nothing said where the difference went — indistinguishable from lost
        // messages. Counted here, once per batch and only when the filter actually dropped
        // something: the per-entry loop above stays a comparison and a branch, and a consumer with
        // no filter never reaches this line at all.
        var dropped = entries.Length - count;
        if (dropped > 0)
        {
            // Two tags, per the metrics table in 06-errors-and-observability.md, passed through the
            // two-tag Add overload — a TagList would be a struct on the stack either way, but this
            // avoids even that, and neither tag boxes.
            StreamsDiagnostics.StreamsFiltered.Add(dropped, TopicTag(in ctx), ConsumerTag(in ctx));
        }

        // The position advances past every entry that was READ, not every entry that survived the
        // filter. A batch filtered down to zero still checkpoints — deliberately: a consumer that
        // wants 1 % of a busy topic's traffic would otherwise never advance its position, and would
        // re-read the same 99 % after every restart until retention finally dropped it.
        if (lastKept != entries.Length - 1)
        {
            last = ParseEntryId(entries[entries.Length - 1].Id);
        }

        return new StreamBatch(items, count, last);
    }

    /// <summary>
    /// Writes a batch to the channel, returning its pooled array if the write does not take.
    /// </summary>
    /// <remarks>
    /// Ownership of the rented array transfers to the processor loop on a successful write, and the
    /// processor returns it in a <c>finally</c>. Until that hand-off completes the reader still owns
    /// it, so a cancelled or failed write has to give it back here or the pool leaks an array per
    /// shutdown.
    /// </remarks>
    private static async ValueTask WriteAsync(
        ChannelWriter<StreamBatch> writer,
        StreamBatch batch,
        CancellationToken ct)
    {
        try
        {
            await writer.WriteAsync(batch, ct).ConfigureAwait(false);
        }
        catch
        {
            batch.Return();
            throw;
        }
    }

    /// <summary>
    /// Applies this partition's pending reset, if it has one, by moving the fetch's cursor.
    /// </summary>
    /// <param name="ctx">The partition's context.</param>
    /// <param name="resets">The reset hand-off.</param>
    /// <param name="seek">Moves the fetch's read cursor.</param>
    /// <param name="rewind">
    /// Notifies the position flusher of the take, so it drops any position pending from before the
    /// seek. Called before <paramref name="seek"/> moves the cursor, so the flusher's pending state is
    /// clear before this partition can possibly read — and record — anything from the new position.
    /// </param>
    /// <remarks>
    /// Taking the reset is what tells the flusher it may delete the marker, so this must happen only
    /// when the cursor has actually moved — hence the call order, and hence the loop taking nothing at
    /// all when it has no <see cref="ResetSeek"/> to move it with.
    /// </remarks>
    private static void ApplyReset(in PartitionContext ctx, ResetSignal resets, ResetSeek seek, Action<int>? rewind)
    {
        if (!resets.TryTake(ctx.Partition, out var marker))
        {
            return;
        }

        rewind?.Invoke(ctx.Partition);
        seek(marker.Target);

        ctx.Log.LogWarning(
            "Streams: reset applied — topic={Topic} partition={Partition} consumer={Consumer} restarting reads after {Target}. Entries after that id are delivered again, and batches already read are still processed, so handlers must be idempotent.",
            ctx.Topic,
            ctx.Partition,
            ctx.Consumer,
            marker.Target.Format());
    }

    /// <summary>
    /// Applies pending resets across a co-located group by rewriting the affected cursors in place.
    /// </summary>
    /// <param name="partitions">The group's contexts.</param>
    /// <param name="resets">The reset hand-off.</param>
    /// <param name="positions">The cursor array the next <c>XREAD</c> will use, mutated in place.</param>
    /// <param name="rewind">
    /// Notifies the position flusher of each take, so it drops any position pending from before the
    /// seek. Called before the cursor is rewritten, so the flusher's pending state is clear before
    /// this partition can possibly read — and record — anything from the new position.
    /// </param>
    /// <remarks>
    /// Only the partitions that were actually reset are touched, so a reset on one partition of a
    /// group of sixteen leaves the other fifteen cursors — and the single round trip — exactly as they
    /// were.
    /// </remarks>
    private static void ApplyResets(PartitionContext[] partitions, ResetSignal resets, StreamPosition[] positions, Action<int>? rewind)
    {
        for (var i = 0; i < partitions.Length; i++)
        {
            ref readonly var ctx = ref partitions[i];

            if (!resets.TryTake(ctx.Partition, out var marker))
            {
                continue;
            }

            rewind?.Invoke(ctx.Partition);
            positions[i] = new StreamPosition(ctx.StreamKey, marker.Target.Format());

            ctx.Log.LogWarning(
                "Streams: reset applied — topic={Topic} partition={Partition} consumer={Consumer} restarting reads after {Target}. Entries after that id are delivered again, and batches already read are still processed, so handlers must be idempotent.",
                ctx.Topic,
                ctx.Partition,
                ctx.Consumer,
                marker.Target.Format());
        }
    }

    /// <summary>Finds the slot a reply's stream key belongs to, starting from <paramref name="hint"/>.</summary>
    private static int SlotOf(PartitionContext[] partitions, in RedisKey key, int hint)
    {
        for (var i = hint; i < partitions.Length; i++)
        {
            if (partitions[i].StreamKey == key)
            {
                return i;
            }
        }

        for (var i = 0; i < hint; i++)
        {
            if (partitions[i].StreamKey == key)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Parses an entry id that was not decoded, which happens only when the filter dropped the last
    /// entry of a batch — at most one small string per batch.
    /// </summary>
    /// <exception cref="StreamTransportException">The id is not a Redis stream id.</exception>
    private static StreamId ParseEntryId(RedisValue id)
    {
        var text = (string?)id;

        if (text is null || !StreamId.TryParse(text.AsSpan(), out var parsed))
        {
            throw new StreamTransportException(
                $"Redis returned a stream entry whose id ('{text}') is not in the expected '<ms>-<seq>' form. " +
                "Refusing to guess a position from it, because a wrong position silently skips or replays entries.");
        }

        return parsed;
    }

    /// <summary>
    /// The channel read loops were handed a context with no writer.
    /// </summary>
    /// <remarks>
    /// R-18. This used to say inline mode was unimplemented and to leave
    /// <c>Backpressure.Enabled</c> at <see langword="true"/>. Inline mode <em>is</em> implemented —
    /// <see cref="RunInlineAsync"/> and the co-located inline group in
    /// <c>PartitionWorker.Inline.cs</c> — and the host dispatches to it before either channel loop is
    /// reached, so a null writer here is no longer a missing feature but a wiring bug: a channel loop
    /// that would read entries and have nowhere to put them.
    /// </remarks>
    private static NotSupportedException MissingChannelWriter() =>
        new("Streams: a channel-backed read loop was started with no channel writer, so it would " +
            "read entries and drop every one of them. Inline (Backpressure.Enabled = false) " +
            "consumption is implemented and runs through PartitionWorker.RunInlineAsync instead — " +
            "reaching this loop without a writer is a wiring bug in the consumer host, not a " +
            "configuration problem.");
}

/// <summary>
/// The consumer's message-type filter, matched on the <c>t</c> field before an entry is decoded.
/// </summary>
/// <remarks>
/// <para>
/// Built once when a read loop starts and then read-only, so the per-entry cost is a comparison and
/// nothing else.
/// </para>
/// <para>
/// <b>Why two implementations.</b> Up to <see cref="LinearScanLimit"/> types — which is nearly every
/// real consumer — a linear scan over UTF-8 patterns beats a hash set: four <c>SequenceEqual</c>s
/// against short spans cost less than hashing the candidate once, and the comparison runs straight
/// off the bytes StackExchange.Redis handed back, so the type string is never materialised at all.
/// Above that the scan stops paying and a <see cref="FrozenSet{T}"/> takes over; that path does
/// materialise the type string, which is the honest trade for O(1) at a size where the scan would
/// be worse.
/// </para>
/// </remarks>
internal readonly struct TypeFilter
{
    /// <summary>The filter size up to which a linear scan beats a hash lookup.</summary>
    internal const int LinearScanLimit = 4;

    /// <summary>The <c>t</c> field name, as a value that can be compared against the reply's names.</summary>
    private static readonly RedisValue TypeFieldName = EntryCodec.TypeField;

    private readonly byte[][]? patterns;
    private readonly FrozenSet<string>? types;

    private TypeFilter(byte[][]? patterns, FrozenSet<string>? types)
    {
        this.patterns = patterns;
        this.types = types;
    }

    /// <summary>Whether this filter keeps everything, which is the default and the common case.</summary>
    internal bool KeepsEverything => this.patterns is null && this.types is null;

    /// <summary>
    /// Builds a filter from the configured type list. <see langword="null"/>, empty, or a list of
    /// nothing but blanks all mean "keep everything".
    /// </summary>
    internal static TypeFilter Create(string[]? filter)
    {
        if (filter is null || filter.Length == 0)
        {
            return default;
        }

        // Startup work, but still no LINQ: the same discipline applied here costs nothing and keeps
        // one style across the file.
        var kept = new string[filter.Length];
        var count = 0;

        for (var i = 0; i < filter.Length; i++)
        {
            var candidate = filter[i];

            if (string.IsNullOrWhiteSpace(candidate))
            {
                continue;
            }

            var duplicate = false;

            for (var j = 0; j < count; j++)
            {
                if (string.Equals(kept[j], candidate, StringComparison.Ordinal))
                {
                    duplicate = true;
                    break;
                }
            }

            if (!duplicate)
            {
                kept[count++] = candidate;
            }
        }

        if (count == 0)
        {
            return default;
        }

        if (count <= LinearScanLimit)
        {
            var patterns = new byte[count][];

            for (var i = 0; i < count; i++)
            {
                patterns[i] = Encoding.UTF8.GetBytes(kept[i]);
            }

            return new TypeFilter(patterns, null);
        }

        var set = new HashSet<string>(count, StringComparer.Ordinal);

        for (var i = 0; i < count; i++)
        {
            set.Add(kept[i]);
        }

        return new TypeFilter(null, set.ToFrozenSet(StringComparer.Ordinal));
    }

    /// <summary>
    /// Whether an entry survives the filter, decided from its field/value pairs alone.
    /// </summary>
    /// <remarks>
    /// An entry with no <c>t</c> field cannot match a filter and is dropped; with no filter
    /// configured, nothing is inspected at all.
    /// </remarks>
    /// <param name="values">The entry's field/value pairs, exactly as Redis returned them.</param>
    internal bool Matches(ReadOnlySpan<NameValueEntry> values)
    {
        if (this.KeepsEverything)
        {
            return true;
        }

        for (var i = 0; i < values.Length; i++)
        {
            ref readonly var field = ref values[i];

            if (field.Name == TypeFieldName)
            {
                return this.MatchesType(field.Value);
            }
        }

        return false;
    }

    private bool MatchesType(in RedisValue value)
    {
        var scan = this.patterns;

        if (scan is not null)
        {
            // The reply's values are raw bytes, so this aliases the read buffer rather than copying
            // it, and the type string is never built.
            ReadOnlyMemory<byte> raw = value;
            var span = raw.Span;

            for (var i = 0; i < scan.Length; i++)
            {
                if (span.SequenceEqual(scan[i]))
                {
                    return true;
                }
            }

            return false;
        }

        var text = (string?)value;

        return text is not null && this.types!.Contains(text);
    }
}
