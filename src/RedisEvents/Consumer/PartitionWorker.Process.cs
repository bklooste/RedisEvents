using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RedisEvents.Config;
using RedisEvents.Diagnostics;
using RedisEvents.Errors;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Consumer;

/// <summary>
/// Persists whatever positions have been recorded so far and does not return until the write has
/// landed. Bound to <c>PositionFlusher.FlushAsync</c> by the host.
/// </summary>
/// <param name="ct">Cancellation token for the write.</param>
/// <returns>A task that completes when the position write has landed (or failed and been re-queued).</returns>
/// <remarks>
/// Only the synchronous <see cref="PersistMode"/>s await this. <see cref="PersistMode.AsyncBatch"/>
/// leaves persistence entirely to the flusher's own timer, which is why the default costs the
/// processing loop nothing at all.
/// </remarks>
internal delegate ValueTask PositionFlush(CancellationToken ct);

internal static partial class PartitionWorker
{
    /// <summary>What the loop should do after a batch.</summary>
    private enum BatchOutcome
    {
        /// <summary>Take the next batch.</summary>
        Continue = 0,

        /// <summary>Stop this partition — either <see cref="ErrorPolicy.StopPartition"/> or shutdown.</summary>
        Stop = 1,

        /// <summary><see cref="ErrorPolicy.Fail"/>: rethrow so the host faults and Kubernetes restarts the pod.</summary>
        Fault = 2,
    }

    /// <summary>
    /// Consumes batches from the channel, invokes the handler, and advances the position — in that
    /// order, and <b>only</b> in that order.
    /// </summary>
    /// <param name="ctx">The partition's context: topic, partition, consumer, error policy, logger.</param>
    /// <param name="handler">The application handler. It is the only indirect call on this path.</param>
    /// <param name="positions">
    /// Records a processed position. Called <b>after</b> the handler has returned successfully, never
    /// before — a position recorded at read time would silently drop everything in flight when the
    /// process dies.
    /// </param>
    /// <param name="ct">The linked host token; it is what the handler receives.</param>
    /// <param name="reader">
    /// The read side of the partition's channel. Required in practice: <see cref="ValidateStartup"/>
    /// refuses a null one at startup, because a processing loop with nothing to drain would consume
    /// nothing while still looking like a healthy worker. It stays optional in the signature only so
    /// that <see cref="ValidateStartup"/> is the thing that reports the mistake, with the topic and
    /// partition named, rather than the compiler reporting it in a test.
    /// </param>
    /// <param name="persist">The persistence mode, per <c>04-positions-and-replay.md</c>.</param>
    /// <param name="flush">
    /// The synchronous position write, required by <see cref="PersistMode.SyncBatch"/> and
    /// <see cref="PersistMode.SyncMessage"/> and ignored by the other two modes.
    /// </param>
    /// <returns>A task that completes when the channel completes, the partition is stopped, or the host shuts down.</returns>
    /// <remarks>
    /// <para>
    /// <b>The position guarantee.</b> Per batch: start the activity, await the handler, and only on
    /// success record <c>batch.Last</c>. On failure the <see cref="ErrorPolicy"/> decides whether the
    /// position moves, and the pooled array goes back to the pool in a <c>finally</c> either way.
    /// Nothing anywhere on this path records a position for a batch the handler has not returned from.
    /// </para>
    /// <para>
    /// <b>Threading.</b> This must be started with <c>Task.Run</c> so it runs on the standard
    /// <c>ThreadPool</c>. It must never run on a reader's dedicated <c>SocketManager</c> thread: a
    /// handler executing there would stall that consumer's <c>XREAD</c>s entirely, which is the exact
    /// failure the dedicated thread exists to prevent.
    /// </para>
    /// <para>
    /// <b>Draining.</b> The wait deliberately uses <see cref="CancellationToken.None"/> rather than
    /// <paramref name="ct"/>. On shutdown the read loop completes the writer, so this loop finishes
    /// the batches already in the channel and then exits on its own — "in-flight batches finish",
    /// per the shutdown sequence. The host bounds that drain with its own timeout; anything still
    /// queued when the loop stops is returned to the pool unprocessed and redelivers on restart.
    /// </para>
    /// </remarks>
    internal static async Task ProcessLoopAsync(
        PartitionContext ctx,
        Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> handler,
        PositionRecorder positions,
        CancellationToken ct,
        ChannelReader<StreamBatch>? reader = null,
        PersistMode persist = PersistMode.AsyncBatch,
        PositionFlush? flush = null)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(positions);

        // P3-03. Startup validation, and the one place a StreamConfigurationException is raised
        // without any retry behind it. See ValidateStartup.
        ValidateStartup(in ctx, reader, persist, flush);

        ctx.Log.LogDebug(
            "Streams: process loop started for topic={Topic} partition={Partition} consumer={Consumer} persist={Persist} onError={OnError}.",
            ctx.Topic,
            ctx.Partition,
            ctx.Consumer,
            persist,
            ctx.OnError);

        try
        {
            // CancellationToken.None: see the draining note above. The read loop always completes
            // the writer in its finally, so this wait is what ends the loop, not the token.
            while (await reader.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
            {
                while (reader.TryRead(out var batch))
                {
                    var outcome = await ProcessBatchAsync(ctx, batch, handler, positions, persist, flush, ct)
                        .ConfigureAwait(false);

                    if (outcome == BatchOutcome.Stop)
                    {
                        return;
                    }
                }
            }
        }
        finally
        {
            // Whatever is still queued when we stop holds rented arrays. Returning them here is a
            // non-blocking drain of what is already in the channel — it never waits for the reader.
            while (reader.TryRead(out var abandoned))
            {
                abandoned.Return();
            }

            ctx.Log.LogDebug(
                "Streams: process loop stopped for topic={Topic} partition={Partition} consumer={Consumer}.",
                ctx.Topic,
                ctx.Partition,
                ctx.Consumer);
        }
    }

    /// <summary>
    /// Runs one batch through the handler and applies the position and error rules to the result.
    /// </summary>
    /// <remarks>
    /// The two error behaviours live here, and there are only two. A <see cref="DontIgnoreException"/>
    /// blocks this partition and retries the same batch until it succeeds
    /// (<see cref="BlockAndRetryAsync"/>); anything else is best effort — logged, position advanced,
    /// carry on — unless the consumer chose one of the two <see cref="ErrorPolicy"/> escapes. There is
    /// no library-level retry of ordinary failures and no dead-letter queue.
    /// </remarks>
    /// <returns><see cref="BatchOutcome.Stop"/> when this partition must stand down, otherwise <see cref="BatchOutcome.Continue"/>.</returns>
    private static async ValueTask<BatchOutcome> ProcessBatchAsync(
        PartitionContext ctx,
        StreamBatch batch,
        Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> handler,
        PositionRecorder positions,
        PersistMode persist,
        PositionFlush? flush,
        CancellationToken ct)
    {
        var span = new StreamSpanContext(ctx.Topic, ctx.Partition, ctx.Consumer);

        // R-10. A batch filtered down to zero still checkpoints — a consumer that wants 1 % of a busy
        // topic would otherwise never advance — but there is nothing to hand a handler, so it gets no
        // span, no correlation scope and no handler call. What it does NOT get any more is its own
        // little advance outside the error handling: a Sync* flush that threw a DontIgnoreException
        // there used to fault the worker instead of blocking the partition, which is the opposite of
        // what the exception means. It now runs through the same try below as a real batch, and
        // InvokeAsync skips only the handler call.
        var empty = batch.IsEmpty;
        var activity = empty ? null : StreamActivity.StartProcess(in span, batch.AsSpan(), batch.Last);
        var scope = empty ? null : StreamActivity.BeginCorrelationScope(ctx.Log, batch.AsSpan());
        var started = Stopwatch.GetTimestamp();

        // Cleared when the blocking-retry path takes over the timing: it records one duration per
        // attempt, so the finally must not add the whole blocked period on top.
        var timing = true;

        try
        {
            try
            {
                await InvokeAsync(ctx, batch, handler, positions, persist, flush, ct).ConfigureAwait(false);

                if (!empty)
                {
                    CountConsumed(in ctx, batch.Count);
                }

                return BatchOutcome.Continue;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Shutdown, not a failure: no log, no metric, no position. The batch redelivers.
                return BatchOutcome.Stop;
            }
            catch (DontIgnoreException ex)
            {
                // P3-02. The handler declared this batch un-skippable, so the partition blocks on it
                // and retries with backoff until it succeeds or the host shuts down. The position is
                // not advanced anywhere on that path, and no other partition is affected.
                //
                // R-10. The failed attempt's own duration is recorded here and the span is CLOSED
                // here, before the block starts. Leaving them open recorded the whole retry period —
                // hours, in the outage this exists for — as one streams.batch.duration sample and one
                // streams.process span: a p99 that says the pipeline takes four hours, and a trace no
                // viewer will render. Each retry then gets its own sample and its own short span.
                RecordBatchMetrics(in ctx, batch.Count, started);
                timing = false;

                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity?.Dispose();
                activity = null;

                return await BlockAndRetryAsync(ctx, batch, ex, span, handler, positions, persist, flush, ct)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var outcome = await OnFailureAsync(ctx, batch, ex, activity, positions, persist, flush, ct)
                    .ConfigureAwait(false);

                if (outcome == BatchOutcome.Fault)
                {
                    // ErrorPolicy.Fail: the original exception, with its original stack, faults the
                    // worker task and through it the host.
                    throw;
                }

                return outcome;
            }
        }
        finally
        {
            // Step 5. The handler contract says the memory must not be retained past the await, so
            // the array goes back to the pool here whatever happened above.
            batch.Return();

            if (timing)
            {
                RecordBatchMetrics(in ctx, batch.Count, started);
            }

            scope?.Dispose();
            activity?.Dispose();
        }
    }

    /// <summary>
    /// Records the size and duration of one <em>attempt</em> at a batch.
    /// </summary>
    /// <param name="ctx">The partition's context, for the tags.</param>
    /// <param name="count">How many messages the attempt handled.</param>
    /// <param name="startedAt">The <see cref="Stopwatch"/> timestamp the attempt began at.</param>
    /// <remarks>
    /// Per attempt, not per batch: a blocked batch is retried for as long as the outage lasts, and
    /// folding that wait into one sample makes <c>streams.batch.duration</c> measure the outage rather
    /// than the handler. See the <see cref="DontIgnoreException"/> branch of <c>ProcessBatchAsync</c>.
    /// </remarks>
    private static void RecordBatchMetrics(in PartitionContext ctx, int count, long startedAt)
    {
        if (count == 0)
        {
            // A batch the filter emptied handed nothing to a handler, so it is not a sample of how
            // long handling takes or of how big a batch is. It was never recorded before R-10 either;
            // routing it through the same try must not quietly start polluting both histograms with
            // zero-size, zero-duration rows.
            return;
        }

        StreamsDiagnostics.StreamsBatchSize.Record(count, TopicTag(in ctx), ConsumerTag(in ctx));
        StreamsDiagnostics.StreamsBatchDuration.Record(
            Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds,
            TopicTag(in ctx),
            ConsumerTag(in ctx));
    }

    /// <summary>
    /// Applies <see cref="PartitionContext.OnError"/> to a handler failure that is not a
    /// <see cref="DontIgnoreException"/> and not shutdown.
    /// </summary>
    /// <remarks>
    /// There is deliberately no retry and no dead-letter queue anywhere in this method. The handler
    /// is the only thing that knows whether a failure is transient, whether its work is idempotent,
    /// or whether the message is worth parking, so retry policy belongs to it.
    /// </remarks>
    private static async ValueTask<BatchOutcome> OnFailureAsync(
        PartitionContext ctx,
        StreamBatch batch,
        Exception ex,
        Activity? activity,
        PositionRecorder positions,
        PersistMode persist,
        PositionFlush? flush,
        CancellationToken ct)
    {
        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);

        CountError(
            in ctx,
            ctx.OnError switch
            {
                ErrorPolicy.StopPartition => "stop_partition",
                ErrorPolicy.Fail => "fail",
                _ => "best_effort",
            },
            ex);

        switch (ctx.OnError)
        {
            case ErrorPolicy.StopPartition:
                ctx.Log.LogError(
                    ex,
                    "Streams: handler threw {ExceptionType} for topic={Topic} partition={Partition} consumer={Consumer} ids={FirstId}..{LastId} count={Count} type={MessageType}. " +
                    "ErrorPolicy.StopPartition: the position was NOT advanced and this partition is stopping; other partitions keep running.",
                    ex.GetType().Name,
                    ctx.Topic,
                    ctx.Partition,
                    ctx.Consumer,
                    FirstId(in batch),
                    batch.Last.Format(),
                    batch.Count,
                    FirstType(in batch));

                ctx.Monitor?.MarkStopped($"ErrorPolicy.StopPartition after {ex.GetType().Name}: {ex.Message}");

                return BatchOutcome.Stop;

            case ErrorPolicy.Fail:
                ctx.Log.LogCritical(
                    ex,
                    "Streams: handler threw {ExceptionType} for topic={Topic} partition={Partition} consumer={Consumer} ids={FirstId}..{LastId} count={Count} type={MessageType}. " +
                    "ErrorPolicy.Fail: faulting the host.",
                    ex.GetType().Name,
                    ctx.Topic,
                    ctx.Partition,
                    ctx.Consumer,
                    FirstId(in batch),
                    batch.Last.Format(),
                    batch.Count,
                    FirstType(in batch));

                ctx.Monitor?.MarkStopped($"ErrorPolicy.Fail after {ex.GetType().Name}: {ex.Message}");

                return BatchOutcome.Fault;

            default:
                // BestEffort: log once, advance past the batch, carry on. The advance is the whole
                // point — a poison message must not wedge the partition forever.
                ctx.Log.LogError(
                    ex,
                    "Streams: handler threw {ExceptionType} for topic={Topic} partition={Partition} consumer={Consumer} ids={FirstId}..{LastId} count={Count} type={MessageType}. " +
                    "ErrorPolicy.BestEffort: advancing past the batch. The library does not retry and has no dead-letter queue — " +
                    "the handler owns retry, or should throw a DontIgnoreException to block instead.",
                    ex.GetType().Name,
                    ctx.Topic,
                    ctx.Partition,
                    ctx.Consumer,
                    FirstId(in batch),
                    batch.Last.Format(),
                    batch.Count,
                    FirstType(in batch));

                try
                {
                    await AdvanceAsync(ctx, batch.Last, positions, persist, flush, ct).ConfigureAwait(false);
                }
                catch (Exception advance) when (advance is not OperationCanceledException)
                {
                    // The advance is a Redis write under the Sync* modes, so it can fail on its own —
                    // and when it does, BestEffort must still mean "carry on". Faulting the worker
                    // here would turn a flush failure into a dead partition under the one policy that
                    // exists to keep going. The position simply does not move, so the batch redelivers.
                    ctx.Log.LogWarning(
                        advance,
                        "Streams: the position write after a best-effort failure on topic={Topic} partition={Partition} consumer={Consumer} " +
                        "itself failed with {ExceptionType}. The partition keeps running and the batch redelivers on the next start.",
                        ctx.Topic,
                        ctx.Partition,
                        ctx.Consumer,
                        advance.GetType().Name);
                }

                return BatchOutcome.Continue;
        }
    }

    /// <summary>
    /// <see cref="PersistMode.SyncMessage"/>: hand the handler one message at a time and persist the
    /// position after each, so a crash can redeliver at most the message in flight.
    /// </summary>
    /// <remarks>
    /// One Redis round trip per message. This is only sane with <c>IMessageHandler</c> on a low-rate
    /// topic — it is the money-movement setting, not a throughput setting — and it still does not
    /// give exactly-once: the handler can succeed and the process die before the <c>HSET</c> lands.
    /// </remarks>
    private static async ValueTask HandlePerMessageAsync(
        PartitionContext ctx,
        StreamBatch batch,
        Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> handler,
        PositionRecorder positions,
        PositionFlush flush,
        CancellationToken ct)
    {
        var memory = batch.AsMemory();

        for (var i = 0; i < batch.Count; i++)
        {
            await handler(memory.Slice(i, 1), ct).ConfigureAwait(false);

            ctx.Monitor?.Observe(memory.Span[i].Id);
            positions(ctx.Partition, memory.Span[i].Id);
            await flush(ct).ConfigureAwait(false);
        }
    }

    /// <summary>Records, and where the mode demands it persists, a processed position.</summary>
    /// <remarks>
    /// <list type="bullet">
    /// <item><see cref="PersistMode.AsyncBatch"/> — record only; the flusher coalesces and writes on its timer.</item>
    /// <item><see cref="PersistMode.SyncBatch"/> — record, then await the <c>HSET</c> before the next batch.</item>
    /// <item><see cref="PersistMode.SyncMessage"/> — handled per message; reaching here means the batch failed under BestEffort, so the batch-level advance is persisted the same way.</item>
    /// <item><see cref="PersistMode.None"/> — nothing at all, not even the in-memory record.</item>
    /// </list>
    /// </remarks>
    private static ValueTask AdvanceAsync(
        PartitionContext ctx,
        StreamId id,
        PositionRecorder positions,
        PersistMode persist,
        PositionFlush? flush,
        CancellationToken ct)
    {
        // Observed before the persistence mode is consulted: how far this partition has got is a
        // health fact, and PersistMode.None means "do not store it", not "do not know it".
        ctx.Monitor?.Observe(id);

        if (persist == PersistMode.None)
        {
            return default;
        }

        positions(ctx.Partition, id);

        if (persist is PersistMode.SyncBatch or PersistMode.SyncMessage && flush is not null)
        {
            return flush(ct);
        }

        return default;
    }

    /// <summary>
    /// Invokes the handler for one batch and, on success, advances the position — the two steps that
    /// must happen together and in that order, whether this is the first attempt or the four
    /// hundredth retry of a blocked partition.
    /// </summary>
    /// <remarks>
    /// Extracted so the blocking-retry loop re-runs <em>exactly</em> the same work as the first
    /// attempt. Under <see cref="PersistMode.SyncMessage"/> a retry replays the whole batch from its
    /// first message, including the ones that already succeeded and already checkpointed; that
    /// re-delivers rather than skips, which is the trade the whole design makes, and is why handlers
    /// must be idempotent.
    /// </remarks>
    private static async ValueTask InvokeAsync(
        PartitionContext ctx,
        StreamBatch batch,
        Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> handler,
        PositionRecorder positions,
        PersistMode persist,
        PositionFlush? flush,
        CancellationToken ct)
    {
        if (batch.Count == 0)
        {
            // Everything in this read was filtered out. There is nothing to hand a handler — under
            // any persist mode — but the position still advances past what was read. Going through
            // this method rather than round it is what puts the advance (and a Sync* flush's failure)
            // inside the caller's error handling; see the note there.
            await AdvanceAsync(ctx, batch.Last, positions, persist, flush, ct).ConfigureAwait(false);
            return;
        }

        if (persist == PersistMode.SyncMessage)
        {
            await HandlePerMessageAsync(ctx, batch, handler, positions, flush!, ct).ConfigureAwait(false);
            return;
        }

        await handler(batch.AsMemory(), ct).ConfigureAwait(false);

        // Step 3, and the only place a position is ever recorded on success.
        await AdvanceAsync(ctx, batch.Last, positions, persist, flush, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Blocks this partition on a failing batch and retries it indefinitely with capped exponential
    /// backoff, per <c>06-errors-and-observability.md</c>.
    /// </summary>
    /// <param name="first">The <see cref="DontIgnoreException"/> that started the block.</param>
    /// <param name="span">
    /// The partition's span identity, so each retry can open its own short <c>streams.process</c> span
    /// rather than the caller holding one open for the whole outage.
    /// </param>
    /// <returns>
    /// <see cref="BatchOutcome.Continue"/> once the batch finally succeeds (its position having been
    /// advanced by the successful attempt), or <see cref="BatchOutcome.Stop"/> if the host shuts down
    /// while blocked — in which case the position was never advanced and the batch redelivers.
    /// </returns>
    /// <remarks>
    /// <para>
    /// <b>Why block rather than skip or crash.</b> These are the failures where continuing is wrong:
    /// Redis unreachable, a downstream the handler has declared mandatory. Skipping loses data the
    /// exception exists to protect; faulting the pod turns a recoverable outage into a crash loop
    /// that makes reconnection harder. So the partition waits it out.
    /// </para>
    /// <para>
    /// <b>The rules.</b> Backoff 1 s → 2 → 4 → 8 → 16 → 30 s and then 30 s forever; there is no attempt
    /// limit. The position never advances, so nothing is skipped and the same batch is re-handled
    /// each attempt. Only this partition blocks — every other partition, and every other consumer,
    /// keeps running, because each has its own loop. Cancellation is honoured <em>during</em> the
    /// delay, so shutdown is never held up by more than the current delay.
    /// </para>
    /// <para>
    /// <b>Logging is rate limited</b> to the first failure (with the full exception) plus one line
    /// every <see cref="BlockLogIntervalMs"/>. A two-hour Redis outage is then ~240 log lines rather
    /// than one per retry per partition.
    /// </para>
    /// <para>
    /// <b>The risk this carries</b> is unbounded lag: an indefinite block plus producer-side
    /// <c>MAXLEN</c> trimming will eventually delete entries this consumer has not read. The
    /// <c>streams.blocked</c> and <c>streams.lag.*</c> alerts are what stand between that and real
    /// data loss, which is why the gauges are maintained here rather than left to a poller.
    /// </para>
    /// <para>
    /// <b>If a retry fails with something that is not a <see cref="DontIgnoreException"/></b>, the
    /// batch is no longer declared un-skippable and the ordinary <see cref="PartitionContext.OnError"/>
    /// policy takes over — the block ends rather than continuing forever on a different error.
    /// </para>
    /// </remarks>
    private static async ValueTask<BatchOutcome> BlockAndRetryAsync(
        PartitionContext ctx,
        StreamBatch batch,
        DontIgnoreException first,
        StreamSpanContext span,
        Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> handler,
        PositionRecorder positions,
        PersistMode persist,
        PositionFlush? flush,
        CancellationToken ct)
    {
        CountError(in ctx, "block", first);

        var key = BlockedKey(in ctx);
        var blockedAt = Stopwatch.GetTimestamp();
        var lastLoggedAt = blockedAt;
        var attempts = 1;
        var delayMs = BlockInitialDelayMs;

        StreamsDiagnostics.SetBlockedGauge(key, 1);

        // What turns the health check Degraded now and Unhealthy once UnhealthyBlockSeconds has
        // passed. The clock starts here and keeps running across every retry.
        ctx.Monitor?.MarkBlocked();

        ctx.Log.LogError(
            first,
            "Streams: handler threw {ExceptionType} (a DontIgnoreException) for topic={Topic} partition={Partition} consumer={Consumer} ids={FirstId}..{LastId} count={Count} type={MessageType}. " +
            "The position was NOT advanced and this partition is now BLOCKED, retrying the same batch with backoff (1s→2→4→8→16→30s cap, no attempt limit) until it succeeds. " +
            "Other partitions and consumers keep running. Further failures are logged once every {LogIntervalMs} ms.",
            first.GetType().Name,
            ctx.Topic,
            ctx.Partition,
            ctx.Consumer,
            FirstId(in batch),
            batch.Last.Format(),
            batch.Count,
            FirstType(in batch),
            BlockLogIntervalMs);

        try
        {
            while (true)
            {
                StreamsDiagnostics.SetBlockDurationMs(key, Stopwatch.GetElapsedTime(blockedAt).TotalMilliseconds);

                try
                {
                    await DelayAsync(delayMs, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Shutdown reached us mid-backoff, which is exactly why the delay takes the
                    // token. Nothing was advanced, so the batch redelivers on restart.
                    ctx.Log.LogInformation(
                        "Streams: shutting down while topic={Topic} partition={Partition} consumer={Consumer} was blocked — {Attempts} attempts over {ElapsedMs} ms. " +
                        "The position was never advanced, so this batch redelivers on restart.",
                        ctx.Topic,
                        ctx.Partition,
                        ctx.Consumer,
                        attempts,
                        (long)Stopwatch.GetElapsedTime(blockedAt).TotalMilliseconds);

                    return BatchOutcome.Stop;
                }

                delayMs = NextBlockDelayMs(delayMs);
                attempts++;

                var elapsed = Stopwatch.GetElapsedTime(blockedAt);
                StreamsDiagnostics.SetBlockDurationMs(key, elapsed.TotalMilliseconds);

                // R-10. One span and one duration sample per ATTEMPT. The alternative — the caller's
                // span held open across the whole block — produced multi-hour spans and folded the
                // outage into streams.batch.duration's p99.
                var attemptStarted = Stopwatch.GetTimestamp();
                var attempt = batch.IsEmpty
                    ? null
                    : StreamActivity.StartProcess(in span, batch.AsSpan(), batch.Last);

                try
                {
                    try
                    {
                        await InvokeAsync(ctx, batch, handler, positions, persist, flush, ct).ConfigureAwait(false);

                        if (!batch.IsEmpty)
                        {
                            CountConsumed(in ctx, batch.Count);
                        }

                        attempt?.SetStatus(ActivityStatusCode.Ok);

                        // Information, not Debug: the recovery is the other half of the Error above and
                        // is what tells an operator the outage is over.
                        ctx.Log.LogInformation(
                            "Streams: topic={Topic} partition={Partition} consumer={Consumer} recovered after {Attempts} attempts over {ElapsedMs} ms; the blocked batch was processed and the position advanced to {LastId}.",
                            ctx.Topic,
                            ctx.Partition,
                            ctx.Consumer,
                            attempts,
                            (long)Stopwatch.GetElapsedTime(blockedAt).TotalMilliseconds,
                            batch.Last.Format());

                        return BatchOutcome.Continue;
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        return BatchOutcome.Stop;
                    }
                    catch (DontIgnoreException ex)
                    {
                        CountError(in ctx, "block", ex);
                        attempt?.SetStatus(ActivityStatusCode.Error, ex.Message);

                        if (Stopwatch.GetElapsedTime(lastLoggedAt).TotalMilliseconds < BlockLogIntervalMs)
                        {
                            // Rate limit. A Redis outage must not emit a million log lines.
                            continue;
                        }

                        lastLoggedAt = Stopwatch.GetTimestamp();

                        // No exception object on the periodic line — the first log carries the stack, and
                        // repeating it every 30 s buys nothing but bytes.
                        ctx.Log.LogError(
                            "Streams: topic={Topic} partition={Partition} consumer={Consumer} is still BLOCKED on ids={FirstId}..{LastId} after {Attempts} attempts over {ElapsedMs} ms — {ExceptionType}: {ExceptionMessage}. Next retry in {DelayMs} ms.",
                            ctx.Topic,
                            ctx.Partition,
                            ctx.Consumer,
                            FirstId(in batch),
                            batch.Last.Format(),
                            attempts,
                            (long)Stopwatch.GetElapsedTime(blockedAt).TotalMilliseconds,
                            ex.GetType().Name,
                            ex.Message,
                            delayMs);
                    }
                    catch (Exception ex)
                    {
                        // A different failure: this is no longer a batch the handler declared
                        // un-skippable, so the ordinary error policy decides, and the block ends.
                        var outcome = await OnFailureAsync(ctx, batch, ex, attempt, positions, persist, flush, ct)
                            .ConfigureAwait(false);

                        if (outcome == BatchOutcome.Fault)
                        {
                            // ErrorPolicy.Fail, with the stack the exception actually has: a bare
                            // `throw ex` here would rewrite it to this line.
                            ExceptionDispatchInfo.Capture(ex).Throw();
                        }

                        return outcome;
                    }
                }
                finally
                {
                    RecordBatchMetrics(in ctx, batch.Count, attemptStarted);
                    attempt?.Dispose();
                }
            }
        }
        finally
        {
            // Zero removes the series rather than pinning it at its last value: an unblocked
            // partition should stop reporting, not report "blocked for 4 hours" forever.
            // Only when the partition is still blocked: a retry that failed with an ordinary
            // exception has already marked it stopped, and unblocking would put it back to Running.
            if (ctx.Monitor is { State: PartitionRunState.Blocked } monitor)
            {
                monitor.MarkUnblocked();
            }

            StreamsDiagnostics.SetBlockedGauge(key, 0);
            StreamsDiagnostics.SetBlockDurationMs(key, 0);
        }
    }

    /// <summary>
    /// P3-03. The startup checks, which fail fast: a <see cref="StreamConfigurationException"/> raised
    /// here faults the worker and through it the host, and never enters the blocking-retry loop.
    /// </summary>
    /// <remarks>
    /// It is a <see cref="DontIgnoreException"/> like any other, but no amount of backoff fixes a
    /// config file — retrying would leave a misconfigured pod alive, Ready, and consuming nothing.
    /// Failing the pod is the correct signal. Only a <see cref="DontIgnoreException"/> thrown at
    /// <em>runtime</em>, from a handler or the transport, is retried.
    /// </remarks>
    private static void ValidateStartup(
        in PartitionContext ctx,
        [NotNull] ChannelReader<StreamBatch>? reader,
        PersistMode persist,
        PositionFlush? flush)
    {
        if (reader is null)
        {
            throw new StreamConfigurationException(
                $"Streams: the processing loop for topic '{ctx.Topic}' partition {ctx.Partition} was started without a " +
                "channel reader, so it would consume nothing while still looking like a healthy worker. " +
                "The consumer host must pass the read side of the partition's channel to ProcessLoopAsync.");
        }

        if (persist is PersistMode.SyncBatch or PersistMode.SyncMessage && flush is null)
        {
            throw new StreamConfigurationException(
                $"Streams: PersistMode.{persist} was configured for topic '{ctx.Topic}' partition {ctx.Partition} " +
                "but no synchronous position flush was supplied, so the mode would silently behave like AsyncBatch. " +
                "Pass PositionFlusher.FlushAsync, or use PersistMode.AsyncBatch.");
        }
    }

    /// <summary>The first blocking-retry delay, in milliseconds.</summary>
    internal const int BlockInitialDelayMs = 1_000;

    /// <summary>The blocking-retry delay ceiling, in milliseconds. There is no attempt limit — only this cap.</summary>
    internal const int BlockMaxDelayMs = 30_000;

    /// <summary>At most one Error line per this many milliseconds while a partition stays blocked.</summary>
    internal const int BlockLogIntervalMs = 30_000;

    /// <summary>
    /// The blocking-retry schedule: <c>1000 → 2000 → 4000 → 8000 → 16000 → 30000</c> ms and then
    /// 30 000 forever.
    /// </summary>
    /// <param name="current">The delay just used, or zero/less for "none yet".</param>
    /// <returns>The next delay, doubled and clamped to <see cref="BlockMaxDelayMs"/>.</returns>
    /// <remarks>
    /// Separate from <see cref="Backoff"/>, which is the sub-millisecond idle-poll ladder: this one
    /// starts at a second and is capped in tens of seconds, and conflating the two would make one of
    /// them wrong.
    /// </remarks>
    internal static int NextBlockDelayMs(int current)
    {
        if (current <= 0)
        {
            return BlockInitialDelayMs;
        }

        // long so an absurd caller-supplied current cannot overflow into a negative delay.
        var next = (long)current * 2L;

        return next >= BlockMaxDelayMs ? BlockMaxDelayMs : (int)next;
    }

    /// <summary>
    /// Scopes a substituted backoff delay to one async flow, so a test cannot shorten the delays of
    /// unrelated code running in parallel.
    /// </summary>
    private static readonly AsyncLocal<Func<int, CancellationToken, Task>?> BlockDelaySubstitute = new();

    /// <summary>
    /// The delay between blocking retries. Always <see cref="Task.Delay(int, CancellationToken)"/> in
    /// production; the setter exists so a unit test can assert the 1s→30s schedule without spending
    /// a real minute doing it, which cannot be observed any other way.
    /// </summary>
    /// <remarks>
    /// <see cref="AsyncLocal{T}"/> rather than a plain static, for the same reason
    /// <c>StreamBatch.Pool</c> is: a process-global swap would be visible to every other test running
    /// in parallel. Production never sets it, so the read is a null check on an uncontended slot.
    /// A substitute must still honour the token — shutdown responsiveness depends on it.
    /// </remarks>
    internal static Func<int, CancellationToken, Task>? BlockDelay
    {
        get => BlockDelaySubstitute.Value;
        set => BlockDelaySubstitute.Value = value;
    }

    private static Task DelayAsync(int milliseconds, CancellationToken ct)
    {
        var substitute = BlockDelaySubstitute.Value;

        return substitute is null ? Task.Delay(milliseconds, ct) : substitute(milliseconds, ct);
    }

    /// <summary>
    /// Whether a fetch failure is a transport outage to be waited out rather than a fault that ends
    /// the partition.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Deliberately narrow. A connection that dropped, a command that timed out, and a server that is
    /// loading, failing over or otherwise temporarily refusing reads all resolve on their own, and
    /// <c>06-errors-and-observability.md</c> lists "Redis unreachable, <c>XREAD</c> failing" as the
    /// canonical block-and-retry case. Everything else — a malformed reply, a decode failure, a
    /// configuration error — is a bug or a corruption, and retrying it forever would spin the reader
    /// at full speed against a problem no amount of waiting fixes.
    /// </para>
    /// <para>
    /// <see cref="RedisServerException"/> is only transient for the handful of error prefixes Redis
    /// uses for "not right now": a <c>WRONGTYPE</c> or a syntax error is permanent and must surface.
    /// </para>
    /// </remarks>
    /// <param name="ex">The exception the fetch threw.</param>
    /// <returns><see langword="true"/> when the read should be retried after a backoff.</returns>
    internal static bool IsTransientTransportFailure(Exception ex)
        => ex switch
        {
            RedisConnectionException => true,
            RedisTimeoutException => true,
            RedisServerException server => IsTemporaryServerError(server.Message),
            _ => false,
        };

    /// <summary>Redis error prefixes that mean "try again shortly", not "this will never work".</summary>
    private static bool IsTemporaryServerError(string message)
        => message.StartsWith("LOADING", StringComparison.Ordinal)
            || message.StartsWith("MASTERDOWN", StringComparison.Ordinal)
            || message.StartsWith("CLUSTERDOWN", StringComparison.Ordinal)
            || message.StartsWith("TRYAGAIN", StringComparison.Ordinal)
            || message.StartsWith("BUSY", StringComparison.Ordinal);

    /// <summary>
    /// Waits out one failed fetch and returns the delay the next failure should use.
    /// </summary>
    /// <remarks>
    /// The same 1 s → 30 s ladder the handler-side block uses, and the same gauges: a partition that
    /// cannot read is blocked in exactly the sense the health check means, so it reports Degraded
    /// immediately and Unhealthy once <c>UnhealthyBlockSeconds</c> has passed. The first failure of
    /// an outage logs at Error with the exception; the rest log at Debug, because a five-minute
    /// outage must not write one Error line per retry per partition.
    /// </remarks>
    /// <param name="ctx">The partition's context.</param>
    /// <param name="ex">The failure just caught.</param>
    /// <param name="currentDelayMs">The delay used by the previous failure, or zero for the first.</param>
    /// <param name="ct">The host token; cancelled during the wait means shutdown.</param>
    /// <returns>The delay to pass back in on the next failure.</returns>
    /// <exception cref="OperationCanceledException">The host is shutting down.</exception>
    private static async ValueTask<int> WaitOutTransportFailureAsync(
        PartitionContext ctx,
        Exception ex,
        int currentDelayMs,
        CancellationToken ct)
    {
        var delayMs = currentDelayMs == 0 ? BlockInitialDelayMs : NextBlockDelayMs(currentDelayMs);

        if (currentDelayMs == 0)
        {
            ctx.Monitor?.MarkBlocked();
            CountError(in ctx, "transport", ex);

            ctx.Log.LogError(
                ex,
                "Streams: reading topic={Topic} partition={Partition} consumer={Consumer} failed with {ExceptionType}. " +
                "The connection is treated as a transient outage: this partition is BLOCKED and will retry the read with backoff " +
                "(1s→2→4→8→16→30s cap, no attempt limit). The position was not advanced, so nothing is skipped, and " +
                "other partitions keep running. Further attempts are logged at Debug until it recovers.",
                ctx.Topic,
                ctx.Partition,
                ctx.Consumer,
                ex.GetType().Name);
        }
        else
        {
            ctx.Log.LogDebug(
                ex,
                "Streams: retrying the read for topic={Topic} partition={Partition} consumer={Consumer} in {DelayMs} ms.",
                ctx.Topic,
                ctx.Partition,
                ctx.Consumer,
                delayMs);
        }

        await DelayAsync(delayMs, ct).ConfigureAwait(false);

        return delayMs;
    }

    /// <summary>
    /// Clears an outage after a fetch finally succeeds, and says so once.
    /// </summary>
    /// <param name="ctx">The partition's context.</param>
    /// <param name="outageDelayMs">The outage backoff, or zero when the connection was never lost.</param>
    /// <returns>Zero — the value the loop carries while the connection is healthy.</returns>
    private static int ResumeAfterOutage(in PartitionContext ctx, int outageDelayMs)
    {
        if (outageDelayMs == 0)
        {
            return 0;
        }

        ctx.Monitor?.MarkRunning();

        // Information, not Debug: it is the other half of the Error above, and the line that tells an
        // operator the outage is over.
        ctx.Log.LogInformation(
            "Streams: reading topic={Topic} partition={Partition} consumer={Consumer} recovered; the read resumed from the " +
            "unchanged cursor, so nothing published during the outage was skipped.",
            ctx.Topic,
            ctx.Partition,
            ctx.Consumer);

        return 0;
    }

    /// <summary>The <c>topic:partition:consumer</c> series key the per-partition gauges are stored under.</summary>
    /// <remarks>Allocated once per blocked episode, never per batch and never per message.</remarks>
    private static string BlockedKey(in PartitionContext ctx)
        => $"{ctx.Topic}:{ctx.Partition}:{ctx.Consumer}";

    private static void CountConsumed(in PartitionContext ctx, int count)
        => StreamsDiagnostics.StreamsConsumed.Add(count, TopicTag(in ctx), PartitionTag(in ctx), ConsumerTag(in ctx));

    private static KeyValuePair<string, object?> TopicTag(in PartitionContext ctx) => new("topic", ctx.Topic);

    private static KeyValuePair<string, object?> PartitionTag(in PartitionContext ctx) => new("partition", ctx.Partition);

    private static KeyValuePair<string, object?> ConsumerTag(in PartitionContext ctx) => new("consumer", ctx.Consumer);

    /// <summary>Counts one processing error with the four tags the dashboards group by.</summary>
    /// <remarks><c>TagList</c> rather than the three-tag overload: it is a struct, so four tags still allocate nothing.</remarks>
    private static void CountError(in PartitionContext ctx, string policy, Exception ex)
    {
        var tags = new TagList
        {
            { "topic", ctx.Topic },
            { "consumer", ctx.Consumer },
            { "policy", policy },
            { "exception.type", ex.GetType().Name },
        };

        StreamsDiagnostics.StreamsErrors.Add(1, tags);
    }

    private static string FirstId(in StreamBatch batch)
        => batch.Count == 0 ? batch.Last.Format() : batch.AsSpan()[0].Id.Format();

    private static string FirstType(in StreamBatch batch)
        => batch.Count == 0 ? string.Empty : batch.AsSpan()[0].Type;
}
