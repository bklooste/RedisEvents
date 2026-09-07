using System.Buffers;
using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Diagnostics;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Wire;

namespace Orange.Lib.Streams.Producer;

/// <summary>
/// Buffers publishes behind a bounded queue and writes them in batches from one background pump.
/// </summary>
/// <remarks>
/// <para>
/// The shape is a single-consumer pipeline:
/// <c>caller → bounded Channel&lt;PendingEntry&gt; → pump → grouped by partition → pipelined XADD → flush</c>.
/// One pump means the flush is single-threaded and needs no locking, and a batch of 500 entries
/// costs one round trip's latency rather than 500.
/// </para>
/// <para>
/// <b>Trigger.</b> A flush fires at <see cref="ProducerOptions.MaxBatch"/> entries or
/// <see cref="ProducerOptions.MaxWaitMs"/> after the <em>oldest</em> waiting entry was enqueued,
/// whichever comes first. Measuring from the oldest entry rather than from the start of the pump's
/// wait is what bounds the latency a caller actually sees: an entry cannot sit behind a busy pump
/// and then start its own fresh timer.
/// </para>
/// <para>
/// <b>Backpressure.</b> The queue is bounded at <see cref="ProducerOptions.MaxQueue"/> and full
/// means wait. A producer outrunning Redis is slowed down at the enqueue call, which is visible and
/// bounded, rather than being allowed to grow the queue until the pod is OOM-killed. Setting
/// <see cref="ProducerOptions.DropOldest"/> switches to shedding the oldest entry instead — correct
/// only for pure telemetry, where the newest value supersedes the one being dropped — and every
/// drop is counted on <c>streams.buffer.dropped</c> and readable from <see cref="DroppedCount"/>.
/// </para>
/// <para>
/// <b>The copy.</b> <see cref="EnqueueAsync"/> copies the body into a pooled buffer. This is the one
/// unavoidable copy in the library's design: the caller's memory belongs to the caller again the
/// moment the call returns, and a pump reading it milliseconds later would publish whatever the
/// caller has since written there. The buffer goes back to the pool once Redis has acknowledged the
/// entry — never before, because StackExchange.Redis reads the bytes as it writes the command out.
/// The direct <see cref="PublishAsync"/> path has no such copy.
/// </para>
/// <para>
/// <b>Loss.</b> Anything still in the queue when the process dies is lost — see
/// <see cref="IStreamBufferedPublisher"/>. Shutdown flushes with a bounded timeout (5 s by default)
/// and logs a warning naming the count it abandoned, so the loss appears in the logs rather than
/// only in a consumer's gap.
/// </para>
/// </remarks>
internal sealed class BufferedStreamPublisher : IStreamBufferedPublisher, IHostedService, IAsyncDisposable
{
    /// <summary>Default bound on the shutdown flush. Long enough for a full queue, short enough not to hold up a pod.</summary>
    public static readonly TimeSpan DefaultShutdownFlushTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Partition keys up to this many UTF-8 bytes are hashed without renting.</summary>
    private const int StackKeyBytes = 256;

    private readonly IStreamPublisher inner;
    private readonly Channel<PendingEntry> channel;
    private readonly ILogger? log;
    private readonly Lock gate = new();

    private readonly string topic;
    private readonly int partitions;
    private readonly int maxBatch;
    private readonly long maxWaitStamps;
    private readonly TimeSpan shutdownFlushTimeout;
    private readonly KeyValuePair<string, object?> topicTag;

    // Round-robin cursor for keyless publishes; advanced with Interlocked by PartitionRouter.
    private uint roundRobin;

    // Touched only by the pump thread, so it needs no synchronisation.
    private CancellationTokenSource? deadlineSource;

    private long dropped;
    private long published;
    private CancellationTokenSource? cts;
    private Task? pump;
    private volatile bool stopping;
    private bool disposed;

    /// <summary>
    /// Creates a buffered publisher over a non-buffered one. Nothing starts until the first enqueue
    /// or <see cref="StartAsync"/>, whichever happens first.
    /// </summary>
    /// <param name="inner">
    /// The direct publisher the pump writes through, and the implementation behind the inherited
    /// <see cref="PublishAsync"/> and <see cref="PublishBatchAsync"/>. Owning the encode, route and
    /// trim logic in exactly one place is the point: the pump adds batching, nothing else.
    /// </param>
    /// <param name="options">
    /// Producer options. <see cref="ProducerOptions.Topic"/> is required and the queue, batch and
    /// wait knobs come from here.
    /// </param>
    /// <param name="topic">
    /// The topic's options, for the partition count used to group a flush. Defaults are used when
    /// <see langword="null"/>, matching what an unconfigured topic gets elsewhere.
    /// </param>
    /// <param name="logger">Logger for flush failures, drops and abandoned entries. Optional, but a service without one is flying blind on loss.</param>
    /// <param name="shutdownFlushTimeout">
    /// How long <see cref="StopAsync"/> spends draining before giving up on the rest.
    /// Defaults to <see cref="DefaultShutdownFlushTimeout"/>.
    /// </param>
    /// <exception cref="StreamConfigurationException">A producer option is out of range, or the topic is missing.</exception>
    public BufferedStreamPublisher(
        IStreamPublisher inner,
        ProducerOptions options,
        TopicOptions? topic = null,
        ILogger? logger = null,
        TimeSpan? shutdownFlushTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(options);

        var topicOptions = topic ?? new TopicOptions();

        if (string.IsNullOrWhiteSpace(options.Topic))
        {
            throw new StreamConfigurationException("Streams: a buffered producer needs a Topic; none was configured.");
        }

        Require(options.MaxBatch > 0, options.Topic, nameof(options.MaxBatch), options.MaxBatch, "must be at least 1");
        Require(options.MaxQueue > 0, options.Topic, nameof(options.MaxQueue), options.MaxQueue, "must be at least 1");
        Require(options.MaxWaitMs >= 0, options.Topic, nameof(options.MaxWaitMs), options.MaxWaitMs, "cannot be negative");
        Require(topicOptions.Partitions > 0, options.Topic, nameof(TopicOptions.Partitions), topicOptions.Partitions, "must be at least 1");

        this.inner = inner;
        this.log = logger;
        this.topic = options.Topic;
        this.partitions = topicOptions.Partitions;
        this.maxBatch = options.MaxBatch;
        this.maxWaitStamps = (long)(Stopwatch.Frequency * (options.MaxWaitMs / 1000d));
        this.shutdownFlushTimeout = shutdownFlushTimeout ?? DefaultShutdownFlushTimeout;
        this.topicTag = new KeyValuePair<string, object?>("topic", options.Topic);

        var bounded = new BoundedChannelOptions(options.MaxQueue)
        {
            // One pump, many callers.
            SingleReader = true,
            SingleWriter = false,
            FullMode = options.DropOldest ? BoundedChannelFullMode.DropOldest : BoundedChannelFullMode.Wait,
        };

        // The drop callback is the only place a dropped entry is ever seen again: without it the
        // pooled buffer would leak and a waiter on a dropped flush marker would hang forever.
        this.channel = Channel.CreateBounded<PendingEntry>(bounded, this.OnDropped);
    }

    /// <summary>The topic every enqueued message is published to.</summary>
    public string Topic => this.topic;

    /// <summary>Entries currently waiting in the queue. A depth pinned at the bound means the producer is outrunning Redis.</summary>
    public int QueueDepth => this.channel.Reader.Count;

    /// <summary>Entries shed because the queue was full. Always zero unless <see cref="ProducerOptions.DropOldest"/> is set.</summary>
    public long DroppedCount => Interlocked.Read(ref this.dropped);

    /// <summary>Entries the pump has successfully written to Redis.</summary>
    public long PublishedCount => Interlocked.Read(ref this.published);

    /// <inheritdoc />
    public ValueTask<StreamId> PublishAsync(
        string partitionKey,
        ReadOnlyMemory<byte> body,
        string type,
        PublishOptions options = default,
        CancellationToken ct = default)
        => this.inner.PublishAsync(partitionKey, body, type, options, ct);

    /// <inheritdoc />
    public ValueTask PublishBatchAsync(
        string partitionKey,
        ReadOnlyMemory<ReadOnlyMemory<byte>> bodies,
        string type,
        PublishOptions options = default,
        CancellationToken ct = default)
        => this.inner.PublishBatchAsync(partitionKey, bodies, type, options, ct);

    /// <inheritdoc />
    public ValueTask EnqueueAsync(
        string partitionKey,
        ReadOnlyMemory<byte> body,
        string type,
        PublishOptions options = default,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(type);
        ObjectDisposedException.ThrowIf(this.disposed, this);

        this.EnsurePump();

        var key = partitionKey ?? string.Empty;
        var partition = this.ResolvePartition(key, options.Partition);

        // The copy. Everything else in this method is bookkeeping.
        var buffer = ArrayPool<byte>.Shared.Rent(body.Length);
        body.Span.CopyTo(buffer);

        var entry = new PendingEntry(
            buffer,
            body.Length,
            key,
            type,
            options with { Partition = partition },
            partition,
            Stopwatch.GetTimestamp());

        return this.channel.Writer.TryWrite(entry)
            ? ValueTask.CompletedTask
            : this.WriteWhenSpaceAsync(entry, ct);
    }

    /// <inheritdoc />
    /// <remarks>
    /// <para>
    /// <b>Attribution.</b> The marker takes its place in the queue and records how many entries were
    /// already ahead of it in the batch it lands in. When the flush writes that batch, a waiter faults
    /// only if one of the entries it actually covers failed, and it faults with <em>that</em>
    /// exception; a waiter whose entries all landed completes normally even though a later entry in
    /// the same batch failed. Faulting every concurrent waiter on any failure — which is what this
    /// used to do — makes the result meaningless for anyone using it to decide whether their own
    /// message is safe.
    /// </para>
    /// <para>
    /// <b>The one thing it cannot see.</b> Coverage is per flush cycle. A message enqueued before the
    /// marker but written in an <em>earlier</em> batch has already been reported — to whoever was
    /// flushing then, and to the log — and a later marker does not re-raise it.
    /// </para>
    /// <para>
    /// <b>The <see cref="ProducerOptions.DropOldest"/> hole.</b> Under <c>DropOldest</c> a full queue
    /// sheds its oldest entry, and that entry can be a flush marker. A shed marker covered nothing and
    /// can promise nothing, so it faults with an <see cref="InvalidOperationException"/> rather than
    /// completing as if the queue had been written — silent success there would let a caller treat
    /// shed data as durable. Entries shed from <em>ahead</em> of a surviving marker are a quieter
    /// version of the same hole: they are counted in <see cref="DroppedCount"/> and
    /// <c>streams.buffer.dropped</c>, and the flush that follows them completes normally because
    /// nothing it covers failed. <c>DropOldest</c> is opt-in and means "shed rather than slow down";
    /// nothing enqueued through it is durable. Use the direct publish path or the outbox for anything
    /// that matters.
    /// </para>
    /// <para>
    /// A flush abandoned by <see cref="StopAsync"/>'s timeout completes normally: shutdown loss is
    /// reported once, at Warning, with the count, rather than as an exception per waiter on the way
    /// out.
    /// </para>
    /// </remarks>
    public async ValueTask FlushAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        this.EnsurePump();

        // Continuations run off the pump thread: a caller resuming inline would otherwise do its
        // work inside the flush loop and stall every other message behind it.
        var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var marker = PendingEntry.Barrier(barrier);

        if (!this.channel.Writer.TryWrite(marker))
        {
            await this.channel.Writer.WriteAsync(marker, ct).ConfigureAwait(false);
        }

        await barrier.Task.WaitAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Starts the pump. Idempotent, and a no-op if an enqueue already started it.</summary>
    /// <param name="cancellationToken">Unused; the pump's lifetime is bound to <see cref="StopAsync"/>.</param>
    /// <returns>A completed task — the pump runs in the background.</returns>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);
        this.EnsurePump();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Stops accepting enqueues and drains what is queued, giving up after the shutdown timeout.
    /// </summary>
    /// <remarks>
    /// Anything still queued when the timeout expires is abandoned and logged at Warning with its
    /// count — that log line is the only trace those messages will ever leave, so it names the topic
    /// and says plainly that the entries were lost.
    /// </remarks>
    /// <param name="cancellationToken">Cuts the drain short; the remainder is abandoned and logged.</param>
    /// <returns>A task that completes when the drain has finished or been abandoned.</returns>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        this.stopping = true;

        // No further enqueues; the pump exits once it has drained what is already queued.
        this.channel.Writer.TryComplete();

        var running = Volatile.Read(ref this.pump);
        if (running is null)
        {
            this.DrainAbandoned();
            return;
        }

        try
        {
            await running.WaitAsync(this.shutdownFlushTimeout, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            this.AbandonRemainder($"the shutdown flush did not finish within {this.shutdownFlushTimeout}");
        }
        catch (OperationCanceledException)
        {
            this.AbandonRemainder("shutdown was cancelled");
        }
        catch (Exception ex)
        {
            this.log?.LogError(ex, "Streams: the buffered publisher's pump for topic '{Topic}' faulted during shutdown.", this.topic);
            this.DrainAbandoned();
        }
    }

    /// <summary>Stops the pump and releases the queue. Safe to call more than once.</summary>
    /// <returns>A task that completes once the shutdown drain has finished.</returns>
    public async ValueTask DisposeAsync()
    {
        if (this.disposed)
        {
            return;
        }

        this.disposed = true;

        await this.StopAsync(CancellationToken.None).ConfigureAwait(false);

        // Only once the pump is definitely finished with it: a pump still parked on a hung publish
        // would see an ObjectDisposedException from its own deadline source.
        var running = Volatile.Read(ref this.pump);
        if (running is null || running.IsCompleted)
        {
            this.cts?.Dispose();
            this.cts = null;
        }
    }

    private static void Require(bool condition, string topic, string name, int value, string requirement)
    {
        if (!condition)
        {
            throw new StreamConfigurationException(
                $"Streams: producer '{topic}' has {name} = {value}, which {requirement}.");
        }
    }

    private static void Release(in PendingEntry entry)
    {
        if (entry.Body is not null)
        {
            ArrayPool<byte>.Shared.Return(entry.Body);
        }
    }

    /// <summary>
    /// Groups a batch by partition with a counting sort: <c>order</c> comes out holding the batch
    /// indices, partition by partition, with the original order preserved inside each partition.
    /// Stability is not cosmetic — it is what keeps two messages on one key in the order they were
    /// enqueued.
    /// </summary>
    private static void GroupByPartition(PendingEntry[] batch, int count, int[] order, int[] offsets)
    {
        Array.Clear(offsets);

        for (var i = 0; i < count; i++)
        {
            offsets[batch[i].Partition]++;
        }

        var running = 0;
        for (var p = 0; p < offsets.Length; p++)
        {
            var size = offsets[p];
            offsets[p] = running;
            running += size;
        }

        for (var i = 0; i < count; i++)
        {
            order[offsets[batch[i].Partition]++] = i;
        }
    }

    private async ValueTask WriteWhenSpaceAsync(PendingEntry entry, CancellationToken ct)
    {
        try
        {
            await this.channel.Writer.WriteAsync(entry, ct).ConfigureAwait(false);
        }
        catch
        {
            // The entry never made it into the queue, so nobody else will return its buffer.
            Release(entry);
            throw;
        }
    }

    private void OnDropped(PendingEntry entry)
    {
        if (entry.Waiter is not null)
        {
            // A shed flush marker covered nothing: the entries it was queued behind are still in the
            // queue, and it will never be reached by a flush. Completing it would tell the caller
            // "everything before this is in Redis", which is exactly false. Fault instead — leaving
            // the caller blocked forever is the only worse answer.
            entry.Waiter.TrySetException(new InvalidOperationException(
                $"Streams: the flush marker for topic '{this.topic}' was shed because the buffer was full and DropOldest is set, " +
                "so the flush covered nothing and cannot report on the entries queued before it. " +
                "DropOldest trades durability for latency; use the direct publish path or the outbox for anything that matters."));
            return;
        }

        Release(entry);

        var total = Interlocked.Increment(ref this.dropped);
        StreamsDiagnostics.StreamsBufferDropped.Add(1, this.topicTag);

        // One line per power-of-two so a sustained overflow is visible without becoming the log.
        if ((total & (total - 1)) == 0)
        {
            this.log?.LogWarning(
                "Streams: buffered publisher for topic '{Topic}' dropped an entry because its queue was full (DropOldest is on); {Dropped} dropped so far.",
                this.topic,
                total);
        }
    }

    private void EnsurePump()
    {
        if (Volatile.Read(ref this.pump) is not null)
        {
            return;
        }

        lock (this.gate)
        {
            if (this.pump is not null)
            {
                return;
            }

            this.cts = new CancellationTokenSource();
            var token = this.cts.Token;

            // Task.Run rather than an inline call: the first enqueue must not run the pump's
            // synchronous prologue on the caller's thread.
            this.pump = Task.Run(() => this.PumpAsync(token), CancellationToken.None);
        }
    }

    private int ResolvePartition(string key, int? explicitPartition)
    {
        if (explicitPartition is { } chosen)
        {
            if ((uint)chosen >= (uint)this.partitions)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(explicitPartition),
                    chosen,
                    $"Streams: topic '{this.topic}' has {this.partitions} partition(s); partition {chosen} does not exist.");
            }

            return chosen;
        }

        if (key.Length == 0)
        {
            return PartitionRouter.RoundRobin(ref this.roundRobin, this.partitions);
        }

        var upper = Encoding.UTF8.GetMaxByteCount(key.Length);
        byte[]? rented = upper > StackKeyBytes ? ArrayPool<byte>.Shared.Rent(upper) : null;
        Span<byte> scratch = rented ?? stackalloc byte[StackKeyBytes];

        var written = Encoding.UTF8.GetBytes(key, scratch);
        var partition = PartitionRouter.ForKey(scratch[..written], this.partitions);

        if (rented is not null)
        {
            ArrayPool<byte>.Shared.Return(rented);
        }

        return partition;
    }

    private async Task PumpAsync(CancellationToken ct)
    {
        var reader = this.channel.Reader;
        Exception? pumpFailure = null;

        // Every buffer the pump needs is allocated once, here: a flush allocates nothing.
        var batch = new PendingEntry[this.maxBatch];
        var order = new int[this.maxBatch];
        var offsets = new int[this.partitions];
        var results = new ValueTask<StreamId>[this.maxBatch];

        // Per-entry outcome, indexed by position in `batch`. Allocated once with everything else so
        // a flush still allocates nothing; cleared by FlushBatchAsync as it reads it.
        var failures = new Exception?[this.maxBatch];
        var waiters = new List<FlushWaiter>();

        try
        {
            while (!ct.IsCancellationRequested)
            {
                bool more;
                try
                {
                    more = await reader.WaitToReadAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (!more)
                {
                    // Writer completed and the queue is empty: a clean end.
                    break;
                }

                var count = 0;
                waiters.Clear();

                while (count < this.maxBatch)
                {
                    if (reader.TryRead(out var entry))
                    {
                        if (entry.Waiter is not null)
                        {
                            // `count` is exactly the entries this marker was queued behind in this
                            // batch, and so exactly what it may be faulted for. See FlushAsync.
                            waiters.Add(new FlushWaiter(entry.Waiter, count));
                        }
                        else
                        {
                            batch[count++] = entry;
                        }

                        continue;
                    }

                    if (count == 0 && waiters.Count == 0)
                    {
                        break;
                    }

                    // Somebody is waiting on a flush: go now rather than sit out the timer.
                    if (waiters.Count > 0)
                    {
                        break;
                    }

                    // Time trigger, measured from the oldest entry in hand rather than from now.
                    var remaining = (batch[0].Stamp + this.maxWaitStamps) - Stopwatch.GetTimestamp();
                    if (remaining <= 0)
                    {
                        break;
                    }

                    var waitMs = (int)Math.Clamp((remaining * 1000L) / Stopwatch.Frequency, 1, int.MaxValue);
                    if (!await this.WaitForMoreAsync(reader, waitMs, ct).ConfigureAwait(false))
                    {
                        break;
                    }
                }

                if (count > 0 || waiters.Count > 0)
                {
                    await this.FlushBatchAsync(batch, count, order, offsets, results, failures, waiters).ConfigureAwait(false);
                }

                // Once per flush cycle, not once per message: the gauge is a depth, and writing it
                // from EnqueueAsync would put a dictionary write on the per-message path.
                StreamsDiagnostics.SetBufferQueued(this.topic, this.channel.Reader.Count);
            }
        }
        catch (Exception ex) when (this.stopping && ex is OperationCanceledException or ObjectDisposedException)
        {
            // The shutdown deadline cut the pump short. StopAsync logs what was abandoned; a second
            // scary line here would only obscure it.
            this.channel.Writer.TryComplete();
        }
        catch (Exception ex)
        {
            // The pump is the only thing draining the queue, so a dead pump means every later
            // enqueue would pile up silently. Close the channel and let callers see it instead.
            pumpFailure = ex;

            this.log?.LogCritical(
                ex,
                "Streams: the buffered publisher's pump for topic '{Topic}' stopped unexpectedly; buffered publishing on this topic is over for the life of the process.",
                this.topic);

            this.channel.Writer.TryComplete(ex);
        }
        finally
        {
            this.deadlineSource?.Dispose();
            this.deadlineSource = null;

            // Waiters still in hand when the pump left the loop. A crash means their entries were
            // never written, so they get the crash rather than a success they cannot rely on; a
            // clean stop means the drain simply ended, which StopAsync reports on its own terms.
            foreach (var waiter in waiters)
            {
                if (pumpFailure is null)
                {
                    waiter.Waiter.TrySetResult();
                }
                else
                {
                    waiter.Waiter.TrySetException(pumpFailure);
                }
            }

            this.DrainAbandoned();
            StreamsDiagnostics.ClearBufferQueued(this.topic);
        }
    }

    /// <summary>
    /// Waits for another entry, giving up when the batch's time trigger comes due. Returns
    /// <see langword="false"/> when the deadline passed, the channel completed, or we are shutting
    /// down — every one of which means "flush what you have".
    /// </summary>
    private async ValueTask<bool> WaitForMoreAsync(
        ChannelReader<PendingEntry> reader,
        int waitMs,
        CancellationToken ct)
    {
        // One source, reset per wait: a fresh linked source per partial batch would be pure garbage
        // at 50 flushes a second per topic.
        var source = this.deadlineSource;
        if (source is null || !source.TryReset())
        {
            source?.Dispose();
            source = CancellationTokenSource.CreateLinkedTokenSource(ct);
            this.deadlineSource = source;
        }

        source.CancelAfter(waitMs);

        try
        {
            return await reader.WaitToReadAsync(source.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    /// Writes one batch and settles the flush markers that landed in it.
    /// </summary>
    /// <remarks>
    /// Failure is attributed per entry, not per batch. Each pipelined <c>XADD</c> has its own
    /// outcome, so a waiter is faulted only when an entry it actually covers failed — see
    /// <see cref="FlushAsync"/>. <paramref name="failures"/> is a scratch buffer owned by the pump
    /// and is left cleared on the way out.
    /// </remarks>
    /// <param name="batch">Entries in enqueue order.</param>
    /// <param name="count">How many of <paramref name="batch"/> are live.</param>
    /// <param name="order">Scratch: batch indices grouped by partition.</param>
    /// <param name="offsets">Scratch: per-partition counting-sort offsets.</param>
    /// <param name="results">Scratch: the pipelined publish tasks, in issue order.</param>
    /// <param name="failures">Scratch: per-batch-index outcome, null where the entry was written.</param>
    /// <param name="waiters">Flush markers that landed in this batch, in queue order.</param>
    /// <returns>A task that completes once the batch is written and every waiter is settled.</returns>
    private async Task FlushBatchAsync(
        PendingEntry[] batch,
        int count,
        int[] order,
        int[] offsets,
        ValueTask<StreamId>[] results,
        Exception?[] failures,
        List<FlushWaiter> waiters)
    {
        Exception? anyFailure = null;
        var failed = 0;
        var issued = 0;

        if (count > 0)
        {
            GroupByPartition(batch, count, order, offsets);

            Exception? synchronous = null;

            try
            {
                // Issue without awaiting: the commands pipeline onto the multiplexer, so the whole
                // batch costs one round trip. Each entry already carries its resolved partition, so
                // grouping puts one partition's XADDs together on the wire.
                for (; issued < count; issued++)
                {
                    var entry = batch[order[issued]];
                    results[issued] = this.inner.PublishAsync(
                        entry.PartitionKey,
                        new ReadOnlyMemory<byte>(entry.Body!, 0, entry.Length),
                        entry.Type,
                        entry.Options,
                        CancellationToken.None);
                }
            }
            catch (Exception ex)
            {
                // A synchronous throw: results[issued] was never assigned, so it is not awaited.
                synchronous = ex;
            }

            var written = 0;
            for (var i = 0; i < issued; i++)
            {
                try
                {
                    _ = await results[i].ConfigureAwait(false);
                    written++;
                }
                catch (Exception ex)
                {
                    // Every issued task must be awaited even after one fails, or the rest become
                    // unobserved and their pooled sources are never returned. The outcome is recorded
                    // against the entry's position in the batch, which is what the markers index by.
                    failures[order[i]] = ex;
                    failed++;
                    anyFailure ??= ex;
                }

                results[i] = default;
            }

            if (synchronous is not null)
            {
                // Everything from `issued` on was never put on the wire, so it is lost just as surely
                // as a rejected XADD, and the markers covering it must say so.
                for (var i = issued; i < count; i++)
                {
                    failures[order[i]] = synchronous;
                    failed++;
                }

                anyFailure ??= synchronous;
            }

            if (written > 0)
            {
                Interlocked.Add(ref this.published, written);
            }

            for (var i = 0; i < count; i++)
            {
                // Redis has answered for every entry, so the bytes are on the wire and the buffers
                // can go back. Returning them any earlier would publish whoever rented them next.
                Release(batch[i]);
                batch[i] = default;
            }

            if (anyFailure is not null)
            {
                this.log?.LogError(
                    anyFailure,
                    "Streams: buffered publisher for topic '{Topic}' failed to write {Failed} of a batch of {Count} entr(ies); they are lost.",
                    this.topic,
                    failed,
                    count);
            }
        }

        this.SettleWaiters(waiters, failures, count, anyFailure is not null);

        if (count > 0)
        {
            Array.Clear(failures, 0, count);
        }

        waiters.Clear();
    }

    /// <summary>
    /// Completes each flush marker against the entries it covers: normally when all of them landed,
    /// otherwise with the first exception among them.
    /// </summary>
    /// <remarks>
    /// Markers arrive in queue order, so their coverage is ascending and one forward pass over
    /// <paramref name="failures"/> settles every one of them — no per-marker scan, no allocation.
    /// </remarks>
    /// <param name="waiters">The markers, in queue order.</param>
    /// <param name="failures">Per-batch-index outcome.</param>
    /// <param name="count">Live entries in the batch.</param>
    /// <param name="anyFailure">Whether the batch failed at all; when false the whole scan is skipped.</param>
    private void SettleWaiters(List<FlushWaiter> waiters, Exception?[] failures, int count, bool anyFailure)
    {
        if (!anyFailure)
        {
            for (var i = 0; i < waiters.Count; i++)
            {
                waiters[i].Waiter.TrySetResult();
            }

            return;
        }

        var cursor = 0;
        Exception? covered = null;

        for (var i = 0; i < waiters.Count; i++)
        {
            var waiter = waiters[i];
            var upTo = waiter.Covers < count ? waiter.Covers : count;

            while (cursor < upTo)
            {
                covered ??= failures[cursor];
                cursor++;
            }

            if (covered is null)
            {
                // Every entry this caller enqueued before its flush landed in Redis. Something later
                // in the same batch failed, but that is somebody else's message.
                waiter.Waiter.TrySetResult();
            }
            else
            {
                waiter.Waiter.TrySetException(covered);
            }
        }
    }

    private void AbandonRemainder(string why)
    {
        this.cts?.Cancel();

        var abandoned = this.DrainAbandoned();

        this.log?.LogWarning(
            "Streams: buffered publisher for topic '{Topic}' abandoned at least {Abandoned} queued entr(ies) because {Reason}. Buffered entries that are not flushed are lost; use the direct publish path or the outbox for anything that matters.",
            this.topic,
            abandoned,
            why);
    }

    /// <summary>
    /// Empties whatever is left in the queue, returning the buffers and releasing anyone waiting on
    /// a flush that will now never happen. Safe to race with the pump: a channel hands each entry to
    /// exactly one reader.
    /// </summary>
    private int DrainAbandoned()
    {
        var abandoned = 0;

        while (this.channel.Reader.TryRead(out var entry))
        {
            if (entry.Waiter is not null)
            {
                entry.Waiter.TrySetResult();
                continue;
            }

            Release(entry);
            abandoned++;
        }

        return abandoned;
    }

    /// <summary>
    /// A flush marker the pump has taken out of the queue, with the number of entries it was queued
    /// behind in the batch it landed in — which is exactly the set of entries it may be faulted for.
    /// </summary>
    /// <param name="Waiter">The caller's completion source.</param>
    /// <param name="Covers">Live entries ahead of it in the batch; it covers <c>[0, Covers)</c>.</param>
    private readonly record struct FlushWaiter(TaskCompletionSource Waiter, int Covers);

    /// <summary>
    /// One queued publish, or a flush marker when <see cref="Waiter"/> is set. A struct, so the
    /// channel's ring buffer holds them inline rather than allocating one object per message.
    /// </summary>
    private readonly struct PendingEntry
    {
        /// <summary>The pooled copy of the caller's body; <see langword="null"/> on a flush marker.</summary>
        public readonly byte[]? Body;

        /// <summary>Bytes of <see cref="Body"/> that are the message — the rental is usually larger.</summary>
        public readonly int Length;

        public readonly string PartitionKey;
        public readonly string Type;

        /// <summary>Publish options, already carrying the resolved partition.</summary>
        public readonly PublishOptions Options;

        /// <summary>The partition this entry routes to, resolved at enqueue so the pump can group without hashing again.</summary>
        public readonly int Partition;

        /// <summary>When the caller enqueued it, for the age-based flush trigger.</summary>
        public readonly long Stamp;

        /// <summary>Set only on a flush marker; completed once everything ahead of it has been written.</summary>
        public readonly TaskCompletionSource? Waiter;

        public PendingEntry(
            byte[] body,
            int length,
            string partitionKey,
            string type,
            PublishOptions options,
            int partition,
            long stamp)
        {
            this.Body = body;
            this.Length = length;
            this.PartitionKey = partitionKey;
            this.Type = type;
            this.Options = options;
            this.Partition = partition;
            this.Stamp = stamp;
            this.Waiter = null;
        }

        private PendingEntry(TaskCompletionSource waiter)
        {
            this.Body = null;
            this.Length = 0;
            this.PartitionKey = string.Empty;
            this.Type = string.Empty;
            this.Options = default;
            this.Partition = 0;
            this.Stamp = 0;
            this.Waiter = waiter;
        }

        public static PendingEntry Barrier(TaskCompletionSource waiter) => new(waiter);
    }
}
