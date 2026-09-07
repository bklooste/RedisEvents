using System.Collections.Generic;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;

namespace Orange.Lib.Streams.Testing;

/// <summary>
/// In-memory test double for <see cref="IStreamBufferedPublisher"/>: the direct-publish half
/// behaves exactly like <see cref="InMemoryStreamPublisher"/>, and the buffered half models the one
/// property that makes buffering worth testing — an enqueued message is <b>not</b> published until
/// something flushes it.
/// </summary>
/// <remarks>
/// <para>
/// R-20 added this; there was no buffered double at all, so a service written against
/// <see cref="IStreamBufferedPublisher"/> had nothing to inject and its tests either reached for
/// Redis or asserted against the non-buffered interface, which cannot express the buffer.
/// </para>
/// <para>
/// <b>What it models faithfully.</b> Bodies are copied on <see cref="EnqueueAsync"/>, exactly as the
/// real pump copies them, so a caller reusing a buffer sees the same behaviour here. Enqueued
/// messages appear in <see cref="Pending"/> and only move into <see cref="Published"/> on
/// <see cref="FlushAsync"/> — so a test that forgets to flush fails here the way it would lose
/// messages in production. Ordering within a partition key is preserved. Direct
/// <see cref="InMemoryStreamPublisher.PublishAsync"/> calls bypass the buffer, which is the real
/// publisher's documented "ordering between the two paths is undefined".
/// </para>
/// <para>
/// <b>What it does not model.</b> There is no background pump, no batch trigger, no bounded queue
/// and therefore no backpressure or <c>DropOldest</c>: nothing flushes on its own, and
/// <see cref="EnqueueAsync"/> never waits. Those are properties of the real
/// <c>BufferedStreamPublisher</c> and are covered by its own tests against Redis. Set
/// <see cref="FailNextFlush"/> to rehearse the failure path a caller has to handle.
/// </para>
/// <para>
/// Every member is thread-safe, and both <see cref="Published"/> and <see cref="Pending"/> return
/// snapshots.
/// </para>
/// </remarks>
public sealed class InMemoryBufferedStreamPublisher : IStreamBufferedPublisher
{
    private readonly InMemoryStreamPublisher inner;
    private readonly Lock gate = new();
    private readonly List<Buffered> pending = new();
    private Exception? failNextFlush;

    /// <summary>
    /// Creates a buffered publisher for a topic with <paramref name="partitions"/> partitions.
    /// </summary>
    /// <param name="partitions">The topic's partition count, used for routing.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partitions"/> is less than 1.</exception>
    public InMemoryBufferedStreamPublisher(int partitions = 1)
    {
        this.inner = new InMemoryStreamPublisher(partitions);
    }

    /// <summary>The topic's partition count that routing is computed against.</summary>
    public int Partitions => this.inner.Partitions;

    /// <summary>
    /// A snapshot of everything that has actually reached the "stream": direct publishes, plus
    /// enqueued messages that a flush has since carried through.
    /// </summary>
    public IReadOnlyList<PublishedMessage> Published => this.inner.Published;

    /// <summary>
    /// A snapshot of the messages sitting in the buffer — enqueued, not yet flushed. In production
    /// these are the messages a crash would lose.
    /// </summary>
    public IReadOnlyList<PendingMessage> Pending
    {
        get
        {
            lock (this.gate)
            {
                var snapshot = new PendingMessage[this.pending.Count];
                for (var i = 0; i < this.pending.Count; i++)
                {
                    var item = this.pending[i];
                    snapshot[i] = new PendingMessage(item.PartitionKey, item.Body, item.Type, item.Options);
                }

                return snapshot;
            }
        }
    }

    /// <summary>The number of messages waiting in the buffer.</summary>
    public int PendingCount
    {
        get
        {
            lock (this.gate)
            {
                return this.pending.Count;
            }
        }
    }

    /// <summary>
    /// Arms the next <see cref="FlushAsync"/> to fault with this exception, leaving the buffered
    /// messages in <see cref="Pending"/>. Set it to <see langword="null"/> to disarm.
    /// </summary>
    /// <remarks>
    /// One-shot: the flush after the failing one succeeds, so a test can prove that a caller which
    /// retries recovers the messages rather than losing them.
    /// </remarks>
    public Exception? FailNextFlush
    {
        get
        {
            lock (this.gate)
            {
                return this.failNextFlush;
            }
        }

        set
        {
            lock (this.gate)
            {
                this.failNextFlush = value;
            }
        }
    }

    /// <inheritdoc/>
    public ValueTask<StreamId> PublishAsync(string partitionKey, ReadOnlyMemory<byte> body, string type,
                                            PublishOptions options = default, CancellationToken ct = default)
        => this.inner.PublishAsync(partitionKey, body, type, options, ct);

    /// <inheritdoc/>
    public ValueTask PublishBatchAsync(string partitionKey, ReadOnlyMemory<ReadOnlyMemory<byte>> bodies, string type,
                                       PublishOptions options = default, CancellationToken ct = default)
        => this.inner.PublishBatchAsync(partitionKey, bodies, type, options, ct);

    /// <inheritdoc/>
    public ValueTask EnqueueAsync(string partitionKey, ReadOnlyMemory<byte> body, string type,
                                  PublishOptions options = default, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(type);
        ct.ThrowIfCancellationRequested();

        // The real pump copies; a caller is entitled to reuse the buffer the moment this returns.
        var copy = body.ToArray();

        lock (this.gate)
        {
            this.pending.Add(new Buffered(partitionKey ?? string.Empty, copy, type, options));
        }

        return default;
    }

    /// <summary>
    /// Publishes everything enqueued before this call, in order, and then completes.
    /// </summary>
    /// <remarks>
    /// Ordered against <see cref="EnqueueAsync"/> the same way the real flush marker is: the batch
    /// is taken under the lock, so a message enqueued concurrently either makes this flush or the
    /// next one, never half of one.
    /// </remarks>
    /// <exception cref="Exception">Whatever <see cref="FailNextFlush"/> was armed with.</exception>
    public async ValueTask FlushAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        var batch = this.TakeBatch();

        for (var i = 0; i < batch.Length; i++)
        {
            var item = batch[i];
            _ = await this.inner.PublishAsync(item.PartitionKey, item.Body, item.Type, item.Options, ct)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Clears recorded publishes <b>and</b> the buffer.
    /// </summary>
    public void Clear()
    {
        lock (this.gate)
        {
            this.pending.Clear();
        }

        this.inner.Clear();
    }

    /// <summary>
    /// A snapshot of the published messages for one partition, in publish order.
    /// </summary>
    public IReadOnlyList<PublishedMessage> ForPartition(int partition) => this.inner.ForPartition(partition);

    /// <summary>The partition a key would route to; see <see cref="InMemoryStreamPublisher.PartitionFor"/>.</summary>
    public int PartitionFor(string? partitionKey) => this.inner.PartitionFor(partitionKey);

    /// <summary>
    /// Takes the buffered batch under the lock, or throws whatever <see cref="FailNextFlush"/> was
    /// armed with — leaving the batch in the buffer, because a failed flush loses nothing that a
    /// retry cannot pick up, which is the behaviour a caller's retry loop is written against.
    /// </summary>
    private Buffered[] TakeBatch()
    {
        lock (this.gate)
        {
            if (this.failNextFlush is Exception armed)
            {
                this.failNextFlush = null;
                throw armed;
            }

            if (this.pending.Count == 0)
            {
                return [];
            }

            var batch = this.pending.ToArray();
            this.pending.Clear();
            return batch;
        }
    }

    private readonly record struct Buffered(string PartitionKey, byte[] Body, string Type, PublishOptions Options);
}

/// <summary>
/// A message sitting in <see cref="InMemoryBufferedStreamPublisher"/>'s buffer: enqueued, not yet
/// flushed, and therefore not yet published.
/// </summary>
/// <param name="PartitionKey">The routing key, never null.</param>
/// <param name="Body">A copy of the enqueued bytes, taken at enqueue time.</param>
/// <param name="Type">The message type string.</param>
/// <param name="Options">The publish options the message was enqueued with.</param>
public sealed record PendingMessage(
    string PartitionKey,
    byte[] Body,
    string Type,
    PublishOptions Options);
