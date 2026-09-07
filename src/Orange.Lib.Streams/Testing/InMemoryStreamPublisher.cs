using System.Collections.Generic;
using System.Text;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;

namespace Orange.Lib.Streams.Testing;

/// <summary>
/// In-memory test double for <see cref="IStreamPublisher"/>.
/// Records every published message, and routes it the way the real publisher would.
/// </summary>
/// <remarks>
/// <para>
/// <b>Routing is real.</b> R-20: this double used to record <c>options.Partition</c> verbatim, so a
/// test that asserted "these two keys land on the same partition" passed against <c>null</c> and
/// proved nothing. It now runs the same <see cref="PartitionRouter"/> the live publisher and the
/// outbox run — an explicit partition wins outright and is range-checked, a non-empty key hashes
/// stably with xxHash3, an empty key round-robins — so a partition assertion written against this
/// double holds against Redis.
/// </para>
/// <para>
/// <b>Ids.</b> Each instance issues its own sequence starting at <c>1-0</c>; ids are unique and
/// increasing within one publisher, and two publishers do not share a counter. Before R-20 the
/// counter was <c>static</c> and incremented without <see cref="Interlocked"/>, so ids collided
/// across instances and were not even unique under concurrency.
/// </para>
/// <para>
/// <b>Thread safety.</b> Every member is safe to call from several threads at once, and
/// <see cref="Published"/> returns a snapshot rather than a live view — enumerating the recorded
/// messages while another thread publishes cannot throw.
/// </para>
/// <para>
/// Bodies are copied on the way in, so a caller reusing a pooled buffer does not corrupt what the
/// test later asserts on.
/// </para>
/// </remarks>
public sealed class InMemoryStreamPublisher : IStreamPublisher
{
    /// <summary>Partition keys up to this UTF-8 length are hashed from a stack buffer.</summary>
    private const int KeyStackBytes = 256;

    private readonly Lock gate = new();
    private readonly List<PublishedMessage> messages = new();
    private long nextMs;
    private uint roundRobin;

    /// <summary>
    /// Creates a publisher for a topic with <paramref name="partitions"/> partitions.
    /// </summary>
    /// <param name="partitions">
    /// The topic's partition count, used for routing. Defaults to 1, which routes everything to
    /// partition 0 — the same short-circuit the live router takes.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partitions"/> is less than 1.</exception>
    public InMemoryStreamPublisher(int partitions = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partitions, 1);
        this.Partitions = partitions;
    }

    /// <summary>The topic's partition count that routing is computed against.</summary>
    public int Partitions { get; }

    /// <summary>
    /// A snapshot of the messages published through this publisher, in publish order.
    /// </summary>
    public IReadOnlyList<PublishedMessage> Published
    {
        get
        {
            lock (this.gate)
            {
                return this.messages.ToArray();
            }
        }
    }

    /// <summary>The number of messages recorded so far.</summary>
    public int Count
    {
        get
        {
            lock (this.gate)
            {
                return this.messages.Count;
            }
        }
    }

    /// <summary>
    /// Publishes a single message and records it.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="type"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="PublishOptions.Partition"/> is outside <c>[0, Partitions)</c>.</exception>
    public ValueTask<StreamId> PublishAsync(string partitionKey, ReadOnlyMemory<byte> body, string type,
                                           PublishOptions options = default, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(type);
        ct.ThrowIfCancellationRequested();

        var partition = this.Route(partitionKey, options.Partition);
        var recorded = Record(partitionKey, body, type, options, partition);

        lock (this.gate)
        {
            var id = this.NextId();
            var message = recorded with { Id = id, Partition = partition };
            this.messages.Add(message);
            return new ValueTask<StreamId>(id);
        }
    }

    /// <summary>
    /// Publishes multiple messages and records them all.
    /// </summary>
    /// <remarks>
    /// Every body in one batch shares the partition key, so — exactly as in the live publisher —
    /// they all land on one partition, and an empty key advances the round-robin cursor once for
    /// the batch rather than once per entry.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="type"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="PublishOptions.Partition"/> is outside <c>[0, Partitions)</c>.</exception>
    public ValueTask PublishBatchAsync(string partitionKey, ReadOnlyMemory<ReadOnlyMemory<byte>> bodies, string type,
                                       PublishOptions options = default, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(type);
        ct.ThrowIfCancellationRequested();

        var partition = this.Route(partitionKey, options.Partition);
        var bodiesSpan = bodies.Span;

        // Copy outside the lock; take it once for the whole batch so the entries stay contiguous
        // and in order, which is what the live publisher's pipelined XADDs give.
        var batch = new PublishedMessage[bodiesSpan.Length];
        for (var i = 0; i < bodiesSpan.Length; i++)
        {
            batch[i] = Record(partitionKey, bodiesSpan[i], type, options, partition);
        }

        lock (this.gate)
        {
            for (var i = 0; i < batch.Length; i++)
            {
                this.messages.Add(batch[i] with { Id = this.NextId(), Partition = partition });
            }
        }

        return default;
    }

    /// <summary>
    /// Clears all recorded messages. Ids continue from where they left off, so a cleared publisher
    /// never reissues an id a test has already seen.
    /// </summary>
    public void Clear()
    {
        lock (this.gate)
        {
            this.messages.Clear();
        }
    }

    /// <summary>
    /// A snapshot of the messages recorded for one partition, in publish order.
    /// </summary>
    /// <param name="partition">The partition to filter on.</param>
    public IReadOnlyList<PublishedMessage> ForPartition(int partition)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partition);

        var result = new List<PublishedMessage>();
        lock (this.gate)
        {
            for (var i = 0; i < this.messages.Count; i++)
            {
                if (this.messages[i].Partition == partition)
                {
                    result.Add(this.messages[i]);
                }
            }
        }

        return result;
    }

    /// <summary>
    /// The partition a key would route to, without publishing anything. Useful for asserting the
    /// routing a test depends on rather than hard-coding a partition index.
    /// </summary>
    /// <remarks>
    /// An empty key round-robins, so this <em>does</em> advance the cursor — the same way a publish
    /// with an empty key would.
    /// </remarks>
    public int PartitionFor(string? partitionKey) => this.Route(partitionKey, explicitPartition: null);

    /// <summary>Same routing as <see cref="StreamPublisher"/> and <see cref="Outbox"/>.</summary>
    private int Route(string? partitionKey, int? explicitPartition)
    {
        if (explicitPartition is int chosen)
        {
            if ((uint)chosen >= (uint)this.Partitions)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(explicitPartition),
                    chosen,
                    $"This publisher has {this.Partitions} partition(s), so an explicit partition must be in [0, {this.Partitions - 1}].");
            }

            return chosen;
        }

        if (string.IsNullOrEmpty(partitionKey))
        {
            return PartitionRouter.RoundRobin(ref this.roundRobin, this.Partitions);
        }

        if (Encoding.UTF8.GetMaxByteCount(partitionKey.Length) <= KeyStackBytes)
        {
            Span<byte> scratch = stackalloc byte[KeyStackBytes];
            var written = Encoding.UTF8.GetBytes(partitionKey.AsSpan(), scratch);
            return PartitionRouter.ForKey(scratch[..written], this.Partitions);
        }

        return PartitionRouter.ForKey(Encoding.UTF8.GetBytes(partitionKey), this.Partitions);
    }

    /// <summary>Builds the record, copying the body. Id and partition are stamped by the caller.</summary>
    private static PublishedMessage Record(
        string? partitionKey,
        ReadOnlyMemory<byte> body,
        string type,
        in PublishOptions options,
        int partition) =>
        new(
            Id: default,
            PartitionKey: partitionKey ?? string.Empty,
            Body: body.ToArray(),
            Type: type,
            CorrelationId: options.CorrelationId,
            Headers: options.Headers,
            Partition: partition,
            RequestedPartition: options.Partition);

    /// <summary>Next id in this instance's sequence. Callers hold <see cref="gate"/>.</summary>
    private StreamId NextId() => new(++this.nextMs, 0);
}

/// <summary>
/// A message recorded by <see cref="InMemoryStreamPublisher"/> or
/// <see cref="InMemoryBufferedStreamPublisher"/>.
/// </summary>
/// <param name="Id">The synthetic id this double assigned.</param>
/// <param name="PartitionKey">The routing key, never null (an empty key round-robins).</param>
/// <param name="Body">A copy of the published bytes.</param>
/// <param name="Type">The message type string.</param>
/// <param name="CorrelationId">The correlation id from <see cref="PublishOptions"/>, if any.</param>
/// <param name="Headers">The headers list from <see cref="PublishOptions"/>, held by reference, if any.</param>
/// <param name="Partition">The partition the message <b>routed to</b>.</param>
/// <param name="RequestedPartition">
/// The explicit <see cref="PublishOptions.Partition"/> the caller asked for, or <see langword="null"/>
/// when it left routing to the key. Kept separate from <see cref="Partition"/> so a test can tell
/// "landed on 3" from "asked for 3".
/// </param>
public sealed record PublishedMessage(
    StreamId Id,
    string PartitionKey,
    byte[] Body,
    string Type,
    string? CorrelationId,
    IReadOnlyList<KeyValuePair<string, string>>? Headers,
    int Partition,
    int? RequestedPartition = null);
