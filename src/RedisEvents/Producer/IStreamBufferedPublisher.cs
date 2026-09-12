namespace RedisEvents.Producer;

/// <summary>
/// A publisher that can also buffer: <see cref="EnqueueAsync"/> hands the message to a background
/// pump and returns, trading durability for throughput.
/// </summary>
/// <remarks>
/// <para>
/// <b>The contract, in one sentence: an enqueued message that has not been flushed is lost if the
/// process dies.</b> Not delayed, not retried on restart — lost. The pump holds it in process
/// memory until a batch trigger fires, and nothing outside the process knows it ever existed. That
/// is the deliberate trade for the throughput, and it is why anything that must not be lost goes
/// through <see cref="IStreamPublisher.PublishAsync"/> — or, when it has to agree with a state
/// write, through the outbox.
/// </para>
/// <para>
/// <b>Use it for.</b> Odds ticks, price snapshots, telemetry — high-rate streams where the next
/// message supersedes the last one and a gap after a crash costs nothing. <b>Do not use it for</b>
/// bets, orders, settlements, or anything a person would notice the absence of.
/// </para>
/// <para>
/// <b>Ordering.</b> Enqueued messages sharing a partition key keep their relative order. Ordering
/// between <see cref="EnqueueAsync"/> and the inherited direct-publish methods is <em>not</em>
/// defined: a direct publish goes to Redis immediately while an enqueued message waits for the
/// pump, so a message enqueued first can land second. Pick one path per key.
/// </para>
/// <para>
/// <b>Backpressure.</b> The queue is bounded. Once it is full, <see cref="EnqueueAsync"/> waits for
/// space rather than growing without limit — so a producer outrunning Redis slows down instead of
/// exhausting memory. A topic that would genuinely rather shed load than slow down can opt into
/// dropping the oldest entries, which is counted rather than silent.
/// </para>
/// </remarks>
public interface IStreamBufferedPublisher : IStreamPublisher
{
    /// <summary>
    /// Copies the message into the publisher's buffer and returns, usually without waiting for
    /// anything. The message reaches Redis on the next batch flush.
    /// </summary>
    /// <param name="partitionKey">
    /// The routing key. Enqueued messages sharing a key stay ordered relative to each other.
    /// <see langword="null"/> or empty round-robins across partitions with no ordering promise.
    /// </param>
    /// <param name="body">
    /// The message body. Unlike <see cref="IStreamPublisher.PublishAsync"/>, these bytes <b>are
    /// copied</b> — the caller may reuse or overwrite the buffer as soon as this call returns. The
    /// copy is unavoidable: the pump reads the bytes long after the caller has moved on.
    /// </param>
    /// <param name="type">The message type string. Required; consumer-side filtering is built on it.</param>
    /// <param name="options">
    /// Correlation, headers and an optional explicit partition. Only the body is copied — the
    /// headers list is held by reference until the flush, so it must not be mutated afterwards.
    /// </param>
    /// <param name="ct">
    /// Cancellation. It is observed only while waiting for queue space; a message already accepted
    /// into the buffer is not cancellable.
    /// </param>
    /// <returns>
    /// A task that completes once the message is in the buffer — <em>not</em> once it is in Redis.
    /// It completes synchronously whenever the queue has room, which is the normal case.
    /// </returns>
    ValueTask EnqueueAsync(
        string partitionKey,
        ReadOnlyMemory<byte> body,
        string type,
        PublishOptions options = default,
        CancellationToken ct = default);

    /// <summary>
    /// Waits until everything enqueued before this call has been written to Redis.
    /// </summary>
    /// <remarks>
    /// Ordered against <see cref="EnqueueAsync"/>: the flush marker takes its place in the queue, so
    /// it covers exactly the messages that went in ahead of it and makes no promise about ones
    /// enqueued concurrently. Messages that failed to publish surface here as the exception the
    /// flush hit; the pump has already logged it.
    /// </remarks>
    /// <param name="ct">Cancellation for the wait itself. Cancelling it does not stop the flush.</param>
    /// <returns>A task that completes when the preceding messages have been accepted by Redis.</returns>
    ValueTask FlushAsync(CancellationToken ct = default);
}
