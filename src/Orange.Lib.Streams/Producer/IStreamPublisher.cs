using Orange.Lib.Streams.Wire;

namespace Orange.Lib.Streams.Producer;

// Note: PublishOptions is defined in this namespace as PublishOptions.cs

/// <summary>
/// Publishes messages to one topic.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two methods, not sixteen.</b> The Kafka <c>IEventHubProducer</c> this replaces grew sixteen
/// overloads because every optional concern — key, headers, correlation, partition, typed versus
/// raw — became another signature. Here the optional concerns live in
/// <see cref="PublishOptions"/> and the typed convenience lives in
/// <see cref="TypedPublishExtensions"/>, so the interface itself stays small enough to implement by
/// hand in a test double.
/// </para>
/// <para>
/// <b>Durability.</b> A returned <see cref="StreamId"/> means Redis accepted the entry: it is in
/// memory on the primary, and on disk within a second under the default AOF <c>everysec</c>
/// policy. That is <em>weaker than Kafka's <c>acks=all</c></em> — a primary lost within that second
/// loses the tail. It is the deliberate trade for the latency and the operational simplicity;
/// anything that must not be lost belongs in an outbox transaction alongside its state write.
/// </para>
/// <para>
/// <b>Failure.</b> There is no retry loop behind this interface beyond StackExchange.Redis's own
/// reconnect. A publish that fails throws and the caller decides what that means. A silent retry
/// would hide a real outage behind rising latency and, worse, reorder messages within a key.
/// </para>
/// </remarks>
public interface IStreamPublisher
{
    /// <summary>
    /// Publishes one message and waits for Redis to assign it an id.
    /// </summary>
    /// <param name="partitionKey">
    /// The routing key. Messages sharing a key land on one partition and are therefore ordered
    /// relative to each other. <see langword="null"/> or empty round-robins across partitions with
    /// no ordering promise.
    /// </param>
    /// <param name="body">
    /// The message body, passed to Redis by reference. It must stay valid until the returned
    /// <see cref="ValueTask{TResult}"/> completes; the non-buffered path never copies it.
    /// </param>
    /// <param name="type">
    /// The message type string. Required, because consumer-side filtering is built on it.
    /// </param>
    /// <param name="options">Correlation, headers and an optional explicit partition.</param>
    /// <param name="ct">Cancellation, observed before the command is issued.</param>
    /// <returns>The id Redis assigned to the entry.</returns>
    ValueTask<StreamId> PublishAsync(
        string partitionKey,
        ReadOnlyMemory<byte> body,
        string type,
        PublishOptions options = default,
        CancellationToken ct = default);

    /// <summary>
    /// Publishes several messages that share a partition key, pipelining the commands so the batch
    /// costs one round trip's latency rather than one per message.
    /// </summary>
    /// <param name="partitionKey">The routing key shared by every body in the batch.</param>
    /// <param name="bodies">
    /// The message bodies, in the order they should appear on the stream. Each must stay valid
    /// until the returned <see cref="ValueTask"/> completes.
    /// </param>
    /// <param name="type">The message type string, shared by every body in the batch.</param>
    /// <param name="options">Correlation, headers and an optional explicit partition, applied to every entry.</param>
    /// <param name="ct">Cancellation, observed before the commands are issued.</param>
    /// <returns>A task that completes when every entry in the batch has been accepted.</returns>
    ValueTask PublishBatchAsync(
        string partitionKey,
        ReadOnlyMemory<ReadOnlyMemory<byte>> bodies,
        string type,
        PublishOptions options = default,
        CancellationToken ct = default);
}
