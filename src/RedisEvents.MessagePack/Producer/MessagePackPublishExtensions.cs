using global::MessagePack;
using RedisEvents.Producer;
using RedisEvents.Wire;

namespace RedisEvents.MessagePack;

/// <summary>
/// Typed convenience over <see cref="IStreamPublisher"/>/<see cref="IStreamBufferedPublisher"/>:
/// serialise as MessagePack and publish in one call, the MessagePack sibling of
/// <c>RedisEvents.Producer.TypedPublishExtensions</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Compression is size-gated, not always-on.</b> A body is serialised once, uncompressed; below
/// <see cref="MessagePackCompressionOptions.ThresholdBytes"/> it is published as-is, above it the
/// message is re-serialised with <see cref="MessagePackCompression.Lz4BlockArray"/>. The second
/// serialise pass on the large-message path is a deliberate simplicity trade: most messages stay
/// under the threshold and pay for exactly one pass, and trusting MessagePack-CSharp's own compressed
/// encoding is worth more than hand-rolling compression to avoid the second pass for the minority that
/// exceed it.
/// </para>
/// <para>
/// <b>Decoding needs no compression option at all</b> — see
/// <see cref="MessagePackConsumeExtensions"/> — because <c>MessagePackSerializer.Deserialize</c>
/// auto-detects the LZ4 extension type in the byte stream regardless of the reader's own
/// <see cref="MessagePackSerializerOptions"/>.
/// </para>
/// </remarks>
public static class MessagePackPublishExtensions
{
    /// <summary>
    /// Serialises <paramref name="message"/> as MessagePack and publishes it.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="publisher">The publisher.</param>
    /// <param name="partitionKey">The routing key; empty round-robins.</param>
    /// <param name="message">The message.</param>
    /// <param name="options">The MessagePack serialiser options.</param>
    /// <param name="type">The message type string; defaults to <c>typeof(T).FullName</c>.</param>
    /// <param name="publishOptions">Correlation, headers and an optional explicit partition.</param>
    /// <param name="compression">Auto-compression threshold; defaults to <see cref="MessagePackCompressionOptions.Default"/>.</param>
    /// <param name="ct">Cancellation, observed before the command is issued.</param>
    /// <returns>The id Redis assigned to the entry.</returns>
    public static ValueTask<StreamId> PublishAsync<T>(
        this IStreamPublisher publisher,
        string partitionKey,
        T message,
        MessagePackSerializerOptions options,
        string? type = null,
        PublishOptions publishOptions = default,
        MessagePackCompressionOptions? compression = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(options);

        var body = Serialize(message, options, compression ?? MessagePackCompressionOptions.Default);
        return publisher.PublishAsync(partitionKey, body, type ?? typeof(T).FullName!, publishOptions, ct);
    }

    /// <summary>
    /// Serialises several messages sharing a partition key and publishes them as a single pipelined
    /// batch. Each message is compressed independently against the same threshold.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="publisher">The publisher.</param>
    /// <param name="partitionKey">The routing key shared by every message in the batch.</param>
    /// <param name="messages">The messages, in the order they should appear on the stream.</param>
    /// <param name="options">The MessagePack serialiser options.</param>
    /// <param name="type">The message type string; defaults to <c>typeof(T).FullName</c>.</param>
    /// <param name="publishOptions">Correlation, headers and an optional explicit partition, applied to every entry.</param>
    /// <param name="compression">Auto-compression threshold; defaults to <see cref="MessagePackCompressionOptions.Default"/>.</param>
    /// <param name="ct">Cancellation, observed before the commands are issued.</param>
    /// <returns>A task that completes when every entry in the batch has been accepted.</returns>
    public static ValueTask PublishBatchAsync<T>(
        this IStreamPublisher publisher,
        string partitionKey,
        IReadOnlyList<T> messages,
        MessagePackSerializerOptions options,
        string? type = null,
        PublishOptions publishOptions = default,
        MessagePackCompressionOptions? compression = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentNullException.ThrowIfNull(options);

        if (messages.Count == 0)
        {
            return ValueTask.CompletedTask;
        }

        var effectiveCompression = compression ?? MessagePackCompressionOptions.Default;
        var bodies = new ReadOnlyMemory<byte>[messages.Count];
        for (var i = 0; i < messages.Count; i++)
        {
            bodies[i] = Serialize(messages[i], options, effectiveCompression);
        }

        return publisher.PublishBatchAsync(partitionKey, bodies, type ?? typeof(T).FullName!, publishOptions, ct);
    }

    /// <summary>
    /// Serialises <paramref name="message"/> as MessagePack and enqueues it for the next batch flush.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="publisher">The buffered publisher.</param>
    /// <param name="partitionKey">The routing key; empty round-robins.</param>
    /// <param name="message">The message.</param>
    /// <param name="options">The MessagePack serialiser options.</param>
    /// <param name="type">The message type string; defaults to <c>typeof(T).FullName</c>.</param>
    /// <param name="publishOptions">Correlation, headers and an optional explicit partition.</param>
    /// <param name="compression">Auto-compression threshold; defaults to <see cref="MessagePackCompressionOptions.Default"/>.</param>
    /// <param name="ct">Cancellation, observed only while waiting for queue space.</param>
    /// <returns>A task that completes once the message is in the buffer — not once it is in Redis.</returns>
    public static ValueTask EnqueueAsync<T>(
        this IStreamBufferedPublisher publisher,
        string partitionKey,
        T message,
        MessagePackSerializerOptions options,
        string? type = null,
        PublishOptions publishOptions = default,
        MessagePackCompressionOptions? compression = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(options);

        var body = Serialize(message, options, compression ?? MessagePackCompressionOptions.Default);
        return publisher.EnqueueAsync(partitionKey, body, type ?? typeof(T).FullName!, publishOptions, ct);
    }

    private static byte[] Serialize<T>(T message, MessagePackSerializerOptions options, MessagePackCompressionOptions compression)
    {
        var uncompressed = MessagePackSerializer.Serialize(message, options);
        if (!compression.Enabled || uncompressed.Length <= compression.ThresholdBytes)
        {
            return uncompressed;
        }

        return MessagePackSerializer.Serialize(message, options.WithCompression(MessagePackCompression.Lz4BlockArray));
    }
}
