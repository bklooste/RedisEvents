using global::MessagePack;
using RedisEvents.Wire;

namespace RedisEvents.MessagePack;

/// <summary>
/// Typed convenience over <see cref="StreamMsg"/>: deserialise a MessagePack message, or every message
/// in a batch, in one call. The MessagePack sibling of
/// <c>RedisEvents.Consumer.TypedConsumeExtensions</c>.
/// </summary>
/// <remarks>
/// No compression option here, unlike the publish side — but reading still needs the reader's
/// <see cref="MessagePackSerializerOptions.Compression"/> to be an LZ4 mode for auto-detection to work
/// at all: with <see cref="MessagePackCompression.None"/> (the default), a compressed body isn't
/// decompressed before hitting the object formatter and throws. Both methods here transparently
/// upgrade a <see cref="MessagePackCompression.None"/> options instance to
/// <see cref="MessagePackCompression.Lz4BlockArray"/> before reading — harmless for a plain
/// uncompressed body, required for a compressed one — so a caller never has to think about which kind
/// of body they're holding, on the read side, either way.
/// </remarks>
public static class MessagePackConsumeExtensions
{
    /// <summary>Deserialises one message's body from MessagePack.</summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="msg">The message. Its <see cref="StreamMsg.Body"/> is read synchronously, so this is safe for the duration of the handler call.</param>
    /// <param name="options">The MessagePack serialiser options.</param>
    /// <returns>The deserialised message.</returns>
    public static T? Deserialize<T>(this in StreamMsg msg, MessagePackSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return MessagePackSerializer.Deserialize<T>(msg.Body, ForReading(options));
    }

    /// <summary>Deserialises every message body in a batch from MessagePack, in order.</summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="batch">The batch, as handed to <c>IBatchHandler.HandleAsync</c>.</param>
    /// <param name="options">The MessagePack serialiser options.</param>
    /// <returns>One deserialised message per entry in the batch, in the same order.</returns>
    public static T?[] Deserialize<T>(this ReadOnlyMemory<StreamMsg> batch, MessagePackSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var source = batch.Span;
        if (source.Length == 0)
        {
            return [];
        }

        var readOptions = ForReading(options);
        var results = new T?[source.Length];
        for (var i = 0; i < source.Length; i++)
        {
            results[i] = MessagePackSerializer.Deserialize<T>(source[i].Body, readOptions);
        }

        return results;
    }

    /// <summary>
    /// Upgrades <see cref="MessagePackCompression.None"/> to <see cref="MessagePackCompression.Lz4BlockArray"/>
    /// so a single read path handles both compressed and uncompressed bodies; leaves an
    /// already-LZ4 options instance untouched rather than reallocating one for nothing.
    /// </summary>
    private static MessagePackSerializerOptions ForReading(MessagePackSerializerOptions options)
        => options.Compression == MessagePackCompression.None
            ? options.WithCompression(MessagePackCompression.Lz4BlockArray)
            : options;
}
