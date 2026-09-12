using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using RedisEvents.Wire;

namespace RedisEvents.Consumer;

/// <summary>
/// Typed convenience over <see cref="StreamMsg"/>: deserialise a message, or every message in a
/// batch, in one call. The mirror image of <see cref="Producer.TypedPublishExtensions"/> on the
/// publish side.
/// </summary>
/// <remarks>
/// <para>
/// <b><see cref="JsonTypeInfo{T}"/> only</b>, for the same reason as the publish side: a
/// <c>JsonSerializerOptions</c> overload would have to carry <c>RequiresUnreferencedCode</c>, and
/// that attribute is viral. Callers pass the context-generated <c>MyJsonContext.Default.MyType</c>
/// instead.
/// </para>
/// <para>
/// <b>No implicit type filtering.</b> This deserialises whatever bytes are on the message; it does
/// not check <see cref="StreamMsg.Type"/> against <typeparamref name="T"/>. A handler whose topic
/// carries more than one message type still branches on <see cref="StreamMsg.Type"/> itself before
/// calling this — the same as it would with a hand-written <c>JsonSerializer.Deserialize</c> call.
/// </para>
/// </remarks>
public static class TypedConsumeExtensions
{
    /// <summary>Deserialises one message's body from UTF-8 JSON.</summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="msg">The message. Its <see cref="StreamMsg.Body"/> is read synchronously, so this is safe for the duration of the handler call.</param>
    /// <param name="typeInfo">Source-generated metadata for <typeparamref name="T"/>.</param>
    /// <returns>The deserialised message.</returns>
    public static T? Deserialize<T>(this in StreamMsg msg, JsonTypeInfo<T> typeInfo)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);

        return JsonSerializer.Deserialize(msg.BodySpan, typeInfo);
    }

    /// <summary>Deserialises every message body in a batch from UTF-8 JSON, in order.</summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="batch">The batch, as handed to <see cref="IBatchHandler.HandleAsync"/>.</param>
    /// <param name="typeInfo">Source-generated metadata for <typeparamref name="T"/>.</param>
    /// <returns>One deserialised message per entry in the batch, in the same order.</returns>
    public static T?[] Deserialize<T>(this ReadOnlyMemory<StreamMsg> batch, JsonTypeInfo<T> typeInfo)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);

        var source = batch.Span;
        if (source.Length == 0)
        {
            return [];
        }

        var results = new T?[source.Length];
        for (var i = 0; i < source.Length; i++)
        {
            results[i] = JsonSerializer.Deserialize(source[i].BodySpan, typeInfo);
        }

        return results;
    }
}
