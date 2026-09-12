namespace RedisEvents.Consumer;

using RedisEvents.Wire;

/// <summary>
/// Handler for a single message, already deserialised.
/// </summary>
/// <remarks>
/// The typed sibling of <see cref="IMessageHandler"/>: register with
/// <c>AddStream&lt;THandler, TMessage&gt;(topic, typeInfo)</c> and the body is deserialised for you
/// before this is called — see <c>TypedMessageHandlerAdapter{T}</c>. Same batching, positions and
/// error handling as <see cref="IMessageHandler"/>; deserialisation failures surface as an ordinary
/// exception from this call, subject to the same <see cref="Config.ErrorPolicy"/> as any other.
/// </remarks>
/// <typeparam name="T">The deserialised message type.</typeparam>
public interface IMessageHandler<T>
{
    /// <summary>
    /// Process a single message.
    /// </summary>
    /// <param name="msg">The deserialised value together with the original envelope.</param>
    /// <param name="ct">Cancellation token to stop processing.</param>
    /// <returns>A task that completes when the message has been processed.</returns>
    ValueTask HandleAsync(in StreamMsg<T> msg, CancellationToken ct);
}
