namespace RedisEvents.Consumer;

using RedisEvents.Wire;

/// <summary>
/// Handler for a batch of messages, already deserialised.
/// </summary>
/// <remarks>
/// The typed sibling of <see cref="IBatchHandler"/>: register with
/// <c>AddStream&lt;THandler, TMessage&gt;(topic, typeInfo)</c> and every body in the batch is
/// deserialised for you before this is called — see <c>TypedBatchHandlerAdapter{T}</c>, which
/// deserialises sequentially for a small batch and in parallel chunks for a large one. Same
/// batching, positions and error handling as <see cref="IBatchHandler"/>; a deserialisation failure
/// surfaces as an ordinary exception from this call, subject to the same
/// <see cref="Config.ErrorPolicy"/> as any other.
/// </remarks>
/// <typeparam name="T">The deserialised message type.</typeparam>
public interface IBatchHandler<T>
{
    /// <summary>
    /// Process a batch of messages.
    /// </summary>
    /// <param name="batch">The deserialised values together with their original envelopes, valid only for the duration of this call.</param>
    /// <param name="ct">Cancellation token to stop processing.</param>
    /// <returns>A task that completes when the batch has been processed.</returns>
    ValueTask HandleAsync(ReadOnlyMemory<StreamMsg<T>> batch, CancellationToken ct);
}
