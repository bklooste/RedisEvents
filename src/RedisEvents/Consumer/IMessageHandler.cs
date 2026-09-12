namespace RedisEvents.Consumer;

using RedisEvents.Wire;

/// <summary>
/// Handler for a single message.
/// </summary>
/// <remarks>
/// <para>
/// Invoked per message, after filtering. Position advances per message when <c>Persist = SyncMessage</c>,
/// otherwise per batch.
/// </para>
/// <para>
/// <see cref="HandleAsync"/> is internally wrapped into the batch path by <see cref="HandlerAdapter"/>,
/// so batching, positions and error handling behave identically regardless of handler shape.
/// </para>
/// </remarks>
public interface IMessageHandler
{
    /// <summary>
    /// Process a single message.
    /// </summary>
    /// <param name="msg">The message to process. References are valid only for the duration of this call.</param>
    /// <param name="ct">Cancellation token to stop processing.</param>
    /// <returns>A task that completes when the message has been processed.</returns>
    ValueTask HandleAsync(in StreamMsg msg, CancellationToken ct);
}
