namespace RedisEvents.Consumer;

using RedisEvents.Wire;

/// <summary>
/// Static adapter turning an <see cref="IMessageHandler"/> into a batch handler delegate,
/// so batching, positions and error handling have one internal code path regardless of handler shape.
/// </summary>
internal static class HandlerAdapter
{
    /// <summary>
    /// Adapt a per-message handler to the batch delegate shape.
    /// </summary>
    /// <param name="handler">The message handler to adapt.</param>
    /// <returns>A delegate that invokes the handler for each message in a batch.</returns>
    public static Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> AdaptMessage(
        IMessageHandler handler)
    {
        return async (batch, ct) =>
        {
            for (int i = 0; i < batch.Length; i++)
            {
                var msg = batch.Span[i];
                await handler.HandleAsync(in msg, ct).ConfigureAwait(false);
            }
        };
    }
}
