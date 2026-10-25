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
    /// <remarks>
    /// <para>
    /// <b>Which message failed matters.</b> A batch delegate that throws tells the worker only that
    /// <em>something</em> in the batch failed, so the worker's error policy has to treat the whole
    /// batch alike: under BestEffort it advanced past the batch and silently dropped every message
    /// after the failing one, against the documented contract that an ordinary exception "logs and
    /// skips the message". The adapter therefore wraps a handler failure in a
    /// <see cref="MessageFailedException"/> carrying the index of the message that threw, and the
    /// worker applies the policy to that one message: BestEffort skips it and resumes with the next;
    /// a <see cref="Errors.DontIgnoreException"/>, StopPartition or Fail records the messages before
    /// it and leaves the position at it, so the retry or restart resumes at that message.
    /// </para>
    /// <para>
    /// Shutdown cancellation is not a failure and passes through unwrapped.
    /// </para>
    /// </remarks>
    public static Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> AdaptMessage(
        IMessageHandler handler)
    {
        return async (batch, ct) =>
        {
            for (int i = 0; i < batch.Length; i++)
            {
                var msg = batch.Span[i];

                try
                {
                    await handler.HandleAsync(in msg, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new MessageFailedException(i, ex);
                }
            }
        };
    }
}

/// <summary>
/// Raised by the per-message adapter to say which message of a batch the handler failed on. The
/// worker unwraps it: the original exception is what gets logged, counted and acted on.
/// </summary>
internal sealed class MessageFailedException : Exception
{
    /// <summary>Creates the wrapper.</summary>
    /// <param name="index">Zero-based index, within the batch passed to the adapter, of the failing message.</param>
    /// <param name="inner">What the handler threw.</param>
    internal MessageFailedException(int index, Exception inner)
        : base(inner.Message, inner)
    {
        this.Index = index;
    }

    /// <summary>Zero-based index of the failing message within the batch the adapter was given.</summary>
    internal int Index { get; }

    /// <summary>The handler's own exception.</summary>
    internal Exception Original => this.InnerException!;
}
