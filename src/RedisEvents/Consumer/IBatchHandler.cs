namespace RedisEvents.Consumer;

using RedisEvents.Wire;

/// <summary>
/// Handler for a batch of messages.
/// </summary>
/// <remarks>
/// <para>
/// Invoked once per batch: one call to <see cref="HandleAsync"/> processes
/// <c>ReadOnlyMemory&lt;StreamMsg&gt;</c> of decoded messages from a partition.
/// Position advances on success, and is not recorded on failure (see <see cref="ErrorPolicy"/>).
/// </para>
/// <para>
/// <see cref="HandleAsync"/> must not retain the memory past the await. If the handler needs to
/// keep message data, it must copy it (a <c>Copy()</c> extension is provided).
/// </para>
/// </remarks>
public interface IBatchHandler
{
    /// <summary>
    /// Process a batch of messages.
    /// </summary>
    /// <param name="batch">The messages in this batch, valid only for the duration of this call.</param>
    /// <param name="ct">Cancellation token to stop processing.</param>
    /// <returns>A task that completes when the batch has been processed.</returns>
    ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct);
}
