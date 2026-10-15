namespace RedisEvents.Projections;

/// <summary>
/// Opts a projection in to writing once per batch instead of once per event: its
/// <see cref="IProjection{TEvent}.HandleAsync"/> buffers into <see cref="EventMeta.Batch"/>, and
/// <see cref="FlushAsync"/> writes what was buffered after every event in the batch has been dispatched
/// and before the consumer's position can move past any of them.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a hook and not a field.</b> A projection instance is a singleton shared by every topic's
/// <see cref="EventProjector"/> and called concurrently, once per partition. A buffer held in a field
/// would mix partitions, and nothing would stop it outliving the batch: once
/// <see cref="EventProjector.HandleAsync"/> returns, core records the batch's position, so a row still
/// held then is lost if the process dies. <see cref="ProjectionBatch"/> is the per-call scratch space
/// that removes both problems: it belongs to one <see cref="EventProjector.HandleAsync"/> call, and it
/// is closed when that call returns.
/// </para>
/// <para>
/// <b>When <see cref="FlushAsync"/> runs.</b> Once per batch, after the last event, for each batch
/// projection that was handed at least one event in it, in the projector's projection order. Still
/// inside <see cref="EventProjector.HandleAsync"/>, so the position cannot advance before it returns.
/// Under <c>PersistMode.SyncMessage</c> core hands the projector one message at a time, so the flush
/// runs per event: correct, but no batching.
/// </para>
/// <para>
/// <b>A flush that throws is a batch that threw.</b> Its exception propagates out of
/// <see cref="EventProjector.HandleAsync"/> unchanged and core applies its ordinary error contract to
/// the whole batch: a <see cref="RedisEvents.Errors.DontIgnoreException"/> subclass blocks the partition
/// and redelivers the same batch, which is dispatched and flushed again from its first event; any other
/// exception is logged and the batch is skipped. A flush must therefore be idempotent across a retried
/// batch (upserts on keys derived from the events are), exactly as a per-event projection must be
/// across a redelivered one.
/// </para>
/// <para>
/// When one flush throws an ordinary exception the projector still flushes the other batch projections
/// before rethrowing, because a skipped batch advances the position past their buffered work too. When
/// one throws a <see cref="RedisEvents.Errors.DontIgnoreException"/> (or shutdown cancels it) the rest
/// are not flushed: the batch will be redelivered whole.
/// </para>
/// <para>
/// <b>When an event throws.</b> If an event's dispatch throws an ordinary exception, the batch is about
/// to be skipped, and per-event projections would already have written the events before it. So the
/// projector flushes the batch projections first, then rethrows the original exception; if that flush
/// throws a <see cref="RedisEvents.Errors.DontIgnoreException"/> it is thrown instead, so the batch blocks
/// rather than advancing past rows that were never written. If the event threw a
/// <see cref="RedisEvents.Errors.DontIgnoreException"/>, or the token was cancelled for shutdown, nothing
/// is flushed: the batch redelivers from its first event.
/// </para>
/// <para>
/// A projection that does not implement this interface is unaffected: it is dispatched exactly as
/// before, writes per event, and may ignore <see cref="EventMeta.Batch"/>.
/// </para>
/// </remarks>
public interface IBatchProjection
{
    /// <summary>
    /// Writes whatever this projection buffered in <paramref name="batch"/> while the batch's events
    /// were dispatched to it.
    /// </summary>
    /// <param name="batch">The batch scope its <see cref="IProjection{TEvent}.HandleAsync"/> calls saw as <see cref="EventMeta.Batch"/>.</param>
    /// <param name="ct">The consumer's token. Cancelled on shutdown; the batch then redelivers.</param>
    /// <returns>A task that completes when the buffered work is durable.</returns>
    ValueTask FlushAsync(ProjectionBatch batch, CancellationToken ct);
}
