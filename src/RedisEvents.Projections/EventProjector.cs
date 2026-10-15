using System.Runtime.ExceptionServices;

using Microsoft.Extensions.Logging;

using RedisEvents.Consumer;
using RedisEvents.Errors;
using RedisEvents.Projections.Diagnostics;
using RedisEvents.Wire;

namespace RedisEvents.Projections;

/// <summary>
/// The read side's <see cref="IBatchHandler"/>: decodes each event in a batch via an
/// <see cref="EventTypeRegistry"/> and dispatches it to every <see cref="IProjection{TEvent}"/> bound
/// to it among a fixed set of projection instances.
/// </summary>
/// <remarks>
/// <para>
/// <b>This projector has no error policy of its own; it has core's.</b> Core's contract
/// (<c>RedisEvents/README.md</c>, "The error contract") is four sentences: an ordinary exception
/// means the message is logged and skipped — not retried, not parked anywhere, the position advances
/// and the next batch is processed; the library does no retrying of its own, so a transient failure
/// is the handler's job to retry; a <see cref="RedisEvents.Errors.DontIgnoreException"/> subclass
/// means the partition blocks and retries the same batch forever with backoff, position frozen,
/// other partitions unaffected; and blocking is not free — it is the right tool for "the store is
/// down", the wrong one for "this one message is malformed". <see cref="HandleAsync"/> therefore puts
/// no <c>try</c>/<c>catch</c> around a projection's dispatch: whatever it throws — ordinary exception
/// or <see cref="RedisEvents.Errors.DontIgnoreException"/> — propagates out of <see cref="HandleAsync"/>
/// unchanged and immediately, and core decides what happens next. Catching here to log-and-continue
/// would silently re-introduce exactly the swallowed-exception bug the reference CQRS implementation
/// shipped (see <c>PLAN.md</c>, "Why"), just one layer further down; catching here to translate into
/// some other exception would compete with core's own classification. Neither is this type's job.
/// </para>
/// <para>
/// <b>Batch projections.</b> A projection that also implements <see cref="IBatchProjection"/> buffers
/// into <see cref="EventMeta.Batch"/> and is flushed once, after the batch's last event and before
/// <see cref="HandleAsync"/> returns, so before core can record the batch's position. The flush is
/// part of the batch for the error contract: what it throws propagates out of
/// <see cref="HandleAsync"/> unchanged, like a projection's own exception. The one place the projector
/// acts on an exception rather than only passing it through is the skip path: when an event throws an
/// ordinary exception it flushes the batch projections before rethrowing it, so the skipped batch does
/// not take the earlier events' buffered work with it. <see cref="IBatchProjection"/> has the rules.
/// </para>
/// <para>
/// <b>Unknown or unprojected events are not errors.</b> A topic legitimately carries event types this
/// particular projector was never told about — nothing requires every reader of a topic to know every
/// event ever published to it — so a wire type <see cref="EventTypeRegistry.TryDecode"/> does not
/// recognise, or one it does recognise but that no supplied projection binds to, is skipped and
/// logged at <see cref="LogLevel.Debug"/> rather than treated as a fault. This is the read side;
/// <c>RedisEventRepository.LoadAsync</c> on the write side makes the opposite call for the same
/// situation — an aggregate's own history must be fully understood or its replayed state is wrong —
/// and both are correct for the context they run in.
/// </para>
/// <para>
/// A malformed body that fails to decode is different: <see cref="EventTypeRegistry.TryDecode"/>
/// itself throws in that case (it does not return <see langword="false"/> for a known type it simply
/// could not parse), and that throw is left to propagate for the same no-catch reason as a
/// projection's own exception.
/// </para>
/// <para>
/// <b>Depends only on standard RedisEvents streams — nothing here assumes an aggregate.</b> This
/// rides core's ordinary partitioned consumer (<see cref="StreamsBuilderExtensions.AddStream"/> under
/// <see cref="ProjectionsBuilderExtensions.AddEventProjector"/>) and needs nothing beyond what any
/// topic already provides: a wire type to look up in <see cref="EventTypeRegistry"/>, and the
/// partition key and stream entry id every <see cref="StreamMsg"/> already carries — see
/// <see cref="EventMeta"/>. A topic does not need an <c>AddEventStore</c> (in the sibling
/// <c>RedisEvents.EventSourcing</c> package) anywhere upstream of it for this type to project it.
/// </para>
/// </remarks>
public sealed class EventProjector : IBatchHandler
{
    private readonly EventTypeRegistry types;
    private readonly IReadOnlyList<object> projections;
    private readonly ILogger? logger;
    private readonly string topic;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> warnedUnknownTypes = new(StringComparer.Ordinal);
    private readonly bool hasBatchProjections;

    /// <summary>
    /// Creates a projector that dispatches through <paramref name="types"/> to <paramref name="projections"/>.
    /// </summary>
    /// <param name="types">
    /// The registry used both to decode each event's body and to discover which of
    /// <paramref name="projections"/> bind to it, via <see cref="EventTypeRegistry.TryBindProjection"/>.
    /// </param>
    /// <param name="projections">
    /// The projection instances to dispatch to, tried in this order for every event. A single
    /// instance may implement <see cref="IProjection{TEvent}"/> for several event types, and several
    /// instances may all bind to the same event type — every match is invoked.
    /// </param>
    /// <param name="logger">Optional logger for the skip notices: Warning the first time an unknown wire type is met, Debug after. May be <see langword="null"/>.</param>
    /// <param name="topic">The topic this projector consumes, named in the skip notices. May be <see langword="null"/>.</param>
    public EventProjector(EventTypeRegistry types, IReadOnlyList<object> projections, ILogger? logger = null, string? topic = null)
    {
        ArgumentNullException.ThrowIfNull(types);
        ArgumentNullException.ThrowIfNull(projections);

        this.types = types;
        this.projections = projections;
        this.logger = logger;
        this.topic = topic ?? "(unknown)";
        this.hasBatchProjections = projections.Any(static p => p is IBatchProjection);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Dispatches every event, then flushes each <see cref="IBatchProjection"/> that was handed one,
    /// then returns. Core records the position only after this returns, so nothing a batch projection
    /// buffered can be passed by the position unflushed. See <see cref="IBatchProjection"/> for what
    /// happens when an event or a flush throws. A projector with no batch projection behaves exactly as
    /// it did before the hook existed.
    /// </remarks>
    public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        if (batch.IsEmpty)
        {
            return;
        }

        var scope = new ProjectionBatch(this.topic, batch.Span[0].Partition);

        // The batch projections handed at least one event in this batch, in first-dispatch order; null when
        // this projector has none, so a projector without the hook allocates nothing more than the scope. A
        // projector has a handful of projections, so a list with a reference Contains is enough.
        var toFlush = this.hasBatchProjections ? new List<IBatchProjection>() : null;

        try
        {
            try
            {
                await this.DispatchAsync(batch, scope, toFlush, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (toFlush is { Count: > 0 } && !Redelivers(ex, ct))
            {
                // An ordinary exception: core will skip this batch and advance past it. Per-event
                // projections have already written the events before the failing one; flush the batch
                // projections so they have too, and the position does not pass rows held in the scope.
                try
                {
                    await this.FlushAllAsync(scope, toFlush, ct).ConfigureAwait(false);
                }
                catch (Exception flush) when (Redelivers(flush, ct))
                {
                    // The store behind the flush is down (or the host is stopping): block or redeliver
                    // instead of skipping, or the held rows would be lost when the position advances.
                    throw;
                }
                catch (Exception flush)
                {
                    this.logger?.LogError(
                        flush,
                        "EventProjector: flushing batch projections on topic={Topic} partition={Partition} after an event failed also failed with {ExceptionType}. " +
                        "The event's {OriginalType} is rethrown and the batch is skipped, so this batch's buffered work is lost.",
                        this.topic,
                        scope.Partition,
                        flush.GetType().Name,
                        ex.GetType().Name);
                }

                throw;
            }

            if (toFlush is { Count: > 0 })
            {
                await this.FlushAllAsync(scope, toFlush, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            scope.Close();
        }
    }

    /// <summary>
    /// Whether core redelivers the batch after <paramref name="ex"/> rather than skipping it: a
    /// <see cref="DontIgnoreException"/> blocks and retries it, and a cancellation on shutdown leaves the
    /// position where it was.
    /// </summary>
    private static bool Redelivers(Exception ex, CancellationToken ct) =>
        ex is DontIgnoreException || (ex is OperationCanceledException && ct.IsCancellationRequested);

    /// <summary>
    /// Flushes every batch projection in order. An ordinary failure does not stop the others (a skipped
    /// batch advances past their work too); the first is rethrown at the end and the rest are logged. A
    /// <see cref="DontIgnoreException"/> or shutdown propagates at once: the batch is redelivered whole.
    /// </summary>
    private async ValueTask FlushAllAsync(ProjectionBatch scope, List<IBatchProjection> toFlush, CancellationToken ct)
    {
        ExceptionDispatchInfo? first = null;

        foreach (var projection in toFlush)
        {
            try
            {
                await FlushOneAsync(projection, scope, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (!Redelivers(ex, ct))
            {
                if (first is null)
                {
                    first = ExceptionDispatchInfo.Capture(ex);
                }
                else
                {
                    this.logger?.LogError(
                        ex,
                        "EventProjector: flush of {Projection} on topic={Topic} partition={Partition} also failed with {ExceptionType}; " +
                        "the first flush failure in the batch is the one rethrown.",
                        projection.GetType().Name,
                        this.topic,
                        scope.Partition,
                        ex.GetType().Name);
                }
            }
        }

        first?.Throw();
    }

    private static async ValueTask FlushOneAsync(IBatchProjection projection, ProjectionBatch scope, CancellationToken ct)
    {
        using var activity = ProjectionsSpans.StartFlush(projection.GetType().Name, scope.Topic, scope.Partition);

        try
        {
            await projection.FlushAsync(scope, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            ProjectionsSpans.Failed(activity, ex);
            throw;
        }
    }

    /// <summary>
    /// Decodes and dispatches each event in batch order: the projector's whole job before the batch-projection hook,
    /// unchanged by it except that each event's meta carries <paramref name="scope"/> and each batch projection handed
    /// an event is recorded in <paramref name="toFlush"/>.
    /// </summary>
    private async ValueTask DispatchAsync(ReadOnlyMemory<StreamMsg> batch, ProjectionBatch scope, List<IBatchProjection>? toFlush, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            var msg = batch.Span[i];

            if (!types.TryDecode(msg.Type, msg.Body, out var @event))
            {
                // Warning the first time a wire type is met and Debug after that: a topic may carry types this
                // projector does not want, so every skip at Warning would flood, and none at all hides a renamed tag.
                if (this.warnedUnknownTypes.TryAdd(msg.Type, 0))
                {
                    logger?.LogWarning(
                        "EventProjector: skipping events of unknown wire type '{WireType}' on topic={Topic} — no EventTypeRegistry registration for it. " +
                        "First seen at partition={Partition} id={Id} partitionKey={PartitionKey}; further events of this type are skipped at Debug. " +
                        "If this projector should handle the type, the producer's wire tag and the registration disagree.",
                        msg.Type, this.topic, msg.Partition, msg.Id.Format(), msg.PartitionKey);
                }
                else
                {
                    logger?.LogDebug(
                        "EventProjector: skipping event of unknown wire type '{WireType}' on topic={Topic} id={Id} — no EventTypeRegistry registration for it.",
                        msg.Type, this.topic, msg.Id.Format());
                }

                continue;
            }

            List<(object Projection, Func<object, object, EventMeta, CancellationToken, ValueTask> Binder)>? bound = null;
            foreach (var projection in projections)
            {
                var binder = types.TryBindProjection(msg.Type, projection);
                if (binder is not null)
                {
                    (bound ??= []).Add((projection, binder));
                }
            }

            if (bound is null)
            {
                logger?.LogDebug(
                    "EventProjector: skipping event of wire type '{WireType}' — known to the registry but no supplied projection implements IProjection<T> for it.",
                    msg.Type);
                continue;
            }

            var meta = new EventMeta(
                PartitionKey: msg.PartitionKey,
                Id: msg.Id,
                CorrelationId: msg.CorrelationId)
            {
                Batch = scope,
            };

            // One span covers dispatch to every projection bound to this event — several may bind to
            // the same wire type, and they run as one unit of work, the same way core reports one
            // streams.process span per batch rather than one per message. It naturally nests under
            // whatever streams.process span core already started for the batch: HandleAsync runs as
            // one continuous async call chain with no reader/processor split to work around, unlike
            // core's consumer side, so plain ActivitySource.StartActivity is correct here.
            using var activity = ProjectionsSpans.StartProject(msg.Type, meta.PartitionKey, meta.Id);

            // No try/catch by design — see the type's <remarks> — except the one below, which exists
            // solely to record the failure on the span. It changes nothing about the error contract:
            // the exact same exception, with its original stack, still propagates out of HandleAsync
            // immediately and unchanged, and the rest of the batch is still not processed. Disposing
            // the (possibly null) activity happens via the enclosing `using`, on every exit path.
            try
            {
                foreach (var (projection, binder) in bound)
                {
                    // Recorded before the call, so a batch projection that buffered part of this event and then
                    // threw is still flushed on the skip path in HandleAsync.
                    if (toFlush is not null && projection is IBatchProjection batched && !toFlush.Contains(batched))
                    {
                        toFlush.Add(batched);
                    }

                    await binder(@event, projection, meta, ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                ProjectionsSpans.Failed(activity, ex);
                throw;
            }
        }
    }
}
