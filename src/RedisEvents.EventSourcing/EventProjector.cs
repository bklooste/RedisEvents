using System.Text;

using Microsoft.Extensions.Logging;

using RedisEvents.Consumer;
using RedisEvents.EventSourcing.Diagnostics;
using RedisEvents.Wire;

namespace RedisEvents.EventSourcing;

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
/// </remarks>
public sealed class EventProjector : IBatchHandler
{
    private static readonly byte[] VersionHeaderUtf8 = Encoding.UTF8.GetBytes("es-version");

    private readonly EventTypeRegistry types;
    private readonly IReadOnlyList<object> projections;
    private readonly ILogger? logger;

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
    /// <param name="logger">Optional logger for the Debug-level skip notices. May be <see langword="null"/>.</param>
    public EventProjector(EventTypeRegistry types, IReadOnlyList<object> projections, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(types);
        ArgumentNullException.ThrowIfNull(projections);

        this.types = types;
        this.projections = projections;
        this.logger = logger;
    }

    /// <inheritdoc />
    /// <exception cref="InvalidOperationException">
    /// A message otherwise recognised by <paramref name="batch"/>'s wire type is missing the
    /// <c>es-version</c> header, or its value does not parse as an integer. This header is stamped by
    /// <c>RedisEventRepository</c> on every event it publishes, so its absence means the message was
    /// produced by something else, or by a corrupted or incompatible codec — either way an event
    /// sourcing consumer cannot know the event's aggregate version, which idempotent projections
    /// depend on, so this is a loud decode-format failure rather than a silent default of <c>0</c>.
    /// </exception>
    public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            var msg = batch.Span[i];

            if (!types.TryDecode(msg.Type, msg.Body, out var @event))
            {
                logger?.LogDebug(
                    "EventProjector: skipping event of unknown wire type '{WireType}' — no EventTypeRegistry registration for it.",
                    msg.Type);
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
                AggregateId: msg.PartitionKey,
                Version: ReadVersionHeader(msg),
                Id: msg.Id,
                CorrelationId: msg.CorrelationId);

            // One span covers dispatch to every projection bound to this event — several may bind to
            // the same wire type, and they run as one unit of work, the same way core reports one
            // streams.process span per batch rather than one per message. It naturally nests under
            // whatever streams.process span core already started for the batch: HandleAsync runs as
            // one continuous async call chain with no reader/processor split to work around, unlike
            // core's consumer side, so plain ActivitySource.StartActivity is correct here.
            using var activity = EventSourcingSpans.StartProject(msg.Type, meta.AggregateId, meta.Version);

            // No try/catch by design — see the type's <remarks> — except the one below, which exists
            // solely to record the failure on the span. It changes nothing about the error contract:
            // the exact same exception, with its original stack, still propagates out of HandleAsync
            // immediately and unchanged, and the rest of the batch is still not processed. Disposing
            // the (possibly null) activity happens via the enclosing `using`, on every exit path.
            try
            {
                foreach (var (projection, binder) in bound)
                {
                    await binder(@event, projection, meta, ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                EventSourcingSpans.Failed(activity, ex);
                throw;
            }
        }
    }

    private static int ReadVersionHeader(in StreamMsg msg)
    {
        if (!msg.Headers.TryGetValueUtf8(VersionHeaderUtf8, out var valueUtf8) ||
            !System.Buffers.Text.Utf8Parser.TryParse(valueUtf8, out int version, out var consumed) ||
            consumed != valueUtf8.Length)
        {
            throw new InvalidOperationException(
                $"Event of wire type '{msg.Type}' is missing a valid integer 'es-version' header; " +
                "found " + (msg.Headers.TryGetValue("es-version", out var raw) ? $"'{raw}'" : "none") + ". " +
                "This header is required to build EventMeta.Version.");
        }

        return version;
    }
}
