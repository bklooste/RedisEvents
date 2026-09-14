using System.Diagnostics;

using RedisEvents.Wire;

namespace RedisEvents.Projections.Diagnostics;

/// <summary>
/// The package's own span — <c>projections.project {wireType}</c> — and the attribute names it uses.
/// </summary>
/// <remarks>
/// <para>
/// <b>Cost when tracing is off.</b> <see cref="ActivitySource.StartActivity(string, ActivityKind)"/>
/// returns <see langword="null"/> when no listener is registered: no span object, no tags, no
/// allocation, one predictable branch. Every attribute set beyond that is guarded by
/// <see cref="Activity.IsAllDataRequested"/>, so a span that is created but sampled out costs nothing
/// beyond its own construction — the same discipline as core's <c>StreamSpans</c>.
/// </para>
/// <para>
/// <see cref="StartProject"/> runs on the caller's own execution context: <c>HandleAsync</c> runs as
/// one continuous async call chain from wherever core's partition worker invoked it, with no
/// reader/processor split, so a plain <see cref="ActivitySource.StartActivity(string, ActivityKind)"/>
/// with no explicit parent or links naturally nests under whatever <c>streams.process</c> span core
/// already started for the batch.
/// </para>
/// </remarks>
internal static class ProjectionsSpans
{
    /// <summary>Span name prefix for one event's projection dispatch; the wire type is appended.</summary>
    private const string ProjectSpanName = "projections.project";

    /// <summary>Attribute: the wire type of the event being projected.</summary>
    internal const string WireTypeKey = "projections.wire_type";

    /// <summary>Attribute: the partition key an event being projected was published under.</summary>
    internal const string PartitionKeyKey = "projections.partition_key";

    /// <summary>Attribute: the Redis stream entry id an event being projected was read at.</summary>
    internal const string StreamIdKey = "projections.stream_id";

    /// <summary>
    /// Starts the span for one event's dispatch to every <see cref="IProjection{TEvent}"/> bound to
    /// it, inside <see cref="EventProjector.HandleAsync"/>.
    /// </summary>
    /// <param name="wireType">The event's wire type.</param>
    /// <param name="partitionKey">The partition key the event was published under, i.e. <see cref="EventMeta.PartitionKey"/>.</param>
    /// <param name="id">The Redis stream entry id the event was read at, i.e. <see cref="EventMeta.Id"/>.</param>
    /// <returns>The started span, or <see langword="null"/> when nothing is listening.</returns>
    /// <remarks>
    /// One span per dispatched event, not one per bound projection: several projections may bind to
    /// the same event, and they run as one unit of work inside the batch, exactly like core reports
    /// one <c>streams.process</c> span per batch rather than one per message. Tagged with the stream's
    /// own partition key and entry id, not an aggregate id or version — the projector depends on
    /// nothing beyond a standard RedisEvents topic, see <see cref="EventMeta"/>.
    /// </remarks>
    internal static Activity? StartProject(string wireType, string partitionKey, StreamId id)
    {
        var activity = ProjectionsDiagnostics.Source.StartActivity(
            $"{ProjectSpanName} {wireType}",
            ActivityKind.Internal);

        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag(WireTypeKey, wireType);
            activity.SetTag(PartitionKeyKey, partitionKey);
            activity.SetTag(StreamIdKey, id.Format());
        }

        return activity;
    }

    /// <summary>
    /// Marks a span as failed by a genuine, unexpected error. Safe on a <see langword="null"/> span,
    /// so a caller can wrap a call in one <c>catch</c> without testing anything first.
    /// </summary>
    /// <param name="activity">The span, if there is one.</param>
    /// <param name="ex">The failure.</param>
    internal static void Failed(Activity? activity, Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);

        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
    }
}
