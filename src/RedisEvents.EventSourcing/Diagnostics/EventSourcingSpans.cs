using System.Diagnostics;

using RedisEvents.Wire;

namespace RedisEvents.EventSourcing.Diagnostics;

/// <summary>
/// The package's own spans — <c>eventsourcing.save {aggregate}</c>,
/// <c>eventsourcing.load {aggregate}</c> and <c>eventsourcing.project {wireType}</c> — and the
/// attribute names they share.
/// </summary>
/// <remarks>
/// <para>
/// <b>Cost when tracing is off.</b> Every span starts at
/// <see cref="ActivitySource.StartActivity(string, ActivityKind)"/>, which returns
/// <see langword="null"/> when no listener is registered: no span object, no tags, no allocation,
/// one predictable branch. Every attribute set beyond that is guarded by
/// <see cref="Activity.IsAllDataRequested"/>, so a span that is created but sampled out costs
/// nothing beyond its own construction — the same discipline as core's <c>StreamSpans</c>.
/// </para>
/// <para>
/// <b>Save and load versus project.</b> <see cref="StartSave"/> and <see cref="StartLoad"/> run on
/// the caller's own execution context — a repository call is a plain request/response over one
/// Redis round trip, not a message hop with a decoupled reader — so <see cref="Activity.Current"/>
/// is genuinely the right parent and no traceparent rebuilding is needed, unlike core's consumer
/// side. <see cref="StartProject"/> is the same story for a projection dispatch: <c>HandleAsync</c>
/// runs as one continuous async call chain from wherever core's partition worker invoked it, with
/// no reader/processor split, so a plain <see cref="ActivitySource.StartActivity(string, ActivityKind)"/>
/// with no explicit parent or links naturally nests under whatever <c>streams.process</c> span core
/// already started for the batch.
/// </para>
/// <para>
/// <b>Conflict versus failure.</b> A <see cref="ConcurrencyException"/> is an expected,
/// caller-recoverable outcome (see the package's README) — not a transport or programming failure —
/// so <see cref="ConcurrencyConflict"/> is a distinct method from <see cref="Failed"/> even though
/// both end in <see cref="ActivityStatusCode.Error"/>: an operator scanning traces should be able to
/// tell "somebody else won the race" apart from "something broke".
/// </para>
/// </remarks>
internal static class EventSourcingSpans
{
    /// <summary>Span name prefix for a save; the aggregate name is appended.</summary>
    private const string SaveSpanName = "eventsourcing.save";

    /// <summary>Span name prefix for a load; the aggregate name is appended.</summary>
    private const string LoadSpanName = "eventsourcing.load";

    /// <summary>Span name prefix for one event's projection dispatch; the wire type is appended.</summary>
    private const string ProjectSpanName = "eventsourcing.project";

    /// <summary>Attribute: the aggregate family, i.e. <see cref="AggregateRoot.AggregateName"/>.</summary>
    internal const string AggregateNameKey = "eventsourcing.aggregate";

    /// <summary>Attribute: the aggregate's id.</summary>
    internal const string AggregateIdKey = "eventsourcing.aggregate_id";

    /// <summary>Attribute: the version the caller expected the aggregate to still be at.</summary>
    internal const string ExpectedVersionKey = "eventsourcing.expected_version";

    /// <summary>Attribute: the version an operation resulted in or loaded.</summary>
    internal const string VersionKey = "eventsourcing.version";

    /// <summary>Attribute: how many events a save wrote.</summary>
    internal const string EventCountKey = "eventsourcing.event_count";

    /// <summary>Attribute: whether a load found an existing aggregate.</summary>
    internal const string FoundKey = "eventsourcing.found";

    /// <summary>Attribute: the wire type of the event being projected.</summary>
    internal const string WireTypeKey = "eventsourcing.wire_type";

    /// <summary>Attribute: the partition key an event being projected was published under.</summary>
    internal const string PartitionKeyKey = "eventsourcing.partition_key";

    /// <summary>Attribute: the Redis stream entry id an event being projected was read at.</summary>
    internal const string StreamIdKey = "eventsourcing.stream_id";

    /// <summary>
    /// Starts the span for one <see cref="IEventRepository.SaveAsync"/> call.
    /// </summary>
    /// <param name="aggregateName">The aggregate family, i.e. <see cref="AggregateRoot.AggregateName"/>.</param>
    /// <param name="id">The aggregate's id.</param>
    /// <param name="expectedVersion">The version the caller expects the aggregate to still be at.</param>
    /// <returns>
    /// The started span, or <see langword="null"/> when nothing is listening — in which case the
    /// caller has spent one branch and allocated nothing.
    /// </returns>
    internal static Activity? StartSave(string aggregateName, string id, long expectedVersion)
    {
        var activity = EventSourcingDiagnostics.Source.StartActivity(
            $"{SaveSpanName} {aggregateName}",
            ActivityKind.Client);

        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag(AggregateNameKey, aggregateName);
            activity.SetTag(AggregateIdKey, id);
            activity.SetTag(ExpectedVersionKey, expectedVersion);
        }

        return activity;
    }

    /// <summary>
    /// Tags a successful save's outcome. A no-op when the span is <see langword="null"/> or sampled
    /// out, but the counter is always incremented regardless of sampling.
    /// </summary>
    /// <param name="activity">The span from <see cref="StartSave"/>.</param>
    /// <param name="newVersion">The aggregate's version after the save committed.</param>
    /// <param name="eventCount">How many events the save wrote.</param>
    internal static void Saved(Activity? activity, int newVersion, int eventCount)
    {
        EventSourcingDiagnostics.Saves.Add(1);

        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag(VersionKey, newVersion);
            activity.SetTag(EventCountKey, eventCount);
        }
    }

    /// <summary>
    /// Marks a save's span as lost to an optimistic concurrency conflict — distinct from
    /// <see cref="Failed"/>, since a <see cref="ConcurrencyException"/> is an expected, caller-recoverable
    /// outcome and not a transport or programming failure.
    /// </summary>
    /// <param name="activity">The span from <see cref="StartSave"/>, if there is one.</param>
    /// <remarks>
    /// The status is set — and the counter incremented — even when the span is sampled out: status
    /// is not an attribute, and it is what a tail sampler keys off, so it must survive
    /// <see cref="Activity.IsAllDataRequested"/> being <see langword="false"/>.
    /// </remarks>
    internal static void ConcurrencyConflict(Activity? activity)
    {
        EventSourcingDiagnostics.SaveConflicts.Add(1);
        activity?.SetStatus(ActivityStatusCode.Error, "Concurrency conflict: the aggregate's version has changed.");
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

    /// <summary>
    /// Starts the span for one <see cref="IEventRepository.LoadAsync{TAggregate}"/> call.
    /// </summary>
    /// <param name="aggregateName">The aggregate family, i.e. <see cref="AggregateRoot.AggregateName"/>.</param>
    /// <param name="id">The aggregate's id.</param>
    /// <returns>The started span, or <see langword="null"/> when nothing is listening.</returns>
    internal static Activity? StartLoad(string aggregateName, string id)
    {
        var activity = EventSourcingDiagnostics.Source.StartActivity(
            $"{LoadSpanName} {aggregateName}",
            ActivityKind.Client);

        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag(AggregateNameKey, aggregateName);
            activity.SetTag(AggregateIdKey, id);
        }

        return activity;
    }

    /// <summary>
    /// Tags a load's outcome. The counter is always incremented, whether or not the aggregate
    /// existed.
    /// </summary>
    /// <param name="activity">The span from <see cref="StartLoad"/>.</param>
    /// <param name="version">
    /// The version loaded, or <see langword="null"/> when no history exists for this id.
    /// </param>
    internal static void Loaded(Activity? activity, int? version)
    {
        EventSourcingDiagnostics.Loads.Add(1);

        if (activity is not { IsAllDataRequested: true })
        {
            return;
        }

        if (version is null)
        {
            activity.SetTag(FoundKey, false);
        }
        else
        {
            activity.SetTag(FoundKey, true);
            activity.SetTag(VersionKey, version.Value);
        }
    }

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
        var activity = EventSourcingDiagnostics.Source.StartActivity(
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
}
