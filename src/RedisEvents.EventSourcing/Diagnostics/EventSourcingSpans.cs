using System.Diagnostics;

namespace RedisEvents.EventSourcing.Diagnostics;

/// <summary>
/// The package's own spans — <c>eventsourcing.save {aggregate}</c> and
/// <c>eventsourcing.load {aggregate}</c> — and the attribute names they share. The equivalent span
/// for a projection dispatch, <c>projections.project {wireType}</c>, lives in the sibling
/// <c>RedisEvents.Projections</c> package's own <c>ProjectionsSpans</c> — this package depends on
/// that one, not the other way round, so the read-side span cannot live here.
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
/// <b>Save and load.</b> <see cref="StartSave"/> and <see cref="StartLoad"/> run on
/// the caller's own execution context — a repository call is a plain request/response over one
/// Redis round trip, not a message hop with a decoupled reader — so <see cref="Activity.Current"/>
/// is genuinely the right parent and no traceparent rebuilding is needed, unlike core's consumer
/// side.
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
}
