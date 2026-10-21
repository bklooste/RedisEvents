using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace RedisEvents.Projections.Diagnostics;

/// <summary>
/// The single <see cref="ActivitySource"/> for RedisEvents.Projections.
/// </summary>
/// <remarks>
/// A separate name from core's own <c>"RedisEvents"</c> source and from the sibling
/// <c>RedisEvents.EventSourcing</c> package's <c>"RedisEvents.EventSourcing"</c> source — this
/// package is independently usable, so it gets its own identity rather than borrowing one from a
/// package it does not depend on.
/// </remarks>
internal static class ProjectionsDiagnostics
{
    /// <summary>
    /// Source name for OpenTelemetry traces. Register it with <c>AddSource</c>, or use
    /// <c>AddRedisEventProjections()</c>.
    /// </summary>
    internal const string SourceName = "RedisEvents.Projections";

    /// <summary>Assembly version for diagnostics.</summary>
    private const string Version = "1.0.0";

    /// <summary>ActivitySource for OpenTelemetry tracing.</summary>
    internal static readonly ActivitySource Source = new(SourceName, Version);

    /// <summary>Meter for OpenTelemetry metrics, under the same name as <see cref="Source"/>.</summary>
    internal static readonly Meter Meter = new(SourceName, Version);

    /// <summary>Events skipped because a known wire type's body failed to decode. Tagged topic, wire_type.</summary>
    internal static readonly Counter<long> MalformedEvents =
        Meter.CreateCounter<long>("redisevents.projections.events.malformed", "events", "Events skipped because their body failed to decode");
}
