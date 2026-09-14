using System.Diagnostics;

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
}
