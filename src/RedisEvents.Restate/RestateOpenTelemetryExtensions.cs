using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

using RedisEvents.Restate.Diagnostics;

namespace RedisEvents.Restate;

/// <summary>
/// One-call OpenTelemetry wiring for RedisEvents.Restate, beside core's <c>AddRedisEvents()</c>.
/// Without these calls the package's <c>restate.start {workflow}</c> spans and <c>restate.start.*</c>
/// counters are produced and dropped.
/// </summary>
public static class RestateOpenTelemetryExtensions
{
    /// <summary>The name of both the activity source and the meter.</summary>
    public const string TelemetrySourceName = RestateDiagnostics.SourceName;

    /// <summary>Emits the <c>restate.start {workflow}</c> client spans into this tracer provider.</summary>
    /// <param name="builder">The tracer provider builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static TracerProviderBuilder AddRedisEventsRestate(this TracerProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        RestateDiagnostics.EnsureInitialized();
        return builder.AddSource(TelemetrySourceName);
    }

    /// <summary>
    /// Emits <c>restate.start.kept</c>, <c>.filtered</c>, <c>.accepted</c>, <c>.already_started</c>,
    /// <c>.retried</c> and <c>.blocked</c> into this meter provider.
    /// </summary>
    /// <param name="builder">The meter provider builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static MeterProviderBuilder AddRedisEventsRestate(this MeterProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        RestateDiagnostics.EnsureInitialized();
        return builder.AddMeter(TelemetrySourceName);
    }
}
