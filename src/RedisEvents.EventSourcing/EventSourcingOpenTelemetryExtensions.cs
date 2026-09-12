using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

using RedisEvents.EventSourcing.Diagnostics;

namespace RedisEvents.EventSourcing;

/// <summary>
/// One-call OpenTelemetry wiring for RedisEvents.EventSourcing, in the shape of the
/// <c>AddRedisEvents</c> call core already exposes — a service that wires up core's tracing and
/// metrics adds one more call next to it for this package's.
///
/// <para>
/// The package's own <see cref="System.Diagnostics.ActivitySource"/> and
/// <see cref="System.Diagnostics.Metrics.Meter"/> are both named <c>RedisEvents.EventSourcing</c>
/// — deliberately distinct from core's <c>RedisEvents</c> — and are static, so nothing here
/// constructs anything; it only tells the provider to listen. Without these calls the package's
/// own <c>eventsourcing.save</c>/<c>eventsourcing.load</c>/<c>eventsourcing.project</c> spans and
/// counters are produced and dropped, exactly as core's are without <c>AddRedisEvents()</c>.
/// </para>
/// </summary>
public static class EventSourcingOpenTelemetryExtensions
{
    /// <summary>
    /// The name of both the <see cref="System.Diagnostics.ActivitySource"/> and the
    /// <see cref="System.Diagnostics.Metrics.Meter"/>. Exposed for services that wire providers by
    /// name from configuration rather than by calling the methods below.
    /// </summary>
    public const string TelemetrySourceName = EventSourcingDiagnostics.SourceName;

    /// <summary>
    /// Emits the package's spans — <c>eventsourcing.save {aggregate}</c>,
    /// <c>eventsourcing.load {aggregate}</c> and <c>eventsourcing.project {wireType}</c> — into this
    /// tracer provider.
    /// </summary>
    /// <param name="builder">The tracer provider builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static TracerProviderBuilder AddRedisEventSourcing(this TracerProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        EventSourcingDiagnostics.EnsureInitialized();
        return builder.AddSource(TelemetrySourceName);
    }

    /// <summary>
    /// Emits the package's metrics — save/conflict/load counts — into this meter provider.
    /// </summary>
    /// <param name="builder">The meter provider builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static MeterProviderBuilder AddRedisEventSourcing(this MeterProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        EventSourcingDiagnostics.EnsureInitialized();
        return builder.AddMeter(TelemetrySourceName);
    }
}
