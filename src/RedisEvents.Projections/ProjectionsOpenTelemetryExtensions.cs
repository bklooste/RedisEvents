using OpenTelemetry.Trace;

using RedisEvents.Projections.Diagnostics;

namespace RedisEvents.Projections;

/// <summary>
/// One-call OpenTelemetry wiring for RedisEvents.Projections, in the shape of the
/// <c>AddRedisEvents</c> call core already exposes — a service that wires up core's tracing adds one
/// more call next to it for this package's.
///
/// <para>
/// The package's own <see cref="System.Diagnostics.ActivitySource"/> is named
/// <c>RedisEvents.Projections</c> — deliberately distinct from core's <c>RedisEvents</c> and from the
/// sibling <c>RedisEvents.EventSourcing</c> package's own source — and is static, so nothing here
/// constructs anything; it only tells the provider to listen. Without this call the package's own
/// <c>projections.project</c> spans are produced and dropped, exactly as core's are without
/// <c>AddRedisEvents()</c>.
/// </para>
/// </summary>
public static class ProjectionsOpenTelemetryExtensions
{
    /// <summary>
    /// The name of the <see cref="System.Diagnostics.ActivitySource"/>. Exposed for services that
    /// wire providers by name from configuration rather than by calling the method below.
    /// </summary>
    public const string TelemetrySourceName = ProjectionsDiagnostics.SourceName;

    /// <summary>
    /// Emits the package's span — <c>projections.project {wireType}</c> — into this tracer provider.
    /// </summary>
    /// <param name="builder">The tracer provider builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static TracerProviderBuilder AddRedisEventProjections(this TracerProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.AddSource(TelemetrySourceName);
    }
}
