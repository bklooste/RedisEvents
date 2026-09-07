using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Orange.Lib.Streams.Diagnostics;

namespace Orange.Lib.Streams.Extensions;

/// <summary>
/// One-call OpenTelemetry wiring for Orange.Lib.Streams, in the shape of the
/// <c>AddRedisInstrumentation</c> call a service already makes in its <c>ConfigureTracing</c> hook.
///
/// <para>
/// The library's <see cref="System.Diagnostics.ActivitySource"/> and
/// <see cref="System.Diagnostics.Metrics.Meter"/> are both named <c>Orange.Lib.Streams</c> and are
/// static, so nothing here constructs anything — it only tells the provider to listen. Without
/// these calls the spans and metrics are produced and dropped, which is the usual reason a service
/// reports no stream telemetry at all.
/// </para>
/// </summary>
public static class OpenTelemetryExtensions
{
    /// <summary>
    /// The name of both the <see cref="System.Diagnostics.ActivitySource"/> and the
    /// <see cref="System.Diagnostics.Metrics.Meter"/>. Exposed for services that wire providers by
    /// name from configuration rather than by calling the methods below.
    /// </summary>
    public const string TelemetrySourceName = StreamsDiagnostics.SourceName;

    /// <summary>
    /// Emits the library's spans — <c>streams.publish {topic}</c> and <c>streams.process {topic}</c> —
    /// into this tracer provider.
    /// </summary>
    /// <param name="builder">The tracer provider builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static TracerProviderBuilder AddOrangeStreams(this TracerProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        StreamsDiagnostics.EnsureInitialized();
        return builder.AddSource(TelemetrySourceName);
    }

    /// <summary>
    /// Emits the library's metrics — publish/consume counts, batch histograms, and the lag,
    /// block, ownership, buffer and stream-length gauges — into this meter provider.
    /// </summary>
    /// <param name="builder">The meter provider builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static MeterProviderBuilder AddOrangeStreams(this MeterProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // The gauges only report once the meter is listened to, and their callbacks read the maps
        // the components keep updated, so the instruments must exist before the first scrape.
        StreamsDiagnostics.EnsureInitialized();
        return builder.AddMeter(TelemetrySourceName);
    }
}
