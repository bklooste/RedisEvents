using OpenTelemetry.Trace;
using RedisEvents.Tracing;
using StackExchange.Redis;

namespace RedisEvents.Tracing.Implementation;

/// <summary>
/// Implementation of IRedisTracingApplier that uses OpenTelemetry to trace Redis operations.
/// </summary>
public class OpenTelemetryRedisTracingApplier : IRedisTracingApplier
{
    private readonly Action<TracerProviderBuilder>? configureTracing;

    /// <summary>
    /// Creates a new instance of OpenTelemetryRedisTracingApplier.
    /// </summary>
    /// <param name="configureTracing">Callback to configure Redis tracing on the tracer provider builder.</param>
    public OpenTelemetryRedisTracingApplier(Action<TracerProviderBuilder>? configureTracing = null)
    {
        this.configureTracing = configureTracing;
    }

    /// <summary>
    /// Adds a connection the library created to the tracer provider's Redis instrumentation, so its
    /// commands are traced. Needs <see cref="RedisTracingExtensions.AddRedisEventsConnectionTracing"/>
    /// on the tracer provider; a connection made before the provider is built is held until then.
    /// </summary>
    /// <param name="multiplexer">The connection multiplexer to apply tracing to.</param>
    public void ApplyTracing(IConnectionMultiplexer multiplexer) => RedisConnectionTracing.Apply(multiplexer);

    /// <summary>
    /// Configures Redis tracing on the tracer provider builder.
    /// </summary>
    /// <param name="tracerProviderBuilder">The tracer provider builder to configure.</param>
    public void ConfigureTracing(TracerProviderBuilder tracerProviderBuilder)
    {
        this.configureTracing?.Invoke(tracerProviderBuilder);
    }


}