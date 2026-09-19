using OpenTelemetry.Trace;
using RedisEvents.Tracing;
using StackExchange.Redis;

namespace RedisEvents.Tracing.Implementation;

/// <summary>
/// Implementation of IRedisTracingApplier that uses OpenTelemetry to trace Redis operations.
/// </summary>
public class OpenTelemetryRedisTracingApplier : IRedisTracingApplier
{
    private readonly TracerProvider? tracerProvider;

    /// <summary>
    /// Creates a new instance of OpenTelemetryRedisTracingApplier.
    /// </summary>
    /// <param name="tracerProvider">The tracer provider to use for Redis operation tracing.</param>
    public OpenTelemetryRedisTracingApplier(TracerProvider? tracerProvider = null)
    {
        this.tracerProvider = tracerProvider;
    }

    /// <summary>
    /// Applies OpenTelemetry tracing to the given connection multiplexer.
    /// </summary>
    /// <param name="multiplexer">The connection multiplexer to apply tracing to.</param>
    public void ApplyTracing(IConnectionMultiplexer multiplexer)
    {
        if (this.tracerProvider != null)
        {
            try
            {
                this.tracerProvider.AddRedisInstrumentation(multiplexer, options =>
                {
                    options.SetVerboseDatabaseStatements = true;
                });
            }
            catch
            {
                // If tracing setup fails, we continue without tracing
                // This ensures Redis functionality is never impacted by tracing issues
            }
        }
    }
}