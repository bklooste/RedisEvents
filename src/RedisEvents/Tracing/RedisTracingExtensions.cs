using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Trace;
using RedisEvents.Config;
using RedisEvents.Extensions;

namespace RedisEvents.Tracing;

/// <summary>
/// Extension methods to enable Redis operation tracing for RedisEvents.
/// </summary>
public static class RedisTracingExtensions
{
    /// <summary>
    /// Enables Redis operation tracing on the shared streams connection and consumer reader connections.
    /// This method configures the RedisEvents library to trace all Redis operations
    /// using the provided tracer provider.
    /// Call this method before registering any RedisEvents components (AddStream, AddStreamPublisher, etc.).
    /// </summary>
    /// <param name="builder">The host application builder.</param>
    /// <param name="tracerProvider">The tracer provider to use for Redis operation tracing.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static IHostApplicationBuilder AddRedisTracing(this IHostApplicationBuilder builder, TracerProvider? tracerProvider = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Override the StreamsConnectionProvider registration to include the tracer provider
        var options = StreamConfigBinder.Bind(builder.Configuration);
        
        builder.Services.AddSingleton<StreamsConnectionProvider>(sp => new StreamsConnectionProvider(
            options,
            sp,
            sp.GetService<ILoggerFactory>()?.CreateLogger("RedisEvents"),
            tracerProvider));

        return builder;
    }
}