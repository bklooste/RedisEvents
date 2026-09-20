using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Instrumentation.StackExchangeRedis;
using OpenTelemetry.Trace;
using RedisEvents.Config;
using RedisEvents.Extensions;
using RedisEvents.Tracing;
using RedisEvents.Tracing.Implementation;

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
    /// <param name="configureTracing">Callback to configure Redis tracing on the tracer provider builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static IHostApplicationBuilder AddRedisTracing(this IHostApplicationBuilder builder, Action<TracerProviderBuilder>? configureTracing = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Register the Redis tracing applier
        builder.Services.AddSingleton<IRedisTracingApplier>(sp => new OpenTelemetryRedisTracingApplier(configureTracing));

        return builder;
    }

    /// <summary>
    /// Traces the Redis connections RedisEvents creates itself — the ones opened when
    /// <c>Streams:ConnectionString</c> differs from the container's <c>IConnectionMultiplexer</c>, and
    /// every consumer reader connection. Pair with <see cref="AddRedisTracing"/> on the host builder.
    /// A multiplexer registered in the container is not touched: instrument that one with
    /// <c>AddRedisInstrumentation(multiplexer)</c>.
    /// </summary>
    /// <param name="builder">The tracer provider builder.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static TracerProviderBuilder AddRedisEventsConnectionTracing(this TracerProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder
            .AddRedisInstrumentation()
            .ConfigureRedisInstrumentation(RedisConnectionTracing.Attach);
    }
}
