using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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
}