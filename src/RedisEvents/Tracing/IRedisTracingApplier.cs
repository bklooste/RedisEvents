using StackExchange.Redis;

namespace RedisEvents.Tracing;

/// <summary>
/// Interface for applying tracing to Redis connections.
/// This allows external tracing libraries to inject tracing functionality
/// without creating tight coupling between RedisEvents and specific tracing implementations.
/// </summary>
public interface IRedisTracingApplier
{
    /// <summary>
    /// Applies tracing to the given connection multiplexer.
    /// </summary>
    /// <param name="multiplexer">The connection multiplexer to apply tracing to.</param>
    void ApplyTracing(IConnectionMultiplexer multiplexer);
}

/// <summary>
/// No-op implementation of IRedisTracingApplier for when tracing is not configured.
/// </summary>
internal class NoOpRedisTracingApplier : IRedisTracingApplier
{
    public static readonly NoOpRedisTracingApplier Instance = new();
    
    private NoOpRedisTracingApplier() { }
    
    public void ApplyTracing(IConnectionMultiplexer multiplexer)
    {
        // No-op implementation
    }
}