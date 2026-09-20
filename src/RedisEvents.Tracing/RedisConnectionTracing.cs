using OpenTelemetry.Instrumentation.StackExchangeRedis;
using StackExchange.Redis;

namespace RedisEvents.Tracing.Implementation;

/// <summary>
/// Meets connections the library creates with the <see cref="StackExchangeRedisInstrumentation"/> of
/// the tracer provider. The two arrive in either order — a service may connect before or after its
/// tracer provider is built — and live in different containers, so this is process-wide state.
/// </summary>
internal static class RedisConnectionTracing
{
    private static readonly Lock Gate = new();
    private static readonly List<IConnectionMultiplexer> Pending = [];
    private static readonly List<StackExchangeRedisInstrumentation> Instrumentations = [];

    /// <summary>Registers a connection with every known instrumentation, or holds it until one appears.</summary>
    public static void Apply(IConnectionMultiplexer multiplexer)
    {
        ArgumentNullException.ThrowIfNull(multiplexer);

        StackExchangeRedisInstrumentation[] targets;
        lock (Gate)
        {
            if (Instrumentations.Count == 0)
            {
                Pending.Add(multiplexer);
                return;
            }

            targets = [.. Instrumentations];
        }

        foreach (var instrumentation in targets)
            instrumentation.AddConnection(multiplexer);
    }

    /// <summary>Adopts an instrumentation and hands it every connection that connected before it existed.</summary>
    public static void Attach(StackExchangeRedisInstrumentation instrumentation)
    {
        ArgumentNullException.ThrowIfNull(instrumentation);

        IConnectionMultiplexer[] waiting;
        lock (Gate)
        {
            if (Instrumentations.Contains(instrumentation))
                return;

            Instrumentations.Add(instrumentation);
            waiting = [.. Pending];
            Pending.Clear();
        }

        foreach (var multiplexer in waiting)
            instrumentation.AddConnection(multiplexer);
    }
}
