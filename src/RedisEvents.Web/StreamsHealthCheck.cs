using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using RedisEvents.Diagnostics;
using RedisEvents.Extensions;

namespace RedisEvents.Web;

/// <summary>
/// The readiness answer for every stream consumer in this process, registered under the tag
/// <c>streams</c>.
/// </summary>
/// <remarks>
/// <list type="table">
/// <item>
///   <term>Healthy</term>
///   <description>Every owned partition worker is running, none is blocked, and lag is under
///   <c>UnhealthyLagMs</c> (default 120 s).</description>
/// </item>
/// <item>
///   <term>Degraded</term>
///   <description>A partition is blocked retrying a <c>DontIgnoreException</c>, has stopped under
///   <c>ErrorPolicy.StopPartition</c>, is lagging past the threshold, the ownership registry shows
///   a gap or an overlap, or no partition has started reading yet.</description>
/// </item>
/// <item>
///   <term>Unhealthy</term>
///   <description>No worker is running at all, the Redis connection is down, a partition has been
///   blocked for longer than <c>UnhealthyBlockSeconds</c> (default 300), a partition that stood down
///   outside any error policy has been stopped past <c>UnhealthyStoppedSeconds</c> with entries
///   waiting, or a partition has sat behind the tail of its stream with a position that does not
///   change for longer than <c>UnhealthyBehindSeconds</c> (default 0 = off).</description>
/// </item>
/// </list>
/// <para>
/// <b>This class is an adapter.</b> The rules live in <see cref="StreamHealth"/> in the core
/// library, so a Generic Host worker that cannot reference this package grades with the same code;
/// this turns its report into a <see cref="HealthCheckResult"/> and nothing else.
/// </para>
/// <para>
/// <b>Lag is Degraded, never Unhealthy, and that is deliberate.</b> Failing readiness on lag asks
/// Kubernetes to restart a pod whose only problem is that it is behind — which drops its in-flight
/// batches, re-reads from the last persisted position, and makes the lag <em>worse</em>, on a loop,
/// for as long as the backlog lasts. A pod that is catching up is doing exactly what it should;
/// Degraded is loud enough to alert on and quiet enough not to kill it. Only a genuinely dead
/// consumer — no workers, no connection, a partition wedged past the block threshold, or one that
/// is behind the tail and not moving at all — fails readiness, because that is the only case a
/// restart can actually fix. Behind-and-frozen is not lag: a consumer that is behind but advancing
/// restarts that clock with every batch and is never failed by it.
/// </para>
/// <para>
/// Ownership gaps and overlaps are Degraded for the same reason: both are configuration mistakes
/// (<c>STREAMS_INSTANCE_COUNT</c> drifting from <c>spec.replicas</c>, or a partitioned consumer on a
/// Deployment). Restarting the pod does not fix either, and failing readiness on every replica would
/// turn a scaling typo into an outage.
/// </para>
/// </remarks>
public sealed class StreamsHealthCheck : IHealthCheck
{
    private readonly StreamsConnectionProvider? connection;
    private readonly IEnumerable<IHostedService>? hostedServices;

    /// <summary>
    /// Creates the check. Both dependencies are optional so the check can be registered in a process
    /// that only publishes, or in a test that supplies neither.
    /// </summary>
    /// <param name="connection">
    /// The shared connection provider, used only to ask whether the multiplexer is connected.
    /// </param>
    /// <param name="hostedServices">
    /// The container's hosted services; the consumer hosts are picked out of it. They are registered
    /// as <see cref="IHostedService"/> (one per consumer, deliberately not de-duplicated by type), so
    /// this is the only way to reach them from a service the container builds separately.
    /// </param>
    internal StreamsHealthCheck(
        StreamsConnectionProvider? connection = null,
        IEnumerable<IHostedService>? hostedServices = null)
    {
        this.connection = connection;
        this.hostedServices = hostedServices;
    }

    /// <summary>The conventional registration name and tag for this check.</summary>
    public const string Name = "streams";

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var report = StreamHealth.Evaluate(this.connection, this.hostedServices);

        // Mapped by name, never by cast: HealthStatus counts down from Healthy = 2 and
        // StreamHealthStatus counts up from Healthy = 0.
        var result = report.Status switch
        {
            StreamHealthStatus.Healthy => HealthCheckResult.Healthy(report.Description, report.Data),
            StreamHealthStatus.Degraded => HealthCheckResult.Degraded(report.Description, exception: null, report.Data),
            _ => HealthCheckResult.Unhealthy(report.Description, exception: null, report.Data),
        };

        return Task.FromResult(result);
    }
}
