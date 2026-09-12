using System.Globalization;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using RedisEvents.Consumer;
using RedisEvents.Diagnostics;
using RedisEvents.Extensions;
using RedisEvents.Ownership;

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
///   <description>No worker is running at all, the Redis connection is down, or a partition has been
///   blocked for longer than <c>UnhealthyBlockSeconds</c> (default 300).</description>
/// </item>
/// </list>
/// <para>
/// <b>Lag is Degraded, never Unhealthy, and that is deliberate.</b> Failing readiness on lag asks
/// Kubernetes to restart a pod whose only problem is that it is behind — which drops its in-flight
/// batches, re-reads from the last persisted position, and makes the lag <em>worse</em>, on a loop,
/// for as long as the backlog lasts. A pod that is catching up is doing exactly what it should;
/// Degraded is loud enough to alert on and quiet enough not to kill it. Only a genuinely dead
/// consumer — no workers, no connection, or a partition wedged past the block threshold — fails
/// readiness, because that is the only case a restart can actually fix.
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
        var hosts = this.Hosts();
        var monitors = Monitors();

        if (hosts.Count == 0 && monitors.Length == 0)
        {
            // A producer-only service, or one whose consumers are not registered here. Nothing to
            // report is not the same as something being wrong.
            return Healthy("Streams: no stream consumers are registered in this process.");
        }

        var data = new Dictionary<string, object>(StringComparer.Ordinal)
        {
            ["consumers"] = hosts.Count,
            ["partitions"] = monitors.Length,
        };

        // StreamConsumerHost.IsRunning is "StartAsync ran and StopAsync has not" — it says nothing
        // about whether anything is being read, which is why it is counted as "started" here and the
        // partition monitors, not this number, decide Healthy. A host that started and owns no live
        // partition is caught by the tally below, not by this loop.
        var started = 0;
        foreach (var host in hosts)
        {
            if (host.IsRunning)
            {
                started++;
            }
        }

        data["consumersStarted"] = started;

        var tally = Tally(monitors, data);

        if (this.ConnectionDown(out var connectionDetail))
        {
            data["connection"] = connectionDetail;

            return Unhealthy(
                $"Streams: the Redis connection is down ({connectionDetail}); nothing is being consumed.",
                data);
        }

        if (tally.BlockedTooLong is { } wedged)
        {
            return Unhealthy(
                $"Streams: partition {Describe(wedged)} has been blocked for {Seconds(wedged.BlockedMs)}s retrying a DontIgnoreException, " +
                $"past its UnhealthyBlockSeconds of {wedged.UnhealthyBlockSeconds}. Its position has not advanced and its backlog is at risk of being trimmed.",
                data);
        }

        if (hosts.Count > 0 && started == 0)
        {
            return Unhealthy(
                $"Streams: none of the {hosts.Count} registered stream consumers has started.",
                data);
        }

        if (monitors.Length > 0 && tally.Live == 0)
        {
            return Unhealthy(
                $"Streams: all {monitors.Length} partition workers have stopped; nothing is being consumed. " +
                (tally.StopReason is null ? string.Empty : $"Last reason: {tally.StopReason}"),
                data);
        }

        var ownershipDegraded = OwnershipRegistry.AnyDegraded();
        data["ownershipDegraded"] = ownershipDegraded;

        // Starting is not Running (R-16). It used to fall into the same bucket, so a consumer whose
        // workers had registered but never reached their first fetch — a reader connection that
        // cannot connect, a start that faulted after the monitors were created — reported Healthy
        // with a full partition count. Reporting Degraded here says "nothing has read yet", which is
        // true for a second or two of every normal startup and stays true for as long as the fault
        // lasts. Degraded, not Unhealthy: restarting a pod that is still starting is a loop.
        if (tally.Running == 0 && tally.Starting > 0)
        {
            return Degraded(
                $"Streams: none of the {monitors.Length} partition worker(s) has started reading yet " +
                $"({tally.Starting} still starting).",
                data);
        }

        if (tally.Blocked > 0 || tally.Stopped > 0 || tally.Lagging is not null || ownershipDegraded)
        {
            return Degraded(Explain(in tally, ownershipDegraded), data);
        }

        return Healthy(
            $"Streams: {started} consumer(s), {tally.Running} of {monitors.Length} partition(s) reading, " +
            $"worst lag {Seconds(tally.WorstLagMs)}s.",
            data);
    }

    private static StreamPartitionMonitor[] Monitors()
    {
        var all = StreamLag.All;
        if (all.Count == 0)
        {
            return [];
        }

        var snapshot = new StreamPartitionMonitor[all.Count];
        var i = 0;

        foreach (var monitor in all)
        {
            if (i == snapshot.Length)
            {
                // The collection grew between the count and the copy; the rest can wait for the next
                // check rather than costing a resize here.
                break;
            }

            snapshot[i++] = monitor;
        }

        return i == snapshot.Length ? snapshot : snapshot[..i];
    }

    /// <summary>
    /// Walks the monitors once, counting states and remembering the worst offender in each category.
    /// </summary>
    private static Counts Tally(StreamPartitionMonitor[] monitors, Dictionary<string, object> data)
    {
        var counts = default(Counts);

        foreach (var monitor in monitors)
        {
            switch (monitor.State)
            {
                case PartitionRunState.Blocked:
                    counts.Blocked++;
                    break;

                case PartitionRunState.Stopped:
                    counts.Stopped++;
                    counts.StopReason ??= monitor.StopReason;
                    break;

                case PartitionRunState.Starting:
                    counts.Starting++;
                    break;

                default:
                    counts.Running++;
                    break;
            }

            var lag = monitor.LagMs;
            if (lag > counts.WorstLagMs)
            {
                counts.WorstLagMs = lag;
                counts.Worst = monitor;
            }

            // A partition whose sampled entry lag is exactly zero is at the tail of its stream, so a
            // large lag.ms there only means the topic is quiet — not that anything is behind.
            if (lag > monitor.UnhealthyLagMs && monitor.LagEntries != 0)
            {
                counts.Lagging ??= monitor;
            }

            if (monitor.State == PartitionRunState.Blocked &&
                monitor.BlockedMs > monitor.UnhealthyBlockSeconds * 1000d)
            {
                counts.BlockedTooLong ??= monitor;
            }
        }

        // Blocked partitions count as live: they are retrying, not gone. Starting ones are live too
        // — they have not stood down — but they are not reading, which is a different question and
        // has its own counter.
        counts.Live = counts.Running + counts.Blocked + counts.Starting;

        data["partitionsBlocked"] = counts.Blocked;
        data["partitionsStopped"] = counts.Stopped;
        data["partitionsStarting"] = counts.Starting;
        data["worstLagMs"] = (long)counts.WorstLagMs;

        if (counts.Worst is not null)
        {
            data["worstLagPartition"] = Describe(counts.Worst);
            data["worstLagEntries"] = counts.Worst.LagEntries;
        }

        return counts;
    }

    private static string Explain(in Counts counts, bool ownershipDegraded)
    {
        var reasons = new List<string>(4);

        if (counts.Blocked > 0)
        {
            reasons.Add(
                $"{counts.Blocked} partition(s) blocked retrying a DontIgnoreException; positions are not advancing");
        }

        if (counts.Stopped > 0)
        {
            reasons.Add(
                counts.StopReason is null
                    ? $"{counts.Stopped} partition(s) stopped"
                    : $"{counts.Stopped} partition(s) stopped ({counts.StopReason})");
        }

        if (counts.Lagging is { } lagging)
        {
            reasons.Add(
                $"partition {Describe(lagging)} is {Seconds(lagging.LagMs)}s behind, past its UnhealthyLagMs of {lagging.UnhealthyLagMs}ms");
        }

        if (ownershipDegraded)
        {
            reasons.Add(
                "the ownership registry shows a gap or an overlap — check STREAMS_INSTANCE_COUNT against spec.replicas, and that a partitioned consumer runs as a StatefulSet");
        }

        return string.Concat("Streams: ", string.Join("; ", reasons), ".");
    }

    private static string Describe(StreamPartitionMonitor monitor)
        => string.Create(
            CultureInfo.InvariantCulture,
            $"{monitor.Topic}[{monitor.Partition}]/{monitor.Consumer}");

    private static string Seconds(double milliseconds)
        => (milliseconds / 1000d).ToString("F1", CultureInfo.InvariantCulture);

    private static Task<HealthCheckResult> Healthy(string description, IReadOnlyDictionary<string, object>? data = null)
        => Task.FromResult(HealthCheckResult.Healthy(description, data));

    private static Task<HealthCheckResult> Degraded(string description, IReadOnlyDictionary<string, object> data)
        => Task.FromResult(HealthCheckResult.Degraded(description, exception: null, data));

    private static Task<HealthCheckResult> Unhealthy(string description, IReadOnlyDictionary<string, object> data)
        => Task.FromResult(HealthCheckResult.Unhealthy(description, exception: null, data));

    private List<StreamConsumerHost> Hosts()
    {
        var hosts = new List<StreamConsumerHost>();

        if (this.hostedServices is null)
        {
            return hosts;
        }

        foreach (var service in this.hostedServices)
        {
            if (service is StreamConsumerHost host)
            {
                hosts.Add(host);
            }
        }

        return hosts;
    }

    /// <summary>
    /// Whether the shared multiplexer is unusable. Resolving it here is safe: it is created once and
    /// cached, and a connect failure is itself the answer the check is looking for.
    /// </summary>
    private bool ConnectionDown(out string detail)
    {
        detail = string.Empty;

        if (this.connection is null)
        {
            return false;
        }

        try
        {
            var multiplexer = this.connection.Connection;

            if (multiplexer.IsConnected)
            {
                return false;
            }

            detail = StreamsConnectionProvider.DescribeEndpoints(multiplexer.GetEndPoints());
            return true;
        }
        catch (Exception ex)
        {
            detail = ex.Message;
            return true;
        }
    }

    /// <summary>One pass's worth of counters over the partition monitors.</summary>
    private struct Counts
    {
        /// <summary>Partitions reading or retrying or still starting — anything that is not stopped.</summary>
        public int Live;

        /// <summary>Partitions reading normally.</summary>
        public int Running;

        /// <summary>Partitions registered but not yet past their first fetch.</summary>
        public int Starting;

        /// <summary>Partitions blocked retrying a <c>DontIgnoreException</c>.</summary>
        public int Blocked;

        /// <summary>Partitions that have stood down.</summary>
        public int Stopped;

        /// <summary>The largest lag seen this pass, in milliseconds.</summary>
        public double WorstLagMs;

        /// <summary>The partition that lag belongs to.</summary>
        public StreamPartitionMonitor? Worst;

        /// <summary>The first partition found past its lag threshold, if any.</summary>
        public StreamPartitionMonitor? Lagging;

        /// <summary>The first partition found blocked past its block threshold, if any.</summary>
        public StreamPartitionMonitor? BlockedTooLong;

        /// <summary>Why the first stopped partition stopped.</summary>
        public string? StopReason;
    }
}
