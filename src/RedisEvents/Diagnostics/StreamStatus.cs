namespace RedisEvents.Diagnostics;

/// <summary>
/// Every stream partition this process is reading, as a snapshot any host can inspect.
/// </summary>
/// <remarks>
/// <para>
/// The public face of the per-partition monitors. <see cref="StreamHealth"/> grades this snapshot
/// into one verdict, and <c>RedisEvents.Web</c>'s <c>StreamsHealthCheck</c> reports that verdict on
/// an ASP.NET <c>/health</c> endpoint; the raw snapshot is for everyone else — a Generic Host worker with no HTTP
/// surface, a host that wants to stop itself when a partition dies, a custom metric, a diagnostic
/// endpoint that reports more than one word.
/// </para>
/// <para>
/// <b>Static, like the monitors behind it.</b> A worker takes its monitor from a static registry at
/// startup, so there is no container instance to resolve and this needs none either. That also
/// means it reports partitions for the whole process, not per host — a service running two
/// consumers sees both.
/// </para>
/// </remarks>
public static class StreamStatus
{
    /// <summary>True when any partition is registered — a cheap pre-check before allocating a snapshot.</summary>
    public static bool Any => StreamLag.Any;

    /// <summary>
    /// Reads every registered partition's signals, each taken at the moment it is read.
    /// </summary>
    /// <returns>
    /// One <see cref="StreamPartitionStatus"/> per partition, in no guaranteed order. Empty in a
    /// producer-only process, which is not the same as something being wrong.
    /// </returns>
    /// <remarks>
    /// Allocates one array and one struct per partition, so it is for health checks, watchdogs and
    /// diagnostics — poll it on a timer, not on a message path.
    /// </remarks>
    public static IReadOnlyList<StreamPartitionStatus> Partitions()
    {
        var monitors = StreamLag.All;
        if (monitors.Count == 0)
        {
            return [];
        }

        var snapshot = new List<StreamPartitionStatus>(monitors.Count);
        foreach (var m in monitors)
        {
            snapshot.Add(Snapshot(m));
        }

        return snapshot;
    }

    internal static StreamPartitionStatus Snapshot(StreamPartitionMonitor m)
    {
        var progress = m.ReadProgress();
        var state = (StreamPartitionRunState)(int)m.State;

        return new()
        {
            Topic = m.Topic,
            Consumer = m.Consumer,
            Partition = m.Partition,
            State = state,
            StopReason = m.StopReason,
            LastProcessed = m.LastProcessed,
            IsCaughtUp = m.IsCaughtUp,
            LagMs = m.LagMs,
            LagEntries = m.LagEntries,
            BlockedMs = m.BlockedMs,
            StoppedMs = m.StoppedMs,
            UnhealthyLagMs = m.UnhealthyLagMs,
            UnhealthyBlockSeconds = m.UnhealthyBlockSeconds,
            UnhealthyStoppedSeconds = m.UnhealthyStoppedSeconds,
            UnhealthyBehindSeconds = m.UnhealthyBehindSeconds,
            Escalates = state == StreamPartitionRunState.Stopped && m.Escalates,
            Position = progress.Position,
            PositionUnchangedMs = progress.PositionUnchangedMs,
            TailId = progress.Tail,
            TailSampleAgeMs = progress.TailSampleAgeMs,
            TailUnchangedMs = progress.TailUnchangedMs,
            IsBehindTail = progress.IsBehind,
            BehindMs = progress.BehindMs,
        };
    }
}
