using RedisEvents.Wire;

namespace RedisEvents.Diagnostics;

/// <summary>What a partition's worker is doing, as the gauges and the health check see it.</summary>
public enum StreamPartitionRunState
{
    /// <summary>Registered, not yet reading — the window between host start and the first fetch.</summary>
    Starting = 0,

    /// <summary>Reading and processing normally.</summary>
    Running = 1,

    /// <summary>Retrying a <c>DontIgnoreException</c>; the position is not advancing.</summary>
    Blocked = 2,

    /// <summary>Stood down — <c>ErrorPolicy.StopPartition</c>, a contested claim, or an ordinary shutdown.</summary>
    Stopped = 3,
}

/// <summary>
/// An immutable reading of one partition's health signals, taken at one instant.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this type exists.</b> <c>StreamsHealthCheck</c> in <c>RedisEvents.Web</c> grades these
/// signals for an ASP.NET <c>/health</c> endpoint. A Generic Host worker has no such endpoint, and
/// a host that wants to act on a dead partition itself — stop the process, emit its own metric,
/// decide its own policy — had no way to read them: the monitors are internal and visible only to
/// <c>RedisEvents.Web</c>. The alternative in the field was to infer liveness from the meter's
/// instrument names, which couples an operational decision to the names of telemetry series and
/// fails silently when one is renamed.
/// </para>
/// <para>
/// <b>A snapshot, not a handle.</b> Every value is read once, through the same volatile reads the
/// health check uses, and copied. Holding one of these does not pin a worker, and reading it twice
/// cannot interleave two different partitions' states. The monitors themselves stay internal, so
/// mutation remains the worker's alone.
/// </para>
/// </remarks>
public readonly record struct StreamPartitionStatus
{
    /// <summary>The topic this partition belongs to.</summary>
    public required string Topic { get; init; }

    /// <summary>The consumer name reading it.</summary>
    public required string Consumer { get; init; }

    /// <summary>The partition index.</summary>
    public required int Partition { get; init; }

    /// <summary>Whether the worker is starting, running, blocked or stopped.</summary>
    public required StreamPartitionRunState State { get; init; }

    /// <summary>Why the partition stopped, when it has; null otherwise.</summary>
    public string? StopReason { get; init; }

    /// <summary>
    /// The last id this partition processed, or <see cref="StreamId.Min"/> before its first batch.
    /// </summary>
    /// <remarks>
    /// This is the progress marker: a position that does not advance while
    /// <see cref="LagEntries"/> is positive is a stalled partition, which is a different condition
    /// from a partition that is merely behind and catching up. Comparing two snapshots is the only
    /// way to tell them apart, and the distinction decides whether restarting the host helps or
    /// makes the backlog worse.
    /// </remarks>
    public required StreamId LastProcessed { get; init; }

    /// <summary>True when the reader last fetched nothing and is sitting at the tail.</summary>
    /// <remarks>
    /// Idle is not lag. A caught-up partition on a quiet topic reports zero lag rather than a
    /// number that climbs overnight, and it is not stalled however long its position stands still.
    /// </remarks>
    public required bool IsCaughtUp { get; init; }

    /// <summary>Milliseconds between the last processed entry and now; 0 when caught up or before the first batch.</summary>
    public required double LagMs { get; init; }

    /// <summary>
    /// Entries behind the tail as of the last <c>XINFO STREAM</c> sample, or <c>-1</c> when the
    /// sampler has not run and the number is genuinely unknown.
    /// </summary>
    public required long LagEntries { get; init; }

    /// <summary>Milliseconds this partition has been blocked retrying, or 0 when it is not blocked.</summary>
    public required double BlockedMs { get; init; }

    /// <summary>Milliseconds since this partition stopped, or 0 when it has not.</summary>
    public required double StoppedMs { get; init; }

    /// <summary>This consumer's <c>ConsumerOptions.UnhealthyLagMs</c>, so a reader can apply the configured threshold.</summary>
    public required int UnhealthyLagMs { get; init; }

    /// <summary>This consumer's <c>ConsumerOptions.UnhealthyBlockSeconds</c>.</summary>
    public required int UnhealthyBlockSeconds { get; init; }

    /// <summary>This consumer's <c>ConsumerOptions.UnhealthyStoppedSeconds</c>.</summary>
    public required int UnhealthyStoppedSeconds { get; init; }
}
