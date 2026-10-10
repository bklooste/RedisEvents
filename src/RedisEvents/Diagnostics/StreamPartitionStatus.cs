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
    /// The raw progress marker. To tell a stalled partition from one that is merely behind and
    /// catching up — the distinction that decides whether restarting the host helps or makes the
    /// backlog worse — read <see cref="IsBehindTail"/> and <see cref="BehindMs"/>, which compare
    /// <see cref="Position"/> against the sampled <see cref="TailId"/> and time how long it has
    /// stood still, rather than comparing two snapshots by hand.
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

    /// <summary>
    /// This consumer's <c>ConsumerOptions.UnhealthyBehindSeconds</c>; 0 means the behind-the-tail
    /// rule is off for it.
    /// </summary>
    public int UnhealthyBehindSeconds { get; init; }

    /// <summary>
    /// Whether a <see cref="StreamPartitionRunState.Stopped"/> partition's stop turns Unhealthy once
    /// it has lasted <see cref="UnhealthyStoppedSeconds"/> with entries waiting.
    /// </summary>
    /// <remarks>
    /// True for a stop nobody decided — a contested position, a stood-down co-located sibling — which
    /// only a restart undoes. False for an <c>ErrorPolicy.StopPartition</c> or <c>ErrorPolicy.Fail</c>
    /// stop, which is the operator's chosen state and stays Degraded, and false whenever the
    /// partition is not stopped. Read this rather than the wording of <see cref="StopReason"/>.
    /// </remarks>
    public bool Escalates { get; init; }

    /// <summary>
    /// Where this partition has read up to: <see cref="LastProcessed"/> once it has processed a
    /// batch, and before that the position this instance started reading after (its stored
    /// position, or wherever <c>StartFromWhenMissing</c> resolved to).
    /// </summary>
    /// <remarks>
    /// Compare the tail against this, not against <see cref="LastProcessed"/>: the latter reads
    /// <c>0-0</c> from a restart until the first batch, which on a quiet topic is indefinitely, and
    /// <c>0-0</c> is behind every non-empty stream.
    /// </remarks>
    public StreamId Position { get; init; }

    /// <summary>Milliseconds since <see cref="Position"/> last changed.</summary>
    /// <remarks>
    /// Counted from the moment this instance began reading the partition — under Lease ownership
    /// that is the acquisition, not process start — and restarted by every change of position,
    /// forwards or (after a reset) backwards. Changes are noticed within about a second and always
    /// before a snapshot is taken, so a snapshot never reports a position as frozen that has moved.
    /// </remarks>
    public double PositionUnchangedMs { get; init; }

    /// <summary>
    /// The id of the last entry in this partition's stream as last sampled, <see cref="StreamId.Min"/>
    /// when the stream was empty or did not exist, or <see langword="null"/> when it has not been
    /// sampled yet.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Taken from the <c>XINFO STREAM</c> the lag sampler already issues every 15 seconds for
    /// <c>streams.lag.entries</c>, so it costs no extra Redis round trip and is at most that old in
    /// normal running; <see cref="TailSampleAgeMs"/> says how old this one is.
    /// </para>
    /// <para>
    /// It is an outside measurement of the stream, not the consumer's account of itself, which is
    /// the point: <see cref="LagEntries"/> and <see cref="IsCaughtUp"/> are exactly what a wedged
    /// consumer gets wrong.
    /// </para>
    /// </remarks>
    public StreamId? TailId { get; init; }

    /// <summary>
    /// Milliseconds since <see cref="TailId"/> was sampled, or <c>-1</c> when it never has been.
    /// </summary>
    /// <remarks>
    /// A failed sample keeps the previous tail, so this is how staleness shows. Past 60 seconds
    /// (four missed samples) the library stops treating the tail as known: <see cref="IsBehindTail"/>
    /// reads false and <see cref="BehindMs"/> reads 0 until a sample succeeds again.
    /// </remarks>
    public double TailSampleAgeMs { get; init; }

    /// <summary>
    /// Milliseconds since a sample last found a new last entry in the stream, or <c>-1</c> when the
    /// stream has not been sampled. Before any entry has been seen it counts from the first sample.
    /// </summary>
    /// <remarks>
    /// "How long has nothing been written here, as far as this process has seen" — accurate to the
    /// 15-second sample interval. A trim that empties the stream does not restart it. Together with
    /// <see cref="PositionUnchangedMs"/> this is what a host needs to judge silence on a topic it
    /// knows should be busy; the library does not judge that itself.
    /// </remarks>
    public double TailUnchangedMs { get; init; }

    /// <summary>
    /// True when a fresh tail sample is strictly greater than <see cref="Position"/>: there is
    /// provably an entry this partition has not read.
    /// </summary>
    /// <remarks>
    /// False when the tail has not been sampled or the sample has gone stale (unknown is not
    /// behind), when the stream is empty, and when the position is at or past the tail — including a
    /// position left ahead of the tail by a trim or a recreated stream.
    /// </remarks>
    public bool IsBehindTail { get; init; }

    /// <summary>
    /// Milliseconds this partition has been continuously behind the tail without its position
    /// changing, or 0 when it is not behind.
    /// </summary>
    /// <remarks>
    /// The clock starts when a sample first finds the partition behind and restarts on any change of
    /// position, so a consumer that is behind but advancing reads a small number however large its
    /// backlog, and a topic that was idle for a day is not a day behind when its next entry lands.
    /// This is what <c>ConsumerOptions.UnhealthyBehindSeconds</c> is compared against.
    /// </remarks>
    public double BehindMs { get; init; }
}
