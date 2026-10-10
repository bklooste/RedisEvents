using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RedisEvents.Consumer;
using RedisEvents.Extensions;
using RedisEvents.Ownership;

namespace RedisEvents.Diagnostics;

/// <summary>The verdict on stream consumption in this process.</summary>
/// <remarks>
/// The library's own three-step scale, so that core needs no reference to
/// <c>Microsoft.Extensions.Diagnostics.HealthChecks</c>. It maps one to one onto that package's
/// <c>HealthStatus</c> by name — note the numeric values run the other way round there, so map it
/// with a <c>switch</c>, never a cast.
/// </remarks>
public enum StreamHealthStatus
{
    /// <summary>Every partition is being read and nothing is past a threshold.</summary>
    Healthy = 0,

    /// <summary>Something wants attention but a restart would not help, or would make it worse.</summary>
    Degraded = 1,

    /// <summary>A partition is not being read and only a restart brings it back.</summary>
    Unhealthy = 2,
}

/// <summary>Which rule produced a <see cref="StreamHealthReport"/> or a <see cref="StreamPartitionHealth"/>.</summary>
/// <remarks>
/// For a host that wants to act or log on the rule rather than parse the description. New members
/// may be added; treat an unknown value as its <see cref="StreamHealthStatus"/> alone.
/// </remarks>
public enum StreamHealthRule
{
    /// <summary>No rule fired: Healthy.</summary>
    None = 0,

    /// <summary>Unhealthy: the shared Redis connection is down.</summary>
    ConnectionDown = 1,

    /// <summary>Unhealthy: a partition has been blocked retrying past <c>UnhealthyBlockSeconds</c>.</summary>
    BlockedTooLong = 2,

    /// <summary>
    /// Unhealthy: a partition whose stop escalates has been stopped past <c>UnhealthyStoppedSeconds</c>
    /// with entries waiting.
    /// </summary>
    StoppedTooLong = 3,

    /// <summary>
    /// Unhealthy: a partition has been behind the tail of its stream, with a position that has not
    /// changed, past <c>UnhealthyBehindSeconds</c>.
    /// </summary>
    BehindTail = 4,

    /// <summary>Unhealthy: consumers are registered and none has started.</summary>
    NoConsumerStarted = 5,

    /// <summary>Unhealthy: every partition worker has stopped.</summary>
    AllPartitionsStopped = 6,

    /// <summary>Degraded: partitions are registered and none has started reading yet.</summary>
    NotReadingYet = 7,

    /// <summary>Degraded: a partition is blocked retrying, inside its threshold.</summary>
    Blocked = 8,

    /// <summary>Degraded: a partition is stopped and has not (or will not) escalate.</summary>
    Stopped = 9,

    /// <summary>Degraded: a partition's lag is past <c>UnhealthyLagMs</c> and it is not at the tail.</summary>
    Lagging = 10,

    /// <summary>Degraded: the ownership registry shows a gap or an overlap.</summary>
    OwnershipDegraded = 11,
}

/// <summary>One partition's own grade, before the process-wide rules are applied.</summary>
/// <param name="Status">Healthy, Degraded or Unhealthy.</param>
/// <param name="Rule">The rule that decided it; <see cref="StreamHealthRule.None"/> when Healthy.</param>
public readonly record struct StreamPartitionHealth(StreamHealthStatus Status, StreamHealthRule Rule);

/// <summary>
/// What <see cref="StreamHealth.Evaluate(IReadOnlyList{StreamPartitionStatus}, in StreamHealthContext)"/>
/// cannot read off the partition snapshot: facts about the process around the partitions.
/// </summary>
/// <remarks>
/// <c>default</c> means "none of that is known or wrong", which grades the partitions alone.
/// <see cref="StreamHealth.Evaluate(IServiceProvider)"/> fills it in from the container.
/// </remarks>
public readonly record struct StreamHealthContext
{
    /// <summary>
    /// A description of the shared Redis connection's fault (its endpoints, or the connect error)
    /// when it is down; <see langword="null"/> when it is up or there is none.
    /// </summary>
    public string? ConnectionFault { get; init; }

    /// <summary>How many stream consumers are registered in the host; 0 when not known.</summary>
    public int Consumers { get; init; }

    /// <summary>How many of those have started and not stopped.</summary>
    public int ConsumersStarted { get; init; }

    /// <summary>Whether the ownership registry last saw a gap or an overlap.</summary>
    public bool OwnershipDegraded { get; init; }
}

/// <summary>The graded answer: a status, the rule behind it, and words and numbers for a log or an endpoint.</summary>
public sealed class StreamHealthReport
{
    internal StreamHealthReport(
        StreamHealthStatus status,
        StreamHealthRule rule,
        string description,
        IReadOnlyDictionary<string, object> data,
        StreamPartitionStatus? partition)
    {
        this.Status = status;
        this.Rule = rule;
        this.Description = description;
        this.Data = data;
        this.Partition = partition;
    }

    /// <summary>Healthy, Degraded or Unhealthy.</summary>
    public StreamHealthStatus Status { get; }

    /// <summary>The rule that decided <see cref="Status"/> — the first to fire, in order of severity.</summary>
    public StreamHealthRule Rule { get; }

    /// <summary>A sentence naming the partition, the numbers and the threshold, for a human.</summary>
    public string Description { get; }

    /// <summary>
    /// Counts and worst cases (<c>partitions</c>, <c>partitionsBlocked</c>, <c>partitionsStopped</c>,
    /// <c>partitionsStarting</c>, <c>partitionsBehind</c>, <c>worstLagMs</c>, …), in the shape a
    /// health-check result's data dictionary takes.
    /// </summary>
    public IReadOnlyDictionary<string, object> Data { get; }

    /// <summary>The partition the rule fired on, when the rule is about one partition.</summary>
    public StreamPartitionStatus? Partition { get; }
}

/// <summary>
/// The one implementation of the stream health rules: it grades a <see cref="StreamStatus"/> snapshot
/// into Healthy, Degraded or Unhealthy.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why it is here and not in <c>RedisEvents.Web</c>.</b> The rules used to live in
/// <c>StreamsHealthCheck</c>, which a Generic Host worker cannot reference — that package carries
/// the ASP.NET Core framework reference. A worker that wanted the same verdict had to re-implement
/// the rules from the snapshot and guess at the signals the snapshot did not carry. Now
/// <c>StreamsHealthCheck</c> is an adapter over this type, and a worker calls it directly and maps
/// <see cref="StreamHealthStatus"/> onto whatever its host uses. This type deliberately returns its
/// own report rather than a <c>HealthCheckResult</c>, so core takes no dependency on the
/// health-check abstractions.
/// </para>
/// <list type="table">
/// <item>
///   <term>Unhealthy</term>
///   <description>The Redis connection is down; a partition blocked past
///   <c>UnhealthyBlockSeconds</c>; a partition whose stop escalates, stopped past
///   <c>UnhealthyStoppedSeconds</c> with entries waiting; a partition behind the tail of its stream
///   with a frozen position past <c>UnhealthyBehindSeconds</c> (off by default); no registered
///   consumer started; every partition stopped.</description>
/// </item>
/// <item>
///   <term>Degraded</term>
///   <description>Nothing has started reading yet; a partition blocked, stopped, or lagging past
///   <c>UnhealthyLagMs</c>; an ownership gap or overlap.</description>
/// </item>
/// </list>
/// <para>
/// <b>Lag is Degraded, never Unhealthy.</b> Restarting a consumer whose only problem is that it is
/// behind drops its in-flight batches and makes the backlog worse, on a loop. <c>UnhealthyLagMs</c>
/// is also the age of the last processed entry, which says how old the work is and nothing about
/// whether work is waiting — on a bursty topic it is large exactly when the consumer is idle and
/// caught up.
/// </para>
/// <para>
/// <b>Behind-and-frozen is the rule that may restart.</b> The tail id is an outside measurement of
/// the stream; if it is strictly ahead of the position there is provably an unread entry, and if the
/// position then does not change for <c>UnhealthyBehindSeconds</c> the consumer is not reading it.
/// That holds on a busy topic and a quiet one alike, needs no guess about traffic, and can never
/// fire on a consumer that is advancing: any change of position restarts the clock. It applies to
/// partitions that are <see cref="StreamPartitionRunState.Running"/> or
/// <see cref="StreamPartitionRunState.Starting"/>. A blocked partition answers to
/// <c>UnhealthyBlockSeconds</c> and a stopped one to <c>UnhealthyStoppedSeconds</c> and
/// <see cref="StreamPartitionStatus.Escalates"/> instead — those are states the consumer has
/// declared, each with its own deliberately chosen patience, and a policy stop in particular must
/// not be turned into a restart loop over the same poison entry.
/// </para>
/// </remarks>
public static class StreamHealth
{
    /// <summary>
    /// Grades every partition this process reads, with the container's view of the consumers and
    /// the shared connection — the same answer <c>StreamsHealthCheck</c> gives.
    /// </summary>
    /// <param name="services">
    /// The host's service provider. The stream consumer hosts and the shared connection are looked
    /// up in it; a process that registered neither is graded on its partitions alone.
    /// </param>
    /// <returns>The report.</returns>
    public static StreamHealthReport Evaluate(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);

        return Evaluate(services.GetService<StreamsConnectionProvider>(), services.GetServices<IHostedService>());
    }

    /// <summary>
    /// Grades a partition snapshot. Pure: the same snapshot and context always give the same report.
    /// </summary>
    /// <param name="partitions">The snapshot, normally <see cref="StreamStatus.Partitions"/>.</param>
    /// <param name="context">Process-level facts; <c>default</c> grades the partitions alone.</param>
    /// <returns>The report.</returns>
    public static StreamHealthReport Evaluate(
        IReadOnlyList<StreamPartitionStatus> partitions,
        in StreamHealthContext context = default)
    {
        ArgumentNullException.ThrowIfNull(partitions);

        if (context.Consumers == 0 && partitions.Count == 0)
        {
            // A producer-only service, or one whose consumers are not registered here. Nothing to
            // report is not the same as something being wrong.
            return new StreamHealthReport(
                StreamHealthStatus.Healthy,
                StreamHealthRule.None,
                "Streams: no stream consumers are registered in this process.",
                new Dictionary<string, object>(StringComparer.Ordinal),
                partition: null);
        }

        var data = new Dictionary<string, object>(StringComparer.Ordinal)
        {
            ["consumers"] = context.Consumers,
            ["partitions"] = partitions.Count,
            ["consumersStarted"] = context.ConsumersStarted,
        };

        var tally = Tally(partitions, data);

        if (context.ConnectionFault is { } fault)
        {
            data["connection"] = fault;

            return Unhealthy(
                StreamHealthRule.ConnectionDown,
                $"Streams: the Redis connection is down ({fault}); nothing is being consumed.",
                data);
        }

        if (tally.BlockedTooLong is { } wedged)
        {
            return Unhealthy(
                StreamHealthRule.BlockedTooLong,
                $"Streams: partition {Describe(wedged)} has been blocked for {Seconds(wedged.BlockedMs)}s retrying a DontIgnoreException, " +
                $"past its UnhealthyBlockSeconds of {wedged.UnhealthyBlockSeconds}. Its position has not advanced and its backlog is at risk of being trimmed.",
                data,
                wedged);
        }

        if (tally.StoppedTooLong is { } abandoned)
        {
            return Unhealthy(
                StreamHealthRule.StoppedTooLong,
                $"Streams: partition {Describe(abandoned)} stood down {Seconds(abandoned.StoppedMs)}s ago ({abandoned.StopReason}) and is " +
                $"{abandoned.LagEntries} entries behind a stream that is still being written, past its UnhealthyStoppedSeconds of " +
                $"{abandoned.UnhealthyStoppedSeconds}. Nothing reads it again without a restart.",
                data,
                abandoned);
        }

        if (tally.BehindTooLong is { } frozen)
        {
            data["behindPartition"] = Describe(frozen);
            data["behindMs"] = (long)frozen.BehindMs;

            return Unhealthy(
                StreamHealthRule.BehindTail,
                $"Streams: partition {Describe(frozen)} is behind the tail of its stream (position {frozen.Position.Format()}, tail " +
                $"{frozen.TailId.GetValueOrDefault().Format()}) and its position has not changed for {Seconds(frozen.BehindMs)}s, past its " +
                $"UnhealthyBehindSeconds of {frozen.UnhealthyBehindSeconds}" +
                (frozen.State == StreamPartitionRunState.Starting ? "; it has not started reading" : string.Empty) +
                ". There is an entry it has not read and it is not reading it; a restart hands the partition on.",
                data,
                frozen);
        }

        if (context.Consumers > 0 && context.ConsumersStarted == 0)
        {
            return Unhealthy(
                StreamHealthRule.NoConsumerStarted,
                $"Streams: none of the {context.Consumers} registered stream consumers has started.",
                data);
        }

        if (partitions.Count > 0 && tally.Live == 0)
        {
            return Unhealthy(
                StreamHealthRule.AllPartitionsStopped,
                $"Streams: all {partitions.Count} partition workers have stopped; nothing is being consumed. " +
                (tally.StopReason is null ? string.Empty : $"Last reason: {tally.StopReason}"),
                data);
        }

        data["ownershipDegraded"] = context.OwnershipDegraded;

        // Starting is not Running (R-16). It used to fall into the same bucket, so a consumer whose
        // workers had registered but never reached their first fetch — a reader connection that
        // cannot connect, a start that faulted after the monitors were created — reported Healthy
        // with a full partition count. Reporting Degraded here says "nothing has read yet", which is
        // true for a second or two of every normal startup and stays true for as long as the fault
        // lasts. Degraded, not Unhealthy: restarting a pod that is still starting is a loop.
        if (tally.Running == 0 && tally.Starting > 0)
        {
            return new StreamHealthReport(
                StreamHealthStatus.Degraded,
                StreamHealthRule.NotReadingYet,
                $"Streams: none of the {partitions.Count} partition worker(s) has started reading yet " +
                $"({tally.Starting} still starting).",
                data,
                partition: null);
        }

        if (tally.Blocked > 0 || tally.Stopped > 0 || tally.Lagging is not null || context.OwnershipDegraded)
        {
            var rule = tally.Blocked > 0 ? StreamHealthRule.Blocked
                : tally.Stopped > 0 ? StreamHealthRule.Stopped
                : tally.Lagging is not null ? StreamHealthRule.Lagging
                : StreamHealthRule.OwnershipDegraded;

            return new StreamHealthReport(
                StreamHealthStatus.Degraded,
                rule,
                Explain(in tally, context.OwnershipDegraded),
                data,
                rule == StreamHealthRule.Lagging ? tally.Lagging : null);
        }

        return new StreamHealthReport(
            StreamHealthStatus.Healthy,
            StreamHealthRule.None,
            $"Streams: {context.ConsumersStarted} consumer(s), {tally.Running} of {partitions.Count} partition(s) reading, " +
            $"worst lag {Seconds(tally.WorstLagMs)}s.",
            data,
            partition: null);
    }

    /// <summary>
    /// Grades one partition on its own signals and thresholds — the per-partition half of
    /// <see cref="Evaluate(IReadOnlyList{StreamPartitionStatus}, in StreamHealthContext)"/>, which
    /// adds the rules that need the whole process (connection, "all stopped", "none started").
    /// </summary>
    /// <param name="partition">One entry of a <see cref="StreamStatus"/> snapshot.</param>
    /// <returns>The partition's status and the rule that decided it.</returns>
    public static StreamPartitionHealth Grade(in StreamPartitionStatus partition)
    {
        switch (partition.State)
        {
            case StreamPartitionRunState.Blocked:
                return partition.BlockedMs > partition.UnhealthyBlockSeconds * 1000d
                    ? new(StreamHealthStatus.Unhealthy, StreamHealthRule.BlockedTooLong)
                    : new(StreamHealthStatus.Degraded, StreamHealthRule.Blocked);

            case StreamPartitionRunState.Stopped:
                // "Not progressed for N seconds and not at head": a stood-down partition nobody
                // chose to stop, with entries piling up behind it that only a restart will ever
                // read. LagEntries comes from the sampler, which keeps sampling a stopped partition;
                // -1 (never sampled) and 0 (at the tail) both mean there is nothing to escalate yet.
                return partition.Escalates
                    && partition.LagEntries > 0
                    && partition.StoppedMs > partition.UnhealthyStoppedSeconds * 1000d
                    ? new(StreamHealthStatus.Unhealthy, StreamHealthRule.StoppedTooLong)
                    : new(StreamHealthStatus.Degraded, StreamHealthRule.Stopped);
        }

        // Running or Starting. BehindMs is already zero unless a fresh tail sample is strictly ahead
        // of the position, and restarts whenever the position changes; IsBehindTail is checked as
        // well so a hand-built snapshot cannot be Unhealthy on a duration alone.
        if (partition.UnhealthyBehindSeconds > 0
            && partition.IsBehindTail
            && partition.BehindMs >= partition.UnhealthyBehindSeconds * 1000d)
        {
            return new(StreamHealthStatus.Unhealthy, StreamHealthRule.BehindTail);
        }

        // A partition whose sampled entry lag is exactly zero is at the tail of its stream, so a
        // large lag.ms there only means the topic is quiet — not that anything is behind.
        return partition.LagMs > partition.UnhealthyLagMs && partition.LagEntries != 0
            ? new(StreamHealthStatus.Degraded, StreamHealthRule.Lagging)
            : new(StreamHealthStatus.Healthy, StreamHealthRule.None);
    }

    /// <summary>
    /// Takes the snapshot and the process-level facts and grades them. The seam
    /// <c>StreamsHealthCheck</c> calls, since it is handed its dependencies rather than a provider.
    /// </summary>
    internal static StreamHealthReport Evaluate(
        StreamsConnectionProvider? connection,
        IEnumerable<IHostedService>? hostedServices)
    {
        // StreamConsumerHost.IsRunning is "StartAsync ran and StopAsync has not" — it says nothing
        // about whether anything is being read, which is why it is counted as "started" here and the
        // partition snapshot, not this number, decides Healthy.
        int consumers = 0, started = 0;

        if (hostedServices is not null)
        {
            foreach (var service in hostedServices)
            {
                if (service is StreamConsumerHost host)
                {
                    consumers++;

                    if (host.IsRunning)
                    {
                        started++;
                    }
                }
            }
        }

        var context = new StreamHealthContext
        {
            ConnectionFault = ConnectionFault(connection),
            Consumers = consumers,
            ConsumersStarted = started,
            OwnershipDegraded = OwnershipRegistry.AnyDegraded(),
        };

        return Evaluate(StreamStatus.Partitions(), in context);
    }

    /// <summary>
    /// Whether the shared multiplexer is unusable. Resolving it here is safe: it is created once and
    /// cached, and a connect failure is itself the answer the check is looking for.
    /// </summary>
    private static string? ConnectionFault(StreamsConnectionProvider? connection)
    {
        if (connection is null)
        {
            return null;
        }

        try
        {
            var multiplexer = connection.Connection;

            return multiplexer.IsConnected
                ? null
                : StreamsConnectionProvider.DescribeEndpoints(multiplexer.GetEndPoints());
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    /// <summary>
    /// Walks the snapshot once, counting states and remembering the first offender in each category.
    /// </summary>
    private static Counts Tally(IReadOnlyList<StreamPartitionStatus> partitions, Dictionary<string, object> data)
    {
        var counts = default(Counts);

        for (var i = 0; i < partitions.Count; i++)
        {
            var partition = partitions[i];

            switch (partition.State)
            {
                case StreamPartitionRunState.Blocked:
                    counts.Blocked++;
                    break;

                case StreamPartitionRunState.Stopped:
                    counts.Stopped++;
                    counts.StopReason ??= partition.StopReason;
                    break;

                case StreamPartitionRunState.Starting:
                    counts.Starting++;
                    break;

                default:
                    counts.Running++;
                    break;
            }

            if (partition.LagMs > counts.WorstLagMs)
            {
                counts.WorstLagMs = partition.LagMs;
                counts.Worst = partition;
            }

            if (partition.IsBehindTail)
            {
                counts.Behind++;
            }

            switch (Grade(in partition).Rule)
            {
                case StreamHealthRule.BlockedTooLong:
                    counts.BlockedTooLong ??= partition;
                    break;

                case StreamHealthRule.StoppedTooLong:
                    counts.StoppedTooLong ??= partition;
                    break;

                case StreamHealthRule.BehindTail:
                    counts.BehindTooLong ??= partition;
                    break;
            }

            // Asked of every partition, not taken from Grade: a blocked or stopped partition is
            // graded on its state there, and its lag still belongs in the Degraded explanation.
            // Zero entries behind is the tail of the stream, where a large lag.ms only means the
            // topic is quiet.
            if (partition.LagMs > partition.UnhealthyLagMs && partition.LagEntries != 0)
            {
                counts.Lagging ??= partition;
            }
        }

        // Blocked partitions count as live: they are retrying, not gone. Starting ones are live too
        // — they have not stood down — but they are not reading, which is a different question and
        // has its own counter.
        counts.Live = counts.Running + counts.Blocked + counts.Starting;

        data["partitionsBlocked"] = counts.Blocked;
        data["partitionsStopped"] = counts.Stopped;
        data["partitionsStarting"] = counts.Starting;
        data["partitionsBehind"] = counts.Behind;
        data["worstLagMs"] = (long)counts.WorstLagMs;

        if (counts.Worst is { } worst)
        {
            data["worstLagPartition"] = Describe(worst);
            data["worstLagEntries"] = worst.LagEntries;
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

    private static StreamHealthReport Unhealthy(
        StreamHealthRule rule,
        string description,
        Dictionary<string, object> data,
        StreamPartitionStatus? partition = null)
        => new(StreamHealthStatus.Unhealthy, rule, description, data, partition);

    private static string Describe(StreamPartitionStatus partition)
        => string.Create(
            CultureInfo.InvariantCulture,
            $"{partition.Topic}[{partition.Partition}]/{partition.Consumer}");

    private static string Seconds(double milliseconds)
        => (milliseconds / 1000d).ToString("F1", CultureInfo.InvariantCulture);

    /// <summary>One pass's worth of counters over the partition snapshot.</summary>
    private struct Counts
    {
        /// <summary>Partitions reading or retrying or still starting — anything that is not stopped.</summary>
        public int Live;

        public int Running;

        /// <summary>Partitions registered but not yet past their first fetch.</summary>
        public int Starting;

        /// <summary>Partitions blocked retrying a <c>DontIgnoreException</c>.</summary>
        public int Blocked;

        public int Stopped;

        /// <summary>Partitions with a fresh tail sample strictly ahead of their position.</summary>
        public int Behind;

        /// <summary>The largest lag seen this pass, in milliseconds.</summary>
        public double WorstLagMs;

        /// <summary>The partition that lag belongs to.</summary>
        public StreamPartitionStatus? Worst;

        /// <summary>The first partition found past its lag threshold, if any.</summary>
        public StreamPartitionStatus? Lagging;

        /// <summary>
        /// The first stood-down partition that is behind a still-moving stream and has been so past
        /// its <c>UnhealthyStoppedSeconds</c>, if any. Only stops that escalate count.
        /// </summary>
        public StreamPartitionStatus? StoppedTooLong;

        /// <summary>The first partition found blocked past its block threshold, if any.</summary>
        public StreamPartitionStatus? BlockedTooLong;

        /// <summary>The first partition behind its tail with a frozen position past its threshold, if any.</summary>
        public StreamPartitionStatus? BehindTooLong;

        /// <summary>Why the first stopped partition stopped.</summary>
        public string? StopReason;
    }
}
