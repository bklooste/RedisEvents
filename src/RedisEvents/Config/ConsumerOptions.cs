namespace RedisEvents.Config;

/// <summary>
/// Configuration options for a stream consumer.
/// </summary>
/// <remarks>
/// Properties are <c>set</c> rather than <c>init</c> because the source-generated configuration
/// binder cannot assign an <c>init</c>-only member and silently bound nothing at all; see the
/// remarks on <see cref="TopicOptions"/> (R-17).
/// </remarks>
public sealed record ConsumerOptions
{
    /// <summary>
    /// The topic to consume from (required).
    /// </summary>
    public string Topic { get; set; } = null!;

    /// <summary>
    /// Consumer group name; overrides StreamOptions.Consumer if set.
    /// </summary>
    public string? Consumer { get; set; }

    /// <summary>
    /// Number of messages to read in a batch (default: 100).
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// Message type filter; if set, only these types are processed.
    /// </summary>
    public string[]? Filter { get; set; }

    /// <summary>
    /// Where to start reading from (default: Stored).
    /// </summary>
    public StartFrom StartFrom { get; set; } = StartFrom.Stored;

    /// <summary>
    /// Specific date to start from when StartFrom == Date.
    /// </summary>
    public DateTimeOffset? StartFromDate { get; set; }

    /// <summary>
    /// How to persist the consumer position (default: AsyncBatch).
    /// </summary>
    public PersistMode Persist { get; set; } = PersistMode.AsyncBatch;

    /// <summary>
    /// Position flush interval in milliseconds (default: 1000).
    /// </summary>
    public int PersistIntervalMs { get; set; } = 1000;

    /// <summary>
    /// Backpressure configuration (default: Enabled=true, Capacity=4).
    /// </summary>
    public BackpressureOptions Backpressure { get; set; } = new();

    /// <summary>
    /// Read mode for consuming messages (default: Block).
    /// </summary>
    public ReadMode ReadMode { get; set; } = ReadMode.Block;

    /// <summary>
    /// Block timeout in milliseconds for XREAD BLOCK (default: 1000).
    /// Must stay well under the sync timeout of the connection.
    /// </summary>
    public int BlockMs { get; set; } = 1000;

    /// <summary>
    /// Maximum idle backoff delay in milliseconds for Poll mode (default: 50).
    /// </summary>
    public int MaxIdleDelayMs { get; set; } = 50;

    /// <summary>
    /// Error handling policy (default: BestEffort).
    /// </summary>
    public ErrorPolicy OnError { get; set; } = ErrorPolicy.BestEffort;

    /// <summary>
    /// Time in seconds before a partition is considered unhealthy if it blocks (default: 300).
    /// </summary>
    public int UnhealthyBlockSeconds { get; set; } = 300;

    /// <summary>
    /// Seconds a partition that stood down outside any <see cref="ErrorPolicy"/> decision (a
    /// contested position, a retired co-located slot) may stay stopped while its stream keeps
    /// moving before the health check reports Unhealthy (default: 300). Nothing reads such a
    /// partition again without a restart, so past this point a restart is the fix.
    /// </summary>
    public int UnhealthyStoppedSeconds { get; set; } = 300;

    /// <summary>
    /// Seconds an overlap with another live instance must persist before a
    /// <see cref="InstanceMode.Static"/> consumer stands the partition down (default: 60).
    /// </summary>
    /// <remarks>
    /// <para>
    /// A <c>Deployment</c> rolling deploy surges to two pods of one ordinal, both of which legitimately
    /// hold a live presence claim and flush the same position for the length of the handover. Under
    /// <see cref="InstanceMode.Static"/> the partition's claim field cannot tell that apart from a
    /// misconfigured replica count — both instances rewrite it every cycle — but time can: the
    /// handover ends when the predecessor exits, and a wrong instance count does not end. An overlap
    /// that outlives this window is judged exactly as it was before, so a genuine second writer still
    /// ends with one side stood down.
    /// </para>
    /// <para>
    /// The cost is that the two instances may both process the window's worth of messages, which is
    /// the at-least-once exposure the overlap creates in any case. Set it to 0 to stand down on the
    /// first sight of an overlap, as versions before this option did.
    /// <see cref="InstanceMode.Lease"/> ignores it: there the claim is exclusive and settles the
    /// question outright.
    /// </para>
    /// </remarks>
    public int ContestedGraceSeconds { get; set; } = 60;

    /// <summary>
    /// Seconds between re-probes of the instance a contested partition stood down for (default: 30);
    /// 0 disables re-arbitration and makes a stand-down permanent, as it was before this option.
    /// </summary>
    /// <remarks>
    /// A stand-down is a stopped partition that nothing else will ever restart, so it used to
    /// outlive the contender that caused it — the pod that won the tiebreak terminates and the
    /// partition is read by nobody until an operator restarts the survivor. Every
    /// <c>ContestedRecheckSeconds</c> a stood-down partition asks the ownership hash whether its
    /// contender is still present; when it is not, the consumer brings its read side back up. Only a
    /// consumer that has actually stood a partition down pays anything for this.
    /// </remarks>
    public int ContestedRecheckSeconds { get; set; } = 30;

    /// <summary>
    /// Whether to use Redis consumer groups (default: false).
    /// </summary>
    public bool UseConsumerGroup { get; set; }

    /// <summary>
    /// Instance configuration for partition ownership; null = single instance.
    /// </summary>
    public InstanceOptions? Instances { get; set; }

    /// <summary>
    /// Number of reader threads for the SocketManager (default: 1).
    /// </summary>
    public int ReaderThreads { get; set; } = 1;

    /// <summary>
    /// Shutdown timeout in seconds for draining the consumer (default: 10).
    /// </summary>
    public int ShutdownTimeoutSeconds { get; set; } = 10;

    /// <summary>
    /// Lag threshold in milliseconds before marking the consumer as unhealthy (default: 120000).
    /// </summary>
    public int UnhealthyLagMs { get; set; } = 120_000;

    /// <summary>
    /// Where to start reading from when Persist=Stored finds no stored position (default: Beginning).
    /// </summary>
    public StartFrom StartFromWhenMissing { get; set; } = StartFrom.Beginning;

    /// <summary>
    /// One-shot flag to reset positions on startup before the first read.
    /// When <see langword="true"/>, the consumer applies <see cref="StartFromDate"/> as a reset
    /// before starting the read loop, reprocessing entries back to that date.
    /// This is meant for "redeploy with this environment variable to replay"; it is not persisted
    /// and has no effect if the consumer has not been configured with a <see cref="StartFromDate"/>.
    /// (default: false).
    /// </summary>
    public bool ResetOnStart { get; set; }
}
