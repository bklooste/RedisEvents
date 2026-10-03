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
    /// How entries of one partition are handed to the members of this consumer (default:
    /// <see cref="DeliveryMode.Ordered"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b><see cref="DeliveryMode.WorkQueue"/> loses per-key order, and that is the first thing to
    /// know about it.</b> It reads with <c>XREADGROUP</c>, so entries of one partition go to
    /// whichever member asks first and two members interleave them. Nothing reassembles that order
    /// afterwards. Its legitimate niche is order-independent commands or verbs that may be rejected
    /// and retried; it is the wrong mode for a projection, where applying two events to one key out
    /// of order silently corrupts the read model.
    /// </para>
    /// <para>
    /// <b>It also costs more.</b> An extra <c>XACK</c> round trip per batch, and Redis-side
    /// pending-entries-list (PEL) bookkeeping per entry — against the default path, where a thousand
    /// processed messages cost one <c>HSET</c> per flush interval and Redis tracks nothing per
    /// consumer at all.
    /// </para>
    /// <para>
    /// <b>The position store is bypassed entirely.</b> In this mode Redis owns the read cursor (the
    /// group's last-delivered id) and the PEL, so there is no <c>p:{topic}:{consumer}</c> hash to
    /// write, no flusher tick, and no start-position resolution after the first run. A position
    /// reset therefore cannot move this consumer by rewriting that hash, so <c>StreamAdmin</c>'s
    /// reset family routes to <c>XGROUP SETID</c> instead.
    /// </para>
    /// <para>
    /// <b>The ordered, named-cursor semantics people expect from a Kafka or EventHub consumer group
    /// are the default — <see cref="DeliveryMode.Ordered"/> — not this.</b> There,
    /// <see cref="Consumer"/> is the group id, <c>p:{topic}:{consumer}</c> is the per-partition
    /// committed cursor, and the ownership registry assigns partitions to members. Reach for
    /// <see cref="DeliveryMode.WorkQueue"/> only when competing consumers with claim and recovery
    /// semantics are genuinely wanted, and not merely because Redis offers them.
    /// </para>
    /// </remarks>
    public DeliveryMode Delivery
    {
        get => this.delivery;
        set
        {
            this.delivery = value;
            this.DeliveryWasSetExplicitly = true;
        }
    }

    /// <summary>
    /// True when <see cref="Delivery"/> was assigned rather than left at its default. Used to tell
    /// a config that asked for <see cref="DeliveryMode.Ordered"/> apart from one that said nothing,
    /// so the legacy <c>UseConsumerGroup</c> key can be refused when the two disagree.
    /// </summary>
    internal bool DeliveryWasSetExplicitly { get; private set; }

    private DeliveryMode delivery = DeliveryMode.Ordered;

    /// <summary>
    /// Latches the obsolete-key warning so binding and then validating reports it once.
    /// </summary>
    internal bool LegacyDeliveryKeyWarned { get; set; }

    /// <summary>
    /// Obsolete spelling of <see cref="Delivery"/>, honoured for one version so an existing
    /// <c>Streams:Consumers:&lt;n&gt;:UseConsumerGroup</c> keeps binding. <c>true</c> maps to
    /// <see cref="DeliveryMode.WorkQueue"/> and logs a warning naming the new key; setting both this
    /// and <see cref="Delivery"/> to values that disagree is refused rather than silently resolved.
    /// </summary>
    [Obsolete("Use Delivery instead: UseConsumerGroup = true is Delivery = DeliveryMode.WorkQueue. The old name invited the Kafka/EventHub reading, which is the Ordered default, not this mode.")]
    public bool? UseConsumerGroup { get; set; }

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
