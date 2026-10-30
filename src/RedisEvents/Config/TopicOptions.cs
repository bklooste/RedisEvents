namespace RedisEvents.Config;

/// <summary>
/// Configuration options for a Redis stream topic.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the properties are <c>set</c> and not <c>init</c> (R-17).</b> Binding goes through the
/// source-generated configuration binder, which assigns members on an already-constructed instance.
/// It cannot assign an <c>init</c>-only property, and — because there is no diagnostic for it — it
/// silently emitted a <c>BindCore</c> that validated the key names and set nothing. Every value under
/// <c>Streams:Topics:*</c> was discarded and every topic ran on record defaults. The records stay
/// records (<c>with</c>-expressions are used throughout the library and the tests); only the
/// accessors changed.
/// </para>
/// </remarks>
public sealed record TopicOptions
{
    /// <summary>The <see cref="MaxLen"/> in force when the key is not configured.</summary>
    public const long DefaultMaxLen = 10_000;

    private long maxLen = DefaultMaxLen;

    /// <summary>
    /// Number of partitions for this topic (default: 2).
    /// </summary>
    public int Partitions { get; set; } = 1;

    /// <summary>
    /// Maximum length of the stream; used for trimming (default: 10000).
    /// </summary>
    /// <remarks>
    /// Assigning this — from configuration or from an object initializer — also records that the
    /// ceiling was chosen rather than defaulted, which is what makes the
    /// <see cref="RetentionSeconds"/> pairing rule in <see cref="StreamConfigBinder"/> reachable.
    /// See <see cref="MaxLenConfigured"/>.
    /// </remarks>
    public long MaxLen
    {
        get => this.maxLen;
        set
        {
            this.maxLen = value;
            this.MaxLenConfigured = true;
        }
    }

    /// <summary>
    /// Whether <see cref="MaxLen"/> was explicitly set rather than defaulted.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Decision (R-17), recorded rather than reverted.</b> The remediation note asked for
    /// <c>MaxLen</c> to become <c>long?</c> so that "RetentionSeconds without MaxLen" could fire.
    /// A nullable <c>MaxLen</c> would have changed the meaning of four call sites outside this file
    /// <em>without</em> a compile error — <c>effective.MaxLen &lt; 1</c> is <see langword="false"/>
    /// for <see langword="null"/>, and <c>maxLength: topicOptions.MaxLen</c> would have passed
    /// <see langword="null"/> to <c>XADD</c>, silently dropping the inline <c>MAXLEN</c> that is the
    /// only thing bounding a stream today. Tracking "was it assigned" keeps <c>MaxLen</c> a non-null
    /// <see cref="long"/> with its 10,000 default, so every consumer of it is unchanged, and still
    /// distinguishes configured from defaulted. Internal so it is neither public API nor a bindable
    /// configuration key.
    /// </para>
    /// </remarks>
    internal bool MaxLenConfigured { get; private set; }

    /// <summary>
    /// Trim mode for this topic (default: Approx).
    /// </summary>
    public TrimMode Trim { get; set; } = TrimMode.Approx;

    /// <summary>
    /// Optional retention time in seconds; if set, must be paired with MaxLen.
    /// </summary>
    public int? RetentionSeconds { get; set; }

    /// <summary>
    /// Background trim interval in seconds; 0 = off.
    /// </summary>
    public int BackgroundTrimIntervalSeconds { get; set; }

    /// <summary>
    /// Hash-tag partitions into one slot to co-locate them (default: true).
    /// </summary>
    public bool CoLocatePartitions { get; set; } = true;

    /// <summary>
    /// The key namespace this topic's <em>stream</em> lives in, when it is not this process's own
    /// (default: <see langword="null"/>, the process namespace).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Set to e.g. <c>"np"</c>, the topic's partition streams and topic-meta hash are read and written
    /// under <c>np:re:</c> — <c>np:re:s:{topic}:&lt;p&gt;</c> and <c>np:re:m:{topic}</c> — instead of
    /// under <see cref="KeyNamespace.Prefix"/>. This is how several environments (<c>dev</c>,
    /// <c>tst</c>, …) consume one feed that a shared tier publishes once.
    /// </para>
    /// <para>
    /// Only the stream moves. Everything that belongs to the <em>consumer</em> stays in the process
    /// namespace, so two environments reading one shared stream never share a cursor:
    /// <list type="bullet">
    ///   <item><description>positions and position meta (<c>p:</c>), including reset markers;</description></item>
    ///   <item><description>ownership and leases (<c>o:</c>);</description></item>
    ///   <item><description>
    ///     the <see cref="DeliveryMode.WorkQueue"/> consumer-group name. The group lives <em>on</em> the
    ///     shared stream key, so on a stream in another namespace it is the consumer name prefixed with
    ///     the process namespace (<c>dev:re:my-service</c>), not the bare consumer name;
    ///   </description></item>
    ///   <item><description>
    ///     outbox state keys, idempotency markers and <c>RedisEvents.Projections</c> view keys.
    ///   </description></item>
    /// </list>
    /// </para>
    /// <para>
    /// A process does not trim a stream in another namespace: <see cref="BackgroundTrimIntervalSeconds"/>
    /// is ignored for it (and logged), because its clamp could only see this namespace's consumers. The
    /// namespace that owns the stream trims it; the inline <c>MAXLEN</c> on every <c>XADD</c> bounds it
    /// regardless. Positions of consumers in other namespaces never hold a shared stream back.
    /// </para>
    /// <para>
    /// The value is lowercased, like the environment name, and may contain only letters, digits,
    /// <c>-</c> and <c>_</c>. Setting it to this process's own environment name changes nothing.
    /// </para>
    /// <para>
    /// <b>Changing it on a running consumer.</b> The topic moves to different stream keys, but the
    /// consumer's positions do not move: they are keyed by topic and consumer, not by stream
    /// namespace. Ids stored against the old stream are then applied to the new one, and any entry
    /// on the new stream older than them is skipped. Reset the consumer when it switches — an
    /// admin reset to the start, or <see cref="ConsumerOptions.ResetOnStart"/> with a
    /// <see cref="ConsumerOptions.StartFromDate"/> for one deploy — or delete its
    /// <c>p:{topic}:&lt;consumer&gt;</c> hash so it starts from
    /// <see cref="ConsumerOptions.StartFromWhenMissing"/>. A <see cref="DeliveryMode.WorkQueue"/>
    /// consumer needs none of this: its group is created afresh on the new stream.
    /// </para>
    /// </remarks>
    public string? StreamNamespace { get; set; }

    /// <summary>
    /// Whether state-stream entries written by a state store on this topic also keep the event's
    /// correlation id, <c>traceparent</c> and headers (default: <see langword="false"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Off, a state entry is the event body and its type and nothing else — all an aggregate load
    /// reads. The topic copy of every event keeps its full metadata either way; that is the copy
    /// projections and consumers read. The partition key is never written on a state entry: the state
    /// stream's name already identifies the aggregate, and a state stream is never trimmed, so a
    /// per-entry copy would cost its bytes forever.
    /// </para>
    /// <para>
    /// Turn it on to keep a permanent record of which request wrote each event (the topic copy's
    /// trace is trimmed away with the topic). Flipping it is safe at any time: readers treat all of
    /// these fields as optional, so one state stream can hold entries of both shapes.
    /// </para>
    /// </remarks>
    public bool StateMetadata { get; set; }

    /// <summary>
    /// Release the trim clamp above this fraction of MaxLen (default: 0.8).
    /// Used to prevent a stuck consumer from holding up trimming and filling the stream.
    /// </summary>
    public double ClampReleaseThreshold { get; set; } = 0.8;
}
