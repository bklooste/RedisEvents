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
    /// Release the trim clamp above this fraction of MaxLen (default: 0.8).
    /// Used to prevent a stuck consumer from holding up trimming and filling the stream.
    /// </summary>
    public double ClampReleaseThreshold { get; set; } = 0.8;
}
