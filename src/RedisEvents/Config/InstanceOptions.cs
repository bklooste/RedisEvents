namespace RedisEvents.Config;

/// <summary>
/// Configuration options for instance identity and partition ownership.
/// </summary>
/// <remarks>
/// Properties are <c>set</c> rather than <c>init</c> because the source-generated configuration
/// binder cannot assign an <c>init</c>-only member and silently bound nothing at all; see the
/// remarks on <see cref="TopicOptions"/> (R-17).
/// </remarks>
public sealed record InstanceOptions
{
    /// <summary>The ownership claim TTL the consumer host actually uses, in seconds.</summary>
    public const int DefaultLeaseTtlSeconds = 30;

    /// <summary>The ownership renewal interval the consumer host actually uses, in seconds.</summary>
    public const int DefaultLeaseRenewSeconds = 10;

    /// <summary>
    /// Instance mode for partition ownership: Lease or Static (default: Lease).
    /// </summary>
    /// <remarks>
    /// Lease is the default because it needs neither a StatefulSet ordinal nor a hand-maintained
    /// <c>STREAMS_INSTANCE_COUNT</c>, and drift between that count and <c>spec.replicas</c> is
    /// silent — partitions consumed by nobody, or consumed twice. Static remains available and
    /// must now be asked for by name.
    /// </remarks>
    public InstanceMode Mode
    {
        get => this.mode;
        set
        {
            this.mode = value;
            this.ModeWasSetExplicitly = true;
        }
    }

    /// <summary>
    /// True when <see cref="Mode"/> was assigned — by configuration binding or by hand — rather than
    /// left at its default. Validation uses it to tell "this config asked for Lease" apart from
    /// "this config predates Lease becoming the default", which <c>default(InstanceMode)</c> can no
    /// longer express now that the enum's zero value and the property default differ.
    /// </summary>
    internal bool ModeWasSetExplicitly { get; private set; }

    private InstanceMode mode = InstanceMode.Lease;

    /// <summary>
    /// Total number of instances in the pool (used in Static mode).
    /// </summary>
    public int? Count { get; set; }

    /// <summary>
    /// This instance's index in the pool (used in Static mode).
    /// </summary>
    public int? Index { get; set; }

    /// <summary>
    /// Lease TTL in seconds (used in Lease mode; default: 30).
    /// </summary>
    public int LeaseTtlSeconds { get; set; } = DefaultLeaseTtlSeconds;

    /// <summary>
    /// Lease renewal interval in seconds (used in Lease mode; default: 10).
    /// </summary>
    public int LeaseRenewSeconds { get; set; } = DefaultLeaseRenewSeconds;
}
