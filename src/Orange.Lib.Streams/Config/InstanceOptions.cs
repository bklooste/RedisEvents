namespace Orange.Lib.Streams.Config;

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
    /// Instance mode for partition ownership: Static or Lease (default: Static).
    /// </summary>
    public InstanceMode Mode { get; set; } = InstanceMode.Static;

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
