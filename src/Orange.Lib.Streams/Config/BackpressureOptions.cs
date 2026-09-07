namespace Orange.Lib.Streams.Config;

/// <summary>
/// Configuration options for ordered backpressure on a consumer.
/// </summary>
/// <remarks>
/// Properties are <c>set</c> rather than <c>init</c> because the source-generated configuration
/// binder cannot assign an <c>init</c>-only member and silently bound nothing at all; see the
/// remarks on <see cref="TopicOptions"/> (R-17).
/// </remarks>
public sealed record BackpressureOptions
{
    /// <summary>
    /// Enable ordered backpressure (default: true).
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Number of batches in flight per partition (default: 4).
    /// </summary>
    public int Capacity { get; set; } = 4;
}
