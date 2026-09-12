namespace RedisEvents.Config;

/// <summary>
/// Configuration options for a stream producer.
/// </summary>
/// <remarks>
/// Properties are <c>set</c> rather than <c>init</c> because the source-generated configuration
/// binder cannot assign an <c>init</c>-only member and silently bound nothing at all; see the
/// remarks on <see cref="TopicOptions"/> (R-17).
/// </remarks>
public sealed record ProducerOptions
{
    /// <summary>
    /// The topic to produce to (required).
    /// </summary>
    public string Topic { get; set; } = null!;

    /// <summary>
    /// Whether to buffer messages before sending (default: false).
    /// </summary>
    public bool Buffered { get; set; }

    /// <summary>
    /// Maximum wait time in milliseconds before flushing a buffer (default: 20).
    /// </summary>
    public int MaxWaitMs { get; set; } = 20;

    /// <summary>
    /// Maximum number of messages in a batch (default: 500).
    /// </summary>
    public int MaxBatch { get; set; } = 500;

    /// <summary>
    /// Maximum number of messages in the queue (default: 100000).
    /// </summary>
    public int MaxQueue { get; set; } = 100_000;

    /// <summary>
    /// Drop oldest messages when queue is full (telemetry topics only; default: false).
    /// Drops are metered when enabled.
    /// </summary>
    public bool DropOldest { get; set; }
}
