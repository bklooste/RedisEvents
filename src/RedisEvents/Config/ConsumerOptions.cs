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
