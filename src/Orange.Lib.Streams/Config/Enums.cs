namespace Orange.Lib.Streams.Config;

/// <summary>
/// Defines where a consumer starts reading from a topic.
/// </summary>
public enum StartFrom
{
    /// <summary>
    /// Start from the last stored position (default).
    /// </summary>
    Stored = 0,

    /// <summary>
    /// Start from the beginning of the stream.
    /// </summary>
    Beginning = 1,

    /// <summary>
    /// Start from the current moment (now).
    /// </summary>
    Now = 2,

    /// <summary>
    /// Start from a specific date (requires StartFromDate to be set).
    /// </summary>
    Date = 3,
}

/// <summary>
/// Defines how consumer position is persisted, balancing durability against latency and throughput.
/// </summary>
/// <remarks>
/// <para>
/// <b>Delivery is at-least-once and the duplicate window is <see cref="ConsumerOptions.PersistIntervalMs"/>.</b>
/// A process crash or Redis failure after a message is processed but before its position is persisted
/// causes that message to be re-delivered on restart. The window is deterministic but non-zero for all modes.
/// </para>
/// <para>
/// <b>AsyncBatch (default):</b> A hard kill can redeliver up to
/// <see cref="ConsumerOptions.PersistIntervalMs"/> of already-processed messages. No latency cost on the
/// handler, and throughput is highest. Best for streams where re-delivery is tolerable.
/// </para>
/// <para>
/// <b>SyncBatch:</b> Position persists at the end of each batch, so the handler waits for the write
/// to complete. A handler that half-succeeds on a blocking retry re-runs its first half again. Latency
/// is per-batch rather than per-message, so the overhead is modest.
/// </para>
/// <para>
/// <b>SyncMessage:</b> Position persists after every message. Highest durability (one-message window)
/// but one-request-per-message overhead to Redis. Do not use this for high-rate streams; the duplicate
/// window is small but not zero — a process can still crash between the handler returning and the
/// position write landing in Redis. There is no exactly-once and the docs must not imply one.
/// </para>
/// <para>
/// <b>None:</b> Positions are never persisted. Consumer always restarts from
/// <see cref="StartPosition.Starting"/> (or <see cref="StartPosition.Now"/> if
/// <see cref="ConsumerOptions.StartFrom"/> is set that way). Useful only for stateless fanout or testing.
/// </para>
/// </remarks>
public enum PersistMode
{
    /// <summary>
    /// Async batch persistence (default) — positions flushed periodically, duplicate window is <see cref="ConsumerOptions.PersistIntervalMs"/>.
    /// </summary>
    AsyncBatch = 0,

    /// <summary>
    /// Sync batch persistence — wait for positions to be flushed per batch, one-batch duplicate window.
    /// </summary>
    SyncBatch = 1,

    /// <summary>
    /// Sync message persistence — wait for each position to be persisted, one-message duplicate window (not exactly-once).
    /// </summary>
    SyncMessage = 2,

    /// <summary>
    /// No persistence — positions are never stored, consumer always starts from <see cref="StartPosition.Starting"/>.
    /// </summary>
    None = 3,
}

/// <summary>
/// Defines error handling policy when message processing fails.
/// </summary>
/// <remarks>
/// <para>
/// <b>The handler author must choose before writing a handler.</b> There is no retrying in the
/// library — if the failure is transient, the handler retries it (Polly, a loop, whatever suits).
/// </para>
/// <para>
/// <b>An ordinary exception means the message is logged and skipped.</b> Not retried, not parked.
/// Gone. Only <see cref="DontIgnoreException"/> subclasses block the partition.
/// </para>
/// <para>
/// <b>BestEffort (default):</b> The position <b>advances</b> and the message is gone. The next batch
/// is processed. The consumer continues. Bluntly: a malformed message that will never deserialize is
/// correctly skipped and forgotten. A transient downstream failure that the handler did not retry is
/// also silently forgotten. Only use this if the handler is robust — retries transient failures,
/// parks unsalvageable messages somewhere durable for manual triage, or both.
/// </para>
/// <para>
/// <b>StopPartition:</b> The partition stops processing and accumulates lag until the error is
/// fixed. Other partitions continue. Lag does not grow above what it was when the error hit, so
/// memory stays bounded and replica failover is safe.
/// </para>
/// <para>
/// <b>Fail:</b> The entire consumer pod is restarted. Used to force a backoff when the whole system
/// is broken (Redis is down, the database is down), not for one-message errors.
/// </para>
/// </remarks>
public enum ErrorPolicy
{
    /// <summary>
    /// Best effort (default) — log error, advance position, skip the message. Handler retries transients.
    /// </summary>
    BestEffort = 0,

    /// <summary>
    /// Stop the partition on error; other partitions continue, position does not advance.
    /// </summary>
    StopPartition = 1,

    /// <summary>
    /// Fail the entire consumer pod; restart forces a backoff.
    /// </summary>
    Fail = 2,
}

/// <summary>
/// Defines the read mode for consuming from Redis streams.
/// </summary>
public enum ReadMode
{
    /// <summary>
    /// Block mode — uses XREAD BLOCK (default, dedicated connection).
    /// </summary>
    Block = 0,

    /// <summary>
    /// Poll mode — uses periodic polling (fallback).
    /// </summary>
    Poll = 1,
}

/// <summary>
/// Defines the instance mode for partition ownership.
/// </summary>
public enum InstanceMode
{
    /// <summary>
    /// Static mode — ownership is fixed based on StatefulSet ordinals (default).
    /// </summary>
    Static = 0,

    /// <summary>
    /// Lease mode — ownership is claimed and released dynamically.
    /// </summary>
    Lease = 1,
}

/// <summary>
/// Defines the trim mode for stream length management.
/// </summary>
public enum TrimMode
{
    /// <summary>
    /// Approximate trimming (default) — uses the '~' form, trims when a macro node can be dropped.
    /// </summary>
    Approx = 0,

    /// <summary>
    /// Exact trimming — trims to the precise length, higher CPU cost in Redis.
    /// </summary>
    Exact = 1,

    /// <summary>
    /// No trimming — stream length is not controlled.
    /// </summary>
    None = 2,
}
