using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Diagnostics;
using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Consumer;

/// <summary>
/// Everything one partition's reader and processor loops need, in one struct.
/// </summary>
/// <remarks>
/// <para>
/// There is no per-partition class and no interface on the path: the loops are <c>static</c>
/// methods and their state travels as this tuple (design principle 1 in the overview). The struct
/// is passed <c>in</c> at the entry point and copied once per loop, so nothing here is allocated
/// per batch, let alone per message.
/// </para>
/// <para>
/// <see cref="Db"/> is the <em>shared</em> multiplexer's database, used for everything except a
/// blocking read. <c>ReadMode.Block</c> never touches it — the blocking fetch closes over its own
/// dedicated reader connection and reaches the loops only through the fetch delegate, which is what
/// keeps the reader connection write-free and the outbox transaction intact.
/// </para>
/// </remarks>
/// <param name="Db">The shared multiplexer's database — position writes, admin, non-blocking reads.</param>
/// <param name="StreamKey">This partition's stream key, <c>s:{topic}:&lt;partition&gt;</c>.</param>
/// <param name="Partition">The partition index this worker owns.</param>
/// <param name="Topic">The topic name, carried for logs, metrics and activity tags.</param>
/// <param name="Consumer">The consumer name — the position hash field owner, and a metric tag.</param>
/// <param name="BatchSize">Entries requested per fetch; also the size of the pooled batch array.</param>
/// <param name="Filter">
/// Message types to keep, matched on the <c>t</c> field before the body is materialised, or
/// <see langword="null"/> to keep everything.
/// </param>
/// <param name="OnError">What a failing handler does to this partition.</param>
/// <param name="Writer">
/// The bounded channel the reader writes batches to, or <see langword="null"/> in inline
/// (no-backpressure) mode where the reader calls the handler itself.
/// </param>
/// <param name="Log">Logger for both loops.</param>
internal readonly record struct PartitionContext(
    IDatabase Db,
    RedisKey StreamKey,
    int Partition,
    string Topic,
    string Consumer,
    int BatchSize,
    string[]? Filter,
    ErrorPolicy OnError,
    ChannelWriter<StreamBatch>? Writer,
    ILogger Log)
{
    /// <summary>
    /// This partition's health signal — the object the health check and the <c>streams.*</c> gauges
    /// read, and the only way either of them can see that a partition is blocked, stopped or behind.
    /// </summary>
    /// <remarks>
    /// <para>
    /// An <c>init</c> property rather than a positional parameter so that every existing construction
    /// site (and every unit test that scripts a context) keeps compiling; a context built without one
    /// simply reports nothing, which is what the unit tests want.
    /// </para>
    /// <para>
    /// Nullable, and every call site is <c>?.</c>: the loops must run identically whether or not a
    /// monitor was supplied, and a null check on a field is cheaper than the two volatile writes it
    /// guards.
    /// </para>
    /// </remarks>
    internal StreamPartitionMonitor? Monitor { get; init; }
}

/// <summary>
/// Records a processed position for a partition. Called on the processor loop only, after a batch
/// has been handled successfully — never at read time.
/// </summary>
/// <remarks>
/// A delegate rather than an interface so the hot path keeps one indirect call and no virtual
/// dispatch; in the host it is bound to <c>PositionFlusher.Record</c>, and in tests to a counter.
/// The implementation must not allocate and must not await.
/// </remarks>
/// <param name="partition">The partition the position belongs to.</param>
/// <param name="id">The last entry id processed in that partition.</param>
internal delegate void PositionRecorder(int partition, StreamId id);
