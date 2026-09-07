using System.Threading.Channels;
using Orange.Lib.Streams.Positions;
using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Consumer;

/// <summary>
/// One fetch's worth of raw Redis stream entries, exactly as the client handed them back.
/// </summary>
/// <remarks>
/// <para>
/// This is the seam that makes the pipeline unit-testable. The read loop never calls Redis itself;
/// it awaits a <c>Func&lt;CancellationToken, ValueTask&lt;StreamEntryBatch&gt;&gt;</c>, so a test
/// scripts a read sequence — full batch, empty batch, empty batch, full batch, cancellation — with
/// no Redis and no <see cref="IDatabase"/> mock. Everything downstream of the fetch (decode, filter,
/// pooled batching, channel write, position advance) is then covered by ordinary unit tests, and the
/// only untested seam is the one Redis call itself, which the service tests cover.
/// </para>
/// <para>
/// It carries the raw <see cref="StreamEntry"/> array rather than decoded messages so that both
/// fetch implementations — <c>ReadMode.Poll</c> on the shared multiplexer and <c>ReadMode.Block</c>
/// on the dedicated reader connection — hand back the same shape and share every line downstream.
/// </para>
/// </remarks>
/// <param name="Entries">The fetched entries; may be longer than <paramref name="Count"/>.</param>
/// <param name="Count">How many leading elements of <paramref name="Entries"/> are live.</param>
internal readonly record struct StreamEntryBatch(StreamEntry[] Entries, int Count)
{
    /// <summary>A fetch that returned nothing — an idle stream, or a block that timed out.</summary>
    internal static StreamEntryBatch Empty => new([], 0);

    /// <summary>Wraps a whole array.</summary>
    internal StreamEntryBatch(StreamEntry[] entries)
        : this(entries, entries.Length)
    {
    }

    /// <summary>True when the fetch returned no entries.</summary>
    internal bool IsEmpty => Count == 0;

    /// <summary>The live entries.</summary>
    internal ReadOnlySpan<StreamEntry> Span => Entries.AsSpan(0, Count);
}

/// <summary>
/// One stream's worth of a multi-stream <c>XREAD</c> reply — the co-located counterpart of
/// <see cref="StreamEntryBatch"/>.
/// </summary>
/// <remarks>
/// <para>
/// StackExchange.Redis's own <c>RedisStream</c> would be the obvious carrier, but it has no
/// accessible constructor, so the hand-parsed <c>ReadMode.Block</c> reply could never be expressed
/// as one. This struct is the same two fields plus a live count, so the block path can hand back a
/// right-sized array without trimming it, and <c>ReadMode.Poll</c> maps <c>RedisStream</c> onto it
/// once per fetch — never per message.
/// </para>
/// </remarks>
/// <param name="Key">The stream key this slice belongs to, as it was requested.</param>
/// <param name="Entries">The entries Redis returned; may be longer than <paramref name="Count"/>.</param>
/// <param name="Count">How many leading elements of <paramref name="Entries"/> are live.</param>
internal readonly record struct StreamSlice(RedisKey Key, StreamEntry[] Entries, int Count)
{
    /// <summary>Wraps a whole array.</summary>
    internal StreamSlice(RedisKey key, StreamEntry[] entries)
        : this(key, entries, entries.Length)
    {
    }

    /// <summary>True when this stream had nothing new.</summary>
    internal bool IsEmpty => Count == 0;

    /// <summary>The live entries.</summary>
    internal ReadOnlySpan<StreamEntry> Span => Entries.AsSpan(0, Count);
}

/// <summary>
/// The two loops that consume one partition: a reader that fetches, decodes and filters, and a
/// processor that invokes the handler and advances the position.
/// </summary>
/// <remarks>
/// <para>
/// Static methods over a <see cref="PartitionContext"/>, not a per-partition class hierarchy. The
/// loops are joined by a bounded channel (<see cref="CreateChannel"/>) so a slow handler stops the
/// reader through backpressure rather than through unbounded memory growth: the reader awaits
/// <c>WriteAsync</c> <em>before</em> issuing the next fetch, so a full channel means the next read is
/// never started and the lag stays in Redis where it belongs.
/// </para>
/// <para>
/// Ordering is per partition: one reader, one processor, no parallel dispatch inside a partition.
/// </para>
/// </remarks>
internal static partial class PartitionWorker
{
    /// <summary>
    /// Builds the reader-to-processor channel for one partition.
    /// </summary>
    /// <param name="capacity">
    /// Capacity in <em>batches</em>, not messages — the default 4 with a batch size of 100 is 400
    /// messages in flight per partition.
    /// </param>
    internal static Channel<StreamBatch> CreateChannel(int capacity)
        => Channel.CreateBounded<StreamBatch>(new BoundedChannelOptions(capacity)
        {
            // Real backpressure: block the reader, never drop a batch.
            FullMode = BoundedChannelFullMode.Wait,

            // One reader loop, one writer loop — lets the channel take its lock-light fast path.
            SingleReader = true,
            SingleWriter = true,

            // LOAD-BEARING, not incidental. With synchronous continuations allowed, the reader
            // thread that completes a WriteAsync can be hijacked to run the processor's continuation
            // inline — i.e. application handler code would execute on the dedicated reader thread,
            // stalling that consumer's XREADs and defeating the point of dedicating a thread at all.
            // Keeping it false guarantees the handler resumes on the ThreadPool.
            AllowSynchronousContinuations = false,
        });
}
