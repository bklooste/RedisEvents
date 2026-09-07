using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Consumer;

/// <summary>
/// The <c>ReadMode.Poll</c> fetch delegate: a plain, non-blocking <c>XREAD COUNT n</c> on the
/// <b>shared</b> multiplexer, with the idle <see cref="Backoff"/> schedule folded into the same call.
/// </summary>
/// <remarks>
/// <para>
/// This is the fallback mode — <c>ReadMode.Block</c> is the default — and it exists for three real
/// cases: a managed Redis tier that caps connections, topics where pickup latency genuinely does not
/// matter, and as a proven escape hatch if the blocking path (which is not a code path
/// StackExchange.Redis officially blesses) misbehaves.
/// </para>
/// <para>
/// <b>No extra connection and no extra thread.</b> Unlike the blocking reader, polling never parks a
/// connection server-side, so it runs on the shared <see cref="IDatabase"/> with no
/// <c>ConnectionMultiplexer</c>, no <c>SocketManager</c> and nothing to dispose. The entire cost of
/// this mode is its command rate: at the default 50&#160;ms cap, ~20 commands per second per
/// partition while idle.
/// </para>
/// <para>
/// <b>It plugs into the same seam as blocking.</b> <see cref="FetchAsync"/> matches
/// <c>Func&lt;CancellationToken, ValueTask&lt;StreamEntryBatch&gt;&gt;</c>, so
/// <see cref="PartitionWorker"/> is identical in both modes and everything downstream of the fetch —
/// decode, filter, pooled batching, channel write, position advance — is shared line for line.
/// </para>
/// <para>
/// <b>Cursor ownership.</b> The fetch delegate takes only a token, so the read cursor lives here: it
/// starts at the resolved start position and advances to the last entry of every non-empty reply.
/// The reply's raw id is kept as-is, which is exactly what the next <c>XREAD</c> needs, so a steady
/// read costs no formatting and no parsing. <see cref="SeekTo"/> exists for the reset-marker
/// protocol (P1-21), which restarts a live reader at an administrator-chosen id.
/// </para>
/// <para>
/// <b>Idle behaviour.</b> A non-empty read resets the backoff and returns immediately, so full
/// batches are never separated by a sleep. An empty read waits <see cref="Backoff.Next"/>
/// milliseconds before returning empty — and the <em>first</em> empty read waits zero, yielding
/// rather than sleeping, so a quiet topic that receives a single message still picks it up in
/// sub-millisecond time.
/// </para>
/// <para>
/// Not thread-safe, and does not need to be: there is exactly one reader loop per partition.
/// </para>
/// </remarks>
internal sealed class PollFetch
{
    private readonly IDatabase db;
    private readonly RedisKey key;
    private readonly int batchSize;
    private readonly int maxIdleDelayMs;

    /// <summary>The id to read <em>after</em> — the raw reply id, ready for the next <c>XREAD</c>.</summary>
    private RedisValue cursor;

    /// <summary>Milliseconds to wait after the next consecutive empty read; zero after any entries.</summary>
    private int idleDelayMs;

    /// <summary>
    /// Creates the poll fetch for one partition.
    /// </summary>
    /// <param name="db">The shared multiplexer's database. Polling adds no connection of its own.</param>
    /// <param name="key">This partition's stream key, <c>s:{topic}:&lt;partition&gt;</c>.</param>
    /// <param name="from">The resolved start position; the first read returns entries after this id.</param>
    /// <param name="batchSize">Entries requested per read (<c>COUNT</c>).</param>
    /// <param name="maxIdleDelayMs">The idle backoff cap in milliseconds (<c>MaxIdleDelayMs</c>, default 50).</param>
    internal PollFetch(IDatabase db, RedisKey key, StreamId from, int batchSize, int maxIdleDelayMs)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);

        this.db = db;
        this.key = key;
        this.batchSize = batchSize;
        this.maxIdleDelayMs = maxIdleDelayMs;
        this.cursor = from.Format();
    }

    /// <summary>
    /// Creates the poll fetch for a <b>co-located group</b>: one non-blocking multi-stream
    /// <c>XREAD</c> covering every partition the worker owns, sharing this class's idle backoff so a
    /// quiet group costs one command per interval rather than one per partition.
    /// </summary>
    /// <remarks>
    /// The cursors are not owned here — the co-located read loop passes its own
    /// <see cref="StreamPosition"/> array to <see cref="FetchManyAsync"/> each round — so the
    /// single-key state (<see cref="Position"/>, <see cref="SeekTo"/>) is unused in this form.
    /// </remarks>
    /// <param name="db">The shared multiplexer's database. Polling adds no connection of its own.</param>
    /// <param name="batchSize">Entries requested per stream (<c>COUNT</c>).</param>
    /// <param name="maxIdleDelayMs">The idle backoff cap in milliseconds (<c>MaxIdleDelayMs</c>, default 50).</param>
    internal PollFetch(IDatabase db, int batchSize, int maxIdleDelayMs)
        : this(db, default, StreamId.Min, batchSize, maxIdleDelayMs)
    {
    }

    /// <summary>The id the next read will start after.</summary>
    internal StreamId Position =>
        StreamId.TryParse(((string?)this.cursor).AsSpan(), out var id) ? id : StreamId.Min;

    /// <summary>The delay, in milliseconds, that the next consecutive empty read will wait.</summary>
    internal int IdleDelayMs => this.idleDelayMs;

    /// <summary>
    /// Moves the cursor, for the reset-marker protocol. The idle backoff is reset too, so a live
    /// reader picks the new position up on its next read rather than after an idle sleep.
    /// </summary>
    /// <param name="id">The id to read after.</param>
    internal void SeekTo(StreamId id)
    {
        this.cursor = id.Format();
        this.idleDelayMs = 0;
    }

    /// <summary>
    /// One poll: <c>XREAD COUNT &lt;batchSize&gt; STREAMS &lt;key&gt; &lt;cursor&gt;</c>, with no
    /// <c>BLOCK</c>. Returns the entries immediately when there are any; otherwise waits out the
    /// current backoff step and returns an empty batch.
    /// </summary>
    /// <param name="ct">The linked host token. Observed before the read and during any idle wait.</param>
    /// <returns>The fetched entries, or <see cref="StreamEntryBatch.Empty"/>.</returns>
    /// <exception cref="OperationCanceledException">The token was cancelled.</exception>
    internal async ValueTask<StreamEntryBatch> FetchAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        // DemandMaster: streams must be read from the primary — a replica can lag arbitrarily and
        // would silently hand the consumer a stale tail.
        var entries = await this.db
            .StreamReadAsync(this.key, this.cursor, this.batchSize, CommandFlags.DemandMaster)
            .ConfigureAwait(false);

        if (entries is { Length: > 0 })
        {
            // Advance to the last entry and reset the backoff: the stream has data, so the reader
            // loops straight back around with no sleep between full batches.
            this.cursor = entries[^1].Id;
            this.idleDelayMs = 0;
            return new StreamEntryBatch(entries);
        }

        await this.WaitIdleAsync(ct).ConfigureAwait(false);

        return StreamEntryBatch.Empty;
    }

    /// <summary>
    /// One multi-stream poll: <c>XREAD COUNT &lt;n&gt; STREAMS k1 … kN id1 … idN</c>, with no
    /// <c>BLOCK</c> and the same idle backoff a single-partition poll uses.
    /// </summary>
    /// <remarks>
    /// One command for the whole co-located group, which is the point of co-location (D2 in the
    /// overview): N partitions cost one round trip and one idle interval, not N of each.
    /// </remarks>
    /// <param name="positions">The group's cursors, owned by the read loop.</param>
    /// <param name="countPerStream">The <c>COUNT</c> applied to each stream.</param>
    /// <param name="ct">The linked host token. Observed before the read and during any idle wait.</param>
    /// <returns>The streams that had entries; streams with nothing new are omitted.</returns>
    /// <exception cref="OperationCanceledException">The token was cancelled.</exception>
    internal async ValueTask<StreamSlice[]> FetchManyAsync(
        StreamPosition[] positions,
        int countPerStream,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(positions);
        ct.ThrowIfCancellationRequested();

        var reply = await this.db
            .StreamReadAsync(
                positions,
                countPerStream >= 1 ? countPerStream : this.batchSize,
                CommandFlags.DemandMaster)
            .ConfigureAwait(false);

        if (reply is { Length: > 0 })
        {
            // The group has data, so the reader loops straight back around with no sleep.
            this.idleDelayMs = 0;
            return PartitionWorker.ToSlices(reply);
        }

        await this.WaitIdleAsync(ct).ConfigureAwait(false);

        return [];
    }

    /// <summary>Waits out the current backoff step and advances it. Shared by both fetch forms.</summary>
    private async ValueTask WaitIdleAsync(CancellationToken ct)
    {
        var delay = this.idleDelayMs;
        this.idleDelayMs = Backoff.Next(delay, this.maxIdleDelayMs);

        if (delay <= 0)
        {
            // Delay(0) semantics, spelled as a yield so it is guaranteed to give the scheduler a
            // turn rather than completing synchronously and spinning the loop on this thread.
            await Task.Yield();
        }
        else
        {
            await Task.Delay(delay, ct).ConfigureAwait(false);
        }
    }
}
