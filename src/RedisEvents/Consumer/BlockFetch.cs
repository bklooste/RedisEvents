using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using RedisEvents.Errors;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Consumer;

/// <summary>
/// The <c>ReadMode.Block</c> fetch delegate — the <b>default</b> read mode: one
/// <c>XREAD BLOCK &lt;BlockMs&gt; COUNT &lt;BatchSize&gt; STREAMS &lt;key&gt; &lt;id&gt;</c> issued on
/// the dedicated, read-only <see cref="StreamReaderConnection"/>, with the RESP reply parsed by
/// hand into the same <see cref="StreamEntryBatch"/> <see cref="PollFetch"/> returns.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the reply is parsed by hand.</b> StackExchange.Redis deliberately exposes no blocking
/// command — a blocking call parks a multiplexed connection server-side and stalls everything
/// queued behind it — so there is no <c>StreamReadAsync(..., block: …)</c> to call. The command goes
/// out through <see cref="StreamReaderConnection.ExecuteXReadAsync"/> as a raw
/// <c>ExecuteAsync("XREAD", …)</c>, which hands back an untyped <see cref="RedisResult"/>, and
/// turning that back into <see cref="StreamEntry"/> values is this file's job.
/// </para>
/// <para>
/// <b>BLOCK is self-adapting: this single call in a loop already IS "drain while there is data, then
/// wait".</b> Redis returns the moment at least one entry exists — it does <em>not</em> wait for
/// <c>COUNT</c> to fill — and it parks only when the stream is empty. So under load this call
/// returns immediately and the read loop comes straight back around, draining as fast as the channel
/// accepts; when the topic goes quiet the very same call parks for up to <c>BlockMs</c>. There is
/// therefore <b>no separate fetch loop, no hybrid mode switch and no backoff in this mode</b>, and
/// adding any of them would be redundant: a "drain-then-wait" wrapper around a call that already
/// drains and waits buys nothing and costs a round trip per iteration. This note exists because the
/// omission looks like a gap and someone will otherwise "fix" it.
/// </para>
/// <para>
/// <b><c>BLOCK 0</c> (wait forever) is deliberately not used.</b> A bounded <c>BlockMs</c> gives the
/// loop a natural point to observe cancellation — StackExchange.Redis cannot abort a command already
/// on the wire, so shutdown latency is exactly one block interval — and it keeps the connection's
/// heartbeat cadence predictable, which is what stops a long block tripping the client's
/// unhealthy-connection detection. The token is observed at every block boundary and before the
/// command is issued.
/// </para>
/// <para>
/// <b>Runs on the reader connection, never the shared multiplexer.</b> The constructor takes a
/// <see cref="StreamReaderConnection"/> rather than an <see cref="IDatabase"/>, which is what makes
/// that structural: there is no way to hand this class the shared connection, and no way to send a
/// write through the one it has.
/// </para>
/// <para>
/// <b>Cursor ownership.</b> As with <see cref="PollFetch"/>, the fetch delegate takes only a token,
/// so the read cursor lives here: it starts at the resolved start position and advances to the last
/// entry of every non-empty reply. <see cref="SeekTo"/> exists for the reset-marker protocol, which
/// restarts a live reader at an administrator-chosen id.
/// </para>
/// <para>
/// Not thread-safe, and does not need to be: there is exactly one reader loop per partition, and the
/// argument array below is mutated in place between reads.
/// </para>
/// </remarks>
internal sealed class BlockFetch
{
    /// <summary>Argument slot holding the <c>COUNT</c> value.</summary>
    private const int CountSlot = 3;

    /// <summary>The number of fixed arguments before the stream keys: BLOCK, ms, COUNT, n, STREAMS.</summary>
    private const int HeaderSlots = 5;

    private readonly StreamReaderConnection reader;
    private readonly RedisKey[] keys;
    private readonly byte[][] keyBytes;
    private readonly int blockMs;

    /// <summary>
    /// The slot of the first id — <c>HeaderSlots + keys.Length</c>. In the single-key form that is
    /// slot 6; with N keys the ids occupy the last N slots, which is exactly the shape
    /// <c>XREAD … STREAMS k1 … kN id1 … idN</c> wants.
    /// </summary>
    private readonly int cursorSlot;

    /// <summary>The <c>COUNT</c> currently in <see cref="args"/>, so a steady read re-boxes nothing.</summary>
    private int count;

    /// <summary>
    /// The command arguments <em>after</em> <c>XREAD</c>, built once:
    /// <c>BLOCK, blockMs, COUNT, batchSize, STREAMS, key, id</c>. Only the id slots are
    /// rewritten, so a steady read costs one small box rather than a fresh argument array.
    /// </summary>
    private readonly object[] args;

    /// <summary>The id to read <em>after</em> — the raw reply id, ready for the next <c>XREAD</c>.</summary>
    private RedisValue cursor;

    /// <summary>
    /// Creates the blocking fetch for one partition.
    /// </summary>
    /// <param name="reader">
    /// The dedicated, read-only reader connection. It also supplies <c>BlockMs</c>, which it has
    /// already validated against its own <c>syncTimeout</c>/<c>asyncTimeout</c> — taking the value
    /// from here rather than from a parameter is what guarantees the block cannot outlive the
    /// client-side timeout.
    /// </param>
    /// <param name="key">This partition's stream key, <c>s:{topic}:&lt;partition&gt;</c>.</param>
    /// <param name="from">The resolved start position; the first read returns entries after this id.</param>
    /// <param name="batchSize">Entries requested per read (<c>COUNT</c>).</param>
    /// <exception cref="StreamConfigurationException">
    /// <c>BlockMs</c> is not positive. Zero would mean <c>BLOCK 0</c> — block forever — which this
    /// library deliberately never issues.
    /// </exception>
    internal BlockFetch(StreamReaderConnection reader, RedisKey key, StreamId from, int batchSize)
        : this(reader, [key], batchSize)
    {
        this.cursor = from.Format();
        this.args[this.cursorSlot] = this.cursor;
    }

    /// <summary>
    /// Creates the blocking fetch for a <b>co-located group</b>: one
    /// <c>XREAD BLOCK … STREAMS k1 … kN id1 … idN</c> covering every partition the worker owns, so
    /// the group costs one round trip rather than N — and, more importantly, so the N reads cannot
    /// head-of-line block each other on the one reader connection they share (D2 in the overview).
    /// </summary>
    /// <remarks>
    /// The cursors are <em>not</em> owned here: the co-located read loop owns one
    /// <see cref="StreamPosition"/> array and hands it to <see cref="FetchManyAsync"/> each round,
    /// which is what lets a reset rewind a single partition without disturbing the others.
    /// <paramref name="keys"/> must be index-aligned with the positions the loop will pass.
    /// </remarks>
    /// <param name="reader">The dedicated, read-only reader connection; it also supplies <c>BlockMs</c>.</param>
    /// <param name="keys">The partitions' stream keys, in the order the loop's positions are ordered.</param>
    /// <param name="batchSize">Entries requested per stream (<c>COUNT</c>).</param>
    /// <exception cref="StreamConfigurationException"><c>BlockMs</c> is not positive.</exception>
    internal BlockFetch(StreamReaderConnection reader, RedisKey[] keys, int batchSize)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);

        if (keys.Length == 0)
        {
            throw new ArgumentException("A blocking fetch needs at least one stream key.", nameof(keys));
        }

        if (reader.BlockMs <= 0)
        {
            throw new StreamConfigurationException(
                $"Streams: BlockMs must be positive (it is {reader.BlockMs}). " +
                "BLOCK 0 — block forever — is deliberately never issued: a bounded block is what gives the reader " +
                "a point to observe cancellation and keeps the connection's heartbeat cadence predictable.");
        }

        this.reader = reader;
        this.keys = keys;
        this.blockMs = reader.BlockMs;
        this.count = batchSize;
        this.cursorSlot = HeaderSlots + keys.Length;
        this.keyBytes = new byte[keys.Length][];

        // Boxed once at startup, not per read. RedisKey is boxed rather than passed as a string so
        // StackExchange.Redis treats it as a key (prefixing and cluster slot routing apply).
        this.args = new object[HeaderSlots + (keys.Length * 2)];
        this.args[0] = "BLOCK";
        this.args[1] = this.blockMs;
        this.args[CountSlot - 1] = "COUNT";
        this.args[CountSlot] = batchSize;
        this.args[HeaderSlots - 1] = "STREAMS";

        for (var i = 0; i < keys.Length; i++)
        {
            this.keyBytes[i] = (byte[]?)keys[i] ?? [];
            this.args[HeaderSlots + i] = keys[i];

            // Overwritten before the first read; a real id is never sent from here.
            this.args[this.cursorSlot + i] = StreamId.Min.Format();
        }
    }

    /// <summary>The id the next read will start after. Single-key form only.</summary>
    internal StreamId Position =>
        StreamId.TryParse(((string?)this.cursor).AsSpan(), out var id) ? id : StreamId.Min;

    /// <summary>The <c>BLOCK</c> timeout, in milliseconds, this fetch issues.</summary>
    internal int BlockMs => this.blockMs;

    /// <summary>
    /// Moves the cursor, for the reset-marker protocol. Takes effect on the next read; a read
    /// already parked in Redis still returns against the old cursor, which is harmless — the entries
    /// it returns are discarded by the position the reset installs.
    /// </summary>
    /// <param name="id">The id to read after.</param>
    internal void SeekTo(StreamId id)
    {
        this.cursor = id.Format();
        this.args[this.cursorSlot] = this.cursor;
    }

    /// <summary>
    /// One blocking read. Returns as soon as Redis has at least one entry; otherwise parks for up to
    /// <see cref="BlockMs"/> and returns an empty batch.
    /// </summary>
    /// <param name="ct">
    /// The linked host token, observed before the command is issued and — by virtue of the bounded
    /// block — at least once every <see cref="BlockMs"/> thereafter. StackExchange.Redis cannot
    /// abort a command already on the wire, so an in-flight block is left to expire rather than
    /// abandoned, which is why the block is bounded in the first place.
    /// </param>
    /// <returns>The fetched entries, or <see cref="StreamEntryBatch.Empty"/> when the block expired.</returns>
    /// <exception cref="OperationCanceledException">The token was cancelled.</exception>
    /// <exception cref="StreamTransportException">The reply was not in the documented <c>XREAD</c> shape.</exception>
    internal async ValueTask<StreamEntryBatch> FetchAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var reply = await this.reader.ExecuteXReadAsync(this.args, ct).ConfigureAwait(false);

        // Cancellation is observed here as well as on entry: the block just ended, so this is the
        // boundary the shutdown path waits for.
        ct.ThrowIfCancellationRequested();

        var batch = Parse(reply, this.keyBytes[0]);

        if (batch.IsEmpty)
        {
            return batch;
        }

        // Advance past the last entry read. The reply id is kept exactly as Redis sent it, which is
        // what the next XREAD wants, so a steady read parses and formats nothing.
        this.cursor = batch.Entries[batch.Count - 1].Id;
        this.args[this.cursorSlot] = this.cursor;

        return batch;
    }

    /// <summary>
    /// One blocking multi-stream read across the whole co-located group: a single
    /// <c>XREAD BLOCK … COUNT … STREAMS k1 … kN id1 … idN</c>, split by stream key.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the whole point of co-location. N separate blocking reads on the one dedicated reader
    /// connection would each park it server-side in turn, so a partition with a backlog would only
    /// get a read turn roughly every N × <c>BlockMs</c> — the head-of-line blocking the dedicated
    /// connection exists to prevent, reintroduced inside it. One command covering every key parks
    /// once and returns the moment <em>any</em> of them has an entry.
    /// </para>
    /// <para>
    /// The cursors belong to the caller: the id slots are rewritten from
    /// <paramref name="positions"/> on every read, which is what makes a per-partition reset a store
    /// into the loop's own array.
    /// </para>
    /// </remarks>
    /// <param name="positions">
    /// One entry per key, index-aligned with the keys this fetch was constructed with. Only the ids
    /// are read; the keys are the ones already boxed into the argument array.
    /// </param>
    /// <param name="countPerStream">The <c>COUNT</c> applied to each stream.</param>
    /// <param name="ct">The linked host token, observed before the command and after the block ends.</param>
    /// <returns>The streams that had entries; streams with nothing new are omitted.</returns>
    /// <exception cref="OperationCanceledException">The token was cancelled.</exception>
    /// <exception cref="StreamTransportException">The reply was not in the documented <c>XREAD</c> shape.</exception>
    internal async ValueTask<StreamSlice[]> FetchManyAsync(
        StreamPosition[] positions,
        int countPerStream,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(positions);
        ct.ThrowIfCancellationRequested();

        if (positions.Length != this.keys.Length)
        {
            throw new ArgumentException(
                $"This fetch covers {this.keys.Length} stream keys but was handed {positions.Length} positions; " +
                "they must be index-aligned.",
                nameof(positions));
        }

        if (countPerStream >= 1 && countPerStream != this.count)
        {
            this.count = countPerStream;
            this.args[CountSlot] = countPerStream;
        }

        for (var i = 0; i < positions.Length; i++)
        {
            // One small box per stream per read — the same cost the single-key form pays, and the
            // only allocation a steady read makes.
            this.args[this.cursorSlot + i] = positions[i].Position;
        }

        var reply = await this.reader.ExecuteXReadAsync(this.args, ct).ConfigureAwait(false);

        // The block just ended, so this is the boundary the shutdown path waits for.
        ct.ThrowIfCancellationRequested();

        return ParseAll(reply, this.keys, this.keyBytes);
    }

    /// <summary>
    /// Parses an <c>XREAD</c> reply into the batch shape the rest of the pipeline consumes.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The reply is <c>[[streamKey, [[id, [field, value, …]], …]], …]</c> in RESP2, and the same
    /// content as a map — flattened <c>streamKey, entries, streamKey, entries…</c> — in RESP3. Which
    /// one arrives depends on the protocol the connection negotiated, so the layout is detected from
    /// the reply's <em>shape</em> rather than from a protocol flag: a stream key is never an array,
    /// so an element that is a two-item array is a RESP2 <c>[key, entries]</c> pair and an element
    /// that is not is a RESP3 map key whose value is the next element. Both layouts are handled by
    /// one walk.
    /// </para>
    /// <para>
    /// A nil reply — the block expired with nothing to return — is the ordinary idle case and comes
    /// back as an empty batch, not an error.
    /// </para>
    /// <para>
    /// Anything that is not one of those shapes throws rather than being skipped. A silently
    /// mis-parsed reply would return an empty batch while the entries are still sitting in Redis
    /// behind an unmoved cursor, and the read loop would spin on it at full speed forever; failing
    /// loudly is the only safe response.
    /// </para>
    /// </remarks>
    /// <param name="reply">The raw reply from <c>XREAD</c>.</param>
    /// <param name="expectedKey">
    /// The stream key that was requested, used only to pick the right stream out of a multi-stream
    /// reply. A single-stream reply is accepted as-is, since it can only be the stream that was
    /// asked for.
    /// </param>
    /// <exception cref="StreamTransportException">The reply was not in the documented shape.</exception>
    internal static StreamEntryBatch Parse(RedisResult? reply, ReadOnlySpan<byte> expectedKey)
    {
        if (reply is null || reply.IsNull)
        {
            // The ordinary idle path: BLOCK expired with no entries.
            return StreamEntryBatch.Empty;
        }

        var length = reply.Length;

        if (length <= 0)
        {
            // Length is -1 for a non-array reply and 0 for an empty one. Neither carries entries;
            // neither is worth failing over, because Redis sends an empty array on some versions
            // where older ones send nil.
            return StreamEntryBatch.Empty;
        }

        RedisResult? matched = null;
        RedisResult? firstStream = null;
        var streams = 0;
        var walk = new StreamWalk(reply, length);

        while (walk.TryNext(out var streamKey, out var streamEntries))
        {
            streams++;
            firstStream ??= streamEntries;

            if (matched is null && Matches(streamKey, expectedKey))
            {
                matched = streamEntries;
            }
        }

        // One key is requested per fetch, so a single-stream reply is ours whatever its key text
        // says — a key prefix configured on the connection, for one, would rewrite it. The key
        // comparison only has to decide between streams, which is why it is not fatal on its own.
        var entries = matched ?? (streams == 1 ? firstStream : null);

        if (entries is null)
        {
            throw Malformed($"the reply held {streams} streams, none of them the one that was requested");
        }

        return ParseEntries(entries);
    }

    /// <summary>
    /// Parses a <b>multi-stream</b> <c>XREAD</c> reply into one slice per stream that had entries.
    /// </summary>
    /// <remarks>
    /// The same walk <see cref="Parse"/> uses — RESP2 pairs and the RESP3 map are told apart by
    /// shape, in one place, so the two forms of the reply cannot drift apart. Each reply key is
    /// matched to a requested key on raw bytes, and the slice carries the <em>requested</em>
    /// <see cref="RedisKey"/> so the read loop's slot lookup compares like with like whatever the
    /// connection's key prefix did to the text on the wire.
    /// </remarks>
    /// <param name="reply">The raw reply from <c>XREAD</c>.</param>
    /// <param name="keys">The keys that were requested, in request order.</param>
    /// <param name="keyBytes">The same keys as raw UTF-8, index-aligned with <paramref name="keys"/>.</param>
    /// <exception cref="StreamTransportException">The reply was not in the documented shape.</exception>
    internal static StreamSlice[] ParseAll(RedisResult? reply, RedisKey[] keys, byte[][] keyBytes)
    {
        if (reply is null || reply.IsNull)
        {
            // The ordinary idle path: BLOCK expired with no entries on any stream.
            return [];
        }

        var length = reply.Length;

        if (length <= 0)
        {
            return [];
        }

        // At most one stream per reply element (RESP2); the RESP3 map uses two elements per stream,
        // so this over-allocates by a factor of two there and is trimmed below.
        var slices = new StreamSlice[length];
        var found = 0;
        var hint = 0;
        var walk = new StreamWalk(reply, length);

        // The slot of the one unmatched stream, if the reply holds exactly one. See the note where it
        // is resolved, below.
        var unmatched = -1;
        var unmatchedCount = 0;

        while (walk.TryNext(out var streamKey, out var streamEntries))
        {
            var batch = ParseEntries(streamEntries);

            if (batch.IsEmpty)
            {
                continue;
            }

            var slot = SlotOf(streamKey, keyBytes, hint);

            if (slot >= 0)
            {
                hint = slot + 1 == keys.Length ? 0 : slot + 1;
                slices[found++] = new StreamSlice(keys[slot], batch.Entries, batch.Count);
                continue;
            }

            // A key that was not requested. Never expected; carried through with the key as it
            // arrived so the read loop can warn about it rather than silently mis-attributing it.
            unmatched = found;
            unmatchedCount++;
            slices[found++] = new StreamSlice((byte[]?)(RedisValue)streamKey ?? [], batch.Entries, batch.Count);
        }

        // The single-key rescue, and the reason it is HERE rather than inside SlotOf. A single-stream
        // reply to a single-key read can only be the stream that was asked for whatever the key text
        // says — a key prefix configured on the connection would rewrite it — which is the same
        // judgement Parse makes. SlotOf made that call without knowing how many streams the reply
        // carried, so a reply holding two streams, one of them a key this worker never asked for,
        // recorded the foreign stream's entries (and its last id) as partition 0's position: a silent
        // skip of everything between. Deciding it once the walk is done is the only place the stream
        // count is known.
        if (keyBytes.Length == 1 && found == 1 && unmatchedCount == 1)
        {
            slices[unmatched] = new StreamSlice(keys[0], slices[unmatched].Entries, slices[unmatched].Count);
        }

        if (found == length)
        {
            return slices;
        }

        return found == 0 ? [] : slices.AsSpan(0, found).ToArray();
    }

    /// <summary>Finds the requested key a reply's stream key matches, starting from <paramref name="hint"/>.</summary>
    private static int SlotOf(RedisResult streamKey, byte[][] keyBytes, int hint)
    {
        ReadOnlyMemory<byte> raw = (RedisValue)streamKey;
        var span = raw.Span;

        for (var i = hint; i < keyBytes.Length; i++)
        {
            if (span.SequenceEqual(keyBytes[i]))
            {
                return i;
            }
        }

        for (var i = 0; i < hint; i++)
        {
            if (span.SequenceEqual(keyBytes[i]))
            {
                return i;
            }
        }

        // Unmatched. The "one key was requested, so slot 0 it is" shortcut used to live here; it now
        // lives in ParseAll, which is the only place that can tell a single-stream reply from a
        // multi-stream one. See the note there.
        return -1;
    }

    /// <summary>
    /// Walks an <c>XREAD</c> reply's streams, handling the RESP2 <c>[key, entries]</c> pairs and the
    /// flattened RESP3 map with one piece of code.
    /// </summary>
    /// <remarks>
    /// The layout is detected from the reply's <em>shape</em> rather than a protocol flag: a stream
    /// key is never an array, so an element that is a two-item array is a RESP2 pair and one that is
    /// not is a RESP3 map key whose value is the next element. This struct exists so the
    /// single-stream and multi-stream parses share that judgement instead of each making it.
    /// </remarks>
    private struct StreamWalk(RedisResult reply, int length)
    {
        private int index;

        /// <summary>Moves to the next stream in the reply.</summary>
        /// <param name="streamKey">The stream's key, as it arrived.</param>
        /// <param name="streamEntries">The stream's entries array.</param>
        /// <returns><see langword="false"/> once the reply is exhausted.</returns>
        /// <exception cref="StreamTransportException">The reply was not in the documented shape.</exception>
        internal bool TryNext(
            [NotNullWhen(true)] out RedisResult? streamKey,
            [NotNullWhen(true)] out RedisResult? streamEntries)
        {
            if (this.index >= length)
            {
                streamKey = null;
                streamEntries = null;
                return false;
            }

            var item = reply[this.index];

            if (IsArray(item))
            {
                // RESP2: this element is the [key, entries] pair itself.
                if (item.Length != 2)
                {
                    throw Malformed(
                        $"a stream element held {item.Length} items where '[streamKey, entries]' was expected");
                }

                streamKey = item[0];
                streamEntries = item[1];
                this.index++;
                return true;
            }

            // RESP3 map: this element is the key and the next one is its entries.
            if (this.index + 1 >= length)
            {
                throw Malformed("a stream key arrived with no entries element after it");
            }

            streamKey = item;
            streamEntries = reply[this.index + 1];
            this.index += 2;
            return true;
        }
    }

    /// <summary>Parses one stream's entries array: <c>[[id, [field, value, …]], …]</c>.</summary>
    /// <exception cref="StreamTransportException">The entries were not in the documented shape.</exception>
    private static StreamEntryBatch ParseEntries(RedisResult entries)
    {
        if (entries.IsNull)
        {
            return StreamEntryBatch.Empty;
        }

        var length = entries.Length;

        if (length <= 0)
        {
            return StreamEntryBatch.Empty;
        }

        // One array per fetch, and one field array per entry — exactly what StackExchange.Redis
        // allocates for a non-blocking StreamReadAsync, so the two read modes cost the same.
        var parsed = new StreamEntry[length];
        var count = 0;

        for (var i = 0; i < length; i++)
        {
            var entry = entries[i];

            if (entry.IsNull)
            {
                // A nil entry means the id was trimmed or deleted between the read and the reply.
                // It carries nothing to hand a handler, so it is dropped — but the cursor still
                // advances past the entries that did arrive, so the read cannot repeat.
                continue;
            }

            if (entry.Length != 2)
            {
                throw Malformed($"an entry held {entry.Length} items where '[id, fields]' was expected");
            }

            var id = (RedisValue)entry[0];

            if (id.IsNull)
            {
                throw Malformed("an entry arrived with no id, so its position could not be recorded");
            }

            parsed[count++] = new StreamEntry(id, ParseFields(entry[1]));
        }

        if (count == 0)
        {
            // Every entry in a non-empty reply was nil. The cursor cannot advance, so returning an
            // empty batch would spin the read loop against the same unchanged cursor at full speed.
            throw Malformed($"all {length} entries in the reply were nil, leaving the read cursor with nowhere to advance to");
        }

        return new StreamEntryBatch(parsed, count);
    }

    /// <summary>Parses one entry's flat <c>[field, value, field, value, …]</c> array.</summary>
    /// <exception cref="StreamTransportException">The field list had an odd length.</exception>
    private static NameValueEntry[] ParseFields(RedisResult fields)
    {
        if (fields.IsNull)
        {
            return [];
        }

        var length = fields.Length;

        if (length <= 0)
        {
            return [];
        }

        if ((length & 1) != 0)
        {
            throw Malformed($"an entry held {length} field items, which is not an even field/value list");
        }

        var pairs = new NameValueEntry[length / 2];

        for (var i = 0; i < pairs.Length; i++)
        {
            var at = i * 2;
            pairs[i] = new NameValueEntry((RedisValue)fields[at], (RedisValue)fields[at + 1]);
        }

        return pairs;
    }

    /// <summary>
    /// Whether a result is an array. <see cref="RedisResult.Length"/> is <c>-1</c> for anything
    /// else, which is what separates a RESP2 <c>[key, entries]</c> pair from a RESP3 map key.
    /// </summary>
    private static bool IsArray(RedisResult result) => !result.IsNull && result.Length >= 0;

    /// <summary>
    /// Compares a reply's stream key against the requested one on raw bytes, so no string is
    /// materialised on the read path.
    /// </summary>
    private static bool Matches(RedisResult streamKey, ReadOnlySpan<byte> expectedKey)
    {
        if (expectedKey.Length == 0)
        {
            return false;
        }

        ReadOnlyMemory<byte> raw = (RedisValue)streamKey;

        return raw.Span.SequenceEqual(expectedKey);
    }

    private static StreamTransportException Malformed(string detail) =>
        new($"Streams: the XREAD reply was not in the expected shape — {detail}. " +
            "Expected '[[streamKey, [[id, [field, value, ...]], ...]], ...]' (RESP2) or the equivalent map (RESP3). " +
            "Refusing to guess, because a mis-parsed reply would leave the read cursor unmoved and spin the reader.");
}
