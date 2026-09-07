using System.Collections.ObjectModel;
using System.Globalization;
using Microsoft.Extensions.Logging;
using Orange.Lib.Streams.Admin;
using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Positions;

/// <summary>
/// Moves a live reader's cursor to a new id, for the reset-marker protocol.
/// </summary>
/// <remarks>
/// A delegate because the cursor lives wherever the fetch seam put it: <c>PollFetch</c> owns a
/// <c>RedisValue</c> cursor of its own (<c>PollFetch.SeekTo</c> binds straight to this), whereas the
/// co-located read loop owns a <c>StreamPosition</c> array and needs no delegate at all. Nothing on
/// the path is an interface, and this is not one either.
/// </remarks>
/// <param name="after">The id the next read must start strictly after.</param>
internal delegate void ResetSeek(StreamId after);

/// <summary>
/// One pending reset: the position an operator told a partition to restart from, and when they
/// said so.
/// </summary>
/// <remarks>
/// The marker lives in the positions hash as <c>__reset:&lt;partition&gt;</c> — see
/// <see cref="StreamAdmin.ResetMarkerPrefix"/>, which owns the encoding. The prefix is not a number,
/// so <see cref="RedisPositionStore.Read"/> skips these fields and a marker can never be mistaken
/// for a position.
/// </remarks>
/// <param name="Partition">The partition the reset applies to.</param>
/// <param name="Target">The id to restart after — same exclusive sense as a stored position.</param>
/// <param name="IssuedUtc">When the reset was issued, or <see cref="DateTimeOffset.MinValue"/> when
/// the marker carried no time.</param>
internal readonly record struct ResetMarker(int Partition, StreamId Target, DateTimeOffset IssuedUtc);

/// <summary>
/// The hand-off between the flusher tick, which notices a reset marker in Redis, and a running read
/// loop, which is the only thing that can act on one.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists.</b> A reset issued while the consumer is running would race the flusher: the
/// admin call writes the target into the position hash and the flusher overwrites it a second later
/// with wherever the handler has got to, so the reset silently evaporates. The marker is what makes
/// it stick — the flusher sees it, hands it to the read loop through this object, and the read loop
/// rewinds its cursor.
/// </para>
/// <para>
/// <b>Three states per partition, and why the marker is cleared last.</b> A slot goes
/// <i>empty → pending</i> (the flusher saw a marker) → <i>taken</i> (a read loop rewound) → <i>empty</i>
/// (the flusher deleted the marker from Redis). Deleting only after a loop has actually taken it is
/// what makes a reset survive a partition whose reader is parked in a five-second blocking
/// <c>XREAD</c>, or not running at all: the marker simply stays in Redis until someone acts on it,
/// and the cold-start path (<see cref="ResetMarkers.TakeAllAsync"/>) applies it instead.
/// </para>
/// <para>
/// <b>Cost on the read path.</b> One volatile read of <see cref="HasPending"/> per read round. Only
/// when that is non-zero does anything scan slots, so the steady state is a single load. Publishing
/// and taking allocate one small object each, which happens once per reset — never per batch.
/// </para>
/// <para>
/// Slots hold an immutable object swapped by reference, so a reader can never see a half-written
/// target. That is deliberately cheaper to reason about than packing two <see cref="long"/>s and a
/// state word into interlocked fields for an event that happens once a month.
/// </para>
/// </remarks>
internal sealed class ResetSignal
{
    private readonly int[] partitions;
    private readonly Pending?[] slots;

    private int pending;

    /// <summary>
    /// Creates a signal covering exactly the partitions one instance owns.
    /// </summary>
    /// <param name="owned">The partitions this instance's workers read.</param>
    /// <remarks>
    /// Only owned partitions, deliberately. A flusher that consumed a marker for a partition another
    /// instance owns would clear it from Redis, and that instance's reader — the one that could
    /// actually rewind — would never see it.
    /// </remarks>
    internal ResetSignal(ReadOnlySpan<int> owned)
    {
        if (owned.IsEmpty)
        {
            throw new ArgumentException("A reset signal needs at least one owned partition.", nameof(owned));
        }

        this.partitions = owned.ToArray();
        this.slots = new Pending?[this.partitions.Length];
    }

    /// <summary>The partitions this signal covers, in the order the flusher polls them.</summary>
    internal ReadOnlySpan<int> Partitions => this.partitions;

    /// <summary>
    /// Whether any partition has a reset waiting to be taken. The read loop's per-round check.
    /// </summary>
    internal bool HasPending => Volatile.Read(ref this.pending) > 0;

    /// <summary>
    /// Publishes a marker the flusher read out of Redis. Idempotent: re-publishing the same marker —
    /// which happens on every tick until the flusher gets to delete the field — changes nothing.
    /// </summary>
    /// <param name="marker">The marker, as parsed from the hash.</param>
    /// <returns><see langword="true"/> when this call actually made a new reset pending, which is the
    /// signal to log it. <see langword="false"/> when the same marker was already pending or already
    /// taken.</returns>
    /// <remarks>Called from the flusher tick only, which is single-threaded.</remarks>
    internal bool Request(in ResetMarker marker)
    {
        var slot = this.IndexOf(marker.Partition);
        if (slot < 0)
        {
            // A marker for a partition we do not own. Left in Redis untouched for its owner.
            return false;
        }

        var current = Volatile.Read(ref this.slots[slot]);

        if (current is not null && current.Marker == marker)
        {
            return false;
        }

        // A brand-new marker replaces whatever was there, taken or not: the newest instruction wins.
        Volatile.Write(ref this.slots[slot], new Pending(marker, taken: false));

        if (current is null || current.Taken)
        {
            Interlocked.Increment(ref this.pending);
        }

        return true;
    }

    /// <summary>
    /// Takes a partition's pending reset, if it has one. Called by the read loop, immediately before
    /// it issues its next fetch.
    /// </summary>
    /// <param name="partition">The partition about to be read.</param>
    /// <param name="marker">The reset to apply, when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the caller must rewind.</returns>
    internal bool TryTake(int partition, out ResetMarker marker)
    {
        marker = default;

        var slot = this.IndexOf(partition);
        if (slot < 0)
        {
            return false;
        }

        while (true)
        {
            var current = Volatile.Read(ref this.slots[slot]);

            if (current is null || current.Taken)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref this.slots[slot], new Pending(current.Marker, taken: true), current) == current)
            {
                Interlocked.Decrement(ref this.pending);
                marker = current.Marker;
                return true;
            }
        }
    }

    /// <summary>
    /// Whether a partition's reset has been taken by its read loop and is now waiting for its marker
    /// field to be deleted.
    /// </summary>
    /// <param name="partition">The partition.</param>
    /// <returns><see langword="true"/> when the rewind has happened and the marker can be cleared.</returns>
    internal bool IsTaken(int partition)
    {
        var slot = this.IndexOf(partition);

        return slot >= 0 && Volatile.Read(ref this.slots[slot]) is { Taken: true };
    }

    /// <summary>
    /// Clears a slot whose reset a read loop has taken, so the flusher can delete the Redis field.
    /// </summary>
    /// <param name="partition">The partition.</param>
    /// <param name="marker">The marker that was applied, when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when a taken reset was cleared — i.e. its marker is now safe to
    /// delete. <see langword="false"/> when the slot is empty or still waiting to be taken.</returns>
    internal bool TryClearTaken(int partition, out ResetMarker marker)
    {
        marker = default;

        var slot = this.IndexOf(partition);
        if (slot < 0)
        {
            return false;
        }

        var current = Volatile.Read(ref this.slots[slot]);

        if (current is null || !current.Taken)
        {
            return false;
        }

        if (Interlocked.CompareExchange(ref this.slots[slot], null, current) != current)
        {
            // A newer marker landed in the slot between the read and the swap; it is not ours to
            // clear, and the field it belongs to must stay in Redis until that one is taken too.
            return false;
        }

        marker = current.Marker;
        return true;
    }

    /// <summary>Finds a partition's slot. Linear, over an array that is one worker's owned set.</summary>
    /// <param name="partition">The partition.</param>
    /// <returns>The slot index, or <c>-1</c> when the partition is not owned here.</returns>
    private int IndexOf(int partition)
    {
        var owned = this.partitions;

        for (var i = 0; i < owned.Length; i++)
        {
            if (owned[i] == partition)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>One slot's immutable contents; replaced wholesale rather than mutated.</summary>
    /// <param name="marker">The reset.</param>
    /// <param name="taken">Whether a read loop has already rewound to it.</param>
    private sealed class Pending(ResetMarker marker, bool taken)
    {
        /// <summary>The reset.</summary>
        internal ResetMarker Marker { get; } = marker;

        /// <summary>Whether a read loop has already rewound to <see cref="Marker"/>.</summary>
        internal bool Taken { get; } = taken;
    }
}

/// <summary>
/// Reading, applying and clearing reset markers — the consumer half of the protocol whose writing
/// half is <see cref="StreamAdmin.ResetPositionAsync(IConnectionMultiplexer, string, string, DateTimeOffset, int?, Config.TopicOptions, ILogger, CancellationToken)"/>.
/// </summary>
/// <remarks>
/// <para>
/// The encoding — field name and value — belongs to <see cref="StreamAdmin"/> and is used from here
/// rather than repeated, so the writer and the reader cannot drift apart.
/// </para>
/// <para>
/// <b>Live and cold, one mechanism.</b> A running consumer picks markers up on the flusher tick
/// (<c>PositionFlusher</c>) and rewinds its read loop; a consumer that is not running picks them up
/// at startup through <see cref="TakeAllAsync"/> and starts there instead. Both clear the marker
/// afterwards, and neither fights the flusher, because in the live case it <em>is</em> the flusher
/// doing the noticing.
/// </para>
/// <para>
/// <b>What a live reset does not do.</b> Batches already read and sitting in the channel are still
/// processed, and their positions still recorded, so the stored position can jump forward once more
/// before the replay overwrites it. Everything after the target is delivered again either way —
/// which is the guarantee — but a process killed inside that window resumes from the forward
/// position and loses the rest of the replay. That is why the runbook's safe path stays: scale to
/// zero, reset, scale up.
/// </para>
/// </remarks>
internal static class ResetMarkers
{
    /// <summary>The marker field for one partition, as a Redis value.</summary>
    /// <param name="partition">The partition.</param>
    /// <returns><c>__reset:&lt;partition&gt;</c>.</returns>
    internal static RedisValue Field(int partition) => StreamAdmin.ResetMarkerField(partition);

    /// <summary>Builds the marker fields for a set of partitions, once, for reuse on every tick.</summary>
    /// <param name="partitions">The partitions to poll.</param>
    /// <returns>The field names, index-aligned with <paramref name="partitions"/>.</returns>
    internal static RedisValue[] Fields(ReadOnlySpan<int> partitions)
    {
        var fields = new RedisValue[partitions.Length];

        for (var i = 0; i < partitions.Length; i++)
        {
            fields[i] = Field(partitions[i]);
        }

        return fields;
    }

    /// <summary>Parses one hash value into a marker for a known partition.</summary>
    /// <param name="partition">The partition the field belonged to.</param>
    /// <param name="value">The raw hash value; a null value simply means "no marker".</param>
    /// <param name="marker">The parsed marker, when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the value held a well-formed target id.</returns>
    internal static bool TryParse(int partition, in RedisValue value, out ResetMarker marker)
    {
        marker = default;

        var text = (string?)value;

        if (text is null || !StreamAdmin.TryParseResetMarker(text.AsSpan(), out var target, out var issued))
        {
            return false;
        }

        marker = new ResetMarker(partition, target, issued);
        return true;
    }

    /// <summary>
    /// Polls the marker fields for a set of partitions in one <c>HMGET</c>.
    /// </summary>
    /// <param name="db">The shared multiplexer's database.</param>
    /// <param name="key">The positions hash key.</param>
    /// <param name="partitions">The partitions polled, index-aligned with <paramref name="fields"/>.</param>
    /// <param name="fields">The field names from <see cref="Fields"/>, built once and reused.</param>
    /// <param name="into">Receives the markers found; must be at least as long as
    /// <paramref name="partitions"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>How many leading entries of <paramref name="into"/> are live.</returns>
    internal static async ValueTask<int> PollAsync(
        IDatabase db,
        RedisKey key,
        int[] partitions,
        RedisValue[] fields,
        ResetMarker[] into,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var values = await db.HashGetAsync(key, fields).ConfigureAwait(false);
        var limit = Math.Min(partitions.Length, values.Length);
        var found = 0;

        for (var i = 0; i < limit; i++)
        {
            if (TryParse(partitions[i], in values[i], out var marker))
            {
                into[found++] = marker;
            }
        }

        return found;
    }

    /// <summary>Deletes marker fields, which is how a reset is acknowledged.</summary>
    /// <param name="db">The shared multiplexer's database.</param>
    /// <param name="key">The positions hash key.</param>
    /// <param name="fields">The fields to delete; only the first <paramref name="count"/> are used.</param>
    /// <param name="count">How many leading entries of <paramref name="fields"/> to delete.</param>
    /// <returns>A task that completes when the delete has landed.</returns>
    internal static Task ClearAsync(IDatabase db, RedisKey key, RedisValue[] fields, int count)
    {
        if (count <= 0)
        {
            return Task.CompletedTask;
        }

        var slice = count == fields.Length ? fields : fields[..count];

        return db.HashDeleteAsync(key, slice);
    }

    /// <summary>
    /// The cold path: reads every marker on a consumer's position hash, clears them, and hands them
    /// back so the caller can start each partition at its target.
    /// </summary>
    /// <param name="db">The shared multiplexer's database.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="log">Logger; each applied marker is recorded at Warning.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Partition to marker, empty when there is nothing to apply.</returns>
    /// <remarks>
    /// Called once at consumer startup, before any read loop is started, so there is no flusher to
    /// race and no in-flight batch to reconcile — this is the branch the runbook calls safe. A
    /// process that dies between reading a marker and starting the loop leaves the marker cleared and
    /// the position already moved by the admin call, so the next start still resumes at the target.
    /// </remarks>
    internal static async ValueTask<IReadOnlyDictionary<int, ResetMarker>> TakeAllAsync(
        IDatabase db,
        string topic,
        string consumer,
        ILogger? log,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        ct.ThrowIfCancellationRequested();

        var key = StreamKeys.Positions(topic, consumer);
        var entries = await db.HashGetAllAsync(key).ConfigureAwait(false);
        var markers = Read(entries);

        if (markers.Count == 0)
        {
            return ReadOnlyDictionary<int, ResetMarker>.Empty;
        }

        var fields = new RedisValue[markers.Count];
        var at = 0;

        foreach (var pair in markers)
        {
            fields[at++] = Field(pair.Key);

            log?.LogWarning(
                "Streams: applying a reset marker at startup for topic {Topic} consumer {Consumer} partition {Partition}: reads start after {Target} (reset issued {IssuedUtc}). Entries after that id are processed again, so handlers must be idempotent.",
                topic,
                consumer,
                pair.Key,
                pair.Value.Target.Format(),
                pair.Value.IssuedUtc.ToString("O", CultureInfo.InvariantCulture));
        }

        await ClearAsync(db, key, fields, fields.Length).ConfigureAwait(false);

        return markers;
    }

    /// <summary>
    /// Overrides a resolved start position with a marker's target, when there is one for that
    /// partition.
    /// </summary>
    /// <param name="resolved">The position <see cref="StartPosition.Resolve(Config.ConsumerOptions, StreamId?)"/> produced.</param>
    /// <param name="markers">The markers from <see cref="TakeAllAsync"/>, or <see langword="null"/>.</param>
    /// <param name="partition">The partition being started.</param>
    /// <returns>The position the worker should start from.</returns>
    /// <remarks>
    /// A marker wins over every <c>StartFrom</c> mode, <see cref="Config.StartFrom.Now"/> included:
    /// an operator who asked for a replay means it, and the config was written long before they asked.
    /// </remarks>
    internal static StartPosition ApplyToStart(
        StartPosition resolved,
        IReadOnlyDictionary<int, ResetMarker>? markers,
        int partition)
        => markers is not null && markers.TryGetValue(partition, out var marker)
            ? StartPosition.Resume(marker.Target)
            : resolved;

    /// <summary>Picks the marker fields out of a whole-hash read.</summary>
    /// <param name="entries">The raw <c>HGETALL</c> result of the positions hash.</param>
    /// <returns>Partition to marker; position fields and anything unparseable are skipped.</returns>
    internal static Dictionary<int, ResetMarker> Read(HashEntry[] entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var markers = new Dictionary<int, ResetMarker>();

        foreach (var entry in entries)
        {
            var name = (string?)entry.Name;

            if (name is null || !TryParseField(name, out var partition))
            {
                continue;
            }

            if (TryParse(partition, entry.Value, out var marker))
            {
                markers[partition] = marker;
            }
        }

        return markers;
    }

    /// <summary>Parses a <c>__reset:&lt;partition&gt;</c> field name.</summary>
    /// <param name="name">The hash field name.</param>
    /// <param name="partition">The partition, when this returns <see langword="true"/>.</param>
    /// <returns><see langword="false"/> for every field that is not a reset marker.</returns>
    private static bool TryParseField(string name, out int partition)
    {
        partition = -1;

        var prefix = StreamAdmin.ResetMarkerPrefix;

        if (name.Length < prefix.Length + 2 ||
            !name.StartsWith(prefix, StringComparison.Ordinal) ||
            name[prefix.Length] != ':')
        {
            return false;
        }

        return int.TryParse(
            name.AsSpan(prefix.Length + 1),
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out partition);
    }
}
