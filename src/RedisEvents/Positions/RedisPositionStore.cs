using System.Collections.ObjectModel;
using System.Globalization;
using RedisEvents.Ownership;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Positions;

/// <summary>
/// The default <see cref="IPositionStore"/>: one Redis hash per (topic, consumer), one field per
/// partition.
/// </summary>
/// <remarks>
/// <para>
/// A hash rather than a key per partition, because that shape makes both operations cheap: a flush
/// writes every dirty partition in one multi-field <c>HSET</c>, and startup — or the trim interlock
/// — reads every partition in one <c>HGETALL</c>. The key carries a Redis Cluster hash tag around
/// the topic name, so a worker's reads and its position write stay on one node.
/// </para>
/// <para>
/// Each value is <c>&lt;streamId&gt;|&lt;instanceId&gt;</c>. The instance id is what makes a second
/// writer visible: a flush that finds a different id in a partition it believes it owns asks the
/// ownership hash whether that id still holds a live claim, and stands the partition down only if
/// it does — a foreign id on its own is as likely to be this pod's own previous life or an
/// administrative rewind. Values without the separator are read as plain ids, so hand-edited and
/// legacy values still load.
/// </para>
/// <para>
/// The sibling hash <c>p:{topic}:&lt;consumer&gt;:meta</c> records, per partition, when the position
/// was last written and by whom. Nothing reads it — it exists so that an operator staring at a
/// consumer that appears stuck can tell "not moving" from "not running".
/// </para>
/// </remarks>
internal sealed class RedisPositionStore : IPositionStore
{
    /// <summary>Suffix of the meta field holding the last write time, ISO-8601 round-trip.</summary>
    public const string MetaUpdatedUtcSuffix = ":updatedUtc";

    /// <summary>Suffix of the meta field holding the instance that last wrote the position.</summary>
    public const string MetaInstanceIdSuffix = ":instanceId";

    /// <summary>
    /// The identity every administrative rewind is stamped with.
    /// </summary>
    /// <remarks>
    /// R-01. A reset is an operator overruling the consumer, not a second consumer competing with
    /// it, and the position flusher must be able to tell the two apart: before this, a reset issued
    /// from an admin CLI or API pod put a stranger's GUID in the field and the running consumer
    /// stood the partition down instead of rewinding. Fixed and well known, because the flusher on
    /// the other side of the reset has nothing else to recognise it by.
    /// </remarks>
    public static readonly Guid AdminInstanceId = new("6f72616e-6765-4164-6d69-6e5265736574");

    /// <summary><see cref="AdminInstanceId"/> in the <c>"D"</c> form the stored values carry.</summary>
    private static readonly string AdminInstanceIdText =
        AdminInstanceId.ToString("D", CultureInfo.InvariantCulture);

    /// <summary>Partition field names cached up to this count; beyond it the name is formatted.</summary>
    private const int CachedFields = 256;

    private static readonly RedisValue[] FieldNames = BuildFieldNames();

    private readonly IConnectionMultiplexer redis;
    private readonly string instanceIdText;

    /// <summary>
    /// Creates a store over an existing multiplexer.
    /// </summary>
    /// <param name="redis">The shared streams multiplexer — it must point at the persistent Redis
    /// that holds the streams themselves, never at a cache instance.</param>
    /// <param name="instanceId">The instance id stamped into every value this store writes.
    /// Defaults to the instance identity shared with the ownership registry — stable across a
    /// restart of the same pod, so a bounced consumer recognises its own history.</param>
    public RedisPositionStore(IConnectionMultiplexer redis, Guid? instanceId = null)
    {
        ArgumentNullException.ThrowIfNull(redis);

        this.redis = redis;
        this.InstanceId = instanceId ?? OwnershipRegistry.StableInstanceId;
        this.instanceIdText = this.InstanceId.ToString("D", CultureInfo.InvariantCulture);
    }

    /// <summary>The instance id written alongside every position.</summary>
    public Guid InstanceId { get; }

    /// <summary>
    /// A store for administrative writes: everything it writes is stamped
    /// <see cref="AdminInstanceId"/>, so a running consumer reads it as a reset rather than as a
    /// rival writer.
    /// </summary>
    /// <param name="redis">The streams multiplexer.</param>
    /// <returns>The admin-identity store.</returns>
    public static RedisPositionStore Admin(IConnectionMultiplexer redis) => new(redis, AdminInstanceId);

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<int, StreamId>> LoadAsync(
        string topic,
        string consumer,
        CancellationToken ct)
    {
        var records = await this.LoadRecordsAsync(topic, consumer, ct).ConfigureAwait(false);
        if (records.Count == 0)
        {
            return ReadOnlyDictionary<int, StreamId>.Empty;
        }

        var positions = new Dictionary<int, StreamId>(records.Count);
        foreach (var pair in records)
        {
            positions[pair.Key] = pair.Value.Id;
        }

        return positions;
    }

    /// <summary>
    /// Reads every recorded position together with the instance that wrote it, in one
    /// <c>HGETALL</c>. This is the form the second-writer check wants; <see cref="LoadAsync"/> is the
    /// same read with the attribution dropped.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Partition to stored record. Fields that are not a partition number, and values that
    /// are not a stream id — the reset marker among them — are skipped.</returns>
    public async ValueTask<IReadOnlyDictionary<int, PositionRecord>> LoadRecordsAsync(
        string topic,
        string consumer,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        ct.ThrowIfCancellationRequested();

        var entries = await this.redis.GetDatabase()
            .HashGetAllAsync(StreamKeys.Positions(topic, consumer))
            .ConfigureAwait(false);

        return Read(entries);
    }

    /// <inheritdoc />
    public ValueTask SaveAsync(
        string topic,
        string consumer,
        ReadOnlySpan<(int Partition, StreamId Id)> positions,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);

        // An idle consumer writes nothing at all.
        return positions.IsEmpty
            ? ValueTask.CompletedTask
            : this.WriteAsync(topic, consumer, positions, this.instanceIdText, ct);
    }

    /// <summary>Writes positions stamped with a caller-chosen identity.</summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="positions">The positions to write.</param>
    /// <param name="instanceIdText">The identity to stamp, already in <c>"D"</c> form.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The write.</returns>
    private ValueTask WriteAsync(
        string topic,
        string consumer,
        ReadOnlySpan<(int Partition, StreamId Id)> positions,
        string instanceIdText,
        CancellationToken ct)
    {
        if (positions.IsEmpty)
        {
            return ValueTask.CompletedTask;
        }

        ct.ThrowIfCancellationRequested();

        var values = new HashEntry[positions.Length];
        var meta = new HashEntry[positions.Length * 2];
        var now = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);

        for (var i = 0; i < positions.Length; i++)
        {
            var (partition, id) = positions[i];
            var field = Field(partition);

            values[i] = new HashEntry(field, PositionRecord.Format(id, instanceIdText));
            meta[i * 2] = new HashEntry(MetaField(partition, MetaUpdatedUtcSuffix), now);
            meta[(i * 2) + 1] = new HashEntry(MetaField(partition, MetaInstanceIdSuffix), instanceIdText);
        }

        return new ValueTask(this.WriteAsync(topic, consumer, values, meta));
    }

    /// <inheritdoc />
    /// <remarks>
    /// A reset is stamped <see cref="AdminInstanceId"/> whatever identity this store carries: it is
    /// an administrative rewind, and the consumer that reads the value back must recognise it as one
    /// rather than stand its partition down (R-01).
    /// </remarks>
    public async ValueTask ResetAsync(
        string topic,
        string consumer,
        StreamId to,
        int? partition,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        ct.ThrowIfCancellationRequested();

        if (partition is { } one)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(one);
            await this.WriteAsync(topic, consumer, new[] { (one, to) }.AsSpan(), AdminInstanceIdText, ct)
                .ConfigureAwait(false);
            return;
        }

        // No partition named: move every partition that currently has a recorded position. Partitions
        // with none are already going to resolve from the StartFrom fallback, so there is nothing to
        // move for them.
        var existing = await this.LoadRecordsAsync(topic, consumer, ct).ConfigureAwait(false);
        if (existing.Count == 0)
        {
            return;
        }

        var targets = new (int Partition, StreamId Id)[existing.Count];
        var at = 0;
        foreach (var pair in existing)
        {
            targets[at++] = (pair.Key, to);
        }

        await this.WriteAsync(topic, consumer, targets.AsSpan(), AdminInstanceIdText, ct).ConfigureAwait(false);
    }

    /// <summary>Parses a positions hash, skipping fields that are not a partition position.</summary>
    /// <param name="entries">The raw <c>HGETALL</c> result.</param>
    /// <returns>Partition to stored record.</returns>
    internal static Dictionary<int, PositionRecord> Read(HashEntry[] entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var positions = new Dictionary<int, PositionRecord>(entries.Length);
        foreach (var entry in entries)
        {
            var name = (string?)entry.Name;
            var value = (string?)entry.Value;
            if (name is null || value is null)
            {
                continue;
            }

            if (int.TryParse(name, NumberStyles.None, CultureInfo.InvariantCulture, out var partition) &&
                PositionRecord.TryParse(value, out var record))
            {
                positions[partition] = record;
            }
        }

        return positions;
    }

    /// <summary>The hash field a partition's position is stored under.</summary>
    internal static RedisValue Field(int partition)
        => (uint)partition < CachedFields
            ? FieldNames[partition]
            : partition.ToString(CultureInfo.InvariantCulture);

    private static RedisValue MetaField(int partition, string suffix)
        => string.Concat(partition.ToString(CultureInfo.InvariantCulture), suffix);

    private static RedisValue[] BuildFieldNames()
    {
        var names = new RedisValue[CachedFields];
        for (var i = 0; i < names.Length; i++)
        {
            names[i] = i.ToString(CultureInfo.InvariantCulture);
        }

        return names;
    }

    private async Task WriteAsync(string topic, string consumer, HashEntry[] values, HashEntry[] meta)
    {
        var db = this.redis.GetDatabase();

        // Both writes are dispatched before either is awaited, so the multiplexer pipelines them into
        // a single round trip. The positions write is the one that matters; the meta write is
        // diagnostics riding along for free.
        var positions = db.HashSetAsync(StreamKeys.Positions(topic, consumer), values);
        var diagnostics = db.HashSetAsync(StreamKeys.PositionsMeta(topic, consumer), meta);

        await positions.ConfigureAwait(false);
        await diagnostics.ConfigureAwait(false);
    }
}
