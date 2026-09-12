using System.Globalization;
using RedisEvents.Ownership;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Admin;

/// <summary>
/// Read-only ownership queries: the answer to "which pod runs partitions 0 and 1, and which 2 and 3"
/// is one <c>HGETALL</c> away.
/// </summary>
public static partial class StreamAdmin
{
    /// <summary>Field on <c>m:{topic}</c> holding the recorded partition count.</summary>
    private const string PartitionsField = "partitions";

    /// <summary>
    /// Reads the live ownership map for one topic and consumer from <c>o:{topic}:&lt;consumer&gt;</c>.
    /// </summary>
    /// <remarks>
    /// Expired claims are already gone — per-field TTL does the expiry — so everything returned is
    /// live by construction, with no staleness filter to get wrong.
    /// <para>
    /// <see cref="OwnershipMap.Unowned"/> is populated, since a gap is visible from outside.
    /// <see cref="OwnershipMap.Contested"/> is always empty here: an overlap is only detectable by an
    /// instance comparing a claim against its own id, so it is reported by
    /// <see cref="OwnershipRegistry"/> and by the <c>streams.partitions.contested</c> gauge, not by
    /// this read.
    /// </para>
    /// </remarks>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="partitions">Total partitions, when the caller knows it. When zero or negative the
    /// count is read from <c>m:{topic}</c>, and failing that inferred from the highest claim — which
    /// can only under-report a gap, never invent one.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The ownership map as recorded in Redis.</returns>
    public static async Task<OwnershipMap> GetOwnershipAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        int partitions = 0,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(redis);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);

        ct.ThrowIfCancellationRequested();

        var db = redis.GetDatabase();
        var entries = await db.HashGetAllAsync(StreamKeys.Ownership(topic, consumer)).ConfigureAwait(false);
        var owners = OwnershipRegistry.ReadOwners(entries);

        var total = partitions;
        if (total < 1)
        {
            ct.ThrowIfCancellationRequested();
            total = await ReadPartitionCountAsync(db, topic).ConfigureAwait(false);
        }

        if (total < 1)
        {
            foreach (var partition in owners.Keys)
            {
                total = Math.Max(total, partition + 1);
            }
        }

        var unowned = new List<int>();
        for (var p = 0; p < total; p++)
        {
            if (!owners.ContainsKey(p))
            {
                unowned.Add(p);
            }
        }

        return new OwnershipMap(
            topic,
            consumer,
            total,
            owners,
            unowned,
            [],
            DateTimeOffset.UtcNow);
    }

    private static async Task<int> ReadPartitionCountAsync(IDatabase db, string topic)
    {
        var recorded = await db.HashGetAsync(StreamKeys.TopicMeta(topic), PartitionsField).ConfigureAwait(false);
        var text = (string?)recorded;

        return text is not null
            && int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var count)
            && count > 0
                ? count
                : 0;
    }
}
