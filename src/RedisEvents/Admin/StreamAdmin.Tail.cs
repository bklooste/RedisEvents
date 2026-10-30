using RedisEvents.Config;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Admin;

public static partial class StreamAdmin
{
    /// <summary>
    /// Reads each partition's tail (the id of the last entry ever added) for a topic, in one
    /// pipelined round trip.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Built for a catch-up gate: capture the tail when a trigger arrives, then wait until a
    /// consumer's position on every partition is at or past it. Once the position is <c>&gt;=</c>
    /// the captured tail, everything published before the capture has been read.
    /// </para>
    /// <para>
    /// The tail is <c>XINFO STREAM … last-generated-id</c>, not the newest surviving entry, so it is
    /// exact for a trimmed stream (including one trimmed to empty): the same read
    /// <c>StartFrom.Now</c> and the behind-tail health check already do. A partition stream that does
    /// not exist, or was never written to, reports <see cref="StreamId.Min"/> (<c>0-0</c>), which any
    /// position already satisfies.
    /// </para>
    /// <para>
    /// Keys honour <see cref="KeyNamespace.Prefix"/> and <see cref="TopicOptions.CoLocatePartitions"/>.
    /// The partition count is the caller's configured <see cref="TopicOptions.Partitions"/>, the same
    /// number its consumer reads; it is not looked up.
    /// </para>
    /// </remarks>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="topic">The topic name.</param>
    /// <param name="options">The topic's configured options: partition count and key layout.</param>
    /// <param name="ct">Cancellation token, observed before the round trip.</param>
    /// <returns>The tail per partition; index <c>p</c> is partition <c>p</c>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="TopicOptions.Partitions"/> is below one.</exception>
    public static async Task<IReadOnlyList<StreamId>> ReadTopicTailAsync(
        IConnectionMultiplexer redis,
        string topic,
        TopicOptions options,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(redis);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.Partitions, 1);
        ct.ThrowIfCancellationRequested();

        var db = redis.GetDatabase();
        var reads = new Task<StreamId>[options.Partitions];
        for (var p = 0; p < reads.Length; p++)
        {
            reads[p] = ReadTailAsync(db, StreamKeys.Stream(topic, p, options));
        }

        return await Task.WhenAll(reads).ConfigureAwait(false);
    }

    private static async Task<StreamId> ReadTailAsync(IDatabase db, RedisKey key)
    {
        try
        {
            var info = await db.StreamInfoAsync(key).ConfigureAwait(false);
            return StreamId.TryParse(((string?)info.LastGeneratedId).AsSpan(), out var last) ? last : StreamId.Min;
        }
        catch (RedisServerException ex) when (ex.Message.Contains("no such key", StringComparison.OrdinalIgnoreCase))
        {
            // Never written and never ensured: nothing has been published, so 0-0 is exact.
            return StreamId.Min;
        }
    }
}
