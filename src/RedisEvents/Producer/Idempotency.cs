using StackExchange.Redis;
using RedisEvents.Config;
using RedisEvents.Errors;

namespace RedisEvents.Producer;

/// <summary>
/// Helpers for idempotent message processing. Since stream delivery is at-least-once,
/// handlers may see the same message multiple times. This class provides a deduplication
/// key for tracking which messages have been seen within a time window.
/// </summary>
public static class Idempotency
{
    /// <summary>
    /// Builds the deduplication key for a scope and id: <c>&lt;prefix&gt;dedupe:{scope}:&lt;id&gt;</c>,
    /// where <c>&lt;prefix&gt;</c> is <see cref="KeyNamespace.Prefix"/>, e.g. <c>dev:re:</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// R-19 decision. The braces are literal — a Redis Cluster hash tag around the <em>scope</em>,
    /// matching every other key the library writes (<c>s:{topic}:&lt;p&gt;</c>,
    /// <c>p:{topic}:&lt;consumer&gt;</c>, <c>{topic}:state:&lt;name&gt;</c> from
    /// <see cref="Outbox.StateKey"/>). Before R-19 the key was untagged, which had two consequences
    /// on a cluster: the marker landed on an arbitrary node relative to the topic it belongs to, and
    /// a handler could never take the marker and its state write in one <c>MULTI</c>/<c>EXEC</c>,
    /// because the two keys were in different slots. Tagging on the scope fixes both, and when the
    /// scope is the topic name the marker joins that topic's slot and can ride inside an outbox
    /// transaction.
    /// </para>
    /// <para>
    /// The cost is deliberate and worth naming: every dedupe marker for one scope now hashes to one
    /// slot, so a very high-rate scope concentrates its markers on a single node. That is the same
    /// trade the whole library already makes for a topic's streams and positions, and the escape
    /// hatch is the same — shard the scope name (<c>"bets-0"</c>, <c>"bets-1"</c>, …) if one node
    /// cannot hold the window.
    /// </para>
    /// </remarks>
    /// <param name="scope">A scope name; must not contain a brace, which would corrupt the hash tag.</param>
    /// <param name="id">The message id or idempotency key.</param>
    /// <returns>The tagged key.</returns>
    /// <exception cref="ArgumentException"><paramref name="scope"/> or <paramref name="id"/> is null, empty or whitespace.</exception>
    /// <exception cref="StreamConfigurationException"><paramref name="scope"/> contains a brace.</exception>
    public static RedisKey Key(string scope, string id)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope);
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        if (scope.AsSpan().IndexOfAny('{', '}') >= 0)
        {
            throw new StreamConfigurationException(
                $"Idempotency: scope '{scope}' contains a brace. Dedupe keys are hash-tagged as dedupe:{{scope}}:<id>, " +
                "so a brace in the scope yields a hash tag nobody intended and scatters the scope's markers across cluster slots. " +
                "Rename the scope.");
        }

        return $"{KeyNamespace.Prefix()}dedupe:{{{scope}}}:{id}";
    }

    /// <summary>
    /// Attempts to mark a message as seen for idempotency purposes.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Returns <see langword="true"/> if this is the first sighting of the id within the scope.
    /// Uses a Redis SET command with NX (set if not exists) and PX (expire in milliseconds)
    /// to track the id. One round trip to Redis.
    /// </para>
    /// <para>
    /// Usage: call this at the start of a message handler. If it returns <see langword="false"/>,
    /// the message has been seen before (within the TTL window) and the handler should skip processing.
    /// </para>
    /// <para>
    /// Example:
    /// <code>
    /// if (!await Idempotency.TryBeginAsync(db, "bet-placed", betId.ToString(), TimeSpan.FromHours(24), ct))
    /// {
    ///     return; // Already processed
    /// }
    /// // ... process the message
    /// </code>
    /// </para>
    /// <para>
    /// The key is <c>dedupe:{scope}:&lt;id&gt;</c> — hash-tagged on the scope, see <see cref="Key"/>.
    /// </para>
    /// <para>
    /// Optional — handlers that are naturally idempotent (last-write-wins state) should not pay
    /// for this overhead.
    /// </para>
    /// <para>
    /// <b>This is a marker, not a lock.</b> It says "someone has started this id", and it never
    /// clears on failure: a handler that claims the id and then throws leaves the marker standing
    /// for the rest of the TTL, and the redelivery is skipped. Where the work must complete,
    /// the marker belongs in the same transaction as the state it guards — see <see cref="Outbox"/>.
    /// </para>
    /// </remarks>
    /// <param name="db">The Redis database.</param>
    /// <param name="scope">A scope name (e.g., topic name or handler name) to avoid collisions between different message types.</param>
    /// <param name="id">The message id or idempotency key to deduplicate.</param>
    /// <param name="ttl">
    /// The time-to-live for the deduplication marker; typically above the replay window. Must be at
    /// least one millisecond: Redis rejects <c>PX 0</c>, so a shorter span would fail the command
    /// rather than mean "no expiry".
    /// </param>
    /// <param name="ct">
    /// Cancellation, observed before the command is issued. StackExchange.Redis takes no token on
    /// the command itself, so — exactly as in <see cref="Outbox"/> — once the SET is on the wire
    /// there is nothing left to cancel and this is not observed after that point.
    /// </param>
    /// <returns><see langword="true"/> if this is the first sighting of the id, <see langword="false"/> if it has been seen before.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="db"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="scope"/> or <paramref name="id"/> is null, empty or whitespace.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ttl"/> rounds to zero milliseconds or is negative.</exception>
    /// <exception cref="StreamConfigurationException"><paramref name="scope"/> contains a brace.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> was already cancelled.</exception>
    public static ValueTask<bool> TryBeginAsync(IDatabase db, string scope, string id, TimeSpan ttl, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(db);
        var key = Key(scope, id);

        // Redis rejects PX <= 0, and SE.Redis renders a TimeSpan as whole milliseconds — so any span
        // under a millisecond becomes PX 0 and the command fails at the server with a message that
        // says nothing about the caller's TimeSpan. Refuse it here instead.
        if (ttl.TotalMilliseconds < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ttl),
                ttl,
                "Idempotency: the dedupe marker's TTL must be at least one millisecond — Redis rejects PX 0, " +
                "and a marker with no expiry would grow the keyspace without bound. Pass the replay window, e.g. TimeSpan.FromHours(24).");
        }

        ct.ThrowIfCancellationRequested();

        // SET key 1 NX PX ttl_ms
        return new ValueTask<bool>(
            db.StringSetAsync(key, "1", ttl, When.NotExists, flags: CommandFlags.None));
    }
}
