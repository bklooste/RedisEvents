using RedisEvents.Config;
using StackExchange.Redis;

namespace RedisEvents.Wire;

/// <summary>
/// Shared internal helper for producing consistent trim arguments across producer and background trimmer.
/// Ensures both paths use the same logic for XADD MAXLEN and XTRIM MINID commands.
/// </summary>
internal static class TrimArgs
{
    /// <summary>
    /// Produces the argument array for XADD MAXLEN trimming.
    /// </summary>
    /// <remarks>
    /// Returns an array to insert into an XADD command after the key.
    /// - For <see cref="TrimMode.Approx"/>: returns ["MAXLEN", "~", maxLen]
    /// - For <see cref="TrimMode.Exact"/>: returns ["MAXLEN", maxLen]
    /// - For <see cref="TrimMode.None"/>: returns an empty array (no trimming)
    /// </remarks>
    /// <param name="trim">The trim mode.</param>
    /// <param name="maxLen">The maximum stream length.</param>
    /// <returns>The argument array for XADD MAXLEN, or empty if trim is None.</returns>
    internal static RedisValue[] GetMaxLenArgs(TrimMode trim, long maxLen)
    {
        return trim switch
        {
            TrimMode.None => [],
            TrimMode.Exact => ["MAXLEN", maxLen],
            TrimMode.Approx => ["MAXLEN", "~", maxLen],
            _ => throw new ArgumentOutOfRangeException(nameof(trim), trim, null),
        };
    }

    /// <summary>
    /// Produces the argument array for XTRIM MINID trimming.
    /// </summary>
    /// <remarks>
    /// Returns an array to insert into an XTRIM command after the key.
    /// - For <see cref="TrimMode.Approx"/>: returns ["MINID", "~", minId]
    /// - For <see cref="TrimMode.Exact"/>: returns ["MINID", minId]
    /// - For <see cref="TrimMode.None"/>: returns an empty array (no trimming)
    /// </remarks>
    /// <param name="trim">The trim mode.</param>
    /// <param name="minId">The minimum stream id to keep.</param>
    /// <returns>The argument array for XTRIM MINID, or empty if trim is None.</returns>
    internal static RedisValue[] GetMinIdArgs(TrimMode trim, StreamId minId)
    {
        return trim switch
        {
            TrimMode.None => [],
            TrimMode.Exact => ["MINID", minId.Format()],
            TrimMode.Approx => ["MINID", "~", minId.Format()],
            _ => throw new ArgumentOutOfRangeException(nameof(trim), trim, null),
        };
    }
}
