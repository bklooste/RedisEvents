namespace RedisEvents.Consumer;

/// <summary>
/// The idle backoff schedule used by <c>ReadMode.Poll</c>: <c>0 → 1 → 2 → 4 → 8 → 16 → 32 → …</c>
/// milliseconds, capped at <c>MaxIdleDelayMs</c> (default 50).
/// </summary>
/// <remarks>
/// <para>
/// The schedule starts at <b>zero</b> deliberately. The first empty read yields rather than sleeps,
/// so a low-rate topic where a single message lands just after a read still gets sub-millisecond
/// pickup; only a stream that stays empty walks up the doubling ladder to the cap, where the steady
/// idle cost settles at ~20 commands per second per partition.
/// </para>
/// <para>
/// A non-empty read resets the state to zero and the reader loops straight back around — polling
/// never sleeps between full batches, so throughput under load is unaffected by the idle path.
/// </para>
/// <para>
/// Pure integer arithmetic on a caller-held <c>int</c>: no timers, no allocation, nothing to
/// dispose, and trivially unit-testable as a sequence (P1-24).
/// </para>
/// </remarks>
internal static class Backoff
{
    /// <summary>
    /// The delay to use after the next consecutive empty read, given the delay used after the
    /// previous one.
    /// </summary>
    /// <param name="current">
    /// The previous delay in milliseconds — <c>0</c> when the last read returned entries, or when
    /// no empty read has happened yet.
    /// </param>
    /// <param name="max">
    /// The cap in milliseconds (<c>ConsumerOptions.MaxIdleDelayMs</c>, default 50). Values of zero
    /// or less pin the schedule at zero, i.e. a pure yield loop.
    /// </param>
    /// <returns>The next delay: <c>1</c> after zero, otherwise double the current, never above <paramref name="max"/>.</returns>
    internal static int Next(int current, int max)
    {
        if (max <= 0)
        {
            return 0;
        }

        // Doubling from a floor of one, so the ladder is 0 → 1 → 2 → 4 → … rather than stuck at 0.
        // long arithmetic keeps the double from overflowing on an absurd caller-supplied current.
        var next = current <= 0 ? 1L : (long)current * 2L;

        return next >= max ? max : (int)next;
    }
}
