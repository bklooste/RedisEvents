using System.IO.Hashing;
using System.Runtime.CompilerServices;

namespace RedisEvents.Wire;

/// <summary>
/// Maps a message onto one of a topic's partitions.
/// Non-empty keys hash stably (xxHash3) so a key's ordering is preserved for its lifetime;
/// empty keys round-robin for spread with no ordering promise.
/// </summary>
internal static class PartitionRouter
{
    /// <summary>
    /// Stable partition for a partition key. Single-partition topics short-circuit before hashing,
    /// and power-of-two partition counts use a mask rather than a modulo.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int ForKey(ReadOnlySpan<byte> key, int partitions)
    {
        // The common case: one partition, no hashing at all.
        if (partitions <= 1)
        {
            return 0;
        }

        var hash = XxHash3.HashToUInt64(key);
        return Reduce(hash, partitions);
    }

    /// <summary>
    /// Next partition for a message with no partition key. <paramref name="counter"/> is advanced
    /// atomically, so callers may share one counter across threads. Wraps cleanly at uint overflow
    /// and never returns a negative index.
    /// </summary>
    internal static int RoundRobin(ref uint counter, int partitions)
    {
        if (partitions <= 1)
        {
            return 0;
        }

        // Unsigned increment wraps rather than overflowing, so the sequence is unbroken.
        var next = Interlocked.Increment(ref counter);
        return Reduce(next, partitions);
    }

    /// <summary>Folds an unsigned value into <c>[0, partitions)</c>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Reduce(ulong value, int partitions)
    {
        var count = (uint)partitions;

        // Power of two: the low bits are already uniform for xxHash3 and for a counter.
        if ((count & (count - 1)) == 0)
        {
            return (int)(value & (count - 1));
        }

        return (int)(value % count);
    }
}
