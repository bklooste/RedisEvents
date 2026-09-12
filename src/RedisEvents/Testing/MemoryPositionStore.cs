using System.Collections.Concurrent;
using RedisEvents.Positions;
using RedisEvents.Wire;

namespace RedisEvents.Testing;

/// <summary>
/// An in-memory <see cref="IPositionStore"/>: real load/save/reset semantics, backed by a
/// <see cref="ConcurrentDictionary{TKey,TValue}"/> instead of a Redis hash.
/// </summary>
/// <remarks>
/// <para>
/// Register it in place of the default <see cref="RedisPositionStore"/> —
/// <c>builder.Services.AddSingleton&lt;IPositionStore, MemoryPositionStore&gt;()</c>, in either order
/// relative to <c>AddStream</c> — for a test that wants positions to actually advance and be
/// inspectable, without a Redis position hash to clean up between runs. This is not the same thing as
/// <c>Persist = PersistMode.None</c>: that discards positions entirely (nothing to assert on); this
/// keeps them, just not in Redis.
/// </para>
/// <para>
/// One instance holds positions for every topic/consumer that shares it, matching how one
/// <see cref="RedisPositionStore"/> instance is shared across a process — register it as a singleton,
/// not once per consumer, if more than one consumer should see the same store.
/// </para>
/// </remarks>
public sealed class MemoryPositionStore : IPositionStore
{
    private readonly ConcurrentDictionary<(string Topic, string Consumer, int Partition), StreamId> positions = new();

    /// <inheritdoc />
    public ValueTask<IReadOnlyDictionary<int, StreamId>> LoadAsync(string topic, string consumer, CancellationToken ct)
    {
        var result = new Dictionary<int, StreamId>();

        foreach (var (key, id) in this.positions)
        {
            if (key.Topic == topic && key.Consumer == consumer)
            {
                result[key.Partition] = id;
            }
        }

        return ValueTask.FromResult<IReadOnlyDictionary<int, StreamId>>(result);
    }

    /// <inheritdoc />
    public ValueTask SaveAsync(
        string topic,
        string consumer,
        ReadOnlySpan<(int Partition, StreamId Id)> positions,
        CancellationToken ct)
    {
        foreach (var (partition, id) in positions)
        {
            this.positions[(topic, consumer, partition)] = id;
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask ResetAsync(string topic, string consumer, StreamId to, int? partition, CancellationToken ct)
    {
        if (partition is { } single)
        {
            this.positions[(topic, consumer, single)] = to;
            return ValueTask.CompletedTask;
        }

        foreach (var key in this.positions.Keys)
        {
            if (key.Topic == topic && key.Consumer == consumer)
            {
                this.positions[key] = to;
            }
        }

        return ValueTask.CompletedTask;
    }
}
