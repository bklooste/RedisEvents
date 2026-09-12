using System.Collections.ObjectModel;
using RedisEvents.Wire;

namespace RedisEvents.Positions;

/// <summary>
/// A position store that remembers nothing — the implementation behind <c>Persist = None</c>.
/// </summary>
/// <remarks>
/// Used by live tails (which pair it with <c>StartFrom = Now</c> and only ever want entries added
/// after they connected), ephemeral consumers, and tests that would otherwise leave position hashes
/// behind in Redis. Every method is a no-op and no Redis command is ever issued, so an idle or busy
/// consumer running this store costs nothing at all in position traffic.
/// </remarks>
internal sealed class NullPositionStore : IPositionStore
{
    /// <summary>The shared instance. The type holds no state, so one is enough.</summary>
    public static readonly NullPositionStore Instance = new();

    private static readonly ValueTask<IReadOnlyDictionary<int, StreamId>> Empty =
        ValueTask.FromResult<IReadOnlyDictionary<int, StreamId>>(ReadOnlyDictionary<int, StreamId>.Empty);

    /// <summary>
    /// Always returns no stored positions, so every partition resolves from the configured
    /// <c>StartFrom</c> / <c>StartFromWhenMissing</c> fallback.
    /// </summary>
    /// <param name="topic">Ignored.</param>
    /// <param name="consumer">Ignored.</param>
    /// <param name="ct">Ignored.</param>
    /// <returns>An empty dictionary.</returns>
    public ValueTask<IReadOnlyDictionary<int, StreamId>> LoadAsync(string topic, string consumer, CancellationToken ct)
        => Empty;

    /// <summary>Discards the positions.</summary>
    /// <param name="topic">Ignored.</param>
    /// <param name="consumer">Ignored.</param>
    /// <param name="positions">Ignored.</param>
    /// <param name="ct">Ignored.</param>
    /// <returns>A completed task.</returns>
    public ValueTask SaveAsync(
        string topic,
        string consumer,
        ReadOnlySpan<(int Partition, StreamId Id)> positions,
        CancellationToken ct)
        => ValueTask.CompletedTask;

    /// <summary>Does nothing: there is no stored position to move.</summary>
    /// <param name="topic">Ignored.</param>
    /// <param name="consumer">Ignored.</param>
    /// <param name="to">Ignored.</param>
    /// <param name="partition">Ignored.</param>
    /// <param name="ct">Ignored.</param>
    /// <returns>A completed task.</returns>
    public ValueTask ResetAsync(string topic, string consumer, StreamId to, int? partition, CancellationToken ct)
        => ValueTask.CompletedTask;
}
