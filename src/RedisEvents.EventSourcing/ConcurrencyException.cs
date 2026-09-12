using RedisEvents.Errors;

namespace RedisEvents.EventSourcing;

/// <summary>
/// Thrown by <see cref="IEventRepository.SaveAsync"/> when the aggregate's stream is no longer at the
/// version the caller decided against: somebody else appended to it first, and nothing was written.
/// </summary>
/// <remarks>
/// <para>
/// <b>Nothing was written.</b> The append and the topic publishes ride in one <c>MULTI</c>/<c>EXEC</c>
/// guarded by a length check on the aggregate's stream, so a lost race applies neither half — the
/// aggregate's history and the projection feed cannot disagree, and the in-memory aggregate still has
/// its uncommitted changes. The fix is always the same three lines: reload, re-decide, save again.
/// </para>
/// <para>
/// <b>Why this is a plain <see cref="Exception"/> and not a
/// <see cref="DontIgnoreException"/>.</b> <see cref="DontIgnoreException"/> means <em>block this
/// partition and retry the identical batch forever with backoff</em>. Retrying an identical
/// <c>SaveAsync(aggregate, expectedVersion)</c> can never succeed after a concurrency loss: the
/// version has moved and only a reload fixes it, so the retry would fail again for exactly the same
/// reason, at every backoff step, indefinitely. Deriving from <see cref="DontIgnoreException"/> would
/// therefore turn a condition the caller can resolve in one round trip into a permanently wedged
/// partition. The same reasoning makes developer errors (an unregistered event type, for instance)
/// an <see cref="InvalidOperationException"/>: blocking fixes neither bugs nor stale state.
/// </para>
/// <para>
/// A service that <em>does</em> want a command that will not settle to block escalates on its own
/// terms, which keeps the decision — and the retry budget — where the domain knowledge is:
/// </para>
/// <code>
/// // after N reload-and-retry attempts, this command is stuck rather than merely contended
/// catch (ConcurrencyException) when (++attempts > 3)
/// {
///     throw new InventoryCommandStuckException(...);   // : DontIgnoreException
/// }
/// </code>
/// </remarks>
public sealed class ConcurrencyException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ConcurrencyException"/> class for a save that lost
    /// its version check.
    /// </summary>
    /// <param name="aggregateName">The aggregate family, i.e. <see cref="AggregateRoot.AggregateName"/>.</param>
    /// <param name="id">The aggregate's id, i.e. <see cref="AggregateRoot.Id"/>.</param>
    /// <param name="expectedVersion">The version the save required the aggregate to still be at.</param>
    public ConcurrencyException(string aggregateName, string id, long expectedVersion)
        : base($"'{aggregateName}' '{id}': expected version {expectedVersion}, but it has changed.")
    {
        this.AggregateName = aggregateName;
        this.Id = id;
        this.ExpectedVersion = expectedVersion;
    }

    /// <summary>The aggregate family whose save lost the race.</summary>
    public string AggregateName { get; }

    /// <summary>The id of the aggregate whose save lost the race.</summary>
    public string Id { get; }

    /// <summary>
    /// The version the save required, i.e. the number of events the caller believed the aggregate's
    /// stream held when it made its decision. The stream now holds some other number.
    /// </summary>
    public long ExpectedVersion { get; }
}
