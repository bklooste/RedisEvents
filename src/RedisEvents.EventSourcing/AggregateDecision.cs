namespace RedisEvents.EventSourcing;

/// <summary>
/// What <see cref="CachedAggregate{TAggregate}.RunAsync{T}"/>'s <c>apply</c> delegate decided: either a
/// final result to save and return, or a request to discard the instance it was handed and try again
/// against a freshly loaded one.
/// </summary>
/// <typeparam name="T">The command's result type.</typeparam>
/// <remarks>
/// <see cref="Redo"/> exists for a decision that finds the instance it was handed unusable for a reason
/// that is not contention — a batched command among others that raised events and then failed and must
/// be dropped before the rest are re-applied, or a no-op decision made against a cached copy that a
/// cheap freshness check found stale. It costs a reload like a lost <see cref="ConcurrencyException"/>
/// does, but is not counted against <c>maxAttempts</c>: nothing was contended, so it is not the kind of
/// retry that should ever run out.
/// </remarks>
public readonly struct AggregateDecision<T>
{
    private readonly bool redo;
    private readonly T? result;

    private AggregateDecision(T? result, bool redo)
    {
        this.result = result;
        this.redo = redo;
    }

    /// <summary>The decision is final: save whatever was raised (nothing, if it decided not to act) and return <paramref name="result"/>.</summary>
    public static AggregateDecision<T> Done(T result) => new(result, false);

    /// <summary>Discard the instance this decision was made against; reload and call <c>apply</c> again, uncounted.</summary>
    public static AggregateDecision<T> Redo() => new(default, true);

    internal bool IsRedo => this.redo;

    internal T Result => this.result!;
}
