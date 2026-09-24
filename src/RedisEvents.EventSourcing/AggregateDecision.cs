namespace RedisEvents.EventSourcing;

/// <summary>
/// What a command's decision came to: a result to save and return, a refusal that must have changed
/// nothing, or a request to discard the instance it was made against and try again against a freshly
/// loaded one.
/// </summary>
/// <typeparam name="T">The command's result type.</typeparam>
/// <remarks>
/// <para>
/// Returned by the decision delegate of both <see cref="CachedAggregate{TAggregate}.RunAsync{T}"/> and
/// <see cref="EventRepositoryExtensions.ExecuteAsync{TAggregate,T}(IEventRepository,string,Func{TAggregate,AggregateDecision{T}},int,Func{int,ConcurrencyException,CancellationToken,Task}?,bool,RedisEvents.Producer.PublishOptions,CancellationToken)"/>.
/// </para>
/// <para>
/// <see cref="Refused"/> is <see cref="Done"/> with one extra promise checked: the decision raised no
/// events. A command must decide before it raises anything — a refusal that has already raised half a
/// change would, if saved, commit that half. Saying "this is a refusal" lets the runner catch that bug
/// in the aggregate loudly, instead of saving it or silently dropping it.
/// </para>
/// <para>
/// <see cref="Redo"/> exists for a decision that finds the instance it was handed unusable for a reason
/// that is not contention — a batched command among others that raised events and then failed and must
/// be dropped before the rest are re-applied, or a no-op decision made against a cached copy that a
/// cheap freshness check found stale. It costs a reload like a lost <see cref="ConcurrencyException"/>
/// does, but is not counted against <c>maxAttempts</c>: nothing was contended, so it is not the kind of
/// retry that should ever run out.
/// </para>
/// </remarks>
public readonly struct AggregateDecision<T>
{
    private readonly Kind kind;
    private readonly T? result;

    private AggregateDecision(T? result, Kind kind)
    {
        this.result = result;
        this.kind = kind;
    }

    private enum Kind : byte
    {
        Done,
        Refused,
        Redo,
    }

    /// <summary>The decision is final: save whatever was raised (nothing, if it decided not to act) and return <paramref name="result"/>.</summary>
    public static AggregateDecision<T> Done(T result) => new(result, Kind.Done);

    /// <summary>
    /// The command was refused: return <paramref name="result"/> — typically carrying the reason — and
    /// save nothing. The runner throws <see cref="InvalidOperationException"/> if the aggregate raised any
    /// event before refusing, because that is a bug in the aggregate, not an outcome.
    /// </summary>
    public static AggregateDecision<T> Refused(T result) => new(result, Kind.Refused);

    /// <summary>Discard the instance this decision was made against; reload and call <c>apply</c> again, uncounted.</summary>
    public static AggregateDecision<T> Redo() => new(default, Kind.Redo);

    internal bool IsRedo => this.kind == Kind.Redo;

    internal bool IsRefused => this.kind == Kind.Refused;

    internal T Result => this.result!;

    /// <summary>Throws when a refusal was made after the aggregate had already raised events.</summary>
    internal void GuardRefusal(AggregateRoot aggregate)
    {
        var raised = aggregate.GetUncommittedChanges().Count;
        if (this.kind == Kind.Refused && raised > 0)
        {
            throw new InvalidOperationException(
                $"{aggregate.AggregateName} '{aggregate.Id}' refused a command after already raising {raised} event(s). " +
                "A command must decide before it raises anything; nothing was saved.");
        }
    }
}
