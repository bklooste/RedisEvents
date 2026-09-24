namespace RedisEvents.EventSourcing;

/// <summary>
/// Base class for an event-sourced aggregate: state is a fold over a sequence of domain events, and
/// every mutation happens through <see cref="Raise{TEvent}"/> so the same events that changed the
/// in-memory state are also the ones an <c>IEventRepository</c> persists.
/// </summary>
/// <remarks>
/// <para>
/// <b>Explicit dispatch, no <c>dynamic</c>.</b> A constructor registers one <see cref="Action{T}"/>
/// per event type via <see cref="On{TEvent}"/>; <see cref="Raise{TEvent}"/> and
/// <see cref="LoadFromHistory"/> look the handler up by the event's runtime type in a
/// <see cref="Dictionary{TKey,TValue}"/>. The reference CQRS implementation this package replaces
/// dispatches with <c>((dynamic)this).Apply((dynamic)@event)</c>: when no matching <c>Apply</c>
/// overload exists, C#'s dynamic binder silently does nothing — a typo in an event class name, or a
/// forgotten handler after adding a new event, drops the event on the floor with no error, and the
/// aggregate's state quietly diverges from its own history. Here, both raising a new event and
/// replaying history throw <see cref="InvalidOperationException"/> the moment a handler is missing.
/// A loud, immediate failure during development is strictly better than state corruption discovered
/// later by an operator staring at a wrong balance.
/// </para>
/// <para>
/// No reflection and no <c>dynamic</c> are used anywhere in this type, so dispatch works unchanged
/// under trimming and Native AOT.
/// </para>
/// </remarks>
public abstract class AggregateRoot
{
    readonly Dictionary<Type, Action<object>> handlers = [];
    readonly List<object> uncommitted = [];
    string? boundId;

    /// <summary>
    /// The aggregate's identity within its <see cref="AggregateName"/> family. Together they form the
    /// key of the aggregate's event stream.
    /// </summary>
    /// <remarks>
    /// <para>
    /// By default this is the id the aggregate was bound to with <see cref="BindId"/> — which
    /// <see cref="IEventRepository.LoadAsync{TAggregate}"/> and
    /// <see cref="EventRepositoryExtensions.LoadOrCreateAsync{TAggregate}"/> do for you — and empty until
    /// then. The id is the address of the aggregate's stream, not a fact about the aggregate, so its
    /// events need not repeat it: every event on <c>{AggregateName}:{Id}</c> belongs to that aggregate by
    /// construction.
    /// </para>
    /// <para>
    /// Override it only for an aggregate that carries its own id in its state — typically set from its
    /// creation event, as the Inventory sample does. An override wins over the bound id.
    /// </para>
    /// </remarks>
    public virtual string Id => boundId ?? string.Empty;

    /// <summary>
    /// Binds this instance to the stream it was loaded from, or is about to be created on.
    /// </summary>
    /// <param name="id">The aggregate's id within its <see cref="AggregateName"/> family.</param>
    /// <remarks>
    /// Loading through the repository already does this; call it yourself only for an instance you
    /// construct directly, such as in a unit test. Binding again to the same id is a no-op.
    /// </remarks>
    /// <exception cref="ArgumentException"><paramref name="id"/> is null, empty or whitespace.</exception>
    /// <exception cref="InvalidOperationException">
    /// The instance is already bound to a different id. An aggregate belongs to exactly one stream;
    /// rebinding it would save one aggregate's decisions onto another's history.
    /// </exception>
    public void BindId(string id)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        if (boundId is null)
        {
            boundId = id;
            return;
        }

        if (!string.Equals(boundId, id, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"{AggregateName} '{boundId}' cannot be rebound to '{id}'.");
        }
    }

    /// <summary>
    /// The stable name of this aggregate family, used to build its event stream's key.
    /// </summary>
    /// <remarks>
    /// Deliberately not <c>GetType().Name</c>: a pure rename of the aggregate class (say
    /// <c>InventoryItem</c> to <c>InventoryLine</c>) would then change the computed key, and the
    /// repository would look for a stream that has never had anything written to it under that name.
    /// The aggregate's old history is not lost — it is simply unreachable, which for an aggregate
    /// whose whole purpose is its history is the same thing. An explicit, hand-chosen name survives
    /// any refactor of the class that carries it.
    /// </remarks>
    public abstract string AggregateName { get; }

    /// <summary>
    /// The number of events this aggregate has committed so far. Starts at <c>0</c> for a brand-new
    /// aggregate; advances only via <see cref="MarkChangesAsCommitted"/> or <see cref="LoadFromHistory"/>.
    /// </summary>
    public int Version { get; private set; }

    /// <summary>
    /// Registers how to apply <typeparamref name="TEvent"/> to this aggregate's state. Called from a
    /// derived class's constructor, once per event type it knows about.
    /// </summary>
    /// <typeparam name="TEvent">The event type the handler applies.</typeparam>
    /// <param name="apply">
    /// Mutates the aggregate's private fields to reflect the event. Must not itself raise further
    /// events.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// A handler for <typeparamref name="TEvent"/> is already registered on this instance. Aggregate
    /// constructors call <see cref="On{TEvent}"/> exactly once per event type, so a second call means
    /// a copy-paste mistake in the constructor, not a legitimate override.
    /// </exception>
    protected void On<TEvent>(Action<TEvent> apply) where TEvent : class
    {
        if (!handlers.TryAdd(typeof(TEvent), e => apply((TEvent)e)))
        {
            throw new InvalidOperationException(
                $"{GetType().Name} already has a handler registered for event type {typeof(TEvent).Name}. " +
                $"Call On<{typeof(TEvent).Name}>(...) only once, in the constructor.");
        }
    }

    /// <summary>
    /// Applies <paramref name="event"/> to this aggregate's state using its registered handler, then
    /// records it as an uncommitted change.
    /// </summary>
    /// <typeparam name="TEvent">The event's compile-time type.</typeparam>
    /// <param name="event">The event to apply and record.</param>
    /// <remarks>
    /// The handler runs before the event is added to <see cref="GetUncommittedChanges"/>: if applying
    /// the event throws, nothing is recorded as having happened, which keeps a partially-applied
    /// mutation from being persisted.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// No handler is registered for <typeparamref name="TEvent"/>. This is the loud replacement for
    /// the reference implementation's silent <c>dynamic</c> no-op described in the type's
    /// <c>remarks</c>: an aggregate must not accept an event it does not know how to apply.
    /// </exception>
    protected void Raise<TEvent>(TEvent @event) where TEvent : class
    {
        if (!handlers.TryGetValue(typeof(TEvent), out var handler))
        {
            throw new InvalidOperationException(
                $"{GetType().Name} has no handler registered for event type {typeof(TEvent).Name}. " +
                $"Call On<{typeof(TEvent).Name}>(...) in the constructor before raising it.");
        }

        handler(@event);
        uncommitted.Add(@event);
    }

    /// <summary>
    /// The events raised since this aggregate was loaded or last committed, in the order they were
    /// raised.
    /// </summary>
    /// <returns>
    /// A read-only view over the live uncommitted-changes list. It reflects subsequent
    /// <see cref="Raise{TEvent}"/> and <see cref="MarkChangesAsCommitted"/> calls rather than being a
    /// snapshot frozen at the time of the call.
    /// </returns>
    public IReadOnlyList<object> GetUncommittedChanges() => uncommitted;

    /// <summary>
    /// Advances <see cref="Version"/> by the number of uncommitted changes and clears them. Called by
    /// an <c>IEventRepository</c> after it has durably persisted <see cref="GetUncommittedChanges"/>.
    /// </summary>
    public void MarkChangesAsCommitted()
    {
        Version += uncommitted.Count;
        uncommitted.Clear();
    }

    /// <summary>
    /// Rebuilds this aggregate's state by replaying previously committed events, without treating any
    /// of them as new activity.
    /// </summary>
    /// <param name="history">
    /// The aggregate's committed events, oldest first, exactly as previously persisted.
    /// </param>
    /// <remarks>
    /// Unlike <see cref="Raise{TEvent}"/>, replayed events are never added to
    /// <see cref="GetUncommittedChanges"/> — they are history being restored, not commands being
    /// recorded. <see cref="Version"/> is set to <paramref name="history"/>'s count rather than
    /// incremented, so this method is meant to run once, against a freshly constructed aggregate.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// An event in <paramref name="history"/> has no registered handler. A gap here means the
    /// aggregate's own recorded history is not fully understood — for example a handler was removed,
    /// or an event type was renamed without a migration — and the resulting state would silently be
    /// wrong. Replay fails loudly instead, for the same reason <see cref="Raise{TEvent}"/> does.
    /// </exception>
    public void LoadFromHistory(IReadOnlyList<object> history)
    {
        foreach (var @event in history)
        {
            if (!handlers.TryGetValue(@event.GetType(), out var handler))
            {
                throw new InvalidOperationException(
                    $"{GetType().Name} has no handler registered for event type {@event.GetType().Name} " +
                    $"found in its history. Call On<{@event.GetType().Name}>(...) in the constructor.");
            }

            handler(@event);
        }

        Version = history.Count;
    }
}
