using System.Diagnostics.CodeAnalysis;

namespace RedisEvents.Projections;

/// <summary>
/// One batch as an <see cref="EventProjector"/> dispatches it: scratch space where an
/// <see cref="IBatchProjection"/> buffers work between events, flushed and then closed before the
/// projector returns.
/// </summary>
/// <remarks>
/// <para>
/// Every event dispatched by one <see cref="EventProjector.HandleAsync"/> call carries the same instance
/// as <see cref="EventMeta.Batch"/>; the next call gets a new one, as does a redelivered batch. State is
/// keyed by an owner object, normally the projection instance itself (<c>this</c>), so several
/// projections share a batch without seeing each other's buffers.
/// </para>
/// <para>
/// <b>Closed when the call returns.</b> After the flush (or when the batch throws), the projector closes
/// the batch: its state is dropped and <see cref="GetOrAdd{TState}"/> and <see cref="TryGet{TState}"/>
/// throw. A projection that kept a reference and buffered into it later would be holding rows past the
/// point where the position advances, which is the bug this type exists to prevent; the throw makes that
/// loud instead of silent.
/// </para>
/// <para>
/// The constructor is public so a unit test can drive a batch projection directly: build one, pass it as
/// <see cref="EventMeta.Batch"/> to each <c>HandleAsync</c>, then call
/// <see cref="IBatchProjection.FlushAsync"/> with it. Thread-safe: one event's handler may buffer from
/// several tasks.
/// </para>
/// </remarks>
public sealed class ProjectionBatch
{
    private readonly Lock gate = new();
    private Dictionary<object, object>? state;
    private bool closed;

    /// <summary>Creates an open batch scope.</summary>
    /// <param name="topic">The topic the batch was read from.</param>
    /// <param name="partition">The partition the batch was read from.</param>
    public ProjectionBatch(string topic, int partition)
    {
        ArgumentNullException.ThrowIfNull(topic);

        this.Topic = topic;
        this.Partition = partition;
    }

    /// <summary>The topic the batch was read from.</summary>
    public string Topic { get; }

    /// <summary>The partition the batch was read from. Every event in one batch shares it.</summary>
    public int Partition { get; }

    /// <summary>Whether the projector has closed this batch (it has returned, or is about to).</summary>
    public bool IsClosed
    {
        get
        {
            lock (this.gate)
            {
                return this.closed;
            }
        }
    }

    /// <summary>
    /// Returns <paramref name="owner"/>'s state in this batch, creating it with <paramref name="create"/>
    /// the first time.
    /// </summary>
    /// <typeparam name="TState">The state's type, the same on every call for one owner.</typeparam>
    /// <param name="owner">The key, normally the projection instance.</param>
    /// <param name="create">Creates the empty state. Called at most once per owner per batch.</param>
    /// <returns>The owner's state.</returns>
    /// <exception cref="InvalidOperationException">The batch is closed, or the owner's state is another type.</exception>
    public TState GetOrAdd<TState>(object owner, Func<TState> create)
        where TState : class
    {
        ArgumentNullException.ThrowIfNull(owner);
        ArgumentNullException.ThrowIfNull(create);

        lock (this.gate)
        {
            this.ThrowIfClosed();

            this.state ??= new Dictionary<object, object>(ReferenceEqualityComparer.Instance);
            if (this.state.TryGetValue(owner, out var existing))
            {
                return Cast<TState>(owner, existing);
            }

            var created = create() ?? throw new InvalidOperationException(
                $"ProjectionBatch: the state factory for {owner.GetType().Name} returned null.");
            this.state[owner] = created;
            return created;
        }
    }

    /// <summary>Returns <paramref name="owner"/>'s state in this batch, if it has any.</summary>
    /// <typeparam name="TState">The state's type.</typeparam>
    /// <param name="owner">The key, normally the projection instance.</param>
    /// <param name="value">The state, when present.</param>
    /// <returns><see langword="true"/> when the owner buffered something in this batch.</returns>
    /// <exception cref="InvalidOperationException">The batch is closed, or the owner's state is another type.</exception>
    public bool TryGet<TState>(object owner, [NotNullWhen(true)] out TState? value)
        where TState : class
    {
        ArgumentNullException.ThrowIfNull(owner);

        lock (this.gate)
        {
            this.ThrowIfClosed();

            if (this.state is not null && this.state.TryGetValue(owner, out var existing))
            {
                value = Cast<TState>(owner, existing);
                return true;
            }

            value = null;
            return false;
        }
    }

    /// <summary>Drops the state and refuses further use. Called by the projector as it returns.</summary>
    internal void Close()
    {
        lock (this.gate)
        {
            this.closed = true;
            this.state = null;
        }
    }

    private static TState Cast<TState>(object owner, object existing)
        where TState : class
        => existing as TState ?? throw new InvalidOperationException(
            $"ProjectionBatch: {owner.GetType().Name} holds state of type {existing.GetType().Name} in this batch, not {typeof(TState).Name}.");

    private void ThrowIfClosed()
    {
        if (this.closed)
        {
            throw new InvalidOperationException(
                $"ProjectionBatch: the batch for topic={this.Topic} partition={this.Partition} is closed. " +
                "Its projector has returned, so the consumer position may already be past these events; " +
                "anything buffered now would never be flushed. Do not keep a ProjectionBatch past HandleAsync/FlushAsync.");
        }
    }
}
