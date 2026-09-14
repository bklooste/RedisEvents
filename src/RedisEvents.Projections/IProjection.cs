namespace RedisEvents.Projections;

/// <summary>
/// Handles one kind of domain event on the read side, updating whatever view <typeparamref name="TEvent"/>
/// contributes to.
/// </summary>
/// <remarks>
/// <para>
/// <b>No registration beyond implementing the interface.</b> A class may implement
/// <see cref="IProjection{TEvent}"/> more than once, for as many event types as it cares about —
/// <c>class InventoryDetailProjection : IProjection&lt;ItemCreated&gt;, IProjection&lt;ItemRenamed&gt;</c>
/// — and needs no companion registration code to be wired up. <see cref="EventTypeRegistry.Register{TEvent}"/>
/// (and <see cref="EventTypeRegistry.RegisterJson{TEvent}"/>) already captures, per event type, an
/// <c>is IProjection&lt;TEvent&gt;</c> check and the matching dispatch closure; <see cref="EventProjector"/>
/// simply asks the registry, via <see cref="EventTypeRegistry.TryBindProjection"/>, whether a given
/// projection instance binds to a given wire type. This costs nothing at runtime beyond a dictionary
/// lookup and an interface check the JIT (or AOT compiler) already knows how to specialise — no
/// reflection, and every generic instantiation is rooted by the <c>Register</c> call site, so
/// trimming and Native AOT stay safe.
/// </para>
/// <para>
/// A projection is invoked at least once per event and must be idempotent: core's delivery is
/// at-least-once, so a batch can be redelivered after a crash between a view being written and the
/// consumer's position being saved. Set-semantics writes (replace the whole view) are idempotent for
/// free; a counter or append should compare <see cref="EventMeta.Id"/> — Redis's own comparable,
/// monotonic, per-partition stream entry id, identical on every redelivery — against the id stored on
/// the view and skip when it is not greater. No aggregate version is needed for this: the topic's own
/// per-partition ordering already gives every event for one key a well-defined, gap-free sequence to
/// compare against.
/// </para>
/// </remarks>
/// <typeparam name="TEvent">The event type this projection handles.</typeparam>
public interface IProjection<in TEvent> where TEvent : class
{
    /// <summary>
    /// Applies <paramref name="event"/> to whatever view this projection maintains.
    /// </summary>
    /// <param name="event">The decoded event.</param>
    /// <param name="meta">The envelope facts — aggregate id, version, stream id and correlation id — that came with it.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when the view has been updated.</returns>
    ValueTask HandleAsync(TEvent @event, EventMeta meta, CancellationToken ct);
}
