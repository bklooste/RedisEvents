namespace RedisEvents.EventSourcing;

/// <summary>
/// An optional read-side seam for projections: get, set, delete and list a view by id.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is a convenience, not a requirement.</b> A projection may write its view anywhere it
/// likes — Redis, Azure Tables, SQL, an in-memory cache — and the rest of this package never demands
/// an <see cref="IViewStore{TView}"/> exist. The seam exists so that the shipped
/// <see cref="RedisViewStore{TView}"/> and a service's own store are interchangeable behind one
/// interface in DI, and so either can be exercised in tests against
/// <see cref="InMemoryViewStore{TView}"/> instead of a real Redis or a real database.
/// </para>
/// <para>
/// <b>Views must be idempotent.</b> Core delivers batches at least once, so a projection can be asked
/// to handle the same event twice — for instance after a crash between "view written" and "position
/// saved". Writing through <see cref="SetAsync"/> is naturally idempotent: replacing a view with the
/// same value it already holds changes nothing observable. Anything accumulative — a running total, a
/// counter, an append to a list — is not automatically safe and needs its own guard: store the
/// event's <c>EventMeta.Id</c> on the view and skip the update when the incoming id is not greater
/// than the one already recorded. Both patterns are shown in the package README.
/// </para>
/// </remarks>
/// <typeparam name="TView">The view model type. Must be a reference type so <c>null</c> can mean "not found".</typeparam>
public interface IViewStore<TView>
    where TView : class
{
    /// <summary>Reads the view with the given id.</summary>
    /// <param name="id">The view's id — typically the partition key it was projected from.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The view, or <see langword="null"/> when no view exists for <paramref name="id"/>.</returns>
    ValueTask<TView?> GetAsync(string id, CancellationToken ct = default);

    /// <summary>
    /// Writes (creating or replacing) the view with the given id.
    /// </summary>
    /// <param name="id">The view's id — typically the partition key it was projected from.</param>
    /// <param name="view">The view to store.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <remarks>
    /// Set-semantics: writing the same <paramref name="id"/> again simply replaces the prior value,
    /// which is what makes this safe under at-least-once redelivery without any extra bookkeeping.
    /// </remarks>
    ValueTask SetAsync(string id, TView view, CancellationToken ct = default);

    /// <summary>
    /// Removes the view with the given id, if any.
    /// </summary>
    /// <param name="id">The view's id.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <remarks>Deleting a view that does not exist is not an error.</remarks>
    ValueTask DeleteAsync(string id, CancellationToken ct = default);

    /// <summary>
    /// Streams every view currently held by this store, in no particular order.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An asynchronous sequence of every stored view.</returns>
    IAsyncEnumerable<TView> ListAsync(CancellationToken ct = default);
}
