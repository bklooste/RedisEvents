using RedisEvents.Producer;
using RedisEvents.Projections;

namespace RedisEvents.EventSourcing;

/// <summary>
/// The command side: loads an aggregate by replaying its own stream, and saves the events it raised
/// with an optimistic version check.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two methods, and no options.</b> Everything a repository could be asked to decide is already
/// decided: the aggregate's stream is the source of truth, its version is that stream's length, a
/// save is one transaction that also publishes to the topic, "not found" is <see langword="null"/>,
/// and a lost version check is <see cref="ConcurrencyException"/>. There is nothing left for a
/// policy enum to select.
/// </para>
/// <para>
/// <b>The whole command-side loop.</b> Load, decide in the aggregate, save — and on contention, do
/// it again from the top, because the decision may come out differently against the new state:
/// </para>
/// <code>
/// var item = await repository.LoadAsync&lt;InventoryItem&gt;(id) ?? throw new KeyNotFoundException(id);
/// item.Rename("New name");
/// await repository.SaveAsync(item);   // expectedVersion defaults to item.Version
/// </code>
/// <para>
/// The interface exists so a service can test its command handlers against a fake, and so the
/// implementation can be swapped without touching a single aggregate — aggregates themselves never
/// see it.
/// </para>
/// </remarks>
public interface IEventRepository
{
    /// <summary>
    /// Rebuilds an aggregate from its complete event history.
    /// </summary>
    /// <typeparam name="TAggregate">
    /// The aggregate type. It needs a public parameterless constructor because loading <em>is</em>
    /// "construct an empty one, then replay": the constructor is where an aggregate registers its
    /// <c>On&lt;TEvent&gt;</c> handlers, and its <see cref="AggregateRoot.AggregateName"/> — which
    /// names the stream to read — must therefore be readable before any event has been applied.
    /// </typeparam>
    /// <param name="id">The aggregate's id, as <see cref="AggregateRoot.Id"/> reports it once loaded.</param>
    /// <param name="ct">Cancellation, observed between pages of history.</param>
    /// <returns>
    /// The aggregate with <see cref="AggregateRoot.Version"/> set to the number of events replayed and
    /// no uncommitted changes, or <see langword="null"/> when its stream holds no events at all. "No
    /// such aggregate" is not an exception: asking whether one exists is an ordinary thing to do.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// An entry in the aggregate's own history carries a wire type no <see cref="EventTypeRegistry"/>
    /// registration covers, or one the aggregate has no handler for. Either way the replayed state
    /// would silently be wrong, which is worse than failing.
    /// </exception>
    ValueTask<TAggregate?> LoadAsync<TAggregate>(string id, CancellationToken ct = default)
        where TAggregate : AggregateRoot, new();

    /// <summary>
    /// Appends the aggregate's uncommitted events to its stream and publishes them to the topic, in
    /// one transaction, if and only if the stream is still at <paramref name="expectedVersion"/>.
    /// </summary>
    /// <param name="aggregate">
    /// The aggregate to save. On success its changes are marked committed and its
    /// <see cref="AggregateRoot.Version"/> advances; on a <see cref="ConcurrencyException"/> it is
    /// left exactly as it was, uncommitted changes and all.
    /// </param>
    /// <param name="expectedVersion">
    /// The version the stream must still be at, defaulting to the aggregate's own
    /// <see cref="AggregateRoot.Version"/> — which is the version it was loaded at, and therefore the
    /// version its decisions were made against. Pass a value explicitly only to assert something
    /// stronger, such as <c>0</c> for "this must be a creation".
    /// </param>
    /// <param name="options">
    /// Correlation id and headers to stamp on every event's topic copy, which is what projections
    /// read; the aggregate's own stream keeps them only when the topic sets
    /// <c>StateMetadata</c>. The repository adds its own <c>es-version</c> and <c>es-id</c> headers
    /// alongside them.
    /// </param>
    /// <param name="ct">Cancellation, observed before the transaction is built.</param>
    /// <returns>
    /// The aggregate's new version — its stream's new length. An aggregate with nothing uncommitted
    /// returns its current version without touching Redis at all.
    /// </returns>
    /// <exception cref="ConcurrencyException">
    /// The stream is no longer at <paramref name="expectedVersion"/>; nothing was written. Reload,
    /// re-decide and save again.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// An uncommitted event's CLR type is not registered with the <see cref="EventTypeRegistry"/>.
    /// </exception>
    ValueTask<int> SaveAsync(
        AggregateRoot aggregate,
        int? expectedVersion = null,
        PublishOptions options = default,
        CancellationToken ct = default);

    /// <summary>
    /// Appends the aggregate's uncommitted events to its stream and publishes them to the topic, in
    /// one transaction, with <b>no</b> optimistic-concurrency check — no <c>WATCH</c>, no
    /// <see cref="ConcurrencyException"/>, no chance of the save being rejected.
    /// </summary>
    /// <remarks>
    /// This is <see cref="SaveAsync"/> with the version check removed and nothing else changed. Use
    /// it only where two callers racing to save the same aggregate is not a concern — e.g. an
    /// aggregate with a single writer, or measuring the cost of the version check itself — since a
    /// lost update here is silent: both callers' events land, interleaved, with no error to either
    /// one.
    /// </remarks>
    /// <param name="aggregate">
    /// The aggregate to save. On success its changes are marked committed and its
    /// <see cref="AggregateRoot.Version"/> advances.
    /// </param>
    /// <param name="options">
    /// Correlation id and headers to stamp on every event's topic copy, and on the aggregate's own
    /// stream when the topic sets <c>StateMetadata</c>.
    /// </param>
    /// <param name="ct">Cancellation, observed before the transaction is built.</param>
    /// <returns>
    /// The aggregate's new version. An aggregate with nothing uncommitted returns its current version
    /// without touching Redis at all.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// An uncommitted event's CLR type is not registered with the <see cref="EventTypeRegistry"/>.
    /// </exception>
    ValueTask<int> SaveWithoutConcurrencyCheckAsync(
        AggregateRoot aggregate,
        PublishOptions options = default,
        CancellationToken ct = default);
}
