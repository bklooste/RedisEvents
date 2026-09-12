using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace RedisEvents.EventSourcing;

/// <summary>
/// An in-process <see cref="IViewStore{TView}"/> backed by a <see cref="ConcurrentDictionary{TKey,TValue}"/>.
/// </summary>
/// <remarks>
/// For tests and samples: exercise a projection or a service's read side against this instead of a
/// real Redis, then swap in <see cref="RedisViewStore{TView}"/> (or a service's own store) for
/// production without touching the projection code, since both implement the same
/// <see cref="IViewStore{TView}"/> seam. Views are not persisted anywhere and vanish with the process.
/// </remarks>
/// <typeparam name="TView">The view model type.</typeparam>
public sealed class InMemoryViewStore<TView> : IViewStore<TView>
    where TView : class
{
    private readonly ConcurrentDictionary<string, TView> views = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public ValueTask<TView?> GetAsync(string id, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        return ValueTask.FromResult(this.views.TryGetValue(id, out var view) ? view : null);
    }

    /// <inheritdoc />
    public ValueTask SetAsync(string id, TView view, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentNullException.ThrowIfNull(view);

        this.views[id] = view;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(string id, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        this.views.TryRemove(id, out _);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TView> ListAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var view in this.views.Values)
        {
            ct.ThrowIfCancellationRequested();
            yield return view;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }
}
