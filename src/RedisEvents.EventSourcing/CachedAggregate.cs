namespace RedisEvents.EventSourcing;

/// <summary>
/// One cached instance of <typeparamref name="TAggregate"/>, and the load-decide-save-retry loop every
/// command-side write path around <see cref="IEventRepository"/> needs — replaying a whole aggregate's
/// history on every command does not scale with a long-lived aggregate's age, so a decided instance is
/// worth keeping, but only for as long as it can be trusted.
/// </summary>
/// <remarks>
/// <para>
/// <b>What this decides.</b> An instance is never trusted across a save whose outcome is unknown — it
/// is discarded before the save is attempted and only put back once that save is known to have
/// committed, or a decision that raised nothing is confirmed to have been made against a still-current
/// aggregate. A lost optimistic-concurrency check discards the instance, reloads, and re-decides,
/// because the decision may come out differently against the state that actually won.
/// </para>
/// <para>
/// <b>What this does not decide.</b> How access to one instance is serialized (a lock, a stripe, a
/// single-reader actor loop), how long an idle instance is worth keeping, and what a caller does once
/// retries are exhausted are all still the caller's call — a per-request command handler and a batching
/// background actor make different choices about every one of those, and forcing them onto one shape
/// would cost more than the duplication this type removes. This is deliberately not an actor, a cache
/// with its own eviction policy, or a lock: it is the one loop both still needed, and used to hand-roll
/// slightly differently every time it was written. For an aggregate cheap enough to replay on every
/// command, <see cref="EventRepositoryExtensions"/>' <c>ExecuteAsync</c> is the same loop without the cache.
/// </para>
/// <para>
/// <b>Not thread-safe on its own.</b> Concurrent calls to <see cref="RunAsync{T}"/> on the same instance
/// race on the cached slot exactly as concurrent calls to a plain field would. Callers serialize access
/// themselves, the same way they always did.
/// </para>
/// <para>
/// <b>On exhausting <c>maxAttempts</c>,</b> the <see cref="ConcurrencyException"/> from the last attempt
/// propagates — the same as calling <see cref="IEventRepository.SaveAsync"/> directly with no retry at
/// all. A caller that wants "give up and tell the requester it's a conflict, not an error" catches it at
/// its own boundary; a caller that wants retries to be part of a larger failure does not have to unwrap
/// anything to get that.
/// </para>
/// <code>
/// var runner = new CachedAggregate&lt;InventoryItem&gt;();
/// var newName = await runner.RunAsync(
///     load: ct => LoadOrCreate(id, ct),
///     repository,
///     apply: async (item, fromCache, ct) =>
///     {
///         item.Rename(requestedName);
///         return AggregateDecision&lt;string&gt;.Done(item.Name);
///     },
///     maxAttempts: 5);
/// </code>
/// </remarks>
public sealed class CachedAggregate<TAggregate>
    where TAggregate : AggregateRoot
{
    private TAggregate? cached;

    /// <summary>
    /// Discards the cached instance, if there is one. Call whenever it can no longer be trusted — the
    /// pattern this type deliberately does not decide for you; see the type's remarks.
    /// </summary>
    public void Evict() => this.cached = null;

    /// <summary>
    /// Runs <paramref name="apply"/> against a fresh-or-cached aggregate, saves what it raised, and
    /// retries against a freshly loaded aggregate on a lost optimistic-concurrency check or an explicit
    /// <see cref="AggregateDecision{T}.Redo"/>.
    /// </summary>
    /// <param name="load">
    /// Materialises a fresh aggregate — typically <see cref="IEventRepository.LoadAsync{TAggregate}"/>
    /// followed by binding a new instance's id when it returns <see langword="null"/>. Called only when
    /// there is no cached instance to reuse.
    /// </param>
    /// <param name="repository">Where the aggregate's uncommitted changes are saved.</param>
    /// <param name="apply">
    /// The decision. May run more than once — once per attempt — against a different aggregate instance
    /// each time, so it must not close over state from an earlier attempt. <paramref name="apply"/> is
    /// told whether the instance it was handed came from the cache, so it can skip a freshness check it
    /// only needs for a cached, not freshly loaded, copy. Returns
    /// <see cref="AggregateDecision{T}.Done"/> with the result to save and return, or
    /// <see cref="AggregateDecision{T}.Redo"/> to discard this instance and try again against a fresh
    /// one, uncounted against <paramref name="maxAttempts"/> — see the type's remarks for why.
    /// </param>
    /// <param name="maxAttempts">
    /// How many times a lost concurrency check is retried before the exception is left to propagate.
    /// Must be at least 1, which is "no retry" — an instance's decision is saved or it fails outright.
    /// </param>
    /// <param name="onConflict">
    /// Runs after a lost concurrency check and before the reload it is followed by, given the 1-based
    /// number of the attempt that just lost — the hook for logging and any backoff delay, since both
    /// are shaped differently by every caller so far. Not called on the attempt that exhausts
    /// <paramref name="maxAttempts"/>, nor for a <see cref="AggregateDecision{T}.Redo"/>, which is not a
    /// concurrency loss.
    /// </param>
    /// <param name="expectedVersion">
    /// The version <see cref="IEventRepository.SaveAsync"/> is told to expect, computed from the
    /// aggregate about to be saved. Defaults to <see cref="AggregateRoot.Version"/>, which is correct
    /// for an aggregate always loaded by full replay from event zero. It is <b>not</b> correct for one
    /// that can be restored from a snapshot plus a partial replay — there, <c>Version</c> only counts
    /// the events replayed onto <em>this instance</em>, not the stream's true length, and the aggregate
    /// must expose its own true version (typically <c>snapshotBaseVersion + Version</c>) for this to
    /// pass in instead.
    /// </param>
    /// <param name="ct">Cancellation.</param>
    /// <returns>The result <paramref name="apply"/> decided, once its decision has been saved (or needed no save).</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxAttempts"/> is less than 1.</exception>
    /// <exception cref="ConcurrencyException">
    /// The concurrency check was still lost after <paramref name="maxAttempts"/> attempts.
    /// </exception>
    public async Task<T> RunAsync<T>(
        Func<CancellationToken, ValueTask<TAggregate>> load,
        IEventRepository repository,
        Func<TAggregate, bool, CancellationToken, ValueTask<AggregateDecision<T>>> apply,
        int maxAttempts = 1,
        Func<int, ConcurrencyException, CancellationToken, Task>? onConflict = null,
        Func<TAggregate, int>? expectedVersion = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(load);
        ArgumentNullException.ThrowIfNull(repository);
        ArgumentNullException.ThrowIfNull(apply);
        if (maxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxAttempts), maxAttempts, "At least one attempt is required.");
        }

        for (var attempt = 1; ; )
        {
            var fromCache = this.cached is not null;
            var aggregate = this.cached ?? await load(ct).ConfigureAwait(false);

            // Not known-good again until a save commits below, or a no-op decision survives the
            // freshness check apply() may run for itself — an exception anywhere past this point, from
            // apply() or from the save, must not leave a stale or half-decided instance cached for the
            // next caller to build on.
            this.cached = null;

            var decision = await apply(aggregate, fromCache, ct).ConfigureAwait(false);

            if (decision.IsRedo)
            {
                continue;
            }

            // Throws before anything is cached again, so the half-decided instance is dropped with it.
            decision.GuardRefusal(aggregate);

            if (aggregate.GetUncommittedChanges().Count == 0)
            {
                this.cached = aggregate;
                return decision.Result;
            }

            try
            {
                await repository.SaveAsync(aggregate, expectedVersion?.Invoke(aggregate), ct: ct).ConfigureAwait(false);
                this.cached = aggregate;
                return decision.Result;
            }
            catch (ConcurrencyException ex) when (attempt < maxAttempts)
            {
                if (onConflict is not null)
                {
                    await onConflict(attempt, ex, ct).ConfigureAwait(false);
                }

                attempt++;
            }
        }
    }
}
