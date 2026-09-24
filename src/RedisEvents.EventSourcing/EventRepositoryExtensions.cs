using RedisEvents.Producer;

namespace RedisEvents.EventSourcing;

/// <summary>
/// The command side's two everyday calls on top of <see cref="IEventRepository"/>: get an aggregate
/// whether or not it exists yet, and run one command against it with the load-decide-save-retry loop.
/// </summary>
/// <remarks>
/// <para>
/// <b>What <see cref="ExecuteAsync{TAggregate,T}(IEventRepository,string,Func{TAggregate,AggregateDecision{T}},int,Func{int,ConcurrencyException,CancellationToken,Task}?,bool,PublishOptions,CancellationToken)"/> is for.</b>
/// An aggregate's decision is only valid against the state it was made on. Two commands racing on one
/// aggregate can both read the same state, both decide yes, and — without a check — both commit, which
/// is how a wallet goes negative or a bet is settled twice. The repository's optimistic version check
/// stops the second save; this method is what to do next: throw the losing instance away, load the
/// state that won, and decide again, because the answer may now be different. It is the loop every
/// command handler used to write by hand, each copy slightly different.
/// </para>
/// <para>
/// <b>Uncached, deliberately.</b> Every attempt loads fresh from the aggregate's stream. That is the
/// right default for aggregates that are short, or written far more often than they are read between
/// writes — a payment, a bet, a wallet behind a per-customer rate limit: a replay per command is cheap,
/// and there is no cache to evict, no stale-refusal class of bug, one code path. When replay cost is
/// real — a long-lived, hot aggregate such as an order book's market — use
/// <see cref="CachedAggregate{TAggregate}"/>, which is the same loop with a cached instance.
/// </para>
/// <para>
/// <b>What it deliberately leaves to the caller.</b> How many attempts are worth making, how to back
/// off between them (<c>onConflict</c>), and what "gave up" means to the requester: on exhausting
/// <c>maxAttempts</c> the last <see cref="ConcurrencyException"/> propagates, exactly as a direct
/// <see cref="IEventRepository.SaveAsync"/> would. An HTTP handler catches it and answers 409; a
/// consumer lets it fail the message. Refusals are not exceptions: they are returned, as
/// <see cref="AggregateDecision{T}.Refused"/>, with whatever result type the service already uses.
/// </para>
/// </remarks>
public static class EventRepositoryExtensions
{
    /// <summary>
    /// Loads an aggregate, or creates an empty one bound to <paramref name="id"/> when its stream has no
    /// history yet.
    /// </summary>
    /// <typeparam name="TAggregate">The aggregate type.</typeparam>
    /// <param name="repository">The repository to load from.</param>
    /// <param name="id">The aggregate's id.</param>
    /// <param name="ct">Cancellation.</param>
    /// <returns>
    /// The aggregate, bound to <paramref name="id"/> either way. A new one has <see cref="AggregateRoot.Version"/>
    /// <c>0</c>, which is also what distinguishes it from a loaded one.
    /// </returns>
    /// <remarks>
    /// For when "no history yet" is the ordinary starting state rather than "not found" — creating a
    /// wallet on its first deposit, a bet on its placement. Where not-found must stay an answer (a read,
    /// or a command that only applies to something that exists), use <see cref="IEventRepository.LoadAsync{TAggregate}"/>,
    /// which still returns <see langword="null"/>.
    /// </remarks>
    public static async ValueTask<TAggregate> LoadOrCreateAsync<TAggregate>(this IEventRepository repository, string id, CancellationToken ct = default)
        where TAggregate : AggregateRoot, new()
    {
        ArgumentNullException.ThrowIfNull(repository);
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        var aggregate = await repository.LoadAsync<TAggregate>(id, ct).ConfigureAwait(false) ?? new TAggregate();

        // Also for a loaded instance: an IEventRepository other than this package's own (a test fake)
        // need not bind what it loads.
        aggregate.BindId(id);
        return aggregate;
    }

    /// <summary>
    /// Runs one command against the aggregate <paramref name="id"/>: load it fresh (or create it),
    /// decide, save what the decision raised with an optimistic version check, and on a lost check
    /// reload and decide again.
    /// </summary>
    /// <typeparam name="TAggregate">The aggregate type.</typeparam>
    /// <typeparam name="T">The command's result type.</typeparam>
    /// <param name="repository">The repository to load from and save to.</param>
    /// <param name="id">The aggregate's id.</param>
    /// <param name="decide">
    /// The decision. It may run more than once — once per attempt — against a different state each
    /// time, so it must not close over anything decided by an earlier attempt. It returns
    /// <see cref="AggregateDecision{T}.Done"/> (save what was raised, if anything),
    /// <see cref="AggregateDecision{T}.Refused"/> (save nothing; it must not have raised anything), or
    /// <see cref="AggregateDecision{T}.Redo"/> (reload and decide again, uncounted).
    /// </param>
    /// <param name="maxAttempts">
    /// How many lost version checks are tolerated before the last <see cref="ConcurrencyException"/>
    /// propagates. At least 1, which is "no retry" — the right value for a command carrying a version
    /// its caller read, since re-deciding it here would only reach the same stale answer.
    /// </param>
    /// <param name="onConflict">
    /// Runs after a lost version check and before the reload that follows it, given the 1-based number
    /// of the attempt that lost: the place to log and back off (a few milliseconds of jitter is usually
    /// enough to stop two writers colliding again). Not called for the attempt that exhausts
    /// <paramref name="maxAttempts"/>.
    /// </param>
    /// <param name="concurrencyCheck">
    /// <see langword="false"/> saves with <see cref="IEventRepository.SaveWithoutConcurrencyCheckAsync"/>:
    /// no version check, so no conflict and no retry. Only for a command that is single-writer by
    /// construction — for example the one that creates the aggregate, before anything else knows it exists.
    /// </param>
    /// <param name="options">Correlation id and headers stamped on every event this command publishes.</param>
    /// <param name="ct">Cancellation.</param>
    /// <returns>The decision's result, and the aggregate's version once its decision was saved (or needed no save).</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxAttempts"/> is less than 1.</exception>
    /// <exception cref="ConcurrencyException">The version check was still lost after <paramref name="maxAttempts"/> attempts.</exception>
    /// <exception cref="InvalidOperationException">The decision refused after raising events — a bug in the aggregate. Nothing was saved.</exception>
    public static Task<CommandResult<T>> ExecuteAsync<TAggregate, T>(
        this IEventRepository repository,
        string id,
        Func<TAggregate, AggregateDecision<T>> decide,
        int maxAttempts = 1,
        Func<int, ConcurrencyException, CancellationToken, Task>? onConflict = null,
        bool concurrencyCheck = true,
        PublishOptions options = default,
        CancellationToken ct = default)
        where TAggregate : AggregateRoot, new()
    {
        ArgumentNullException.ThrowIfNull(decide);
        return repository.ExecuteAsync<TAggregate, T>(id, (a, _) => new ValueTask<AggregateDecision<T>>(decide(a)), maxAttempts, onConflict, concurrencyCheck, options, ct);
    }

    /// <inheritdoc cref="ExecuteAsync{TAggregate,T}(IEventRepository,string,Func{TAggregate,AggregateDecision{T}},int,Func{int,ConcurrencyException,CancellationToken,Task}?,bool,PublishOptions,CancellationToken)"/>
    public static async Task<CommandResult<T>> ExecuteAsync<TAggregate, T>(
        this IEventRepository repository,
        string id,
        Func<TAggregate, CancellationToken, ValueTask<AggregateDecision<T>>> decide,
        int maxAttempts = 1,
        Func<int, ConcurrencyException, CancellationToken, Task>? onConflict = null,
        bool concurrencyCheck = true,
        PublishOptions options = default,
        CancellationToken ct = default)
        where TAggregate : AggregateRoot, new()
    {
        ArgumentNullException.ThrowIfNull(repository);
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentNullException.ThrowIfNull(decide);
        if (maxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxAttempts), maxAttempts, "At least one attempt is required.");
        }

        for (var attempt = 1; ; )
        {
            var aggregate = await repository.LoadOrCreateAsync<TAggregate>(id, ct).ConfigureAwait(false);

            var decision = await decide(aggregate, ct).ConfigureAwait(false);

            if (decision.IsRedo)
            {
                continue;
            }

            decision.GuardRefusal(aggregate);

            if (aggregate.GetUncommittedChanges().Count == 0)
            {
                return new CommandResult<T>(decision.Result, aggregate.Version);
            }

            if (!concurrencyCheck)
            {
                var saved = await repository.SaveWithoutConcurrencyCheckAsync(aggregate, options, ct).ConfigureAwait(false);
                return new CommandResult<T>(decision.Result, saved);
            }

            try
            {
                var version = await repository.SaveAsync(aggregate, options: options, ct: ct).ConfigureAwait(false);
                return new CommandResult<T>(decision.Result, version);
            }
            catch (ConcurrencyException ex) when (attempt < maxAttempts)
            {
                if (onConflict is not null)
                {
                    await onConflict(attempt, ex, ct).ConfigureAwait(false);
                }

                attempt++;
            }

            // Any other exception — a connection lost around EXEC — leaves the outcome unknown (the save
            // wrote everything or nothing) and propagates. There is no cached instance to distrust; a
            // caller's retry reloads and decides against whichever of the two actually happened.
        }
    }
}

/// <summary>What a command run by <see cref="EventRepositoryExtensions"/> decided.</summary>
/// <typeparam name="T">The command's result type.</typeparam>
/// <param name="Value">The decision's result.</param>
/// <param name="Version">
/// The aggregate's version after the command: the new version when it saved, otherwise the version the
/// decision was made against.
/// </param>
public readonly record struct CommandResult<T>(T Value, int Version);
