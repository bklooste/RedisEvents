using FluentAssertions;

using RedisEvents.EventSourcing;
using RedisEvents.Producer;

namespace RedisEvents.UnitTests.EventSourcing;

/// <summary>
/// <see cref="CachedAggregate{TAggregate}"/>: caching, the retry-on-conflict loop, <see cref="AggregateDecision{T}.Redo"/>,
/// and that a cached instance is never trusted across a save whose outcome is unknown.
/// </summary>
[Trait("TestType", "UnitTest")]
public sealed class CachedAggregateTests
{
    [Fact]
    public async Task First_run_loads_and_the_next_reuses_the_cached_instance()
    {
        var repository = new FakeEventRepository();
        var runner = new CachedAggregate<Counter>();

        await runner.RunAsync(LoaderFor(repository), repository, (c, _, _) => Done(c.Increment(1)), ct: default);
        await runner.RunAsync(LoaderFor(repository), repository, (c, _, _) => Done(c.Increment(1)), ct: default);

        repository.Loads.Should().Be(1, "the second run should have reused the cached instance instead of reloading");
        repository.Saves.Should().Be(2);
    }

    [Fact]
    public async Task A_decision_that_raises_nothing_is_not_saved_but_is_still_cached()
    {
        var repository = new FakeEventRepository();
        var runner = new CachedAggregate<Counter>();

        var result = await runner.RunAsync(LoaderFor(repository), repository, (c, _, _) => Done(c.Value), ct: default);

        result.Should().Be(0);
        repository.Saves.Should().Be(0);

        // The instance is still cached even though nothing was saved.
        await runner.RunAsync(LoaderFor(repository), repository, (c, _, _) => Done(c.Value), ct: default);
        repository.Loads.Should().Be(1);
    }

    [Fact]
    public async Task A_lost_concurrency_check_reloads_and_redecides_against_the_new_state()
    {
        var repository = new FakeEventRepository { ConflictsToForce = 1 };
        var runner = new CachedAggregate<Counter>();

        var result = await runner.RunAsync(LoaderFor(repository), repository, (c, _, _) => Done(c.Increment(1)), maxAttempts: 3, ct: default);

        result.Should().Be(1, "the second attempt's decision, made against the reloaded state, is what gets returned");
        repository.Loads.Should().Be(2, "the lost check must reload before deciding again");
        repository.Saves.Should().Be(2, "the first, losing save attempt still counts as an attempt against the store");
    }

    [Fact]
    public async Task Exhausting_max_attempts_lets_the_concurrency_exception_propagate()
    {
        var repository = new FakeEventRepository { ConflictsToForce = int.MaxValue };
        var runner = new CachedAggregate<Counter>();

        var act = () => runner.RunAsync(LoaderFor(repository), repository, (c, _, _) => Done(c.Increment(1)), maxAttempts: 3, ct: default);

        await act.Should().ThrowAsync<ConcurrencyException>();
        repository.Saves.Should().Be(3, "exactly maxAttempts saves are tried, no more");
    }

    [Fact]
    public async Task Onconflict_runs_once_per_lost_attempt_with_its_one_based_number_and_not_on_the_final_loss()
    {
        var repository = new FakeEventRepository { ConflictsToForce = int.MaxValue };
        var runner = new CachedAggregate<Counter>();
        var seen = new List<int>();

        var act = () => runner.RunAsync(
            LoaderFor(repository),
            repository,
            (c, _, _) => Done(c.Increment(1)),
            maxAttempts: 3,
            onConflict: (attempt, _, _) => { seen.Add(attempt); return Task.CompletedTask; },
            ct: default);

        await act.Should().ThrowAsync<ConcurrencyException>();
        seen.Should().Equal(new[] { 1, 2 }, "attempts 1 and 2 lost and were retried; attempt 3's loss is left to propagate, uncalled");
    }

    [Fact]
    public async Task A_failed_save_evicts_the_cache_so_the_next_run_reloads()
    {
        var repository = new FakeEventRepository { ConflictsToForce = int.MaxValue };
        var runner = new CachedAggregate<Counter>();

        await Assert.ThrowsAsync<ConcurrencyException>(() =>
            runner.RunAsync(LoaderFor(repository), repository, (c, _, _) => Done(c.Increment(1)), maxAttempts: 1, ct: default));

        repository.ConflictsToForce = 0;
        await runner.RunAsync(LoaderFor(repository), repository, (c, _, _) => Done(c.Increment(1)), ct: default);

        repository.Loads.Should().Be(2, "the exhausted instance must not be reused by the next call");
    }

    [Fact]
    public async Task Apply_throwing_evicts_the_cache_rather_than_leaving_a_half_decided_instance()
    {
        var repository = new FakeEventRepository();
        var runner = new CachedAggregate<Counter>();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            runner.RunAsync<int>(LoaderFor(repository), repository, (_, _, _) => throw new InvalidOperationException(), ct: default));

        await runner.RunAsync(LoaderFor(repository), repository, (c, _, _) => Done(c.Value), ct: default);
        repository.Loads.Should().Be(2, "the instance apply() blew up against must not be reused");
    }

    [Fact]
    public async Task Redo_discards_the_instance_and_tries_again_uncounted()
    {
        var repository = new FakeEventRepository();
        var runner = new CachedAggregate<Counter>();
        var calls = 0;

        var result = await runner.RunAsync(
            LoaderFor(repository),
            repository,
            (c, _, _) =>
            {
                calls++;
                return calls == 1 ? Redo() : Done(c.Increment(1));
            },
            maxAttempts: 1, // Redo must not be counted against this — a single conflict retry would not have been enough.
            ct: default);

        result.Should().Be(1);
        calls.Should().Be(2);
        repository.Loads.Should().Be(2, "Redo discards the instance it was handed, exactly like a lost concurrency check");
    }

    [Fact]
    public async Task Apply_learns_whether_its_instance_came_from_the_cache()
    {
        var repository = new FakeEventRepository();
        var runner = new CachedAggregate<Counter>();
        var seen = new List<bool>();

        await runner.RunAsync(LoaderFor(repository), repository, (c, fromCache, _) => { seen.Add(fromCache); return Done(c.Value); }, ct: default);
        await runner.RunAsync(LoaderFor(repository), repository, (c, fromCache, _) => { seen.Add(fromCache); return Done(c.Value); }, ct: default);

        seen.Should().Equal(new[] { false, true });
    }

    [Fact]
    public async Task Without_an_expectedversion_selector_the_aggregates_own_version_is_used()
    {
        var repository = new FakeEventRepository();
        var runner = new CachedAggregate<Counter>();

        await runner.RunAsync(LoaderFor(repository), repository, (c, _, _) => Done(c.Increment(1)), ct: default);

        repository.LastExpectedVersionArgument.Should().BeNull("SaveAsync defaults an unset expectedVersion to the aggregate's own Version itself");
    }

    [Fact]
    public async Task An_expectedversion_selector_overrides_the_aggregates_own_version()
    {
        // Simulates an aggregate whose true stream position isn't AggregateRoot.Version — one restored
        // from a snapshot plus a partial replay, for instance, where Version only counts the events
        // replayed onto this instance. 41 stands in for "the real version", deliberately not what the
        // aggregate's own Version would be (0, for a freshly constructed Counter).
        var repository = new FakeEventRepository();
        var runner = new CachedAggregate<Counter>();

        await runner.RunAsync(
            LoaderFor(repository),
            repository,
            (c, _, _) => Done(c.Increment(1)),
            expectedVersion: _ => 41,
            ct: default);

        repository.LastExpectedVersionArgument.Should().Be(41);
    }

    [Fact]
    public async Task Maxattempts_below_one_is_rejected()
    {
        var repository = new FakeEventRepository();
        var runner = new CachedAggregate<Counter>();

        var act = () => runner.RunAsync(LoaderFor(repository), repository, (c, _, _) => Done(c.Value), maxAttempts: 0, ct: default);

        await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
    }

    private static ValueTask<AggregateDecision<int>> Done(int value) => new(AggregateDecision<int>.Done(value));

    private static ValueTask<AggregateDecision<int>> Redo() => new(AggregateDecision<int>.Redo());

    private static Func<CancellationToken, ValueTask<Counter>> LoaderFor(FakeEventRepository repository) =>
        async ct => await repository.LoadAsync<Counter>("counter-1", ct) ?? new Counter();

    private sealed record Incremented(int By);

    private sealed class Counter : AggregateRoot
    {
        public override string Id => "counter-1";
        public override string AggregateName => "Counter";

        public int Value { get; private set; }

        public Counter() => On<Incremented>(e => Value += e.By);

        public int Increment(int by)
        {
            Raise(new Incremented(by));
            return Value;
        }
    }

    /// <summary>
    /// The minimum <see cref="IEventRepository"/> needed to exercise the retry loop: an in-memory
    /// history per aggregate id, and a knob to force the next N saves to lose their concurrency check.
    /// </summary>
    private sealed class FakeEventRepository : IEventRepository
    {
        private readonly Dictionary<string, List<object>> streams = new(StringComparer.Ordinal);

        public int Loads { get; private set; }

        public int Saves { get; private set; }

        /// <summary>How many upcoming <see cref="SaveAsync"/> calls throw <see cref="ConcurrencyException"/> instead of landing.</summary>
        public int ConflictsToForce { get; set; }

        /// <summary>The exact <c>expectedVersion</c> argument the last <see cref="SaveAsync"/> call received, null included.</summary>
        public int? LastExpectedVersionArgument { get; private set; }

        public ValueTask<TAggregate?> LoadAsync<TAggregate>(string id, CancellationToken ct = default)
            where TAggregate : AggregateRoot, new()
        {
            Loads++;

            if (!this.streams.TryGetValue(id, out var history))
            {
                return ValueTask.FromResult<TAggregate?>(null);
            }

            var aggregate = new TAggregate();
            aggregate.LoadFromHistory(history);
            return ValueTask.FromResult<TAggregate?>(aggregate);
        }

        public ValueTask<int> SaveAsync(AggregateRoot aggregate, int? expectedVersion = null, PublishOptions options = default, CancellationToken ct = default)
        {
            Saves++;
            this.LastExpectedVersionArgument = expectedVersion;

            if (this.ConflictsToForce > 0)
            {
                this.ConflictsToForce--;
                throw new ConcurrencyException(aggregate.AggregateName, aggregate.Id, expectedVersion ?? aggregate.Version);
            }

            var history = this.streams.TryGetValue(aggregate.Id, out var existing) ? existing : this.streams[aggregate.Id] = [];
            history.AddRange(aggregate.GetUncommittedChanges());
            aggregate.MarkChangesAsCommitted();
            return ValueTask.FromResult(aggregate.Version);
        }

        public ValueTask<int> SaveWithoutConcurrencyCheckAsync(AggregateRoot aggregate, PublishOptions options = default, CancellationToken ct = default) =>
            this.SaveAsync(aggregate, null, options, ct);
    }
}
