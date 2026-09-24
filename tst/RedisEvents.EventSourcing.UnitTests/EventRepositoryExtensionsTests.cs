using FluentAssertions;

using RedisEvents.EventSourcing;
using RedisEvents.Producer;

namespace RedisEvents.UnitTests.EventSourcing;

/// <summary>
/// <see cref="EventRepositoryExtensions"/>: <c>LoadOrCreateAsync</c>'s binding, and <c>ExecuteAsync</c>'s
/// uncached load-decide-save-retry loop, refusal guard and unchecked save; plus <see cref="AggregateRoot.BindId"/>.
/// </summary>
[Trait("TestType", "UnitTest")]
public sealed class EventRepositoryExtensionsTests
{
    private const string Id = "counter-1";

    [Fact]
    public async Task LoadOrCreate_returns_a_new_instance_bound_to_the_id_when_there_is_no_history()
    {
        var repository = new FakeEventRepository();

        var counter = await repository.LoadOrCreateAsync<Counter>(Id);

        counter.Id.Should().Be(Id);
        counter.Version.Should().Be(0);
    }

    [Fact]
    public async Task LoadOrCreate_binds_a_loaded_instance_even_when_the_repository_did_not()
    {
        var repository = new FakeEventRepository();
        await repository.ExecuteAsync<Counter, int>(Id, c => AggregateDecision<int>.Done(c.Increment(1)));

        var counter = await repository.LoadOrCreateAsync<Counter>(Id);

        counter.Id.Should().Be(Id, "the fake does not bind what it loads, so the extension must");
        counter.Version.Should().Be(1);
    }

    [Fact]
    public async Task Execute_creates_saves_and_returns_the_result_and_new_version()
    {
        var repository = new FakeEventRepository();

        var result = await repository.ExecuteAsync<Counter, int>(Id, c => AggregateDecision<int>.Done(c.Increment(2)));

        result.Should().Be(new CommandResult<int>(2, 1));
        repository.Saves.Should().Be(1);
    }

    [Fact]
    public async Task Execute_loads_fresh_on_every_call()
    {
        var repository = new FakeEventRepository();

        await repository.ExecuteAsync<Counter, int>(Id, c => AggregateDecision<int>.Done(c.Increment(1)));
        await repository.ExecuteAsync<Counter, int>(Id, c => AggregateDecision<int>.Done(c.Increment(1)));

        repository.Loads.Should().Be(2, "ExecuteAsync is the uncached runner; CachedAggregate is the cached one");
    }

    [Fact]
    public async Task A_decision_that_raises_nothing_is_not_saved()
    {
        var repository = new FakeEventRepository();

        var result = await repository.ExecuteAsync<Counter, int>(Id, c => AggregateDecision<int>.Done(c.Value));

        result.Should().Be(new CommandResult<int>(0, 0));
        repository.Saves.Should().Be(0);
    }

    [Fact]
    public async Task A_lost_version_check_reloads_and_redecides_against_the_new_state()
    {
        var repository = new FakeEventRepository();
        await repository.ExecuteAsync<Counter, int>(Id, c => AggregateDecision<int>.Done(c.Increment(1)));
        repository.ConflictsToForce = 1;
        var decisions = 0;

        var result = await repository.ExecuteAsync<Counter, int>(
            Id,
            c =>
            {
                decisions++;
                return AggregateDecision<int>.Done(c.Increment(1));
            },
            maxAttempts: 3);

        decisions.Should().Be(2);
        result.Should().Be(new CommandResult<int>(2, 2));
    }

    [Fact]
    public async Task Exhausting_max_attempts_lets_the_concurrency_exception_propagate_and_calls_onconflict_for_each_retried_loss()
    {
        var repository = new FakeEventRepository { ConflictsToForce = int.MaxValue };
        var seen = new List<int>();

        var act = () => repository.ExecuteAsync<Counter, int>(
            Id,
            c => AggregateDecision<int>.Done(c.Increment(1)),
            maxAttempts: 3,
            onConflict: (attempt, _, _) => { seen.Add(attempt); return Task.CompletedTask; });

        await act.Should().ThrowAsync<ConcurrencyException>();
        repository.Saves.Should().Be(3);
        seen.Should().Equal(1, 2);
    }

    [Fact]
    public async Task A_refusal_saves_nothing_and_returns_its_result()
    {
        var repository = new FakeEventRepository();

        var result = await repository.ExecuteAsync<Counter, string>(Id, _ => AggregateDecision<string>.Refused("no"));

        result.Value.Should().Be("no");
        repository.Saves.Should().Be(0);
    }

    [Fact]
    public async Task A_refusal_after_raising_throws_and_saves_nothing()
    {
        var repository = new FakeEventRepository();

        var act = () => repository.ExecuteAsync<Counter, string>(Id, c =>
        {
            c.Increment(1);
            return AggregateDecision<string>.Refused("changed my mind");
        });

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*refused a command after already raising 1 event*");
        repository.Saves.Should().Be(0);
    }

    [Fact]
    public async Task Redo_reloads_and_decides_again_uncounted()
    {
        var repository = new FakeEventRepository();
        var calls = 0;

        var result = await repository.ExecuteAsync<Counter, int>(
            Id,
            c => ++calls == 1 ? AggregateDecision<int>.Redo() : AggregateDecision<int>.Done(c.Increment(1)),
            maxAttempts: 1);

        result.Value.Should().Be(1);
        repository.Loads.Should().Be(2);
    }

    [Fact]
    public async Task Without_a_concurrency_check_the_unchecked_save_is_used()
    {
        var repository = new FakeEventRepository { ConflictsToForce = int.MaxValue };

        var result = await repository.ExecuteAsync<Counter, int>(Id, c => AggregateDecision<int>.Done(c.Increment(1)), concurrencyCheck: false);

        result.Should().Be(new CommandResult<int>(1, 1));
        repository.UncheckedSaves.Should().Be(1);
        repository.Saves.Should().Be(0);
    }

    [Fact]
    public async Task Maxattempts_below_one_is_rejected()
    {
        var act = () => new FakeEventRepository().ExecuteAsync<Counter, int>(Id, c => AggregateDecision<int>.Done(c.Value), maxAttempts: 0);

        await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void BindId_is_idempotent_for_the_same_id_and_refuses_a_different_one()
    {
        var counter = new Counter();

        counter.BindId(Id);
        counter.BindId(Id);

        counter.Id.Should().Be(Id);
        counter.Invoking(c => c.BindId("other")).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void An_unbound_aggregate_has_an_empty_id()
    {
        new Counter().Id.Should().BeEmpty();
    }

    [Fact]
    public void An_overridden_id_wins_over_the_bound_one()
    {
        var item = new SelfIdentified();

        item.BindId("stream-id");

        item.Id.Should().Be("from-state");
    }

    [Fact]
    public async Task CachedAggregate_applies_the_same_refusal_guard_and_drops_the_instance()
    {
        var repository = new FakeEventRepository();
        var runner = new CachedAggregate<Counter>();

        var act = () => runner.RunAsync(
            ct => repository.LoadOrCreateAsync<Counter>(Id, ct),
            repository,
            (c, _, _) =>
            {
                c.Increment(1);
                return new ValueTask<AggregateDecision<int>>(AggregateDecision<int>.Refused(0));
            });

        await act.Should().ThrowAsync<InvalidOperationException>();

        await runner.RunAsync(ct => repository.LoadOrCreateAsync<Counter>(Id, ct), repository, (c, _, _) => new ValueTask<AggregateDecision<int>>(AggregateDecision<int>.Done(c.Value)));
        repository.Loads.Should().Be(2, "the instance that refused after raising must not be reused");
        repository.Saves.Should().Be(0);
    }

    private sealed record Incremented(int By);

    /// <summary>Takes its id from the stream: no <c>Id</c> override.</summary>
    private sealed class Counter : AggregateRoot
    {
        public Counter() => On<Incremented>(e => Value += e.By);

        public override string AggregateName => "Counter";

        public int Value { get; private set; }

        public int Increment(int by)
        {
            Raise(new Incremented(by));
            return Value;
        }
    }

    private sealed class SelfIdentified : AggregateRoot
    {
        public override string AggregateName => "SelfIdentified";

        public override string Id => "from-state";
    }

    /// <summary>In-memory histories per id, a knob to force lost version checks, and — like most test fakes — no binding on load.</summary>
    private sealed class FakeEventRepository : IEventRepository
    {
        private readonly Dictionary<string, List<object>> streams = new(StringComparer.Ordinal);

        public int Loads { get; private set; }

        public int Saves { get; private set; }

        public int UncheckedSaves { get; private set; }

        public int ConflictsToForce { get; set; }

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

            if (this.ConflictsToForce > 0)
            {
                this.ConflictsToForce--;
                throw new ConcurrencyException(aggregate.AggregateName, aggregate.Id, expectedVersion ?? aggregate.Version);
            }

            return new ValueTask<int>(this.Append(aggregate));
        }

        public ValueTask<int> SaveWithoutConcurrencyCheckAsync(AggregateRoot aggregate, PublishOptions options = default, CancellationToken ct = default)
        {
            UncheckedSaves++;
            return new ValueTask<int>(this.Append(aggregate));
        }

        private int Append(AggregateRoot aggregate)
        {
            aggregate.Id.Should().NotBeEmpty("every save must be of a bound aggregate");
            var history = this.streams.TryGetValue(aggregate.Id, out var existing) ? existing : this.streams[aggregate.Id] = [];
            history.AddRange(aggregate.GetUncommittedChanges());
            aggregate.MarkChangesAsCommitted();
            return aggregate.Version;
        }
    }
}
