using FluentAssertions;
using RedisEvents.EventSourcing;

namespace RedisEvents.UnitTests.EventSourcing;

/// <summary>
/// <see cref="AggregateRoot"/>'s explicit dispatch, uncommitted-changes tracking and history replay.
/// </summary>
public class AggregateRootTests
{
    [Fact]
    public void Raise_applies_the_event_immediately_and_records_it_as_uncommitted()
    {
        var counter = new TestCounter();

        counter.Increment(3);

        counter.Value.Should().Be(3);
        counter.GetUncommittedChanges().Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new Incremented(3));
    }

    [Fact]
    public void Raise_with_no_registered_handler_throws()
    {
        var counter = new TestCounter();

        var act = () => counter.RaiseUnregistered();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage($"*{nameof(TestCounter)}*{nameof(Unregistered)}*");
    }

    [Fact]
    public void LoadFromHistory_applies_every_event_sets_version_and_adds_nothing_uncommitted()
    {
        var counter = new TestCounter();
        var history = new object[] { new Incremented(2), new Incremented(5) };

        counter.LoadFromHistory(history);

        counter.Value.Should().Be(7);
        counter.Version.Should().Be(2);
        counter.GetUncommittedChanges().Should().BeEmpty();
    }

    [Fact]
    public void MarkChangesAsCommitted_advances_version_by_uncommitted_count_and_clears_them()
    {
        var counter = new TestCounter();
        counter.Increment(1);
        counter.Increment(1);

        counter.MarkChangesAsCommitted();

        counter.Version.Should().Be(2);
        counter.GetUncommittedChanges().Should().BeEmpty();
    }

    [Fact]
    public void Registering_the_same_event_type_twice_throws()
    {
        var act = () => new DoubleRegisteringCounter();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage($"*{nameof(DoubleRegisteringCounter)}*{nameof(Incremented)}*");
    }

    [Fact]
    public void LoadFromHistory_with_an_unregistered_event_type_throws()
    {
        var counter = new TestCounter();
        var history = new object[] { new Unregistered() };

        var act = () => counter.LoadFromHistory(history);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage($"*{nameof(TestCounter)}*{nameof(Unregistered)}*");
    }

    sealed record Incremented(int By);

    sealed record Unregistered;

    sealed class TestCounter : AggregateRoot
    {
        public override string Id => "test-counter";
        public override string AggregateName => "TestCounter";

        public int Value { get; private set; }

        public TestCounter() => On<Incremented>(e => Value += e.By);

        public void Increment(int by) => Raise(new Incremented(by));

        public void RaiseUnregistered() => Raise(new Unregistered());
    }

    sealed class DoubleRegisteringCounter : AggregateRoot
    {
        public override string Id => "double";
        public override string AggregateName => "DoubleRegisteringCounter";

        public DoubleRegisteringCounter()
        {
            On<Incremented>(e => { });
            On<Incremented>(e => { });
        }
    }
}
