using System.Globalization;
using System.Text.Json.Serialization;

using FluentAssertions;

using RedisEvents.Config;
using RedisEvents.EventSourcing;
using RedisEvents.Producer;
using RedisEvents.Projections;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Tests;

#pragma warning disable CA1852 // The JSON context has to be a partial class for the generator.

/// <summary>One test aggregate's events. Plain records, exactly as a service would write them.</summary>
internal sealed record WidgetCreated(string Id, string Name);

/// <summary>A rename, carrying only what changed.</summary>
internal sealed record WidgetRenamed(string NewName);

/// <summary>
/// A cheap event to raise in bulk, so a test can build a history longer than
/// <c>RedisEventRepository.LoadPageSize</c> and prove that every one of them comes back, in order.
/// </summary>
internal sealed record WidgetTicked(int Sequence);

/// <summary>Source-generated JSON metadata: no reflection, so the AOT story holds in tests too.</summary>
[JsonSerializable(typeof(WidgetCreated))]
[JsonSerializable(typeof(WidgetRenamed))]
[JsonSerializable(typeof(WidgetTicked))]
internal partial class WidgetJson : JsonSerializerContext;

/// <summary>
/// A minimal event-sourced aggregate, defined here rather than taken from the sample so these tests
/// exercise <see cref="RedisEventRepository"/> and nothing else.
/// </summary>
internal sealed class TestWidget : AggregateRoot
{
    private string id = string.Empty;
    private readonly List<int> ticks = [];

    /// <summary>Registers one handler per event type; the constructor is all the wiring there is.</summary>
    public TestWidget()
    {
        this.On<WidgetCreated>(e =>
        {
            this.id = e.Id;
            this.Name = e.Name;
        });

        this.On<WidgetRenamed>(e => this.Name = e.NewName);
        this.On<WidgetTicked>(e => this.ticks.Add(e.Sequence));
    }

    /// <inheritdoc />
    public override string AggregateName => "TestWidget";

    /// <inheritdoc />
    public override string Id => this.id;

    /// <summary>The widget's current name, folded from its history.</summary>
    public string Name { get; private set; } = string.Empty;

    /// <summary>Every tick sequence seen, in the order the events were applied.</summary>
    public IReadOnlyList<int> Ticks => this.ticks;

    /// <summary>Creates a widget by raising its first event, the way a factory method should.</summary>
    public static TestWidget Create(string id, string name)
    {
        var widget = new TestWidget();
        widget.Raise(new WidgetCreated(id, name));
        return widget;
    }

    /// <summary>Renames the widget.</summary>
    public void Rename(string newName) => this.Raise(new WidgetRenamed(newName));

    /// <summary>Records one tick.</summary>
    public void Tick(int sequence) => this.Raise(new WidgetTicked(sequence));
}

/// <summary>
/// Service tests for <see cref="RedisEventRepository"/> against a real Redis: the load/save round
/// trip, the version check, the topic publish that rides with the append, and the paged replay that
/// replaces the reference implementation's silently-truncating single read.
/// </summary>
/// <remarks>
/// <para>
/// None of these can be written against a fake <see cref="IStreamStore"/>. Every one is about the
/// server arbitrating: whether a <c>WATCH</c>ed <c>XLEN</c> check really blocks a second writer,
/// whether a lost check leaves <b>both</b> streams untouched, and whether an aggregate longer than
/// one page comes back whole.
/// </para>
/// <para>
/// Every test takes its own topic from <see cref="RedisStreamsFixture.NewTopic"/>, so nothing here
/// depends on a flush or on the order the suite runs in.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class EventRepositoryTests(RedisStreamsFixture fixture)
{
    /// <summary>Trimming an aggregate stream would break the version, so no test may rely on it either.</summary>
    private static readonly TopicOptions Options = new() { Partitions = 1, Trim = TrimMode.None };

    /// <summary>
    /// Save a new aggregate, load it back: the state folds to the same thing and the version is the
    /// number of events, because the version <em>is</em> the stream's length.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Save_then_load_round_trips_state_and_version()
    {
        var (repository, _, _) = this.Repository();

        var widget = TestWidget.Create("w-1", "First");

        var version = await repository.SaveAsync(widget);

        version.Should().Be(1);
        widget.Version.Should().Be(1, "MarkChangesAsCommitted advances it");
        widget.GetUncommittedChanges().Should().BeEmpty();

        var loaded = await repository.LoadAsync<TestWidget>("w-1");

        loaded.Should().NotBeNull();
        loaded!.Id.Should().Be("w-1");
        loaded.Name.Should().Be("First");
        loaded.Version.Should().Be(1);
        loaded.GetUncommittedChanges().Should().BeEmpty("replayed history is not new activity");
    }

    /// <summary>
    /// The second save's expected version defaults to the version the aggregate is at, so the normal
    /// load-decide-save loop needs no version bookkeeping in the caller at all.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Second_save_appends_and_accumulates_state()
    {
        var (repository, _, _) = this.Repository();

        var created = TestWidget.Create("w-2", "First");
        _ = await repository.SaveAsync(created);

        var widget = await repository.LoadAsync<TestWidget>("w-2");
        widget!.Rename("Second");
        widget.Tick(7);

        var version = await repository.SaveAsync(widget);

        version.Should().Be(3);

        var loaded = await repository.LoadAsync<TestWidget>("w-2");

        loaded!.Name.Should().Be("Second");
        loaded.Ticks.Should().Equal(7);
        loaded.Version.Should().Be(3);
    }

    /// <summary>
    /// An aggregate that decided to do nothing saves without touching Redis: no entries, and the
    /// version it already had comes straight back.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Save_with_nothing_uncommitted_writes_nothing()
    {
        var (repository, topic, _) = this.Repository();

        var widget = TestWidget.Create("w-3", "First");
        _ = await repository.SaveAsync(widget);

        var version = await repository.SaveAsync(widget);

        version.Should().Be(1);
        (await fixture.Db.StreamLengthAsync(StateKey(topic, "w-3"))).Should().Be(1);
    }

    /// <summary>
    /// A stale expected version throws <see cref="ConcurrencyException"/> and writes <b>nothing</b> —
    /// not the aggregate's history, not the projection feed. The aggregate keeps its uncommitted
    /// changes, which is what makes "reload, re-decide, save" the whole recovery story.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Stale_expected_version_throws_and_writes_nothing()
    {
        var (repository, topic, _) = this.Repository();

        var created = TestWidget.Create("w-4", "First");
        _ = await repository.SaveAsync(created);

        var widget = await repository.LoadAsync<TestWidget>("w-4");
        widget!.Rename("Doomed");

        var act = async () => await repository.SaveAsync(widget, expectedVersion: 5);

        var thrown = await act.Should().ThrowAsync<ConcurrencyException>();
        thrown.Which.AggregateName.Should().Be("TestWidget");
        thrown.Which.Id.Should().Be("w-4");
        thrown.Which.ExpectedVersion.Should().Be(5);

        widget.GetUncommittedChanges().Should().HaveCount(1, "nothing was committed, so nothing was cleared");
        widget.Version.Should().Be(1);

        (await fixture.Db.StreamLengthAsync(StateKey(topic, "w-4"))).Should().Be(1);
        (await fixture.Db.StreamLengthAsync(StreamKeys.Stream(topic, 0))).Should().Be(1);

        var reloaded = await repository.LoadAsync<TestWidget>("w-4");
        reloaded!.Version.Should().Be(1);
        reloaded.Name.Should().Be("First", "the doomed rename never landed");
    }

    /// <summary>
    /// Two savers that both loaded version 1: exactly one commits, the other gets a
    /// <see cref="ConcurrencyException"/>. The condition compiles to <c>WATCH</c>, so this is the
    /// server arbitrating — which is why each repository gets a connection of its own.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Two_concurrent_saves_leave_exactly_one_winner()
    {
        var topic = fixture.NewTopic();
        var registry = Registry();

        await using var leftConnection = await fixture.ConnectAsync();
        await using var rightConnection = await fixture.ConnectAsync();

        var seeder = new RedisEventRepository(new StreamStore(fixture.Db, topic, Options), registry);
        _ = await seeder.SaveAsync(TestWidget.Create("w-5", "First"));

        var left = new RedisEventRepository(new StreamStore(leftConnection.GetDatabase(), topic, Options), registry);
        var right = new RedisEventRepository(new StreamStore(rightConnection.GetDatabase(), topic, Options), registry);

        var one = (await left.LoadAsync<TestWidget>("w-5"))!;
        var two = (await right.LoadAsync<TestWidget>("w-5"))!;

        one.Version.Should().Be(1);
        two.Version.Should().Be(1);

        one.Rename("From one");
        two.Rename("From two");

        // Started before either is awaited, so both WATCH/EXEC blocks are genuinely in flight.
        var first = left.SaveAsync(one).AsTask();
        var second = right.SaveAsync(two).AsTask();

        var outcomes = await Task.WhenAll(
            Outcome(first),
            Outcome(second));

        outcomes.Count(o => o is null).Should().Be(1, "one saver must win");
        outcomes.Count(o => o is ConcurrencyException).Should().Be(1, "and the other must lose the version check");

        (await fixture.Db.StreamLengthAsync(StateKey(topic, "w-5"))).Should().Be(2);
        (await fixture.Db.StreamLengthAsync(StreamKeys.Stream(topic, 0))).Should().Be(2);

        static async Task<Exception?> Outcome(Task<int> save)
        {
            try
            {
                _ = await save;
                return null;
            }
            catch (ConcurrencyException e)
            {
                return e;
            }
        }
    }

    /// <summary>
    /// The saved events are on the topic too, in order, each stamped with its 1-based
    /// <c>es-version</c> — that is the projection feed, and it is in the same transaction as the
    /// append, so it can neither lag nor be lost. The caller's correlation id and headers ride along
    /// with ours rather than being replaced by them.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Save_publishes_every_event_to_the_topic_with_an_ascending_version_header()
    {
        var (repository, topic, _) = this.Repository();

        var widget = TestWidget.Create("w-6", "First");
        widget.Rename("Second");
        widget.Tick(1);

        _ = await repository.SaveAsync(
            widget,
            options: new PublishOptions(
                CorrelationId: "corr-e2f9",
                Headers: [new KeyValuePair<string, string>("tenant", "acme")]));

        widget.Tick(2);
        _ = await repository.SaveAsync(widget);

        var entries = await fixture.Db.StreamRangeAsync(StreamKeys.Stream(topic, 0), count: null);
        var published = entries.Select(e => EntryCodec.Decode(e, partition: 0)).ToArray();

        published.Should().HaveCount(4);
        published.Select(m => m.PartitionKey).Should().AllBe("w-6", "the aggregate id routes every event to one partition");

        var versions = published.Select(Version).ToArray();
        versions.Should().Equal(new[] { 1, 2, 3, 4 }, "es-version is 1-based and continues across saves");

        var ids = published.Select(m => Header(m, "es-id")).ToArray();
        ids.Should().OnlyHaveUniqueItems("every event gets an identity of its own");

        // The caller's metadata flowed through on the events of the save it was passed to.
        published.Take(3).Should().AllSatisfy(m =>
        {
            m.CorrelationId.Should().Be("corr-e2f9");
            Header(m, "tenant").Should().Be("acme");
        });

        // And the aggregate's own history carries the same headers, entry for entry.
        var store = new StreamStore(fixture.Db, topic, Options);
        var history = await store.ReadAsync(RedisEventRepository.StreamName("TestWidget", "w-6"), StreamId.Min, max: 100);

        history.Should().HaveCount(4);
        history.Select(Version).Should().Equal(1, 2, 3, 4);
    }

    /// <summary>
    /// An id with no events is <see langword="null"/>, not an exception: asking whether an aggregate
    /// exists is an ordinary thing for a command handler to do.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Load_of_an_unknown_id_returns_null()
    {
        var (repository, _, _) = this.Repository();

        (await repository.LoadAsync<TestWidget>("nobody-home")).Should().BeNull();
    }

    /// <summary>
    /// A history longer than one page loads completely. This is the test the paging loop exists for:
    /// the reference implementation read history with a single <c>XRANGE … COUNT 4000</c> and an
    /// aggregate past that limit silently loaded a prefix of itself, then accepted commands against
    /// the wrong state.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task An_aggregate_longer_than_one_page_loads_completely_and_in_order()
    {
        const int Batches = 15;
        const int PerBatch = 100;
        const int Total = (Batches * PerBatch) + 1;   // + the creation event

        Total.Should().BeGreaterThan(RedisEventRepository.LoadPageSize, "the point is to cross a page boundary");

        var (repository, topic, _) = this.Repository();

        var widget = TestWidget.Create("w-7", "First");
        _ = await repository.SaveAsync(widget);

        for (var batch = 0; batch < Batches; batch++)
        {
            for (var i = 0; i < PerBatch; i++)
            {
                widget.Tick((batch * PerBatch) + i);
            }

            _ = await repository.SaveAsync(widget);
        }

        widget.Version.Should().Be(Total);
        (await fixture.Db.StreamLengthAsync(StateKey(topic, "w-7"))).Should().Be(Total);

        var loaded = await repository.LoadAsync<TestWidget>("w-7");

        loaded!.Version.Should().Be(Total);
        loaded.Ticks.Should().HaveCount(Batches * PerBatch);
        loaded.Ticks.Should().Equal(Enumerable.Range(0, Batches * PerBatch), "replay is in stream order, with no gaps");

        // And a save on top of a paged load still has the right expected version.
        loaded.Rename("After the long replay");
        (await repository.SaveAsync(loaded)).Should().Be(Total + 1);
    }

    /// <summary>
    /// A history entry whose wire type the registry does not know throws rather than being skipped:
    /// an aggregate's own history must be fully understood or its state is quietly wrong. (The
    /// projector side deliberately differs — a topic legitimately carries events a given projection
    /// knows nothing about.)
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Load_of_an_unregistered_wire_type_in_history_throws()
    {
        var topic = fixture.NewTopic();
        var store = new StreamStore(fixture.Db, topic, Options);
        var repository = new RedisEventRepository(store, Registry());

        _ = await repository.SaveAsync(TestWidget.Create("w-8", "First"));

        // Something else wrote to this aggregate's stream under a type this registry never heard of.
        _ = await store.AppendAndPublishAsync(
            RedisEventRepository.StreamName("TestWidget", "w-8"),
            expectedLength: 1,
            "w-8",
            [new StateEvent(System.Text.Encoding.UTF8.GetBytes("{}"), "widget.from-the-future")]);

        var act = async () => await repository.LoadAsync<TestWidget>("w-8");

        (await act.Should().ThrowAsync<InvalidOperationException>())
            .WithMessage("*widget.from-the-future*");
    }

    /// <summary>The Redis key an aggregate's history lives under, for raw <c>XLEN</c> assertions.</summary>
    private static RedisKey StateKey(string topic, string id)
        => Outbox.StateKey(topic, RedisEventRepository.StreamName("TestWidget", id));

    /// <summary>The registry both sides of a test share, exactly as a service registers one once.</summary>
    private static EventTypeRegistry Registry() => new EventTypeRegistry()
        .RegisterJson("widget.created", WidgetJson.Default.WidgetCreated)
        .RegisterJson("widget.renamed", WidgetJson.Default.WidgetRenamed)
        .RegisterJson("widget.ticked", WidgetJson.Default.WidgetTicked);

    /// <summary>The <c>es-version</c> header as a number, which is what "in order" is asserted on.</summary>
    private static int Version(StreamMsg message)
        => int.Parse(Header(message, "es-version"), CultureInfo.InvariantCulture);

    private static string Header(StreamMsg message, string key)
    {
        message.Headers.TryGetValue(key, out var value).Should().BeTrue($"'{key}' must be stamped on every event");
        return value;
    }

    /// <summary>A repository on the fixture's shared connection, over a topic of this test's own.</summary>
    private (RedisEventRepository Repository, string Topic, EventTypeRegistry Registry) Repository(
        [System.Runtime.CompilerServices.CallerMemberName] string hint = "repository")
    {
        var topic = fixture.NewTopic(hint);
        var registry = Registry();

        return (new RedisEventRepository(new StreamStore(fixture.Db, topic, Options), registry), topic, registry);
    }
}
