using System.Text;

using FluentAssertions;

using RedisEvents.Config;
using RedisEvents.Producer;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>
/// Service tests for <see cref="IStreamStore"/> against a real Redis: the state stream, the topic
/// publish that rides in the same <c>MULTI</c>/<c>EXEC</c>, and the <c>XLEN</c> condition that makes
/// the pair an event store.
/// </summary>
/// <remarks>
/// <para>
/// None of these can be written against a fake <see cref="IDatabase"/>. Every one of them is about
/// something the server does: the ids it assigns, the order the queued commands land in, whether a
/// <c>WATCH</c>ed length check actually blocks a second writer, and — the one that matters most —
/// that a failed condition leaves <b>both</b> streams untouched rather than half-written.
/// </para>
/// <para>
/// Every test takes its own topic from <see cref="RedisStreamsFixture.NewTopic"/>, so nothing here
/// depends on a flush or on the order the suite runs in.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class StreamStoreTests(RedisStreamsFixture fixture)
{
    /// <summary>Trimming a state stream would break the version, so no test may rely on it either.</summary>
    private static readonly TopicOptions Options = new() { Partitions = 1, Trim = TrimMode.None };

    private const string Name = "es:Inventory:42";
    private const string AggregateId = "42";

    /// <summary>
    /// Append then read round-trips every event's bytes and type, in order, and the ids handed back
    /// are the state stream's own — the aggregate's version numbers, not the topic publishes' ids.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Append_then_read_round_trips_in_order()
    {
        var topic = fixture.NewTopic();
        var store = new StreamStore(fixture.Db, topic, Options);

        var events = new[]
        {
            Event("created", "inventory.created"),
            Event("renamed — ünicode, and a \0 NUL byte", "inventory.renamed"),
            Event("deactivated", "inventory.deactivated"),
        };

        var ids = await store.AppendAndPublishAsync(Name, expectedLength: 0, AggregateId, events);

        ids.Should().NotBeNull().And.HaveCount(3);
        ids!.Should().BeInAscendingOrder();

        var read = await store.ReadAsync(Name, StreamId.Min, max: 100);

        read.Should().HaveCount(3);
        read.Select(m => m.Type).Should().Equal("inventory.created", "inventory.renamed", "inventory.deactivated");
        read.Select(m => Encoding.UTF8.GetString(m.Body.Span)).Should().Equal(
            "created", "renamed — ünicode, and a \0 NUL byte", "deactivated");
        read.Select(m => m.PartitionKey).Should().AllBe(AggregateId);

        // The ids are the state entries', so they are exactly what a read back reports.
        read.Select(m => m.Id).Should().Equal(ids);

        // The key is the topic's slot, not a bare name.
        (await fixture.Db.StreamLengthAsync(Outbox.StateKey(topic, Name))).Should().Be(3);
    }

    /// <summary>
    /// The events are also on the topic, in the same order — that is the projection feed, and it is
    /// in the same transaction, so it cannot lag or be lost.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Appended_events_are_published_to_the_topic_in_order()
    {
        var topic = fixture.NewTopic();
        var store = new StreamStore(fixture.Db, topic, Options);

        var events = new[] { Event("one", "t.one"), Event("two", "t.two") };

        _ = await store.AppendAndPublishAsync(Name, expectedLength: 0, AggregateId, events);

        var entries = await fixture.Db.StreamRangeAsync(StreamKeys.Stream(topic, 0), count: null);

        entries.Should().HaveCount(2);

        var published = entries.Select(e => EntryCodec.Decode(e, partition: 0)).ToArray();
        published.Select(m => m.Type).Should().Equal("t.one", "t.two");
        published.Select(m => Encoding.UTF8.GetString(m.Body.Span)).Should().Equal("one", "two");
        published.Select(m => m.PartitionKey).Should().AllBe(AggregateId);
    }

    /// <summary>
    /// A wrong <c>expectedLength</c> returns <see langword="null"/> and writes <b>nothing</b> —
    /// neither half of the transaction. This is the test the whole design exists for: a torn write
    /// here would mean an aggregate whose history and whose projection feed disagree.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Length_mismatch_returns_null_and_writes_nothing()
    {
        var topic = fixture.NewTopic();
        var store = new StreamStore(fixture.Db, topic, Options);

        var stateKey = Outbox.StateKey(topic, Name);
        var streamKey = StreamKeys.Stream(topic, 0);

        _ = await store.AppendAndPublishAsync(Name, expectedLength: 0, AggregateId, [Event("first", "t.first")]);

        var stale = await store.AppendAndPublishAsync(
            Name,
            expectedLength: 5,
            AggregateId,
            [Event("stale", "t.stale"), Event("also-stale", "t.stale")]);

        stale.Should().BeNull("XLEN is 1, not 5");

        (await fixture.Db.StreamLengthAsync(stateKey)).Should().Be(1);
        (await fixture.Db.StreamLengthAsync(streamKey)).Should().Be(1);

        // And the one entry on each side is still the first append's, not a stale one.
        var state = await store.ReadAsync(Name, StreamId.Min, max: 10);
        state.Should().ContainSingle().Which.Type.Should().Be("t.first");
    }

    /// <summary>
    /// <c>expectedLength: 0</c> creates the stream with no special case — <c>XLEN</c> of a missing
    /// key is <c>0</c> — and a second attempt at version 0 then loses.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Expected_length_zero_creates_once()
    {
        var topic = fixture.NewTopic();
        var store = new StreamStore(fixture.Db, topic, Options);

        var created = await store.AppendAndPublishAsync(Name, expectedLength: 0, AggregateId, [Event("a", "t.a")]);
        var again = await store.AppendAndPublishAsync(Name, expectedLength: 0, AggregateId, [Event("b", "t.b")]);

        created.Should().NotBeNull();
        again.Should().BeNull();

        (await fixture.Db.StreamLengthAsync(Outbox.StateKey(topic, Name))).Should().Be(1);
    }

    /// <summary>
    /// Two concurrent appends at the same expected length: exactly one commits, the other reports a
    /// concurrency loss. The condition is compiled to <c>WATCH</c>, so this is the server arbitrating,
    /// not the client — which is why the two writers are given connections of their own.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Two_concurrent_appends_at_the_same_version_leave_exactly_one_winner()
    {
        var topic = fixture.NewTopic();

        await using var left = await fixture.ConnectAsync();
        await using var right = await fixture.ConnectAsync();

        var seeder = new StreamStore(fixture.Db, topic, Options);
        _ = await seeder.AppendAndPublishAsync(Name, expectedLength: 0, AggregateId, [Event("seed", "t.seed")]);

        var first = new StreamStore(left.GetDatabase(), topic, Options);
        var second = new StreamStore(right.GetDatabase(), topic, Options);

        // Started before either is awaited, so both WATCH/EXEC blocks are genuinely in flight.
        var one = first.AppendAndPublishAsync(Name, expectedLength: 1, AggregateId, [Event("one", "t.one")]).AsTask();
        var two = second.AppendAndPublishAsync(Name, expectedLength: 1, AggregateId, [Event("two", "t.two")]).AsTask();

        var results = await Task.WhenAll(one, two);

        results.Count(r => r is not null).Should().Be(1, "one writer must lose the length check");

        (await fixture.Db.StreamLengthAsync(Outbox.StateKey(topic, Name))).Should().Be(2);
        (await fixture.Db.StreamLengthAsync(StreamKeys.Stream(topic, 0))).Should().Be(2);
    }

    /// <summary>
    /// <c>after</c> is exclusive, so paging with the last id of a page never re-reads it, and
    /// <c>max</c> caps a page. A missing stream reads as empty rather than throwing: "no such
    /// aggregate" and "an aggregate with no events" are the same thing.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Read_pages_exclusively_and_a_missing_stream_is_empty()
    {
        var topic = fixture.NewTopic();
        var store = new StreamStore(fixture.Db, topic, Options);

        (await store.ReadAsync(Name, StreamId.Min, max: 10)).Should().BeEmpty();

        var events = Enumerable.Range(0, 5).Select(i => Event($"e{i}", "t.e")).ToArray();
        var ids = await store.AppendAndPublishAsync(Name, expectedLength: 0, AggregateId, events);

        var page = await store.ReadAsync(Name, StreamId.Min, max: 2);
        page.Should().HaveCount(2);
        page.Select(m => m.Id).Should().Equal(ids![0], ids[1]);

        var next = await store.ReadAsync(Name, page[^1].Id, max: 2);
        next.Select(m => m.Id).Should().Equal(ids[2], ids[3]);

        var last = await store.ReadAsync(Name, next[^1].Id, max: 100);
        last.Select(m => m.Id).Should().Equal(ids[4]);

        (await store.ReadAsync(Name, ids[^1], max: 100)).Should().BeEmpty();
    }

    /// <summary>
    /// Correlation id and headers survive on both copies of an event, so a projection reads the same
    /// metadata the aggregate's own history carries.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Options_are_stamped_on_both_the_state_entry_and_the_topic_entry()
    {
        var topic = fixture.NewTopic();
        var store = new StreamStore(fixture.Db, topic, Options);

        var options = new PublishOptions(
            CorrelationId: "corr-0f4a1c",
            Headers: [new KeyValuePair<string, string>("es-version", "1")]);

        _ = await store.AppendAndPublishAsync(
            Name,
            expectedLength: 0,
            AggregateId,
            [new StateEvent(Encoding.UTF8.GetBytes("body"), "t.one", options)]);

        var state = (await store.ReadAsync(Name, StreamId.Min, max: 10)).Single();
        var entries = await fixture.Db.StreamRangeAsync(StreamKeys.Stream(topic, 0), count: null);
        var published = EntryCodec.Decode(entries.Single(), partition: 0);

        foreach (var message in new[] { state, published })
        {
            message.CorrelationId.Should().Be("corr-0f4a1c");
            message.Headers.TryGetValue("es-version", out var version).Should().BeTrue();
            version.Should().Be("1");
        }
    }

    /// <summary>An append of nothing is refused: there is no state change and nothing to announce.</summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Append_of_no_events_is_refused()
    {
        var store = new StreamStore(fixture.Db, fixture.NewTopic(), Options);

        var act = async () => await store.AppendAndPublishAsync(Name, expectedLength: 0, AggregateId, []);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    private static StateEvent Event(string body, string type) => new(Encoding.UTF8.GetBytes(body), type);
}
