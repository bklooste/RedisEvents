using System.Diagnostics;
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

        // The state copy carries no partition key: the stream's name already is the aggregate.
        read.Select(m => m.PartitionKey).Should().AllBe(string.Empty);

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
    /// By default a state entry is body and type only, while its topic entry keeps the partition key,
    /// correlation id, trace and headers that projections read.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task State_entries_are_body_and_type_only_while_the_topic_entry_keeps_its_metadata()
    {
        var topic = fixture.NewTopic();
        var store = new StreamStore(fixture.Db, topic, Options);

        using (TracedActivity())
        {
            _ = await store.AppendAndPublishAsync(Name, expectedLength: 0, AggregateId, [Event("body", "t.one", WithMetadata)]);
        }

        var raw = (await fixture.Db.StreamRangeAsync(Outbox.StateKey(topic, Name))).Single();
        raw.Values.Select(v => (string?)v.Name).Should().Equal(EntryCodec.BodyField, EntryCodec.TypeField);

        var state = (await store.ReadAsync(Name, StreamId.Min, max: 10)).Single();
        state.PartitionKey.Should().BeEmpty();
        state.CorrelationId.Should().BeEmpty();
        state.TraceParent.Should().BeNull();
        state.Headers.IsEmpty.Should().BeTrue();

        var published = await PublishedEntry(topic);
        published.PartitionKey.Should().Be(AggregateId);
        published.CorrelationId.Should().Be("corr-0f4a1c");
        published.TraceParent.Should().NotBeNull();
        published.Headers.GetValueOrDefault("es-version").Should().Be("1");
    }

    /// <summary>
    /// With <see cref="TopicOptions.StateMetadata"/> the state entry keeps correlation id, trace and
    /// headers too — everything but the partition key, which it never carries.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task StateMetadata_keeps_everything_but_the_partition_key_on_the_state_entry()
    {
        var topic = fixture.NewTopic();
        var store = new StreamStore(fixture.Db, topic, Options with { StateMetadata = true });

        using (TracedActivity())
        {
            _ = await store.AppendAndPublishAsync(Name, AggregateId, [Event("body", "t.one", WithMetadata)]);
        }

        var raw = (await fixture.Db.StreamRangeAsync(Outbox.StateKey(topic, Name))).Single();
        raw.Values.Select(v => (string?)v.Name).Should().NotContain(EntryCodec.PartitionKeyField);

        var state = (await store.ReadAsync(Name, StreamId.Min, max: 10)).Single();
        var published = await PublishedEntry(topic);

        state.PartitionKey.Should().BeEmpty();
        published.PartitionKey.Should().Be(AggregateId);

        foreach (var message in new[] { state, published })
        {
            message.CorrelationId.Should().Be("corr-0f4a1c");
            message.TraceParent.Should().NotBeNull();
            message.Headers.GetValueOrDefault("es-version").Should().Be("1");
        }

        state.TraceParent.Should().Be(published.TraceParent);
    }

    /// <summary>
    /// A state stream written before entries went lean — every entry with <c>k</c> and metadata —
    /// keeps working: it reads back mixed with new entries, and its length is still the version.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_stream_holding_old_full_entries_and_new_lean_entries_reads_and_versions_as_one()
    {
        var topic = fixture.NewTopic();
        var store = new StreamStore(fixture.Db, topic, Options);
        var key = Outbox.StateKey(topic, Name);

        // What earlier builds wrote: the topic codec, partition key and all.
        _ = await fixture.Db.StreamAddAsync(
            key,
            EntryCodec.Encode(Encoding.UTF8.GetBytes("old"), "t.old", AggregateId, "corr-old", headers: [new("es-version", "1")]));

        var ids = await store.AppendAndPublishAsync(Name, expectedLength: 1, AggregateId, [Event("new", "t.new")]);
        ids.Should().NotBeNull();

        var read = await store.ReadAsync(Name, StreamId.Min, max: 10);

        read.Select(m => m.Type).Should().Equal("t.old", "t.new");
        read.Select(m => Encoding.UTF8.GetString(m.Body.Span)).Should().Equal("old", "new");
        read[0].PartitionKey.Should().Be(AggregateId);
        read[0].CorrelationId.Should().Be("corr-old");
        read[1].PartitionKey.Should().BeEmpty();

        (await store.AppendAndPublishAsync(Name, expectedLength: 1, AggregateId, [Event("stale", "t.new")]))
            .Should().BeNull("the stream is two long, whatever shape its entries are");
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

    private static readonly PublishOptions WithMetadata = new(
        CorrelationId: "corr-0f4a1c",
        Headers: [new KeyValuePair<string, string>("es-version", "1")]);

    private static StateEvent Event(string body, string type, PublishOptions options = default)
        => new(Encoding.UTF8.GetBytes(body), type, options);

    /// <summary>The single entry the append published to partition 0 of the topic, decoded.</summary>
    private async Task<StreamMsg> PublishedEntry(string topic)
    {
        var entries = await fixture.Db.StreamRangeAsync(StreamKeys.Stream(topic, 0), count: null);
        return EntryCodec.Decode(entries.Single(), partition: 0);
    }

    /// <summary>An ambient W3C activity, so the store has a <c>traceparent</c> to stamp.</summary>
    private static Activity TracedActivity()
    {
        var activity = new Activity("state-store-test");
        activity.SetIdFormat(ActivityIdFormat.W3C);
        return activity.Start();
    }
}
