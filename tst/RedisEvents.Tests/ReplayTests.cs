using System.Diagnostics;
using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Logging.Abstractions;

using RedisEvents.Admin;
using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Extensions;
using RedisEvents.Positions;
using RedisEvents.Producer;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>
/// Replay against a real Redis: S9 (reset to a date reprocesses exactly what
/// <see cref="StreamAdmin.PreviewResetAsync(IConnectionMultiplexer, string, string, DateTimeOffset, int?, TopicOptions, long, CancellationToken)"/>
/// predicted), S10 (a reset issued while the consumer is running is picked up on a flush tick rather
/// than being overwritten by it), and the honest "that data is gone" answer when the requested date
/// predates what survived trimming.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the counts here are exact rather than approximate.</b> A Redis stream id is
/// <c>&lt;unix-millis&gt;-&lt;seq&gt;</c>, so "resume from this instant" is arithmetic — the position
/// immediately before <c>&lt;ms&gt;-0</c> — and not a search with a tolerance. These tests lean on
/// that: entries are written at chosen millisecond ids, the preview's count is compared to the
/// number of messages a real consumer then delivers, and the two must be equal on the nose. An
/// implementation that probed, rounded, or scanned "close enough" would show up as an off-by-one
/// here rather than as a mystery in production.
/// </para>
/// <para>
/// <b>Entries are written with explicit ids.</b> Publishing sixty messages and hoping the clock
/// spreads them over an hour is not an option, and sleeping between publishes would trade an hour of
/// wall time for the same information. <see cref="SeedAsync"/> therefore encodes each entry with
/// <see cref="EntryCodec"/> — exactly what <see cref="StreamPublisher"/> writes — and hands
/// <c>XADD</c> the id itself. The consumer cannot tell the difference, and the timestamps under test
/// are the ones the test chose.
/// </para>
/// <para>
/// Topic and consumer names are unique per test, so nothing here flushes the database or depends on
/// what any other test class in the collection left behind.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class ReplayTests(RedisStreamsFixture fixture)
{
    /// <summary>The type string every seeded entry carries; no consumer here filters on it.</summary>
    private const string MessageType = "replay-test";

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S9_reset_to_a_date_reprocesses_exactly_the_count_the_preview_predicted()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();

        // One partition: the arithmetic under test is per-partition, and a second one would only add
        // a routing question the test is not about.
        var topicOptions = new TopicOptions { Partitions = 1, Trim = TrimMode.None };
        var consumerOptions = ConsumerFor(topic);

        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, topicOptions, NullLogger.Instance, ct);

        // Sixty entries a minute apart, ending a minute ago. The window is an hour of stream ids and
        // no wall-clock time at all.
        const int Total = 60;
        const int Midpoint = 30;
        var firstAt = DateTimeOffset.UtcNow.AddMinutes(-Total);
        var ids = await SeedAsync(fixture.Db, topic, partition: 0, firstAt, TimeSpan.FromMinutes(1), Total, ct);

        // A first pass, so the consumer has a real stored position at the tail — which is what the
        // reset has to move, and what the preview measures the distance from.
        var first = new TestHandler();
        await using (var run = await StartConsumerAsync(fixture.ConnectionString, topic, consumer, topicOptions, consumerOptions, first))
        {
            await first.WaitForAsync(Total, TimeSpan.FromSeconds(30), ct);
        }

        first.Bodies.Should().Equal(Bodies(0, Total), "the first pass reads the whole stream");

        var stored = await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, ct);
        stored.Should().ContainKey(0).WhoseValue.Should().Be(
            ids[^1],
            "a clean stop flushes the final position, so the consumer is parked on the newest entry");

        // The midpoint of the timestamp range, which is also — exactly — the instant entry 30 was
        // written at. An entry written on the requested millisecond must be delivered, not skipped,
        // so this is the boundary case as well as the midpoint.
        var midpoint = ids[Midpoint].Timestamp;

        var preview = await StreamAdmin.PreviewResetAsync(
            fixture.Redis, topic, consumer, midpoint, partition: null, topicOptions, ct: ct);

        preview.Partitions.Should().HaveCount(1);
        var partition = preview.Partitions[0];

        partition.Direction.Should().Be(ResetDirection.Rewind);
        partition.Current.Should().Be(ids[^1]);
        partition.Target.Should().Be(
            StartPosition.FromDate(midpoint).After,
            "the target sits immediately before <ms>-0 so the entry on that millisecond is replayed");
        partition.Oldest.Should().Be(ids[0]);
        partition.Newest.Should().Be(ids[^1]);
        partition.Length.Should().Be(Total);
        partition.TrimmedAway.Should().BeFalse("nothing has been trimmed, so the whole window is still there");

        partition.EntriesExact.Should().BeTrue(
            "the range is far below the scan cap, so the count is the real number and not a floor");
        preview.TotalToReprocess.Should().Be(
            Total - Midpoint,
            "stream ids embed the millisecond, so the count is arithmetic rather than an estimate");
        preview.TotalToSkip.Should().Be(0);
        preview.AnyTrimmedAway.Should().BeFalse();

        var targets = await StreamAdmin.ResetPositionAsync(
            fixture.Redis, topic, consumer, midpoint, partition: null, topicOptions, NullLogger.Instance, ct);

        targets.Should().ContainSingle().Which.Target.Should().Be(partition.Target);

        // The second pass. StartFrom=Stored, so it resumes from what the reset wrote — and the reset
        // marker, taken at startup, would win even if it did not.
        var replayed = new TestHandler();
        await using (var run = await StartConsumerAsync(fixture.ConnectionString, topic, consumer, topicOptions, consumerOptions, replayed))
        {
            // Exactly, not at least: an off-by-one at either end of the range is the failure this
            // whole test exists to catch, so a 31st message is as much a failure as a 29th.
            await replayed.WaitForExactlyAsync(
                (int)preview.TotalToReprocess, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), ct);
        }

        replayed.Count.Should().Be(
            (int)preview.TotalToReprocess,
            "the number reprocessed must equal the number the preview promised, exactly");

        replayed.Messages.Select(static m => m.Id).Should().Equal(
            ids.Skip(Midpoint),
            "the replay starts on the entry written at the requested millisecond and runs to the tail");

        replayed.Bodies.Should().Equal(Bodies(Midpoint, Total));

        replayed.Messages.Should().AllSatisfy(
            m => m.Id.Timestamp.Should().BeOnOrAfter(midpoint),
            "nothing older than the requested instant may be delivered");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S9_preview_warns_when_the_requested_date_precedes_the_oldest_surviving_entry()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var topicOptions = new TopicOptions { Partitions = 1, Trim = TrimMode.None };

        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, topicOptions, NullLogger.Instance, ct);

        const int Total = 40;
        const int Survivors = 10;
        var firstAt = DateTimeOffset.UtcNow.AddMinutes(-Total);
        var ids = await SeedAsync(fixture.Db, topic, partition: 0, firstAt, TimeSpan.FromMinutes(1), Total, ct);

        // The consumer has read everything; the position is the tail.
        await new RedisPositionStore(fixture.Redis)
            .SaveAsync(topic, consumer, new[] { (0, ids[^1]) }.AsSpan(), ct);

        // Trimming is what makes this the common case rather than an edge one: at the default MaxLen
        // of 10,000 a busy topic holds minutes, and an operator asking for "yesterday" is asking for
        // something that no longer exists.
        var key = StreamKeys.Stream(topic, 0, topicOptions.CoLocatePartitions);
        _ = await fixture.Db.StreamTrimAsync(key, Survivors, useApproximateMaxLength: false);
        (await fixture.Db.StreamLengthAsync(key)).Should().Be(Survivors);

        var tooOld = await StreamAdmin.PreviewResetAsync(
            fixture.Redis, topic, consumer, ids[0].Timestamp, partition: null, topicOptions, ct: ct);

        var gone = tooOld.Partitions[0];

        gone.TrimmedAway.Should().BeTrue(
            "the requested start predates the oldest surviving entry, and no position can bring the rest back");
        tooOld.AnyTrimmedAway.Should().BeTrue();
        gone.Oldest.Should().Be(ids[Total - Survivors], "trimming removed everything before this entry");
        gone.Length.Should().Be(Survivors);
        gone.Entries.Should().Be(
            Survivors,
            "the reset can only ever deliver what survived, which is a good deal less than the forty asked for");

        tooOld.Describe().Should().Contain(
            "that data is gone",
            "the operator has to be told the request cannot be honoured, not handed a quiet success");

        // The control: a date inside the surviving window carries no warning, so the flag is really
        // reporting the trim boundary and is not simply always set.
        var inWindow = await StreamAdmin.PreviewResetAsync(
            fixture.Redis, topic, consumer, ids[Total - Survivors].Timestamp, partition: null, topicOptions, ct: ct);

        inWindow.AnyTrimmedAway.Should().BeFalse();
        inWindow.Partitions[0].TrimmedAway.Should().BeFalse();
        inWindow.TotalToReprocess.Should().Be(Survivors);
        inWindow.Describe().Should().NotContain("that data is gone");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S10_a_running_consumer_picks_up_the_reset_marker_and_rewinds_on_a_flush_tick()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var topicOptions = new TopicOptions { Partitions = 1 };

        // A short flush interval keeps the test quick, and — much more to the point — makes the
        // flusher an active adversary: it is writing the position roughly five times a second, so a
        // reset that was not protected by the marker protocol would be gone before the reader saw it.
        var flushInterval = TimeSpan.FromMilliseconds(200);
        var blockFor = TimeSpan.FromMilliseconds(200);

        var consumerOptions = ConsumerFor(topic) with
        {
            PersistIntervalMs = (int)flushInterval.TotalMilliseconds,
            BlockMs = (int)blockFor.TotalMilliseconds,
            ReadMode = ReadMode.Block,
        };

        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, topicOptions, NullLogger.Instance, ct);

        const int Total = 20;
        const int RewindTo = 9;

        var handler = new TestHandler();
        await using var run = await StartConsumerAsync(
            fixture.ConnectionString, topic, consumer, topicOptions, consumerOptions, handler);

        run.Host.OwnedPartitions.Should().Equal(0);

        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);
        var ids = new StreamId[Total];
        for (var i = 0; i < Total; i++)
        {
            ids[i] = await publisher.PublishAsync("replay", Encoding.UTF8.GetBytes(Body(i)), MessageType, ct: ct);
        }

        await handler.WaitForAsync(Total, TimeSpan.FromSeconds(30), ct);
        handler.Bodies.Should().Equal(Bodies(0, Total));

        // Wait for the flusher to have written the tail. This is the state the reset has to survive:
        // a live flusher whose next tick would otherwise overwrite whatever the admin call wrote.
        var positions = new RedisPositionStore(fixture.Redis);
        await RedisStreamsFixture.WaitUntilAsync(
            async () => (await positions.LoadAsync(topic, consumer, ct)).TryGetValue(0, out var at) && at == ids[^1],
            TimeSpan.FromSeconds(15),
            "the running consumer to flush its position at the tail",
            ct);

        var markerField = StreamAdmin.ResetMarkerField(0);
        var positionsKey = StreamKeys.Positions(topic, consumer);

        var clock = Stopwatch.StartNew();

        var targets = await StreamAdmin.ResetPositionAsync(
            fixture.Redis, topic, consumer, ids[RewindTo], partition: null, topicOptions, NullLogger.Instance, ct);

        targets.Should().ContainSingle().Which.Target.Should().Be(ids[RewindTo]);

        // The consumer is still the same running process — never stopped, never restarted.
        await handler.WaitForAsync(Total + (Total - 1 - RewindTo), TimeSpan.FromSeconds(30), ct);
        var rewound = clock.Elapsed;

        run.Host.IsRunning.Should().BeTrue("the rewind happened live, not across a restart");

        handler.Bodies.Skip(Total).Should().Equal(
            Bodies(RewindTo + 1, Total),
            "everything after the target is delivered a second time, and nothing before it is");

        // One flush tick to notice the marker, and at most one block for the reader to come back out
        // of XREAD and act on it. The bound is loose enough to survive a busy CI box and still an
        // order of magnitude below anything that would need a restart to explain.
        rewound.Should().BeLessThan(
            TimeSpan.FromSeconds(3),
            "a live reset is picked up on the next flush tick — expected around {0}",
            flushInterval + blockFor);

        // The marker is deleted only once a read loop has actually taken it, so its disappearance is
        // the protocol reporting that the rewind happened rather than that the field was overwritten.
        await RedisStreamsFixture.WaitUntilAsync(
            async () => !await fixture.Db.HashExistsAsync(positionsKey, markerField),
            TimeSpan.FromSeconds(15),
            "the reset marker to be cleared once the reader had taken it",
            ct);

        // And it rewinds once, not on every tick: a marker that was re-applied would keep replaying.
        await handler.WaitForExactlyAsync(
            Total + (Total - 1 - RewindTo), TimeSpan.FromSeconds(1.5), TimeSpan.FromSeconds(30), ct);

        await RedisStreamsFixture.WaitUntilAsync(
            async () => (await positions.LoadAsync(topic, consumer, ct)).TryGetValue(0, out var at) && at == ids[^1],
            TimeSpan.FromSeconds(15),
            "the position to return to the tail once the replay had drained",
            ct);
    }

    /// <summary>The options every consumer in this file uses, bar the per-test overrides.</summary>
    /// <param name="topic">The topic to consume.</param>
    /// <returns>Consumer options resolving to "resume where you were, else the beginning".</returns>
    private static ConsumerOptions ConsumerFor(string topic) => new()
    {
        Topic = topic,
        BatchSize = 10,
        StartFrom = StartFrom.Stored,
        StartFromWhenMissing = StartFrom.Beginning,
        Persist = PersistMode.AsyncBatch,
        PersistIntervalMs = 100,
        ReadMode = ReadMode.Block,
        BlockMs = 200,
        ShutdownTimeoutSeconds = 5,
    };

    /// <summary>The body of the <paramref name="index"/>th seeded message.</summary>
    private static string Body(int index) => $"msg-{index:D3}";

    /// <summary>The bodies of <c>[from, to)</c>, for an ordered comparison against what arrived.</summary>
    private static string[] Bodies(int from, int to) => Enumerable.Range(from, to - from).Select(Body).ToArray();

    /// <summary>
    /// Writes <paramref name="count"/> entries at chosen millisecond ids, <paramref name="step"/>
    /// apart, in the wire format a real publisher produces.
    /// </summary>
    /// <param name="db">The database to write through.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="partition">The partition to write to.</param>
    /// <param name="first">The instant the first entry is stamped with.</param>
    /// <param name="step">The gap between consecutive entries.</param>
    /// <param name="count">How many entries to write.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The ids written, in order.</returns>
    private static async Task<StreamId[]> SeedAsync(
        IDatabase db,
        string topic,
        int partition,
        DateTimeOffset first,
        TimeSpan step,
        int count,
        CancellationToken ct)
    {
        var key = StreamKeys.Stream(topic, partition);
        var ids = new StreamId[count];

        for (var i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();

            var wanted = new StreamId(first.Add(step * i).ToUnixTimeMilliseconds(), 0);
            var fields = EntryCodec.Encode(Encoding.UTF8.GetBytes(Body(i)), MessageType, partitionKey: string.Empty);
            var assigned = await db.StreamAddAsync(key, fields, wanted.Format());

            // XADD echoes the id it stored; taking it back rather than assuming keeps the test honest
            // about what is actually in the stream.
            ids[i] = StreamId.Parse(assigned.ToString());
            ids[i].Should().Be(wanted, "an explicit XADD id is stored verbatim");
        }

        return ids;
    }

    /// <summary>
    /// Builds and starts a real <see cref="StreamConsumerHost"/> against the fixture's Redis.
    /// </summary>
    /// <param name="connectionString">The fixture's non-admin connection string.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name — the owner of the position hash.</param>
    /// <param name="topicOptions">The topic's options, registered so the host resolves them.</param>
    /// <param name="consumerOptions">The consumer's options.</param>
    /// <param name="handler">The recording handler to wire in.</param>
    /// <returns>The running host, which the caller disposes to stop and flush.</returns>
    private static async Task<RunningConsumer> StartConsumerAsync(
        string connectionString,
        string topic,
        string consumer,
        TopicOptions topicOptions,
        ConsumerOptions consumerOptions,
        TestHandler handler)
    {
        var options = new StreamOptions
        {
            ConnectionString = connectionString,
            Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
        };

        // The provider is given no container, so it connects its own multiplexer — the same path a
        // service takes when nothing suitable is registered.
        var provider = new StreamsConnectionProvider(options, services: null, logger: null);

        var host = new StreamConsumerHost(
            options,
            consumerOptions,
            consumer,
            ((IBatchHandler)handler).HandleAsync,
            provider,
            NullLogger.Instance);

        try
        {
            await host.StartAsync(CancellationToken.None);
        }
        catch
        {
            await provider.DisposeAsync();
            throw;
        }

        return new RunningConsumer(host, provider);
    }

    /// <summary>A started host and the connection it was given, disposed together.</summary>
    /// <param name="host">The running consumer host.</param>
    /// <param name="provider">The multiplexer provider the host was built with.</param>
    private sealed class RunningConsumer(StreamConsumerHost host, StreamsConnectionProvider provider) : IAsyncDisposable
    {
        /// <summary>The running host.</summary>
        internal StreamConsumerHost Host { get; } = host;

        /// <inheritdoc />
        /// <remarks>
        /// Disposing the host drains and performs the final synchronous position flush, which is what
        /// makes "stop, then look at the stored position" meaningful.
        /// </remarks>
        public async ValueTask DisposeAsync()
        {
            await this.Host.DisposeAsync();
            await provider.DisposeAsync();
        }
    }
}
