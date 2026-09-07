using System.Buffers.Binary;
using System.Globalization;

using FluentAssertions;

using Microsoft.Extensions.Logging.Abstractions;

using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;

using StackExchange.Redis;

namespace Orange.Lib.Streams.Tests;

/// <summary>
/// Delivery and ordering against real Redis — plan rows S4, S5, S7, S8 and S11.
/// </summary>
/// <remarks>
/// <para>
/// These five are the library's delivery contract stated as executable assertions: messages sharing
/// a partition key arrive in publish order (S4); every message is delivered exactly once across a
/// multi-instance consumer and per-key order survives the split (S5); a restart resumes rather than
/// replays, within the documented at-least-once window (S7); <c>Persist = None</c> with
/// <c>StartFrom = Now</c> skips both the backlog and everything published while the consumer was
/// down (S8); and a handler slower than the stream leaves the lag in Redis rather than on the heap
/// (S11).
/// </para>
/// <para>
/// <b>Every message body is its own global index</b>, big-endian in the first four bytes. That makes
/// "exactly once" a count over a bitmap rather than a set of strings, and it makes "in publish
/// order" an ascending-sequence check — both cheap enough to run over 80 000 messages without the
/// assertion itself becoming the slow part of the test.
/// </para>
/// <para>
/// <b>Recording is lock-free by construction.</b> One partition has exactly one processing loop, and
/// two hosts split the partitions disjointly, so a per-partition list is only ever appended to by a
/// single thread. The tests exploit that rather than serialising every batch behind a lock, which
/// would perturb the very backpressure behaviour S11 is measuring.
/// </para>
/// <para>
/// <b>No topic is shared and no test flushes the server.</b> Topic names come from
/// <see cref="RedisStreamsFixture.NewTopic"/>, so these tests cannot see each other's entries or
/// another class's; the volume tests delete their own streams at the end rather than calling
/// <c>FLUSHALL</c>, which would be a hand grenade in a shared collection.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class DeliveryTests(RedisStreamsFixture fixture)
{
    /// <summary>The type string every message in this file carries. Nothing here filters on it.</summary>
    private const string MessageType = "delivery";

    /// <summary>Body size in bytes: a four-byte index plus filler, so volumes are realistic.</summary>
    private const int BodyBytes = 64;

    // ---------------------------------------------------------------------------------------
    // S4 — ordering
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// S4. Ten thousand messages published under one partition key are handed to the handler in
    /// publish order, with nothing lost, duplicated or reordered.
    /// </summary>
    /// <remarks>
    /// One key means one partition, and one partition means one reader and one processor — so this
    /// is the test that would catch a parallel dispatch inside a partition, a batch handed over
    /// out of order, or a cursor that moved backwards mid-run. The topic has four partitions on
    /// purpose: the key must be routed to exactly one of them, and the other three must stay empty.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S4_ten_thousand_messages_on_one_partition_key_arrive_in_publish_order()
    {
        const int total = 10_000;
        const int partitions = 4;

        var topic = fixture.NewTopic();
        var topicOptions = Volume(partitions);
        var root = this.Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        await PublishAsync(publisher, "one-key", Enumerable.Range(0, total).ToArray());

        var recorder = new Recorder(partitions);

        var consumer = new ConsumerOptions
        {
            Topic = topic,
            BatchSize = 250,
            BlockMs = 250,
            Persist = PersistMode.AsyncBatch,
            PersistIntervalMs = 100,
            StartFrom = StartFrom.Beginning,
        };

        var host = new StreamConsumerHost(root, consumer, "s4-consumer", recorder.HandleAsync, connection, NullLogger.Instance);

        try
        {
            await host.StartAsync(CancellationToken.None);

            await RedisStreamsFixture.WaitUntilAsync(
                () => recorder.Count >= total,
                TimeSpan.FromSeconds(60),
                $"all {total} messages to be delivered");
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        recorder.Count.Should().Be(total, "one key on one partition must not be redelivered when nothing failed");

        var used = recorder.NonEmptyPartitions();
        used.Should().HaveCount(1, "a single partition key hashes to exactly one partition");

        var delivered = recorder.Sequence(used[0]);
        delivered.Should().HaveCount(total);
        delivered.Should().BeInAscendingOrder("a partition has one reader and one processor, so publish order is delivery order");
        delivered[0].Should().Be(0);
        delivered[^1].Should().Be(total - 1);

        await DeleteTopicAsync(connection, topic, partitions);
    }

    // ---------------------------------------------------------------------------------------
    // S5 — multi-partition, multi-instance
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// S5. Eighty thousand messages across eight partitions and thirty-two keys, consumed by two
    /// instances that split the partitions 0–3 / 4–7: every message is delivered exactly once, no
    /// partition is consumed by both instances, and each key's messages stay in publish order.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The two hosts are the point. A single host would prove ordering but not that the ownership
    /// split is exclusive — the failure this guards against is two workers reading one partition and
    /// double-delivering it, which a one-instance test cannot see.
    /// </para>
    /// <para>
    /// Counts are as the plan specifies: 8 partitions, 80 000 messages. Nothing has been shrunk.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S5_eighty_thousand_messages_across_eight_partitions_are_delivered_exactly_once_in_key_order()
    {
        const int partitions = 8;
        const int keys = 32;
        const int perKey = 2_500;
        const int total = keys * perKey;

        var topic = fixture.NewTopic();
        var topicOptions = Volume(partitions);
        var root = this.Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        // Global index i belongs to key i % keys, so a key's indexes are strictly ascending and the
        // per-key order check below is just "ascending".
        var byKey = new int[keys][];
        for (var k = 0; k < keys; k++)
        {
            var owned = new int[perKey];
            for (var n = 0; n < perKey; n++)
            {
                owned[n] = (n * keys) + k;
            }

            byKey[k] = owned;
        }

        // Keys are published concurrently, each key's own chunks strictly in sequence: that is the
        // only ordering the library promises, and publishing the keys serially would just make the
        // test slower without testing anything more.
        await Task.WhenAll(Enumerable
            .Range(0, keys)
            .Select(k => PublishAsync(publisher, KeyName(k), byKey[k]))
            .ToArray());

        var recorder = new Recorder(partitions);

        ConsumerOptions ForInstance(int index) => new()
        {
            Topic = topic,
            BatchSize = 250,
            // Eight blocking reads share one reader connection per host, and Redis parks that
            // connection for the whole block — so a long BlockMs would starve the sibling partitions
            // once their stream drains. It bounds the tail of this test, nothing else.
            BlockMs = 100,
            Persist = PersistMode.AsyncBatch,
            PersistIntervalMs = 100,
            StartFrom = StartFrom.Beginning,
            Instances = new InstanceOptions { Count = 2, Index = index },
        };

        var first = new StreamConsumerHost(root, ForInstance(0), "s5-consumer", recorder.HandleAsync, connection, NullLogger.Instance);
        var second = new StreamConsumerHost(root, ForInstance(1), "s5-consumer", recorder.HandleAsync, connection, NullLogger.Instance);

        try
        {
            await first.StartAsync(CancellationToken.None);
            await second.StartAsync(CancellationToken.None);

            first.OwnedPartitions.Should().Equal(0, 1, 2, 3);
            second.OwnedPartitions.Should().Equal(4, 5, 6, 7);
            first.OwnedPartitions.Should().NotIntersectWith(second.OwnedPartitions, "the split must have no overlap");

            await RedisStreamsFixture.WaitUntilAsync(
                () => recorder.Count >= total,
                TimeSpan.FromSeconds(180),
                $"all {total} messages to be delivered across both instances");
        }
        finally
        {
            await first.StopAsync(CancellationToken.None);
            await second.StopAsync(CancellationToken.None);
        }

        // Exactly once: every index seen, none seen twice. Reported as counts rather than as a
        // 80 000-element diff so a failure is readable.
        var (missing, duplicated) = recorder.Audit(total);
        missing.Should().Be(0, "no message may be lost");
        duplicated.Should().Be(0, "nothing failed and nothing restarted, so nothing may be redelivered");
        recorder.Count.Should().Be(total);

        recorder.NonEmptyPartitions().Should().HaveCount(
            partitions,
            "thirty-two keys hashed over eight partitions must reach all of them");

        // Per-key order. A key lives on exactly one partition, so its deliveries appear in that
        // partition's list and must be ascending there.
        for (var p = 0; p < partitions; p++)
        {
            var keysHere = recorder.KeysOn(p);

            foreach (var key in keysHere)
            {
                var forKey = recorder.SequenceFor(p, key);
                forKey.Should().BeInAscendingOrder($"messages under key {key} must keep publish order");
                forKey.Should().HaveCount(perKey, $"key {key} was published {perKey} times");
            }
        }

        await DeleteTopicAsync(connection, topic, partitions);
    }

    // ---------------------------------------------------------------------------------------
    // S7 — restart resume
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// S7. Consume 5 000, stop, publish 5 000 more, restart: the second run sees the new messages and
    /// at most one flush interval of the old ones.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The bound, not zero, is the assertion.</b> Delivery is at-least-once and the duplicate
    /// window is <see cref="ConsumerOptions.PersistIntervalMs"/>, so demanding zero duplicates would
    /// be asserting a guarantee the library explicitly does not make — and would go green only
    /// because a clean <see cref="StreamConsumerHost.StopAsync"/> happens to flush synchronously. The
    /// bound asserted here is one un-flushed batch per partition, which is what a lost final flush
    /// can cost; a re-read of the whole 5 000 fails it, which is the regression that matters.
    /// </para>
    /// <para>
    /// The complementary assertion is that nothing is <em>lost</em>: the union of both runs covers
    /// all 10 000. A consumer that resumed too far ahead would satisfy the duplicate bound and fail
    /// this one.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S7_a_restarted_consumer_resumes_after_its_stored_position_rather_than_replaying()
    {
        const int firstRun = 5_000;
        const int secondRun = 5_000;
        const int total = firstRun + secondRun;
        const int partitions = 2;
        const int batchSize = 100;
        const int keys = 8;

        var topic = fixture.NewTopic();
        var topicOptions = Volume(partitions);
        var root = this.Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        ConsumerOptions Options() => new()
        {
            Topic = topic,
            BatchSize = batchSize,
            BlockMs = 200,
            Persist = PersistMode.AsyncBatch,
            PersistIntervalMs = 100,
            StartFrom = StartFrom.Stored,
            StartFromWhenMissing = StartFrom.Beginning,
        };

        await PublishSpreadAsync(publisher, 0, firstRun, keys);

        var runOne = new Recorder(partitions);
        var host = new StreamConsumerHost(root, Options(), "s7-consumer", runOne.HandleAsync, connection, NullLogger.Instance);

        try
        {
            await host.StartAsync(CancellationToken.None);
            await RedisStreamsFixture.WaitUntilAsync(
                () => runOne.Count >= firstRun,
                TimeSpan.FromSeconds(60),
                $"the first {firstRun} messages to be delivered");
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        await PublishSpreadAsync(publisher, firstRun, secondRun, keys);

        var runTwo = new Recorder(partitions);
        var restarted = new StreamConsumerHost(root, Options(), "s7-consumer", runTwo.HandleAsync, connection, NullLogger.Instance);

        try
        {
            await restarted.StartAsync(CancellationToken.None);

            // The wait is on the LAST published index, not on a count. A consumer that wrongly
            // replayed from the beginning would have to deliver all 10 000 before reaching it, so it
            // cannot slip past this wait at exactly 5 000 and make the count assertion pass by luck.
            await RedisStreamsFixture.WaitUntilAsync(
                () => runTwo.MaxIndex >= total - 1 && runTwo.Count >= secondRun,
                TimeSpan.FromSeconds(60),
                $"the second {secondRun} messages, up to index {total - 1}, to be delivered after the restart");
        }
        finally
        {
            await restarted.StopAsync(CancellationToken.None);
        }

        var (_, duplicated) = runTwo.Audit(total);

        // One un-flushed batch per partition is the whole tolerance; anything larger means the
        // stored position was not honoured.
        var tolerance = batchSize * partitions;

        var replayed = runTwo.Count - secondRun;
        replayed.Should().BeGreaterThanOrEqualTo(0, "the restart must deliver at least the new messages");
        replayed.Should().BeLessThanOrEqualTo(
            tolerance,
            "a restart may redeliver at most the positions not yet flushed, not the whole stream");
        duplicated.Should().Be(0, "within one run nothing is delivered twice");

        // Nothing lost across the two runs.
        var seen = new bool[total];
        foreach (var index in runOne.All())
        {
            seen[index] = true;
        }

        foreach (var index in runTwo.All())
        {
            seen[index] = true;
        }

        seen.Count(static s => !s).Should().Be(0, "every published message must be delivered to one of the two runs");

        // And specifically: the restart delivered everything published while it was down. A consumer
        // that resumed too far ahead would satisfy the duplicate bound above and fail here.
        var afterStop = new bool[secondRun];
        foreach (var index in runTwo.All())
        {
            if (index >= firstRun)
            {
                afterStop[index - firstRun] = true;
            }
        }

        afterStop.Count(static s => !s).Should().Be(
            0,
            "every message published while the consumer was stopped must be delivered after the restart");

        await DeleteTopicAsync(connection, topic, partitions);
    }

    // ---------------------------------------------------------------------------------------
    // S8 — Persist = None + StartFrom = Now
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// S8. With <c>Persist = None</c> and <c>StartFrom = Now</c> the consumer skips the backlog on
    /// its first start and, on a restart, skips everything published while it was down as well.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The restart half is the interesting one: with no stored position there is nothing to resume
    /// from, so a bug that silently fell back to <c>Beginning</c> would replay the entire stream, and
    /// a bug that stored a position anyway would replay the gap. Both are caught here.
    /// </para>
    /// <para>
    /// <b>The millisecond wait is not a sleep.</b> <c>StartFrom.Now</c> pins the cursor to
    /// <c>&lt;nowMs&gt;-0</c>, so an entry written in that same millisecond with sequence 0 would sit
    /// exactly on the cursor and be skipped. The test waits for the clock to leave that millisecond —
    /// a condition, polled, not a fixed delay — so the boundary case cannot decide the result.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S8_persist_none_with_start_from_now_skips_the_backlog_and_survives_a_restart()
    {
        const int backlog = 1_000;
        const int live = 500;
        const int gap = 500;
        const int afterRestart = 300;
        const int partitions = 2;
        const int keys = 8;

        var topic = fixture.NewTopic();
        var topicOptions = Volume(partitions);
        var root = this.Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        ConsumerOptions Options() => new()
        {
            Topic = topic,
            BatchSize = 100,
            BlockMs = 200,
            Persist = PersistMode.None,
            StartFrom = StartFrom.Now,
        };

        // Indexes: [0, backlog) published before the first start, [backlog, backlog + live) while it
        // runs, [.., + gap) while it is stopped, and the last block after the restart.
        await PublishSpreadAsync(publisher, 0, backlog, keys);

        var runOne = new Recorder(partitions);
        var host = new StreamConsumerHost(root, Options(), "s8-consumer", runOne.HandleAsync, connection, NullLogger.Instance);

        try
        {
            await host.StartAsync(CancellationToken.None);
            await NextMillisecondAsync();

            await PublishSpreadAsync(publisher, backlog, live, keys);

            // Again the wait is on the last index published, so a consumer that wrongly started at
            // the beginning cannot satisfy it without also blowing the exact-count assertion below.
            await RedisStreamsFixture.WaitUntilAsync(
                () => runOne.MaxIndex >= backlog + live - 1 && runOne.Count >= live,
                TimeSpan.FromSeconds(60),
                $"the {live} live messages to be delivered");
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        runOne.Count.Should().Be(live, "StartFrom.Now must skip the backlog entirely");
        runOne.All().Should().AllSatisfy(
            i => i.Should().BeGreaterThanOrEqualTo(backlog, "no backlog entry may be delivered"));

        // Published while nobody is consuming. With Persist = None there is no position to resume
        // from, so a correct restart never sees these.
        await PublishSpreadAsync(publisher, backlog + live, gap, keys);

        (await connection.Connection.GetDatabase()
            .HashGetAllAsync(PositionsKey(topic, "s8-consumer")))
            .Should().BeEmpty("Persist = None must never write a position hash");

        var runTwo = new Recorder(partitions);
        var restarted = new StreamConsumerHost(root, Options(), "s8-consumer", runTwo.HandleAsync, connection, NullLogger.Instance);

        try
        {
            await restarted.StartAsync(CancellationToken.None);
            await NextMillisecondAsync();

            await PublishSpreadAsync(publisher, backlog + live + gap, afterRestart, keys);

            await RedisStreamsFixture.WaitUntilAsync(
                () => runTwo.MaxIndex >= backlog + live + gap + afterRestart - 1 && runTwo.Count >= afterRestart,
                TimeSpan.FromSeconds(60),
                $"the {afterRestart} post-restart messages to be delivered");
        }
        finally
        {
            await restarted.StopAsync(CancellationToken.None);
        }

        runTwo.Count.Should().Be(
            afterRestart,
            "a restart with Persist = None and StartFrom = Now re-reads neither the backlog nor the gap");

        runTwo.All().Should().AllSatisfy(
            i => i.Should().BeGreaterThanOrEqualTo(
                backlog + live + gap,
                "only messages published after the restart may be delivered"));

        await DeleteTopicAsync(connection, topic, partitions);
    }

    // ---------------------------------------------------------------------------------------
    // S11 — backpressure
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// S11. A handler that takes ~50 ms per batch never has more than <c>Capacity × BatchSize</c>
    /// messages in flight while 200 000 are published, the managed heap stays flat, and the
    /// undelivered remainder stays in Redis rather than in this process.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This is the bounded-memory test.</b> Three things are asserted, and all three are needed:
    /// in-flight messages stay within the configured bound (the reader stops at the channel, not at
    /// some larger internal buffer); the managed heap barely moves while ~13 MB of bodies are pushed
    /// through it (nothing is accumulating); and at the moment publishing finishes the consumer is
    /// still far behind (the lag really did stay in Redis — a consumer that had somehow kept up
    /// would satisfy the first two assertions vacuously).
    /// </para>
    /// <para>
    /// <b>Managed heap, not RSS.</b> The plan says "process RSS flat"; RSS is not a deterministic
    /// quantity in a test process — the GC returns segments to the OS on its own schedule and the
    /// test host allocates alongside us — so the assertion here is on
    /// <see cref="GC.GetTotalMemory(bool)"/> after a forced collection, which measures the thing the
    /// requirement is actually about: nothing is being retained. The publish loop reuses its buffers
    /// so that the measurement reflects the library rather than the test.
    /// </para>
    /// <para>
    /// The consumer is deliberately <em>not</em> drained: at ~2 000 msg/s per partition, draining
    /// 200 000 would take the best part of a minute and would prove nothing that the first ten
    /// seconds have not already proved.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S11_a_slow_handler_bounds_in_flight_messages_and_leaves_the_backlog_in_redis()
    {
        const int total = 200_000;
        const int partitions = 2;
        const int batchSize = 100;
        const int capacity = 4;
        const int keys = 16;

        var topic = fixture.NewTopic();
        var topicOptions = Volume(partitions);
        var root = this.Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var db = connection.Connection.GetDatabase();
        var publisher = new StreamPublisher(db, topic, topicOptions);

        var recorder = new Recorder(partitions, retain: false)
        {
            BatchDelay = TimeSpan.FromMilliseconds(50),
        };

        var consumer = new ConsumerOptions
        {
            Topic = topic,
            BatchSize = batchSize,
            BlockMs = 200,
            Persist = PersistMode.AsyncBatch,
            PersistIntervalMs = 250,
            StartFrom = StartFrom.Beginning,
            Backpressure = new BackpressureOptions { Enabled = true, Capacity = capacity },
        };

        var host = new StreamConsumerHost(root, consumer, "s11-consumer", recorder.HandleAsync, connection, NullLogger.Instance);

        long consumedWhenPublishFinished;
        long baseline;
        long afterPublish;

        try
        {
            await host.StartAsync(CancellationToken.None);

            // Wait until the pipeline is actually running before the heap is measured, so startup
            // allocation is inside the baseline rather than inside the delta.
            await PublishSpreadAsync(publisher, 0, batchSize * partitions * 2, keys);
            await RedisStreamsFixture.WaitUntilAsync(
                () => recorder.Count > 0,
                TimeSpan.FromSeconds(30),
                "the consumer to start delivering");

            baseline = GC.GetTotalMemory(forceFullCollection: true);

            await PublishSpreadAsync(publisher, batchSize * partitions * 2, total - (batchSize * partitions * 2), keys);

            consumedWhenPublishFinished = recorder.Count;
            afterPublish = GC.GetTotalMemory(forceFullCollection: true);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        // 1. In flight is bounded by the configured window.
        recorder.PeakInFlight.Should().BeLessThanOrEqualTo(
            capacity * batchSize,
            "the reader awaits the bounded channel before issuing the next fetch, so the handler can never be holding more than Capacity x BatchSize");

        // 2. The heap did not grow with the stream.
        var growth = afterPublish - baseline;
        growth.Should().BeLessThan(
            32L * 1024 * 1024,
            "200 000 x {0}-byte bodies is roughly {1} MB of traffic; a bounded pipeline retains none of it",
            BodyBytes,
            (total * (long)BodyBytes) / (1024 * 1024));

        // 3. The lag stayed in Redis: the consumer is still a long way behind, and the entries are
        //    still on the streams rather than buffered in this process.
        var outstanding = total - consumedWhenPublishFinished;
        outstanding.Should().BeGreaterThan(
            capacity * batchSize * partitions * 10L,
            "a handler taking 50 ms per {0}-message batch cannot keep up with the publisher, so the backlog must be orders of magnitude larger than anything this process can hold",
            batchSize);

        long stored = 0;
        for (var p = 0; p < partitions; p++)
        {
            stored += await db.StreamLengthAsync(StreamKey(topic, p));
        }

        stored.Should().Be(total, "trimming is off for this topic, so every published entry is still on a stream");

        await DeleteTopicAsync(connection, topic, partitions);
    }

    // ---------------------------------------------------------------------------------------
    // Rig
    // ---------------------------------------------------------------------------------------

    /// <summary>Topic options for a volume test: no trimming, so nothing published is ever dropped.</summary>
    private static TopicOptions Volume(int partitions) => new()
    {
        Partitions = partitions,
        Trim = TrimMode.None,
        MaxLen = long.MaxValue,
        BackgroundTrimIntervalSeconds = 0,
    };

    private static string KeyName(int index) => string.Create(CultureInfo.InvariantCulture, $"key-{index}");

    private static RedisKey StreamKey(string topic, int partition)
        => (RedisKey)string.Create(CultureInfo.InvariantCulture, $"s:{{{topic}}}:{partition}");

    private static RedisKey PositionsKey(string topic, string consumer)
        => (RedisKey)string.Create(CultureInfo.InvariantCulture, $"p:{{{topic}}}:{consumer}");

    /// <summary>
    /// Waits for the wall clock to leave the current millisecond. A polled condition, not a delay:
    /// it is what makes <c>StartFrom.Now</c>'s same-millisecond boundary case unable to flip a test.
    /// </summary>
    private static async Task NextMillisecondAsync()
    {
        var start = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await RedisStreamsFixture.WaitUntilAsync(
            () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > start,
            TimeSpan.FromSeconds(5),
            "the clock to leave the current millisecond");
    }

    private static async Task DeleteTopicAsync(StreamsConnectionProvider connection, string topic, int partitions)
    {
        var db = connection.Connection.GetDatabase();
        for (var p = 0; p < partitions; p++)
        {
            _ = await db.KeyDeleteAsync(StreamKey(topic, p));
        }
    }

    private StreamOptions Root(string topic, TopicOptions topicOptions) => new()
    {
        ConnectionString = fixture.ConnectionString,
        Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
    };

    /// <summary>Publishes <paramref name="indexes"/> under one key, in order, in pipelined chunks.</summary>
    private static async Task PublishAsync(StreamPublisher publisher, string key, IReadOnlyList<int> indexes, int chunk = 500)
    {
        var buffers = new byte[Math.Min(chunk, indexes.Count)][];
        for (var i = 0; i < buffers.Length; i++)
        {
            buffers[i] = new byte[BodyBytes];
        }

        var bodies = new ReadOnlyMemory<byte>[buffers.Length];

        for (var offset = 0; offset < indexes.Count; offset += buffers.Length)
        {
            var count = Math.Min(buffers.Length, indexes.Count - offset);

            for (var i = 0; i < count; i++)
            {
                BinaryPrimitives.WriteInt32BigEndian(buffers[i], indexes[offset + i]);
                bodies[i] = buffers[i];
            }

            // Awaited per chunk: the buffers are reused, so the publish must have completed before
            // they are rewritten. It is also what keeps this loop's allocation flat for S11.
            await publisher.PublishBatchAsync(key, bodies.AsMemory(0, count), MessageType);
        }
    }

    /// <summary>
    /// Publishes <paramref name="count"/> consecutive indexes starting at <paramref name="from"/>,
    /// spread over <paramref name="keys"/> partition keys so more than one partition is exercised.
    /// </summary>
    private static async Task PublishSpreadAsync(StreamPublisher publisher, int from, int count, int keys)
    {
        var byKey = new List<int>[keys];
        for (var k = 0; k < keys; k++)
        {
            byKey[k] = new List<int>((count / keys) + 1);
        }

        for (var i = 0; i < count; i++)
        {
            var index = from + i;
            byKey[index % keys].Add(index);
        }

        await Task.WhenAll(Enumerable
            .Range(0, keys)
            .Select(k => PublishAsync(publisher, KeyName(k), byKey[k]))
            .ToArray());
    }

    // ---------------------------------------------------------------------------------------
    // Recorder
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// The batch handler these tests consume with: it records the global index and partition key of
    /// every message, per partition, and tracks the peak number of messages inside the handler.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Per-partition lists, no locks.</b> One partition has exactly one processing loop and two
    /// hosts own disjoint partitions, so each list has a single writer. Serialising every batch
    /// behind one lock would distort the timing S11 measures, which is why this is not simply
    /// <c>TestHandler</c>.
    /// </para>
    /// <para>
    /// <b><c>retain: false</c> records nothing but counts.</b> S11 pushes 200 000 messages through
    /// the handler and asserts the heap does not grow — a recorder that kept them all would be the
    /// leak it is looking for.
    /// </para>
    /// </remarks>
    private sealed class Recorder(int partitions, bool retain = true)
    {
        private readonly List<(int Index, string Key)>[] perPartition =
            Enumerable.Range(0, partitions).Select(static _ => new List<(int, string)>()).ToArray();

        private int count;
        private int inFlight;
        private int peakInFlight;
        private int maxIndex = -1;

        /// <summary>Time spent inside every batch. Not a synchronisation device — it is the load.</summary>
        public TimeSpan BatchDelay { get; init; } = TimeSpan.Zero;

        /// <summary>Messages delivered so far. Safe to poll from another thread.</summary>
        public int Count => Volatile.Read(ref this.count);

        /// <summary>The most messages that were ever inside the handler at one moment.</summary>
        public int PeakInFlight => Volatile.Read(ref this.peakInFlight);

        /// <summary>
        /// The highest global index delivered so far, or <c>-1</c>. Waiting on this rather than on a
        /// raw count is what makes the restart tests unable to pass by arriving at the right total on
        /// the way past it.
        /// </summary>
        public int MaxIndex => Volatile.Read(ref this.maxIndex);

        public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            var depth = Interlocked.Add(ref this.inFlight, batch.Length);
            Bump(ref this.peakInFlight, depth);

            try
            {
                var span = batch.Span;

                for (var i = 0; i < span.Length; i++)
                {
                    ref readonly var msg = ref span[i];
                    var index = BinaryPrimitives.ReadInt32BigEndian(msg.Body.Span);

                    if (retain)
                    {
                        this.perPartition[msg.Partition].Add((index, msg.PartitionKey));
                    }

                    Bump(ref this.maxIndex, index);
                }

                // Published after the rows are recorded, so a poll that sees the count also sees them.
                _ = Interlocked.Add(ref this.count, span.Length);

                if (this.BatchDelay > TimeSpan.Zero)
                {
                    await Task.Delay(this.BatchDelay, ct).ConfigureAwait(false);
                }
            }
            finally
            {
                _ = Interlocked.Add(ref this.inFlight, -batch.Length);
            }
        }

        /// <summary>Partitions that received at least one message, ascending.</summary>
        public IReadOnlyList<int> NonEmptyPartitions()
            => Enumerable.Range(0, this.perPartition.Length).Where(p => this.perPartition[p].Count > 0).ToArray();

        /// <summary>Every index delivered on one partition, in delivery order.</summary>
        public IReadOnlyList<int> Sequence(int partition)
            => this.perPartition[partition].Select(static r => r.Index).ToArray();

        /// <summary>The distinct partition keys seen on one partition.</summary>
        public IReadOnlyList<string> KeysOn(int partition)
            => this.perPartition[partition].Select(static r => r.Key).Distinct(StringComparer.Ordinal).ToArray();

        /// <summary>Every index delivered on one partition under one key, in delivery order.</summary>
        public IReadOnlyList<int> SequenceFor(int partition, string key)
            => this.perPartition[partition]
                .Where(r => string.Equals(r.Key, key, StringComparison.Ordinal))
                .Select(static r => r.Index)
                .ToArray();

        /// <summary>Every index delivered, across all partitions.</summary>
        public IEnumerable<int> All()
            => this.perPartition.SelectMany(static rows => rows).Select(static r => r.Index);

        /// <summary>
        /// How many of <c>[0, total)</c> were never delivered, and how many were delivered more than
        /// once. Counts rather than sets, so a failure message stays readable at 80 000 messages.
        /// </summary>
        public (int Missing, int Duplicated) Audit(int total)
        {
            var seen = new int[total];

            foreach (var index in this.All())
            {
                if ((uint)index < (uint)total)
                {
                    seen[index]++;
                }
            }

            var missing = 0;
            var duplicated = 0;

            for (var i = 0; i < total; i++)
            {
                if (seen[i] == 0)
                {
                    missing++;
                }
                else if (seen[i] > 1)
                {
                    duplicated += seen[i] - 1;
                }
            }

            return (missing, duplicated);
        }

        private static void Bump(ref int target, int candidate)
        {
            var current = Volatile.Read(ref target);

            while (candidate > current)
            {
                var seen = Interlocked.CompareExchange(ref target, candidate, current);

                if (seen == current)
                {
                    return;
                }

                current = seen;
            }
        }
    }
}
