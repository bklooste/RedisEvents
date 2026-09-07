using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;

using FluentAssertions;

using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Diagnostics;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;

using StackExchange.Redis;

namespace Orange.Lib.Streams.Tests;

/// <summary>
/// Service tests for the two producers and the outbox, against a real Redis (S1, S16, S17, S21b).
/// </summary>
/// <remarks>
/// <para>
/// These are the tests that cannot be written against a fake <c>IDatabase</c>: every one of them is
/// about something Redis itself does — the id it assigns, the order a pipeline lands in, and above
/// all the atomicity of <c>MULTI</c>/<c>EXEC</c>, which is the entire reason the outbox exists.
/// </para>
/// <para>
/// Every test takes its own topic from <see cref="RedisStreamsFixture.NewTopic"/>, so nothing here
/// depends on a flush or on the order the suite runs in, and other test classes sharing the
/// container cannot collide with these keys.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class ProducerServiceTests(RedisStreamsFixture fixture)
{
    /// <summary>
    /// The hard ceiling on anything that could hang rather than fail. A wedged CI job is far worse
    /// than a failed one: it burns the whole runner budget and reports nothing useful.
    /// </summary>
    private static readonly TimeSpan HardTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// S1 — a publish survives the round trip intact: body bytes, type, partition key, correlation
    /// id, custom headers, the partition it routed to, and the broker-assigned enqueued time.
    /// </summary>
    /// <remarks>
    /// This is the test that pins the wire format from the outside. The codec has its own unit
    /// tests, but only a real <c>XADD</c> followed by a real read proves that what the publisher
    /// encodes is what the consumer decodes — including <see cref="StreamMsg.EnqueuedTime"/>, which
    /// is not a field at all but Redis's own clock recovered from the entry id.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S1_publish_then_consume_preserves_every_field()
    {
        var topic = fixture.NewTopic();
        var topicOptions = new TopicOptions { Partitions = 4, Trim = TrimMode.None };

        const string PartitionKey = "round-trip-key-42";
        const string Type = "S1.RoundTrip";
        const string CorrelationId = "corr-0f4a1c";

        var body = Encoding.UTF8.GetBytes("round trip payload — ünicode, and a \0 NUL byte");
        var headers = new[]
        {
            new KeyValuePair<string, string>("tenant", "acme"),
            new KeyValuePair<string, string>("schema", "v3"),
        };

        var expectedPartition = PartitionRouter.ForKey(Encoding.UTF8.GetBytes(PartitionKey), topicOptions.Partitions);

        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);

        var before = DateTimeOffset.UtcNow;
        var id = await publisher.PublishAsync(
            PartitionKey,
            body,
            Type,
            new PublishOptions(CorrelationId, headers));
        var after = DateTimeOffset.UtcNow;

        var handler = new TestHandler();
        await using var host = NewHost(topic, topicOptions, handler, c => c with { StartFrom = StartFrom.Beginning });

        await host.StartAsync(CancellationToken.None);
        try
        {
            await handler.WaitForAsync(1, HardTimeout);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        var msg = handler.Messages.Should().ContainSingle().Subject;

        msg.Body.ToArray().Should().Equal(body, "the body must survive the codec byte for byte, NUL included");
        msg.Type.Should().Be(Type);
        msg.PartitionKey.Should().Be(PartitionKey);
        msg.CorrelationId.Should().Be(CorrelationId);
        msg.Partition.Should().Be(expectedPartition, "the key hashes to exactly one partition");
        msg.Id.Should().Be(id, "the consumer must see the id XADD handed back to the publisher");

        msg.Headers.TryGetValue("tenant", out var tenant).Should().BeTrue();
        tenant.Should().Be("acme");
        msg.Headers.TryGetValue("schema", out var schema).Should().BeTrue();
        schema.Should().Be("v3");
        msg.Headers.TryGetValue("absent", out _).Should().BeFalse();

        // EnqueuedTime is Redis's clock, recovered from the id — not a field anyone wrote.
        msg.EnqueuedTime.Should().Be(id.Timestamp);
        msg.EnqueuedTime.Should().BeOnOrAfter(before.AddSeconds(-5)).And.BeOnOrBefore(after.AddSeconds(5));
    }

    /// <summary>
    /// S16 — 100 000 enqueues on the buffered producer all reach Redis once
    /// <see cref="BufferedStreamPublisher.FlushAsync"/> returns, exactly once each.
    /// </summary>
    /// <remarks>
    /// The count alone would not catch the interesting failure. The bodies carry their own index, so
    /// the assertion is that the set of delivered indexes is exactly 0..99 999 — a pump that dropped
    /// a batch, double-issued one, or reused a pooled buffer across two entries all fail here and
    /// would pass a bare <c>XLEN</c> check.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S16_buffered_producer_delivers_every_enqueue_after_flush()
    {
        const int Count = 100_000;

        var topic = fixture.NewTopic();
        var topicOptions = new TopicOptions { Partitions = 4, Trim = TrimMode.None };

        var inner = new StreamPublisher(fixture.Db, topic, topicOptions);
        await using var buffered = new BufferedStreamPublisher(
            inner,
            new ProducerOptions { Topic = topic, MaxBatch = 500, MaxWaitMs = 20, MaxQueue = Count },
            topicOptions);

        for (var i = 0; i < Count; i++)
        {
            var body = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(body, i);

            await buffered.EnqueueAsync(i.ToString(System.Globalization.CultureInfo.InvariantCulture), body, "S16.Buffered");
        }

        await buffered.FlushAsync(CancellationToken.None).AsTask().WaitAsync(HardTimeout);

        buffered.DroppedCount.Should().Be(0, "the queue was sized for the whole run and DropOldest is off");
        buffered.PublishedCount.Should().Be(Count);

        var seen = new bool[Count];
        var total = 0;

        for (var partition = 0; partition < topicOptions.Partitions; partition++)
        {
            var entries = await fixture.Db.StreamRangeAsync(StreamKeys.Stream(topic, partition), count: null);

            foreach (var entry in entries)
            {
                var msg = EntryCodec.Decode(entry, partition);
                msg.Type.Should().Be("S16.Buffered");

                var index = BinaryPrimitives.ReadInt32BigEndian(msg.Body.Span);
                seen[index].Should().BeFalse($"message {index} must be published exactly once");
                seen[index] = true;
                total++;
            }
        }

        total.Should().Be(Count, "every enqueue must be on a stream once FlushAsync has returned");
    }

    /// <summary>
    /// S16 — the buffered path copies the caller's buffer, so mutating it after
    /// <see cref="BufferedStreamPublisher.EnqueueAsync"/> returns cannot corrupt what is published.
    /// The non-buffered path does not copy, and is safe for the opposite reason: it is awaited.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the one behavioural difference between the two publishers that a caller can get
    /// wrong, so the test makes the window real rather than hoping to hit it: the producer is built
    /// with a 60-second time trigger and a batch ceiling far above the message count, so the pump
    /// cannot flush until <see cref="BufferedStreamPublisher.FlushAsync"/> posts its barrier. The
    /// mutation therefore always lands strictly between the enqueue and the write.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S16_buffered_enqueue_copies_the_caller_buffer()
    {
        var topic = fixture.NewTopic();
        var topicOptions = new TopicOptions { Partitions = 1, Trim = TrimMode.None };

        var inner = new StreamPublisher(fixture.Db, topic, topicOptions);
        await using var buffered = new BufferedStreamPublisher(
            inner,
            // No flush can happen on its own: not on time (60s) and not on size (1000 > 1).
            new ProducerOptions { Topic = topic, MaxBatch = 1000, MaxWaitMs = 60_000, MaxQueue = 64 },
            topicOptions);

        var caller = new byte[32];
        caller.AsSpan().Fill(0xA5);

        await buffered.EnqueueAsync("mutation", caller, "S16.Mutation");

        // The caller reuses its buffer the instant the enqueue returns — the documented, supported
        // thing to do on the buffered path, and the reason the copy exists.
        caller.AsSpan().Fill(0xFF);

        await buffered.FlushAsync(CancellationToken.None).AsTask().WaitAsync(HardTimeout);

        var buffedEntries = await fixture.Db.StreamRangeAsync(StreamKeys.Stream(topic, 0), count: null);
        var buffedBody = EntryCodec.Decode(buffedEntries.Should().ContainSingle().Subject, 0).Body.ToArray();

        buffedBody.Should().AllSatisfy(
            b => b.Should().Be(0xA5),
            "EnqueueAsync copies into a pooled buffer, so a later mutation of the caller's array cannot reach Redis");

        // The contrast: the non-buffered publisher hands the caller's memory straight to XADD and
        // never copies it. That is safe only because the caller awaits, which this asserts.
        var direct = new StreamPublisher(fixture.Db, topic, topicOptions);
        await direct.PublishAsync("mutation", caller, "S16.Direct");
        caller.AsSpan().Clear();

        var afterDirect = await fixture.Db.StreamRangeAsync(StreamKeys.Stream(topic, 0), count: null);
        afterDirect.Should().HaveCount(2);

        EntryCodec.Decode(afterDirect[1], 0).Body.ToArray().Should().AllSatisfy(
            b => b.Should().Be(0xFF),
            "the awaited publish captured the buffer's contents at the time it was awaited, not afterwards");
    }

    /// <summary>
    /// S17 — a concurrent reader on a second connection never sees the state write without its
    /// event, or the event without its state write.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The reader has to be atomic too, or it proves nothing: two pipelined reads are two commands,
    /// and Redis is free to run somebody's whole <c>EXEC</c> between them, which would produce a
    /// mismatched pair from a perfectly atomic writer. So the reader wraps its <c>GET</c> and
    /// <c>XLEN</c> in a <c>MULTI</c>/<c>EXEC</c> of its own, and the invariant it checks —
    /// <c>counter == XLEN</c> — can then only break if the outbox itself is not atomic.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S17_outbox_is_never_observed_half_applied()
    {
        const int Writes = 300;

        var topic = fixture.NewTopic();
        var topicOptions = new TopicOptions { Partitions = 1, Trim = TrimMode.None };

        var stateKey = Outbox.StateKey(topic, "counter");
        var streamKey = StreamKeys.Stream(topic, 0);

        await using var observer = await fixture.ConnectAsync();
        var observerDb = observer.GetDatabase();

        var mismatches = new ConcurrentBag<string>();
        var samples = 0;
        using var reading = new CancellationTokenSource();

        var watching = Task.Run(
            async () =>
            {
                while (!reading.IsCancellationRequested)
                {
                    // One MULTI/EXEC: nothing of anyone else's can run between these two reads.
                    var read = observerDb.CreateTransaction();
                    var counter = read.StringGetAsync(stateKey);
                    var length = read.StreamLengthAsync(streamKey);

                    if (!await read.ExecuteAsync().ConfigureAwait(false))
                    {
                        continue;
                    }

                    var state = (long?)await counter.ConfigureAwait(false) ?? 0L;
                    var entries = await length.ConfigureAwait(false);

                    Interlocked.Increment(ref samples);

                    if (state != entries)
                    {
                        mismatches.Add($"counter={state} but XLEN={entries}");
                    }
                }
            },
            CancellationToken.None);

        var body = new byte[16];

        for (var i = 1; i <= Writes; i++)
        {
            var value = i;
            var id = await Outbox.WriteAndPublishAsync(
                fixture.Db,
                tran => _ = tran.StringSetAsync(stateKey, value),
                topic,
                "outbox-key",
                body,
                "S17.Atomic",
                stateKeys: [stateKey],
                topicOptions: topicOptions);

            id.Should().NotBeNull("no condition was supplied, so nothing can legitimately return null");
        }

        await reading.CancelAsync();
        await watching.WaitAsync(HardTimeout);

        mismatches.Should().BeEmpty("a MULTI/EXEC outbox can never be observed half-applied");
        samples.Should().BeGreaterThan(10, "the observer must actually have sampled while the writes were running");

        ((long?)await fixture.Db.StringGetAsync(stateKey)).Should().Be(Writes);
        (await fixture.Db.StreamLengthAsync(streamKey)).Should().Be(Writes);
    }

    /// <summary>
    /// S17 — a failed <see cref="Condition"/> applies nothing at all and reports itself as a null
    /// id; the same call with the condition satisfied applies both halves.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S17_failed_condition_applies_nothing_and_returns_null()
    {
        var topic = fixture.NewTopic();
        var topicOptions = new TopicOptions { Partitions = 1, Trim = TrimMode.None };

        var stateKey = Outbox.StateKey(topic, "state");
        var guardKey = Outbox.StateKey(topic, "guard");
        var streamKey = StreamKeys.Stream(topic, 0);

        // The guard exists, so KeyNotExists cannot hold and the transaction must be abandoned.
        (await fixture.Db.StringSetAsync(guardKey, "taken")).Should().BeTrue();

        var body = Encoding.UTF8.GetBytes("conditional");

        var refused = await Outbox.WriteAndPublishAsync(
            fixture.Db,
            tran => _ = tran.StringSetAsync(stateKey, "written"),
            topic,
            "outbox-key",
            body,
            "S17.Conditional",
            conditions: [Condition.KeyNotExists(guardKey)],
            stateKeys: [stateKey, guardKey],
            topicOptions: topicOptions);

        refused.Should().BeNull("a failed condition is reported as a null id, not an exception");
        (await fixture.Db.KeyExistsAsync(stateKey)).Should().BeFalse("the state write must not have been applied");
        (await fixture.Db.StreamLengthAsync(streamKey)).Should().Be(0, "and neither must the publish");

        // Same call, condition now satisfied: both halves land.
        (await fixture.Db.KeyDeleteAsync(guardKey)).Should().BeTrue();

        var applied = await Outbox.WriteAndPublishAsync(
            fixture.Db,
            tran => _ = tran.StringSetAsync(stateKey, "written"),
            topic,
            "outbox-key",
            body,
            "S17.Conditional",
            conditions: [Condition.KeyNotExists(guardKey)],
            stateKeys: [stateKey, guardKey],
            topicOptions: topicOptions);

        applied.Should().NotBeNull();
        ((string?)await fixture.Db.StringGetAsync(stateKey)).Should().Be("written");
        (await fixture.Db.StreamLengthAsync(streamKey)).Should().Be(1);
    }

    /// <summary>
    /// S17 — the documented deadlock trap: a command queued on the transaction does not complete
    /// until <c>EXEC</c> runs, and <c>EXEC</c> cannot run until the state-write delegate returns.
    /// Waiting on that task inside the delegate therefore blocks forever.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the one test in the file that could wedge a CI runner, so it is written so it cannot.
    /// The delegate's block is <em>bounded</em> — a 1-second <see cref="Task.Wait(TimeSpan)"/>, not
    /// an <c>await</c> — so the thread it parks is always released, and the whole operation is run
    /// on a background task and joined with a hard timeout. A regression that made the outbox hang
    /// fails this test in seconds instead of hanging the run.
    /// </para>
    /// <para>
    /// The assertion is the deadlock condition itself: <c>Wait</c> returns <see langword="false"/>,
    /// proving the queued task was still incomplete inside the delegate. Once <c>EXEC</c> has run
    /// the same task completes, which is the second half of the proof — nothing was lost, the
    /// caller simply must not wait for it in the wrong place.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S17_state_write_delegate_that_awaits_is_caught_by_a_timeout()
    {
        var topic = fixture.NewTopic();
        var topicOptions = new TopicOptions { Partitions = 1, Trim = TrimMode.None };

        var stateKey = Outbox.StateKey(topic, "deadlock");
        var streamKey = StreamKeys.Stream(topic, 0);

        Task<bool>? queued = null;
        var completedInsideDelegate = true;
        var body = Encoding.UTF8.GetBytes("deadlock probe");

        var attempt = Task.Run(
            () => Outbox.WriteAndPublishAsync(
                fixture.Db,
                tran =>
                {
                    queued = tran.StringSetAsync(stateKey, "written");

                    // A bounded stand-in for `await`. Awaiting here would never return, because the
                    // task is only released by ExecuteAsync, which is downstream of this delegate.
                    completedInsideDelegate = queued.Wait(TimeSpan.FromSeconds(1));
                },
                topic,
                "outbox-key",
                body,
                "S17.Deadlock",
                stateKeys: [stateKey],
                topicOptions: topicOptions),
            CancellationToken.None);

        // The hard timeout. WriteAndPublishAsync invokes the delegate synchronously, so a delegate
        // that blocked forever would block this task's thread — the join is what keeps that from
        // becoming a hung test run.
        var completed = await Task.WhenAny(attempt, Task.Delay(HardTimeout));
        completed.Should().BeSameAs(
            attempt,
            $"the outbox call must not still be running after {HardTimeout.TotalSeconds:0}s — that is the hang this test exists to catch");

        var id = await attempt;

        completedInsideDelegate.Should().BeFalse(
            "a command queued on an ITransaction cannot complete before ExecuteAsync, which is why the delegate must not await");

        id.Should().NotBeNull("the transaction itself still commits — only the delegate's wait was doomed");
        queued.Should().NotBeNull();
        (await queued!.WaitAsync(HardTimeout)).Should().BeTrue("EXEC released the queued command, after the delegate returned");

        ((string?)await fixture.Db.StringGetAsync(stateKey)).Should().Be("written");
        (await fixture.Db.StreamLengthAsync(streamKey)).Should().Be(1);
    }

    /// <summary>
    /// S21b — with a consumer parked in a blocking <c>XREAD</c>, the outbox still commits
    /// atomically and its messages are delivered. The write path never moved onto the reader
    /// connection.
    /// </summary>
    /// <remarks>
    /// A <c>MULTI</c>/<c>EXEC</c> is bound to one connection. If the outbox ever took the consumer's
    /// dedicated reader multiplexer — which is read-only and sits parked in <c>XREAD BLOCK</c> — the
    /// transaction would queue behind the block and this test would time out rather than fail
    /// cleanly, which is why the waits here are all bounded.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S21b_outbox_commits_atomically_while_a_blocking_consumer_runs()
    {
        const int Writes = 50;

        var topic = fixture.NewTopic();
        var topicOptions = new TopicOptions { Partitions = 2, Trim = TrimMode.None };

        var stateKey = Outbox.StateKey(topic, "counter");

        var handler = new TestHandler();
        await using var host = NewHost(
            topic,
            topicOptions,
            handler,
            c => c with { ReadMode = ReadMode.Block, BlockMs = 500, StartFrom = StartFrom.Beginning });

        await host.StartAsync(CancellationToken.None);

        try
        {
            host.IsRunning.Should().BeTrue();
            host.OwnedPartitions.Should().Equal(0, 1);

            var body = new byte[8];

            for (var i = 1; i <= Writes; i++)
            {
                var value = i;
                BinaryPrimitives.WriteInt32BigEndian(body, i);

                var id = await Outbox
                    .WriteAndPublishAsync(
                        fixture.Db,
                        tran => _ = tran.StringSetAsync(stateKey, value),
                        topic,
                        // Alternating keys so both partitions are exercised while both are blocked
                        // in XREAD.
                        (i % 2 == 0) ? "even" : "odd",
                        body,
                        "S21b.Outbox",
                        stateKeys: [stateKey],
                        topicOptions: topicOptions)
                    .WaitAsync(HardTimeout);

                id.Should().NotBeNull($"outbox write {i} must commit while the consumer is blocked in XREAD");
            }

            await handler.WaitForAsync(Writes, HardTimeout);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        // The state and the stream agree, read atomically, exactly as in S17.
        var read = fixture.Db.CreateTransaction();
        var counter = read.StringGetAsync(stateKey);
        var p0 = read.StreamLengthAsync(StreamKeys.Stream(topic, 0));
        var p1 = read.StreamLengthAsync(StreamKeys.Stream(topic, 1));

        (await read.ExecuteAsync()).Should().BeTrue();

        ((long?)await counter).Should().Be(Writes);
        ((await p0) + (await p1)).Should().Be(Writes);

        handler.Messages.Should().HaveCount(Writes);
        handler.Messages.Should().AllSatisfy(m => m.Type.Should().Be("S21b.Outbox"));
    }

    /// <summary>
    /// S21b — the guard behind that: handed a database on a consumer's dedicated reader
    /// multiplexer, the outbox refuses rather than issuing a transaction on a read-only connection.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S21b_outbox_refuses_a_reader_multiplexer()
    {
        var topic = fixture.NewTopic();
        var topicOptions = new TopicOptions { Partitions = 1, Trim = TrimMode.None };

        var config = ConfigurationOptions.Parse(fixture.ConnectionString);
        config.ClientName = StreamNames.ReaderClientName("svc", "consumer", 0, "pod-0");

        await using var reader = await ConnectionMultiplexer.ConnectAsync(config);

        var act = async () => await Outbox.WriteAndPublishAsync(
            reader.GetDatabase(),
            _ => { },
            topic,
            "key",
            new byte[4],
            "S21b.Refused",
            topicOptions: topicOptions);

        (await act.Should().ThrowAsync<StreamConfigurationException>())
            .WithMessage("*read-only*", "the message has to say why, or the caller will just retry it");

        (await fixture.Db.StreamLengthAsync(StreamKeys.Stream(topic, 0))).Should().Be(0);
    }

    /// <summary>
    /// Builds a consumer host wired to the fixture's Redis, with its own connection provider
    /// disposed alongside it.
    /// </summary>
    private OwnedHost NewHost(
        string topic,
        TopicOptions topicOptions,
        TestHandler handler,
        Func<ConsumerOptions, ConsumerOptions>? configure = null)
    {
        var options = new StreamOptions
        {
            ConnectionString = fixture.ConnectionString,
            Topics = new Dictionary<string, TopicOptions> { [topic] = topicOptions },
        };

        var consumer = new ConsumerOptions { Topic = topic, BatchSize = 64 };
        consumer = configure is null ? consumer : configure(consumer);

        var connection = new StreamsConnectionProvider(options, null, null);

        var host = new StreamConsumerHost(
            options,
            consumer,
            fixture.NewConsumer(topic),
            ((IBatchHandler)handler).HandleAsync,
            connection);

        return new OwnedHost(host, connection);
    }

    /// <summary>A <see cref="StreamConsumerHost"/> that disposes the connection it was given.</summary>
    private sealed class OwnedHost(StreamConsumerHost host, StreamsConnectionProvider connection) : IAsyncDisposable
    {
        internal bool IsRunning => host.IsRunning;

        internal IReadOnlyList<int> OwnedPartitions => host.OwnedPartitions;

        internal Task StartAsync(CancellationToken ct) => host.StartAsync(ct);

        internal Task StopAsync(CancellationToken ct) => host.StopAsync(ct);

        public async ValueTask DisposeAsync()
        {
            await host.DisposeAsync().ConfigureAwait(false);
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
