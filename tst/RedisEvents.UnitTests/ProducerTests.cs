using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using RedisEvents.Config;
using RedisEvents.Errors;
using RedisEvents.Producer;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// Producer unit tests (P2-12): routing, the buffered pump's flush triggers, the copy that
/// separates the buffered path from the direct one, flush ordering, drop accounting, and the
/// outbox's up-front slot validation and condition-failure contract.
/// </summary>
/// <remarks>
/// No Redis and no Docker. The direct publisher runs against a <see cref="DispatchProxy"/> stand-in
/// for <see cref="IDatabase"/> that records the raw <c>XADD</c> argument list; the buffered
/// publisher runs against a hand-written <see cref="IStreamPublisher"/>, which is the seam the pump
/// actually writes through.
/// </remarks>
public class ProducerTests
{
    private const string Topic = "producer-tests";

    // ---------------------------------------------------------------------------------------
    // Routing
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// An explicit <see cref="PublishOptions.Partition"/> wins outright: the key is still written to
    /// the entry (consumers see it) but it does not choose the stream.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ExplicitPartitionOverridesKeyHashing()
    {
        var redis = new RecordingDatabase();
        var publisher = new StreamPublisher(redis.Database, Topic, new TopicOptions { Partitions = 8 });

        const string key = "customer-42";

        // First establish where the key would go on its own...
        await publisher.PublishAsync(key, Body("a"), "T");
        var hashed = redis.LastStreamKey;

        // ...then pick every other partition explicitly and confirm the key is ignored for routing.
        for (var p = 0; p < 8; p++)
        {
            await publisher.PublishAsync(key, Body("a"), "T", new PublishOptions(Partition: p));
            redis.LastStreamKey.Should().Be($"{KeyNamespace.Prefix()}s:{{{Topic}}}:{p}");
        }

        // The hashed partition is deterministic, so the loop above genuinely overrode it at least once.
        hashed.Should().StartWith($"{KeyNamespace.Prefix()}s:{{{Topic}}}:");

        // The partition key still travels on the entry even when it did not pick the partition.
        await publisher.PublishAsync(key, Body("a"), "T", new PublishOptions(Partition: 3));
        redis.LastFields[EntryCodec.PartitionKeyField].Should().Be(key);
    }

    /// <summary>An explicit partition outside the topic's range is a bug, not a routing hint.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ExplicitPartitionOutsideRangeThrows()
    {
        var redis = new RecordingDatabase();
        var publisher = new StreamPublisher(redis.Database, Topic, new TopicOptions { Partitions = 4 });

        var act = async () => await publisher.PublishAsync("k", Body("a"), "T", new PublishOptions(Partition: 4));
        await act.Should().ThrowAsync<ArgumentOutOfRangeException>();

        var negative = async () => await publisher.PublishAsync("k", Body("a"), "T", new PublishOptions(Partition: -1));
        await negative.Should().ThrowAsync<ArgumentOutOfRangeException>();
    }

    /// <summary>The buffered path routes identically, so a message keeps its partition either way.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task BufferedExplicitPartitionOverridesKeyHashing()
    {
        var inner = new RecordingPublisher();
        await using var buffered = new BufferedStreamPublisher(
            inner,
            new ProducerOptions { Topic = Topic, MaxBatch = 1, MaxWaitMs = 0 },
            new TopicOptions { Partitions = 8 });

        await buffered.EnqueueAsync("customer-42", Body("a"), "T", new PublishOptions(Partition: 6));
        await buffered.FlushAsync();

        inner.Sent.Should().ContainSingle();
        inner.Sent[0].Options.Partition.Should().Be(6);
    }

    // ---------------------------------------------------------------------------------------
    // Buffered flush triggers
    // ---------------------------------------------------------------------------------------

    /// <summary>A batch fires as soon as <see cref="ProducerOptions.MaxBatch"/> entries are in hand.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task FlushFiresAtMaxBatchWithoutWaitingForTheTimer()
    {
        var inner = new RecordingPublisher();

        // A wait long enough that only the size trigger can explain a flush inside the timeout.
        await using var buffered = new BufferedStreamPublisher(
            inner,
            new ProducerOptions { Topic = Topic, MaxBatch = 4, MaxWaitMs = 60_000 });

        for (var i = 0; i < 3; i++)
        {
            await buffered.EnqueueAsync("k", Body($"m{i}"), "T");
        }

        // Three of four: the timer is 60 s away, so nothing may go out.
        await Task.Delay(200);
        inner.Sent.Should().BeEmpty("MaxBatch was not reached and MaxWaitMs is 60 s away");

        await buffered.EnqueueAsync("k", Body("m3"), "T");

        await WaitUntil(() => inner.Sent.Count == 4, TimeSpan.FromSeconds(5));
        inner.Sent.Should().HaveCount(4);
    }

    /// <summary>
    /// The time trigger is measured from the <em>oldest</em> pending entry, not from the last
    /// arrival. A steady trickle slower than the pump therefore still flushes on schedule; were the
    /// timer restarted per arrival it would be pushed out indefinitely.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task FlushFiresAtMaxWaitMeasuredFromTheOldestEntry()
    {
        var inner = new RecordingPublisher();
        const int maxWaitMs = 150;
        const int trickleMs = 50;

        await using var buffered = new BufferedStreamPublisher(
            inner,
            new ProducerOptions { Topic = Topic, MaxBatch = 10_000, MaxWaitMs = maxWaitMs });

        using var stop = new CancellationTokenSource();
        var started = Stopwatch.GetTimestamp();

        // A trickle that never reaches MaxBatch and never pauses: only an oldest-anchored timer can
        // fire while it is running.
        var trickle = Task.Run(
            async () =>
            {
                var i = 0;
                while (!stop.IsCancellationRequested)
                {
                    await buffered.EnqueueAsync("k", Body($"m{i++}"), "T", ct: CancellationToken.None);
                    try
                    {
                        await Task.Delay(trickleMs, stop.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                }
            },
            CancellationToken.None);

        await WaitUntil(() => inner.Sent.Count > 0, TimeSpan.FromSeconds(5));

        var elapsedMs = Stopwatch.GetElapsedTime(started).TotalMilliseconds;

        await stop.CancelAsync();
        await trickle;

        inner.Sent.Should().NotBeEmpty("the timer is anchored on the oldest entry, so the trickle cannot defer it");

        // Generous upper bound: the point is that it fired at all while entries kept arriving, and
        // that it did so on roughly the oldest entry's clock rather than after N × trickle.
        elapsedMs.Should().BeLessThan(2_000);

        // A per-arrival timer would have accumulated the whole trickle; an oldest-anchored one lets
        // only about MaxWaitMs / trickleMs entries build up before going.
        inner.Sent.Count.Should().BeLessThan(20);
    }

    // ---------------------------------------------------------------------------------------
    // THE copy — the difference between the two publishers
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// The buffered contract: once <see cref="IStreamBufferedPublisher.EnqueueAsync"/> returns, the
    /// caller's buffer is the caller's again. The pump reads its own pooled copy, so overwriting the
    /// original cannot corrupt the published bytes.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task BufferedEnqueueCopiesSoPostReturnMutationCannotCorruptThePublishedBytes()
    {
        // The inner publisher does not look at the bytes until released, which guarantees the
        // mutation below happens strictly before the pump's read — the case a copy must survive.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inner = new RecordingPublisher { Gate = gate.Task };

        await using var buffered = new BufferedStreamPublisher(
            inner,
            new ProducerOptions { Topic = Topic, MaxBatch = 1, MaxWaitMs = 0 });

        var caller = Encoding.UTF8.GetBytes("the original payload");
        var expected = caller.ToArray();

        await buffered.EnqueueAsync("k", caller, "T");

        // The caller reuses its buffer immediately, exactly as a hot loop would.
        caller.AsSpan().Fill((byte)'Z');

        gate.SetResult();
        await buffered.FlushAsync();

        inner.Sent.Should().ContainSingle();
        inner.Sent[0].Body.Should().Equal(expected, "the buffered path copies the body at enqueue time");
    }

    /// <summary>
    /// The direct contract, stated as a test so the asymmetry is not folklore: the non-buffered path
    /// hands Redis the caller's memory by reference. The body must stay valid until the returned
    /// task completes, and the entry the command carried aliases the caller's array.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task DirectPublishDoesNotCopyTheBody()
    {
        var redis = new RecordingDatabase();
        var publisher = new StreamPublisher(redis.Database, Topic, new TopicOptions { Partitions = 1 });

        var caller = Encoding.UTF8.GetBytes("the original payload");
        await publisher.PublishAsync("k", caller, "T");

        // Same length, different content: aliasing shows through, a copy would not.
        caller.AsSpan().Fill((byte)'Z');

        var onTheWire = redis.LastBody;
        onTheWire.Should().Equal(caller, "the direct path passes the body by reference and never copies it");
        onTheWire.Should().NotEqual(Encoding.UTF8.GetBytes("the original payload"));
    }

    // ---------------------------------------------------------------------------------------
    // FlushAsync
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// <see cref="IStreamBufferedPublisher.FlushAsync"/> is a barrier: it completes only once every
    /// message enqueued ahead of it has been written.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task FlushCompletesOnlyAfterEverythingEnqueuedBeforeItIsWritten()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inner = new RecordingPublisher { Gate = gate.Task };

        await using var buffered = new BufferedStreamPublisher(
            inner,
            // Big enough that only the barrier can trigger the flush.
            new ProducerOptions { Topic = Topic, MaxBatch = 1_000, MaxWaitMs = 60_000 });

        const int count = 10;
        for (var i = 0; i < count; i++)
        {
            await buffered.EnqueueAsync("k", Body($"m{i}"), "T");
        }

        var flush = buffered.FlushAsync().AsTask();

        await Task.Delay(200);
        flush.IsCompleted.Should().BeFalse("the writes behind the barrier have not completed yet");
        inner.Sent.Should().BeEmpty();

        gate.SetResult();
        await flush.WaitAsync(TimeSpan.FromSeconds(5));

        inner.Sent.Should().HaveCount(count, "the barrier covers exactly what went in ahead of it");
        buffered.PublishedCount.Should().Be(count);
    }

    // ---------------------------------------------------------------------------------------
    // Backpressure and drops
    // ---------------------------------------------------------------------------------------

    /// <summary>Waiting for space, not dropping, is the default: a full queue slows the producer.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task FullQueueWaitsByDefaultAndDropsNothing()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inner = new RecordingPublisher { Gate = gate.Task };

        var options = new ProducerOptions { Topic = Topic, MaxBatch = 1, MaxQueue = 2, MaxWaitMs = 0 };
        options.DropOldest.Should().BeFalse("DropOldest must be opt-in — it sheds data");

        await using var buffered = new BufferedStreamPublisher(inner, options);

        try
        {
            // The pump takes one entry and parks in the gated publish, so the queue fills and stays full.
            var producer = Task.Run(async () =>
            {
                for (var i = 0; i < 50; i++)
                {
                    await buffered.EnqueueAsync("k", Body($"m{i}"), "T");
                }
            });

            await Task.Delay(300);

            producer.IsCompleted.Should().BeFalse("a full queue must block the producer rather than grow or shed");
            buffered.DroppedCount.Should().Be(0);
        }
        finally
        {
            gate.SetResult();
        }
    }

    /// <summary>
    /// <see cref="ProducerOptions.DropOldest"/> sheds instead of blocking, and every shed entry is
    /// counted — the whole point of the mode being explicit.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task DropOldestShedsAndCountsInsteadOfBlocking()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inner = new RecordingPublisher { Gate = gate.Task };

        await using var buffered = new BufferedStreamPublisher(
            inner,
            new ProducerOptions
            {
                Topic = Topic,
                MaxBatch = 1,
                MaxQueue = 2,
                MaxWaitMs = 0,
                DropOldest = true,
            });

        try
        {
            const int enqueued = 50;
            var producer = Task.Run(async () =>
            {
                for (var i = 0; i < enqueued; i++)
                {
                    await buffered.EnqueueAsync("k", Body($"m{i}"), "T");
                }
            });

            // Nothing blocks: the queue sheds rather than waiting on the parked pump.
            await producer.WaitAsync(TimeSpan.FromSeconds(5));

            buffered.DroppedCount.Should().BeGreaterThan(0, "the queue held 2 of 50 while the pump was parked");
            buffered.DroppedCount.Should().BeLessThan(enqueued);
        }
        finally
        {
            gate.SetResult();
        }
    }

    // ---------------------------------------------------------------------------------------
    // R-13 — flush failure attribution, bounded shutdown, and the DropOldest hole
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// R-13. One failed <c>XADD</c> in a pipelined flush must fault only the waiters whose own
    /// entries failed. Faulting every concurrent waiter — which is what the pump used to do — tells
    /// a caller whose message landed that it did not, and there is nothing it can do with that.
    /// </summary>
    /// <remarks>
    /// The batch is built deterministically: the pump is parked inside a gated publish while the
    /// entries and the two flush markers are queued behind it, so they are all read into the same
    /// batch in a known order. <c>flushA</c> is queued after A only; <c>flushB</c> after A and B,
    /// and B is the entry that fails.
    /// </remarks>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task FlushFaultsOnlyTheWaitersWhoseOwnEntriesFailed()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var boom = new InvalidOperationException("XADD rejected");
        var inner = new FailingPublisher(body => body == "B" ? boom : null) { Gate = gate.Task };

        await using var buffered = new BufferedStreamPublisher(
            inner,
            new ProducerOptions { Topic = Topic, MaxBatch = 16, MaxQueue = 64, MaxWaitMs = 0 },
            new TopicOptions { Partitions = 1 });

        // Park the pump: it takes this entry on its own (MaxWaitMs 0 flushes immediately) and then
        // blocks inside the gated publish, so everything below queues up behind it.
        await buffered.EnqueueAsync("k", Body("warm"), "T");
        await WaitUntil(() => inner.Started == 1, TimeSpan.FromSeconds(5));

        await buffered.EnqueueAsync("k", Body("A"), "T");
        var flushA = buffered.FlushAsync();

        await buffered.EnqueueAsync("k", Body("B"), "T");
        var flushB = buffered.FlushAsync();

        await buffered.EnqueueAsync("k", Body("C"), "T");

        gate.SetResult();

        // A landed, so the flush queued directly behind it succeeds...
        await flushA.AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        // ...while the flush that also covered B gets B's exception, not a generic one.
        var faulted = async () => await flushB;
        (await faulted.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(boom);

        // warm, A and C landed; only B did not.
        buffered.PublishedCount.Should().Be(3);
    }

    /// <summary>
    /// R-13. <see cref="BufferedStreamPublisher.StopAsync"/> drains on a bound: a pump wedged in a
    /// publish that never returns must not hold the pod open, and what it gave up on has to be said
    /// out loud, because that log line is the only trace those messages leave.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task StopAsyncBoundsTheShutdownFlushAndReportsWhatItAbandoned()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inner = new FailingPublisher(_ => null) { Gate = gate.Task };
        var log = new ListLogger();

        var buffered = new BufferedStreamPublisher(
            inner,
            new ProducerOptions { Topic = Topic, MaxBatch = 1, MaxQueue = 64, MaxWaitMs = 0 },
            new TopicOptions { Partitions = 1 },
            log,
            TimeSpan.FromMilliseconds(250));

        try
        {
            for (var i = 0; i < 20; i++)
            {
                await buffered.EnqueueAsync("k", Body($"m{i}"), "T");
            }

            await WaitUntil(() => inner.Started == 1, TimeSpan.FromSeconds(5));

            var started = Stopwatch.GetTimestamp();
            await buffered.StopAsync(CancellationToken.None);
            var elapsed = Stopwatch.GetElapsedTime(started);

            elapsed.Should().BeLessThan(
                TimeSpan.FromSeconds(5),
                "the drain is bounded by the shutdown flush timeout, not by the wedged publish");

            log.Entries.Should().Contain(
                e => e.Level == LogLevel.Warning && e.Message.Contains("abandoned", StringComparison.Ordinal),
                "abandoned entries are lost, and the Warning naming the count is the only record of it");
        }
        finally
        {
            gate.SetResult();
        }
    }

    /// <summary>
    /// R-13. Under <see cref="ProducerOptions.DropOldest"/> the queue can shed the flush marker
    /// itself. A shed marker covered nothing, so completing it would tell the caller its queue is in
    /// Redis when none of it is; it faults instead.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ADroppedFlushMarkerFaultsRatherThanClaimingTheQueueWasWritten()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inner = new FailingPublisher(_ => null) { Gate = gate.Task };

        await using var buffered = new BufferedStreamPublisher(
            inner,
            new ProducerOptions
            {
                Topic = Topic,
                MaxBatch = 1,
                MaxQueue = 1,
                MaxWaitMs = 0,
                DropOldest = true,
            },
            new TopicOptions { Partitions = 1 });

        try
        {
            // Park the pump so the one queue slot stays occupied by whatever is written next.
            await buffered.EnqueueAsync("k", Body("warm"), "T");
            await WaitUntil(() => inner.Started == 1, TimeSpan.FromSeconds(5));

            var flush = buffered.FlushAsync();

            // The queue is full and holds only the marker, so this shed is the marker's.
            for (var i = 0; i < 5; i++)
            {
                await buffered.EnqueueAsync("k", Body($"m{i}"), "T");
            }

            var faulted = async () => await flush;
            await faulted.Should().ThrowAsync<InvalidOperationException>()
                .WithMessage("*DropOldest*", "the caller has to be told why its flush promised nothing");
        }
        finally
        {
            gate.SetResult();
        }
    }

    // ---------------------------------------------------------------------------------------
    // R-14 — the direct publisher
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// R-14. A pipelined batch that half-lands counted nothing at all towards
    /// <c>streams.published</c>, so a partial outage read as a total one on the dashboard. The
    /// successes are counted, and the exception says how many did not land.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task APartiallyFailedBatchStillCountsTheEntriesThatLanded()
    {
        var redis = new RecordingDatabase { FailFrom = 4 };
        var publisher = new StreamPublisher(redis.Database, Topic, new TopicOptions { Partitions = 1 });

        using var published = new PublishedProbe();

        ReadOnlyMemory<byte>[] bodies = [Body("a"), Body("b"), Body("c"), Body("d"), Body("e")];

        var act = async () => await publisher.PublishBatchAsync("k", bodies, "T");

        (await act.Should().ThrowAsync<StreamTransportException>())
            .WithMessage("*2 message(s)*", "the message reports what did not land, not the batch size");

        published.TotalFor(Topic).Should().Be(3, "three XADDs were accepted before the connection gave way");
    }

    /// <summary>
    /// R-14. The publisher wraps more than <see cref="RedisException"/>. A multiplexer disposed under
    /// a publish throws <see cref="ObjectDisposedException"/>, which used to escape unwrapped past
    /// every caller catching the documented <see cref="StreamTransportException"/>.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ANonRedisFailureIsStillWrappedAsATransportFailure()
    {
        var redis = new RecordingDatabase { FailWith = new ObjectDisposedException("multiplexer") };
        var publisher = new StreamPublisher(redis.Database, Topic, new TopicOptions { Partitions = 1 });

        var act = async () => await publisher.PublishAsync("k", Body("a"), "T");

        var thrown = await act.Should().ThrowAsync<StreamTransportException>();
        thrown.Which.InnerException.Should().BeOfType<ObjectDisposedException>();
    }

    /// <summary>
    /// R-14. An <see cref="IDatabase"/> that cannot produce its multiplexer — only test stand-ins —
    /// must still publish rather than fail on the reconcile it cannot run. This is the guard that
    /// keeps the reconcile from being a breaking change to the database-shaped constructor.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task APublisherOverAStandInDatabasePublishesWithoutReconciling()
    {
        var redis = new RecordingDatabase();
        var publisher = new StreamPublisher(redis.Database, Topic, new TopicOptions { Partitions = 1 });

        await publisher.EnsureTopicAsync();

        _ = await publisher.PublishAsync("k", Body("a"), "T");
        redis.LastStreamKey.Should().Be($"{KeyNamespace.Prefix()}s:{{{Topic}}}:0");
    }

    // ---------------------------------------------------------------------------------------
    // Outbox
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// A state key that would hash to another slot is rejected up front, with a message naming both
    /// keys — on a single node it would work and only fail once the deployment became a cluster.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void OutboxRejectsAStateKeyInAnotherSlot()
    {
        var act = () => Outbox.EnsureSameSlot("bets", new RedisKey[] { "customer:123" });

        act.Should().Throw<StreamConfigurationException>()
            .WithMessage("*customer:123*")
            .WithMessage("*bets*");

        // A key tagged on the topic shares the stream's slot and is accepted.
        var tagged = () => Outbox.EnsureSameSlot("bets", new[] { Outbox.StateKey("bets", "123") });
        tagged.Should().NotThrow();

        Outbox.StateKey("bets", "123").ToString().Should().Be($"{KeyNamespace.Prefix()}{{bets}}:state:123");

        // An empty declared key is a mistake rather than "no key".
        var empty = () => Outbox.EnsureSameSlot("bets", new RedisKey[] { default });
        empty.Should().Throw<StreamConfigurationException>();
    }

    /// <summary>
    /// The same check runs inside the publish call, so a bad key never reaches the wire. Nothing is
    /// queued: the transaction is not even created.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task OutboxPublishRejectsACrossSlotStateKeyBeforeQueueingAnything()
    {
        var redis = new FakeTransactionalDatabase(commits: false);

        var act = async () => await Outbox.WriteAndPublishAsync(
            redis.Database,
            _ => { },
            "bets",
            "customer-1",
            Body("x"),
            "BetPlaced",
            stateKeys: new RedisKey[] { "customer:1" });

        await act.Should().ThrowAsync<StreamConfigurationException>();

        redis.TransactionsCreated.Should().Be(0, "the check runs before anything is built");
    }

    /// <summary>
    /// A failed <c>WATCH</c> condition is a normal outcome, not an error: nothing was applied and the
    /// call returns <see langword="null"/>. Not <see langword="false"/>, not an exception.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task OutboxReturnsNullWhenAConditionFails()
    {
        var redis = new FakeTransactionalDatabase(commits: false);
        var stateWritten = false;

        var id = await Outbox.WriteAndPublishAsync(
            redis.Database,
            tran =>
            {
                stateWritten = true;
                _ = tran.StringSetAsync(Outbox.StateKey("bets", "1"), "v");
            },
            "bets",
            "customer-1",
            Body("x"),
            "BetPlaced",
            conditions: new[] { Condition.KeyExists(Outbox.StateKey("bets", "1")) },
            stateKeys: new[] { Outbox.StateKey("bets", "1") });

        id.Should().BeNull("a failed condition applies nothing and is reported as a null id");
        stateWritten.Should().BeTrue("the state writes are queued onto the transaction that then did not execute");
        redis.ConditionsAdded.Should().Be(1);
    }

    /// <summary>The many-publish overload reports a failed condition the same way.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task OutboxManyReturnsNullWhenAConditionFails()
    {
        var redis = new FakeTransactionalDatabase(commits: false);

        var publishes = new[]
        {
            new OutboxPublish("bets", "customer-1", Body("a"), "BetPlaced"),
            new OutboxPublish("bets", "customer-1", Body("b"), "BetMatched"),
        };

        var ids = await Outbox.WriteAndPublishManyAsync(
            redis.Database,
            _ => { },
            publishes,
            conditions: new[] { Condition.KeyExists(Outbox.StateKey("bets", "1")) });

        ids.Should().BeNull();
    }

    /// <summary>A committed transaction hands back the ids Redis assigned.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task OutboxReturnsTheAssignedIdWhenTheTransactionCommits()
    {
        var redis = new FakeTransactionalDatabase(commits: true);

        var id = await Outbox.WriteAndPublishAsync(
            redis.Database,
            _ => { },
            "bets",
            "customer-1",
            Body("x"),
            "BetPlaced");

        id.Should().NotBeNull();
        id!.Value.Should().Be(new StreamId(FakeTransactionalDatabase.IdMs, FakeTransactionalDatabase.IdSeq));
    }

    // ---------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------

    private static ReadOnlyMemory<byte> Body(string text) => Encoding.UTF8.GetBytes(text);

    private static async Task WaitUntil(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = Stopwatch.GetTimestamp() + (long)(Stopwatch.Frequency * timeout.TotalSeconds);
        while (Stopwatch.GetTimestamp() < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(10);
        }
    }

    /// <summary>One publish as the pump handed it to the inner publisher.</summary>
    internal sealed record SentMessage(string PartitionKey, byte[] Body, string Type, PublishOptions Options);

    /// <summary>
    /// The seam the buffered pump writes through: a hand-written <see cref="IStreamPublisher"/> that
    /// records what it was given. <see cref="Gate"/> holds a publish open so a test can act between
    /// the enqueue and the pump's read of the bytes.
    /// </summary>
    internal sealed class RecordingPublisher : IStreamPublisher
    {
        private readonly Lock gate = new();
        private readonly List<SentMessage> sent = [];

        /// <summary>Optional gate awaited before the body is read; unset publishes complete immediately.</summary>
        internal Task? Gate { get; init; }

        internal IReadOnlyList<SentMessage> Sent
        {
            get
            {
                lock (this.gate)
                {
                    return this.sent.ToArray();
                }
            }
        }

        public async ValueTask<StreamId> PublishAsync(
            string partitionKey,
            ReadOnlyMemory<byte> body,
            string type,
            PublishOptions options = default,
            CancellationToken ct = default)
        {
            if (this.Gate is not null)
            {
                await this.Gate.ConfigureAwait(false);
            }

            lock (this.gate)
            {
                this.sent.Add(new SentMessage(partitionKey, body.ToArray(), type, options));
                return new StreamId(1, this.sent.Count);
            }
        }

        public async ValueTask PublishBatchAsync(
            string partitionKey,
            ReadOnlyMemory<ReadOnlyMemory<byte>> bodies,
            string type,
            PublishOptions options = default,
            CancellationToken ct = default)
        {
            for (var i = 0; i < bodies.Length; i++)
            {
                _ = await this.PublishAsync(partitionKey, bodies.Span[i], type, options, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// An <see cref="IStreamPublisher"/> that fails the entries a predicate picks out, so a flush can
    /// be made to half-succeed on demand. <see cref="Gate"/> parks the pump inside a publish, which
    /// is how a test gets several entries and flush markers into one deterministic batch.
    /// </summary>
    internal sealed class FailingPublisher : IStreamPublisher
    {
        private readonly Func<string, Exception?> fails;
        private int started;
        private int id;

        internal FailingPublisher(Func<string, Exception?> fails) => this.fails = fails;

        /// <summary>Optional gate awaited before each publish resolves.</summary>
        internal Task? Gate { get; init; }

        /// <summary>How many publishes have been entered, gate or no gate.</summary>
        internal int Started => Volatile.Read(ref this.started);

        public async ValueTask<StreamId> PublishAsync(
            string partitionKey,
            ReadOnlyMemory<byte> body,
            string type,
            PublishOptions options = default,
            CancellationToken ct = default)
        {
            _ = Interlocked.Increment(ref this.started);

            var text = Encoding.UTF8.GetString(body.Span);

            if (this.Gate is not null)
            {
                await this.Gate.ConfigureAwait(false);
            }

            var failure = this.fails(text);
            return failure is null
                ? new StreamId(1, Interlocked.Increment(ref this.id))
                : throw failure;
        }

        public async ValueTask PublishBatchAsync(
            string partitionKey,
            ReadOnlyMemory<ReadOnlyMemory<byte>> bodies,
            string type,
            PublishOptions options = default,
            CancellationToken ct = default)
        {
            for (var i = 0; i < bodies.Length; i++)
            {
                _ = await this.PublishAsync(partitionKey, bodies.Span[i], type, options, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Captures log entries so a test can assert on level and text.</summary>
    internal sealed class ListLogger : ILogger
    {
        private readonly List<(LogLevel Level, string Message)> entries = [];

        internal IReadOnlyList<(LogLevel Level, string Message)> Entries
        {
            get
            {
                lock (this.entries)
                {
                    return this.entries.ToArray();
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (this.entries)
            {
                this.entries.Add((logLevel, formatter(state, exception)));
            }
        }
    }

    /// <summary>Sums <c>streams.published</c> per topic tag, so a partial batch's count is observable.</summary>
    internal sealed class PublishedProbe : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly Dictionary<string, long> totals = new(StringComparer.Ordinal);

        internal PublishedProbe()
        {
            this.listener.InstrumentPublished = (published, listening) =>
            {
                if (published.Meter.Name == "RedisEvents" &&
                    string.Equals(published.Name, "streams.published", StringComparison.Ordinal))
                {
                    listening.EnableMeasurementEvents(published);
                }
            };

            this.listener.SetMeasurementEventCallback<long>(this.OnMeasurement);
            this.listener.Start();
        }

        internal long TotalFor(string topic)
        {
            lock (this.totals)
            {
                return this.totals.TryGetValue(topic, out var total) ? total : 0L;
            }
        }

        public void Dispose() => this.listener.Dispose();

        private void OnMeasurement(
            Instrument instrument,
            long measurement,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state)
        {
            _ = instrument;
            _ = state;

            foreach (var tag in tags)
            {
                if (!string.Equals(tag.Key, "topic", StringComparison.Ordinal))
                {
                    continue;
                }

                var topic = tag.Value?.ToString() ?? string.Empty;
                lock (this.totals)
                {
                    this.totals[topic] = (this.totals.TryGetValue(topic, out var current) ? current : 0L) + measurement;
                }
            }
        }
    }

    /// <summary>
    /// An <see cref="IDatabase"/> stand-in that records the raw <c>XADD</c> argument list, built with
    /// <see cref="DispatchProxy"/> so no mocking package is needed. Only <c>ExecuteAsync</c> is
    /// implemented; anything else throws, because the direct publisher should not be calling it.
    /// </summary>
    internal sealed class RecordingDatabase
    {
        internal RecordingDatabase()
        {
            var db = DispatchProxy.Create<IDatabase, Proxy>();
            ((Proxy)db).Owner = this;
            this.Database = db;
        }

        internal IDatabase Database { get; }

        /// <summary>One-based index of the first <c>XADD</c> to fail; null never fails.</summary>
        internal int? FailFrom { get; init; }

        /// <summary>The exception a failing <c>XADD</c> throws; defaults to a connection failure.</summary>
        internal Exception? FailWith { get; init; }

        /// <summary>The key of the most recent <c>XADD</c>.</summary>
        internal string LastStreamKey { get; private set; } = string.Empty;

        /// <summary>The most recent entry's fields, keyed by their one-byte names.</summary>
        internal Dictionary<string, string> LastFields { get; } = [];

        /// <summary>
        /// The body value of the most recent <c>XADD</c>, read lazily so the test can observe whether
        /// it aliases the caller's array.
        /// </summary>
        internal byte[] LastBody => (byte[])this.lastBodyValue!;

        private RedisValue lastBodyValue;
        private int executed;

        private object Invoke(MethodInfo method, object?[] args)
        {
            if (method.Name != "ExecuteAsync" || args.Length < 2 || args[1] is not object[] command)
            {
                throw new NotSupportedException(
                    $"RecordingDatabase does not implement {method.Name}; the publish path should not be calling it.");
            }

            this.executed++;

            if (this.FailWith is not null || (this.FailFrom is { } from && this.executed >= from))
            {
                return Task.FromException<RedisResult>(
                    this.FailWith ?? new RedisConnectionException(ConnectionFailureType.SocketFailure, "the connection went away"));
            }

            this.Record(command);
            return Task.FromResult(RedisResult.Create((RedisValue)"1700000000000-7"));
        }

        private void Record(object[] command)
        {
            this.LastStreamKey = ((RedisKey)command[0]).ToString()!;
            this.LastFields.Clear();

            // key [MAXLEN ~ n] * then name/value pairs. The auto-id '*' marks where the fields start.
            var at = 1;
            while (at < command.Length && (RedisValue)command[at] != "*")
            {
                at++;
            }

            for (var i = at + 1; i + 1 < command.Length; i += 2)
            {
                var name = ((RedisValue)command[i]).ToString()!;
                var value = (RedisValue)command[i + 1];

                if (name == EntryCodec.BodyField)
                {
                    this.lastBodyValue = value;
                    continue;
                }

                this.LastFields[name] = value.ToString()!;
            }
        }

        /// <summary>The generated proxy's base; public because <see cref="DispatchProxy"/> subclasses it.</summary>
        public class Proxy : DispatchProxy
        {
            internal RecordingDatabase Owner { get; set; } = null!;

            /// <inheritdoc />
            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            {
                ArgumentNullException.ThrowIfNull(targetMethod);
                return this.Owner.Invoke(targetMethod, args ?? []);
            }
        }
    }

    /// <summary>
    /// An <see cref="IDatabase"/> plus <see cref="ITransaction"/> stand-in for the outbox: it counts
    /// the transactions and conditions it was given and reports whether <c>EXEC</c> committed. A
    /// non-committing transaction cancels its queued command tasks, exactly as SE.Redis does, so the
    /// abandoned-task handling is exercised rather than mocked away.
    /// </summary>
    internal sealed class FakeTransactionalDatabase
    {
        internal const long IdMs = 1_700_000_000_000;
        internal const long IdSeq = 7;

        private readonly bool commits;

        internal FakeTransactionalDatabase(bool commits)
        {
            this.commits = commits;

            var multiplexer = DispatchProxy.Create<IConnectionMultiplexer, Proxy>();
            ((Proxy)multiplexer).Owner = this;

            var db = DispatchProxy.Create<IDatabase, Proxy>();
            ((Proxy)db).Owner = this;
            ((Proxy)db).Multiplexer = multiplexer;

            this.Database = db;
        }

        internal IDatabase Database { get; }

        /// <summary>How many transactions the outbox created.</summary>
        internal int TransactionsCreated { get; private set; }

        /// <summary>How many <c>WATCH</c> conditions were attached.</summary>
        internal int ConditionsAdded { get; private set; }

        private object? Invoke(Proxy proxy, MethodInfo method, object?[] args)
        {
            switch (method.Name)
            {
                case "get_ClientName":
                    // Anything but the reader prefix; the outbox refuses a reader multiplexer.
                    return "streams-sh:tests";

                case "get_Multiplexer":
                    return proxy.Multiplexer
                        ?? throw new NotSupportedException("Multiplexer was asked of the multiplexer proxy itself.");

                case "CreateTransaction":
                    this.TransactionsCreated++;
                    var tran = DispatchProxy.Create<ITransaction, Proxy>();
                    ((Proxy)tran).Owner = this;
                    return tran;

                case "AddCondition":
                    this.ConditionsAdded++;

                    // The outbox discards the result; a failed condition surfaces as a false EXEC.
                    return null;

                case "StringSetAsync":
                    return this.Queued(true);

                case "StreamAddAsync":
                    return this.Queued((RedisValue)$"{IdMs}-{IdSeq}");

                case "ExecuteAsync":
                    return Task.FromResult(this.commits);

                default:
                    throw new NotSupportedException(
                        $"FakeTransactionalDatabase does not implement {method.Name}; the outbox should not be calling it.");
            }
        }

        /// <summary>
        /// A queued command's task: completed on a transaction that will commit, cancelled on one that
        /// will not — which is what SE.Redis does to the commands of an abandoned MULTI.
        /// </summary>
        private Task<T> Queued<T>(T value)
            => this.commits ? Task.FromResult(value) : Task.FromCanceled<T>(new CancellationToken(canceled: true));

        /// <summary>The generated proxy's base; public because <see cref="DispatchProxy"/> subclasses it.</summary>
        public class Proxy : DispatchProxy
        {
            internal FakeTransactionalDatabase Owner { get; set; } = null!;

            internal IConnectionMultiplexer? Multiplexer { get; set; }

            /// <inheritdoc />
            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            {
                ArgumentNullException.ThrowIfNull(targetMethod);
                return this.Owner.Invoke(this, targetMethod, args ?? []);
            }
        }
    }
}
