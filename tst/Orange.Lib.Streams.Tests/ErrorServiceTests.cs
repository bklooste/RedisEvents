using System.Collections.Concurrent;
using System.Text;

using DotNet.Testcontainers.Builders;

using FluentAssertions;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Diagnostics;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Positions;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;
using Orange.Lib.Streams.Web;

using StackExchange.Redis;

namespace Orange.Lib.Streams.Tests;

/// <summary>
/// P3-11 and P3-12 — what a real consumer does to a real Redis when a handler throws, when a
/// partition has to stand down, and when Redis itself goes away underneath it.
/// </summary>
/// <remarks>
/// <para>
/// Covers <c>07-testing.md</c> rows S12, S12b, S13, S14, S18 and S20. These are the tests that
/// cannot be written against a scripted fetch delegate: every one of them asserts on something only
/// the real transport produces — a position hash that did or did not move, a consumer group's
/// pending-entries list, a connection that dropped and came back.
/// </para>
/// <para>
/// <b>Two things every test here does.</b> It flushes Redis and clears the process-wide partition
/// monitors first, because <see cref="StreamsHealthCheck"/> aggregates every monitor in the process
/// and a leftover one from an earlier test would answer for this one. And it uses
/// <c>BatchSize = 1</c> wherever a failure has to be attributed to a specific message: with a larger
/// batch, "the handler saw the poison once" and "the batch containing the poison was delivered once"
/// are different statements, and only the first is the behaviour under test.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class ErrorServiceTests(RedisStreamsFixture fixture)
{
    private const string MessageType = "test.event";

    // ------------------------------------------------------------------ S12

    /// <summary>
    /// S12 — an ordinary exception is best effort: logged once, position advanced, next messages
    /// processed, and the library retries nothing.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S12_A_poison_message_is_logged_once_advanced_past_and_never_retried()
    {
        await this.ResetAsync();

        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var topicOptions = new TopicOptions { Partitions = 1 };

        var handler = new TestHandler();
        handler.OnBatch = (batch, _) =>
        {
            if (Bodies(batch).Contains("poison"))
            {
                throw new InvalidOperationException("this message can never be deserialised");
            }

            return ValueTask.CompletedTask;
        };

        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);
        var published = new List<StreamId>();

        foreach (var body in new[] { "one", "two", "poison", "four", "five" })
        {
            published.Add(await publisher.PublishAsync("k", Utf8(body), MessageType, new PublishOptions(Partition: 0)));
        }

        await using var rig = this.Build(
            topic,
            topicOptions,
            new ConsumerOptions
            {
                Topic = topic,
                BatchSize = 1,
                Persist = PersistMode.SyncBatch,
                OnError = ErrorPolicy.BestEffort,
            },
            consumer,
            handler);

        await rig.Host.StartAsync(CancellationToken.None);

        // Exactly five: a library-level retry of the poison would push this to six or more, and the
        // settle window is what turns "at least five" into "five and no more".
        await handler.WaitForExactlyAsync(5, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));

        // The position advances past a best-effort failure, so the messages after the poison are
        // processed rather than the partition wedging on it.
        handler.Bodies.Should().Equal(new[] { "one", "two", "poison", "four", "five" });

        handler.Bodies.Count(static b => b == "poison").Should()
            .Be(1, "the library does not retry an ordinary exception — the handler owns retry");

        rig.Log.Entries(LogLevel.Error, "ErrorPolicy.BestEffort").Should()
            .ContainSingle("the poison is logged once at Error, not once per retry");

        var positions = await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, CancellationToken.None);

        positions.Should().ContainKey(0);
        positions[0].Should().Be(published[^1], "the position advanced past every message, poison included");
    }

    // ----------------------------------------------------------------- S12b

    /// <summary>
    /// S12b — a service's own <see cref="DontIgnoreException"/> subclass blocks its partition,
    /// retries the same batch with backoff, never advances, leaves the other partitions alone, and
    /// drives health Degraded then Unhealthy. On recovery the same batch is processed and the loop
    /// carries on.
    /// </summary>
    /// <remarks>
    /// The exception class is declared in this file on purpose: the requirement is that a
    /// <em>service</em> can derive its own and have the library respect it, which a test using one
    /// of the library's own subclasses would not prove.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S12b_A_custom_DontIgnoreException_blocks_its_partition_and_recovers()
    {
        await this.ResetAsync();

        // The blocking-retry ladder is 1s → 30s; shortening it keeps the test honest (the same code
        // path, the same number of attempts) without spending a real minute on it. Wall-clock block
        // duration, which is what UnhealthyBlockSeconds measures, is untouched by this.
        PartitionWorker.BlockDelay = static (ms, ct) => Task.Delay(Math.Min(ms, 100), ct);

        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var topicOptions = new TopicOptions { Partitions = 2 };

        var downstreamDown = 1;

        var handler = new TestHandler();
        handler.OnBatch = (batch, _) =>
        {
            if (Volatile.Read(ref downstreamDown) == 1 && Bodies(batch).Any(static b => b.StartsWith("p0", StringComparison.Ordinal)))
            {
                throw new PaymentsUnavailableException("the mandatory downstream is unreachable");
            }

            return ValueTask.CompletedTask;
        };

        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);
        var blocked = await publisher.PublishAsync("k", Utf8("p0-a"), MessageType, new PublishOptions(Partition: 0));
        await publisher.PublishAsync("k", Utf8("p1-a"), MessageType, new PublishOptions(Partition: 1));
        await publisher.PublishAsync("k", Utf8("p1-b"), MessageType, new PublishOptions(Partition: 1));

        await using var rig = this.Build(
            topic,
            topicOptions,
            new ConsumerOptions
            {
                Topic = topic,
                BatchSize = 1,
                Persist = PersistMode.SyncBatch,
                OnError = ErrorPolicy.BestEffort,

                // Short enough that the Degraded → Unhealthy transition is observable in a test, and
                // still the same comparison the 300-second default drives in production.
                UnhealthyBlockSeconds = 3,
            },
            consumer,
            handler);

        await rig.Host.StartAsync(CancellationToken.None);

        // Partition 1 is untouched by partition 0's block — the whole point of a loop per partition.
        await RedisStreamsFixture.WaitUntilAsync(
            () => handler.Bodies.Contains("p1-a") && handler.Bodies.Contains("p1-b"),
            TimeSpan.FromSeconds(30),
            "the other partition to keep consuming while partition 0 is blocked");

        // The same batch, handed to the handler again and again: that is the retry.
        await RedisStreamsFixture.WaitUntilAsync(
            () => handler.Bodies.Count(static b => b == "p0-a") >= 3,
            TimeSpan.FromSeconds(30),
            "the blocked batch to be retried");

        // Degraded first, while the block is still young: the Unhealthy transition below is what
        // proves the threshold is applied, so this reading has to be taken before it trips.
        var degraded = await rig.CheckHealthAsync();
        degraded.Status.Should().Be(HealthStatus.Degraded);
        degraded.Data["partitionsBlocked"].Should().Be(1);
        degraded.Description.Should().Contain("blocked retrying a DontIgnoreException");

        rig.Log.Entries(LogLevel.Error, "is now BLOCKED").Should()
            .ContainSingle("the first failure carries the stack; the rest are rate limited");

        var duringBlock = await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, CancellationToken.None);
        duringBlock.Should().NotContainKey(0, "a blocked partition never advances its position");
        duringBlock.Should().ContainKey(1, "the sibling partition is checkpointing normally");

        // ...and Unhealthy once it has been blocked past UnhealthyBlockSeconds.
        await RedisStreamsFixture.WaitUntilAsync(
            async () => (await rig.CheckHealthAsync()).Status == HealthStatus.Unhealthy,
            TimeSpan.FromSeconds(30),
            "health to fail readiness once the block passes UnhealthyBlockSeconds");

        (await rig.CheckHealthAsync()).Description.Should().Contain("UnhealthyBlockSeconds");

        // Recovery: the same batch succeeds, and the partition carries on from where it stopped.
        Volatile.Write(ref downstreamDown, 0);

        await RedisStreamsFixture.WaitUntilAsync(
            async () => (await new RedisPositionStore(fixture.Redis)
                .LoadAsync(topic, consumer, CancellationToken.None)).ContainsKey(0),
            TimeSpan.FromSeconds(30),
            "the recovered batch to advance the position it was holding");

        var afterRecovery = await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, CancellationToken.None);
        afterRecovery[0].Should().Be(blocked, "the blocked batch was processed, not skipped");

        rig.Log.Entries(LogLevel.Information, "recovered after").Should()
            .NotBeEmpty("the recovery is logged as the other half of the block");

        // ...and the loop continues: a message published after the outage flows straight through.
        var next = await publisher.PublishAsync("k", Utf8("p0-b"), MessageType, new PublishOptions(Partition: 0));

        await RedisStreamsFixture.WaitUntilAsync(
            () => handler.Bodies.Contains("p0-b"),
            TimeSpan.FromSeconds(30),
            "the partition to keep consuming after it recovered");

        await RedisStreamsFixture.WaitUntilAsync(
            async () => (await new RedisPositionStore(fixture.Redis)
                .LoadAsync(topic, consumer, CancellationToken.None))[0] == next,
            TimeSpan.FromSeconds(30),
            "the position to follow the recovered partition forward");

        var healthy = await rig.CheckHealthAsync();
        healthy.Data["partitionsBlocked"].Should().Be(0);
        healthy.Data["partitionsStopped"].Should().Be(0);
        healthy.Status.Should().NotBe(HealthStatus.Unhealthy);
    }

    // ------------------------------------------------------------------ S13

    /// <summary>
    /// S13 — <see cref="ErrorPolicy.StopPartition"/>: the failing partition stands down without
    /// advancing, the other three keep consuming, and health reports Degraded.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S13_StopPartition_stands_one_partition_down_and_leaves_the_others_running()
    {
        await this.ResetAsync();

        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var topicOptions = new TopicOptions { Partitions = 4 };

        var handler = new TestHandler();
        handler.OnBatch = (batch, _) =>
        {
            if (Bodies(batch).Any(static b => b.StartsWith("p0", StringComparison.Ordinal)))
            {
                throw new InvalidOperationException("partition 0 is wedged");
            }

            return ValueTask.CompletedTask;
        };

        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);

        await publisher.PublishAsync("k", Utf8("p0-poison"), MessageType, new PublishOptions(Partition: 0));
        await publisher.PublishAsync("k", Utf8("p0-after"), MessageType, new PublishOptions(Partition: 0));

        for (var partition = 1; partition < 4; partition++)
        {
            for (var i = 0; i < 3; i++)
            {
                await publisher.PublishAsync(
                    "k",
                    Utf8($"p{partition}-{i}"),
                    MessageType,
                    new PublishOptions(Partition: partition));
            }
        }

        await using var rig = this.Build(
            topic,
            topicOptions,
            new ConsumerOptions
            {
                Topic = topic,
                BatchSize = 1,
                Persist = PersistMode.SyncBatch,
                OnError = ErrorPolicy.StopPartition,
            },
            consumer,
            handler);

        await rig.Host.StartAsync(CancellationToken.None);

        // The nine messages on partitions 1–3 all arrive, plus the one poison on partition 0.
        await handler.WaitForExactlyAsync(10, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));

        handler.Bodies.Should().Contain("p0-poison");
        handler.Bodies.Should().NotContain(
            "p0-after",
            "a stopped partition processes nothing further, even though its reader had already read it");

        for (var partition = 1; partition < 4; partition++)
        {
            for (var i = 0; i < 3; i++)
            {
                handler.Bodies.Should().Contain($"p{partition}-{i}");
            }
        }

        rig.Log.Entries(LogLevel.Error, "ErrorPolicy.StopPartition").Should().ContainSingle();

        var positions = await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, CancellationToken.None);

        positions.Should().NotContainKey(0, "the stopped partition did not advance past the failing batch");
        positions.Keys.Should().BeEquivalentTo(new[] { 1, 2, 3 }, "the other partitions checkpointed normally");

        var health = await rig.CheckHealthAsync();

        health.Status.Should().Be(HealthStatus.Degraded);
        health.Data["partitionsStopped"].Should().Be(1);
        health.Data["partitionsBlocked"].Should().Be(0);
        health.Description.Should().Contain("stopped");
    }

    // ------------------------------------------------------------------ S14

    /// <summary>
    /// S14 — <see cref="ErrorPolicy.Fail"/> faults the host and does not advance. The blocking retry
    /// of S12b is a different mechanism entirely: that is the exception class, this is the policy.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S14_ErrorPolicy_Fail_faults_the_host_and_does_not_advance()
    {
        await this.ResetAsync();

        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var topicOptions = new TopicOptions { Partitions = 1 };

        var handler = new TestHandler();
        handler.OnBatch = (batch, _) =>
        {
            if (Bodies(batch).Contains("boom"))
            {
                throw new InvalidOperationException("the whole downstream is gone");
            }

            return ValueTask.CompletedTask;
        };

        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);
        await publisher.PublishAsync("k", Utf8("boom"), MessageType, new PublishOptions(Partition: 0));
        await publisher.PublishAsync("k", Utf8("never"), MessageType, new PublishOptions(Partition: 0));

        await using var rig = this.Build(
            topic,
            topicOptions,
            new ConsumerOptions
            {
                Topic = topic,
                BatchSize = 1,
                Persist = PersistMode.SyncBatch,
                OnError = ErrorPolicy.Fail,
            },
            consumer,
            handler);

        await rig.Host.StartAsync(CancellationToken.None);

        await RedisStreamsFixture.WaitUntilAsync(
            () => rig.Log.Entries(LogLevel.Critical, "ErrorPolicy.Fail: faulting the host").Count > 0,
            TimeSpan.FromSeconds(30),
            "the fail policy to log at Critical");

        // The worker task itself faulted — the host reports it through its own continuation, which
        // is how the failure reaches a service that never awaits the worker directly.
        await RedisStreamsFixture.WaitUntilAsync(
            () => rig.Log.Entries(LogLevel.Error, "stopped with an error").Count > 0,
            TimeSpan.FromSeconds(30),
            "the host to report the faulted worker");

        rig.Log.Entries(LogLevel.Error, "stopped with an error")
            .Should()
            .Contain(entry => entry.Exception != null, "the fault carries the original exception");

        var positions = await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, CancellationToken.None);
        positions.Should().NotContainKey(0, "ErrorPolicy.Fail must not advance past the batch it failed on");

        handler.Bodies.Should().NotContain("never", "the worker is gone, so nothing after the failure is processed");

        var health = await rig.CheckHealthAsync();

        health.Status.Should().Be(HealthStatus.Unhealthy);
        health.Description.Should().Contain("stopped");
    }

    // ------------------------------------------------------------------ S18

    /// <summary>
    /// S18a — <c>UseConsumerGroup = true</c>: two instances of one consumer split the load, and
    /// between them see every message exactly once.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S18_Consumer_group_mode_splits_the_load_between_competing_consumers()
    {
        await this.ResetAsync();

        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var topicOptions = new TopicOptions { Partitions = 1 };

        const int total = 30;
        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);

        for (var i = 0; i < total; i++)
        {
            await publisher.PublishAsync("k", Utf8($"m{i}"), MessageType, new PublishOptions(Partition: 0));
        }

        // The gate is what makes the split deterministic rather than a race: the first instance
        // stalls inside its first batch, so the second is guaranteed to take work off it.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gated = 0;

        var handlerA = new TestHandler();
        handlerA.OnBatch = async (_, ct) =>
        {
            if (Interlocked.Exchange(ref gated, 1) == 0)
            {
                await gate.Task.WaitAsync(TimeSpan.FromSeconds(60), ct);
            }
        };

        var handlerB = new TestHandler();

        var options = new ConsumerOptions
        {
            Topic = topic,
            BatchSize = 1,
            UseConsumerGroup = true,
            Backpressure = new BackpressureOptions { Capacity = 1 },
        };

        var previousPod = Environment.GetEnvironmentVariable(InstanceResolver.PodNameVariable);

        try
        {
            // Distinct group members. The name has no trailing "-<n>", so it is not mistaken for a
            // StatefulSet ordinal and both instances still own the whole (single-partition) topic.
            Environment.SetEnvironmentVariable(InstanceResolver.PodNameVariable, "s18a");
            await using var rigA = this.Build(topic, topicOptions, options, consumer, handlerA);
            await rigA.Host.StartAsync(CancellationToken.None);

            await handlerA.WaitForAsync(1, TimeSpan.FromSeconds(30));

            Environment.SetEnvironmentVariable(InstanceResolver.PodNameVariable, "s18b");
            await using var rigB = this.Build(topic, topicOptions, options, consumer, handlerB);
            await rigB.Host.StartAsync(CancellationToken.None);

            await handlerB.WaitForAsync(5, TimeSpan.FromSeconds(30));

            gate.SetResult();

            await RedisStreamsFixture.WaitUntilAsync(
                () => handlerA.Count + handlerB.Count >= total,
                TimeSpan.FromSeconds(60),
                "both group members between them to consume every message");

            handlerA.Count.Should().BeGreaterThan(0);
            handlerB.Count.Should().BeGreaterThan(0);

            var seen = handlerA.Bodies.Concat(handlerB.Bodies).ToArray();

            seen.Should().OnlyHaveUniqueItems("a consumer group delivers each entry to one member only");
            seen.Should().BeEquivalentTo(Enumerable.Range(0, total).Select(static i => $"m{i}"));

            // Every entry acknowledged: XACK is this mode's position advance.
            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await fixture.Db.StreamPendingAsync(
                    StreamKeys.Stream(topic, 0), consumer)).PendingMessageCount == 0,
                TimeSpan.FromSeconds(30),
                "the pending-entries list to drain as batches are acknowledged");
        }
        finally
        {
            Environment.SetEnvironmentVariable(InstanceResolver.PodNameVariable, previousPod);
        }
    }

    /// <summary>
    /// S18b — <c>XAUTOCLAIM</c> recovers entries an instance was delivered and then abandoned
    /// mid-batch, which is the only reason to pay for consumer-group mode in the first place.
    /// </summary>
    /// <remarks>
    /// Driven at the <see cref="ConsumerGroupFetch"/> seam rather than through two hosts, because
    /// the claim threshold is a constructor argument: at its 30-second production default this test
    /// would have to sit out half a minute to prove a rule it can prove in a second.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S18_XAutoClaim_recovers_a_partition_abandoned_mid_batch()
    {
        await this.ResetAsync();

        var topic = fixture.NewTopic();
        var group = fixture.NewConsumer();
        var key = StreamKeys.Stream(topic, 0);
        var db = fixture.Db;

        const int claimMinIdleMs = 300;

        ConsumerGroupFetch Member(string instance) => new(
            db,
            key,
            group,
            instance,
            StartPosition.Beginning,
            batchSize: 10,
            maxIdleDelayMs: 10,
            NullLogger.Instance,
            claimMinIdleMs,
            claimIntervalMs: 10);

        var dying = Member("instance-a");
        await dying.EnsureGroupAsync(CancellationToken.None);

        var publisher = new StreamPublisher(db, topic, new TopicOptions { Partitions = 1 });
        var ids = new List<string>();

        for (var i = 0; i < 5; i++)
        {
            ids.Add((await publisher.PublishAsync("k", Utf8($"m{i}"), MessageType, new PublishOptions(Partition: 0)))
                .Format());
        }

        var delivered = await dying.FetchAsync(CancellationToken.None);

        delivered.Count.Should().Be(5, "the first instance was handed the whole batch");

        // ...and then it dies, without acknowledging any of it.
        var pending = await db.StreamPendingAsync(key, group);
        pending.PendingMessageCount.Should().Be(5);

        var survivor = Member("instance-b");
        StreamEntryBatch claimed = default;

        await RedisStreamsFixture.WaitUntilAsync(
            async () =>
            {
                claimed = await survivor.FetchAsync(CancellationToken.None);
                return !claimed.IsEmpty;
            },
            TimeSpan.FromSeconds(30),
            "the surviving instance to claim the abandoned entries");

        claimed.Span.ToArray().Select(static e => (string?)e.Id).Should()
            .BeEquivalentTo(ids, "XAUTOCLAIM moves the abandoned entries, unchanged, into the survivor's PEL");

        var afterClaim = await db.StreamPendingAsync(key, group);
        afterClaim.Consumers.Should().Contain(c => c.Name == "instance-b" && c.PendingMessageCount == 5);

        // The survivor processes and acknowledges them, and the backlog is finally gone.
        var last = StreamId.TryParse(((string?)claimed.Span[^1].Id).AsSpan(), out var parsed) ? parsed : StreamId.Max;
        await survivor.AckAsync(last, CancellationToken.None);

        (await db.StreamPendingAsync(key, group)).PendingMessageCount.Should().Be(0);
    }

    // ------------------------------------------------------------------ S20

    /// <summary>
    /// S20 — Redis is paused and resumed mid-run: the consumer reports the outage, reconnects, and
    /// resumes from the cursor it was holding, with nothing lost past the flush window.
    /// </summary>
    /// <remarks>
    /// This test runs against a Redis container of its own. Pausing the shared fixture's container
    /// would stall every other service test in the collection, and the failure would look like a
    /// timeout in whichever test happened to be running rather than like this one.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S20_The_consumer_reconnects_and_resumes_after_Redis_is_paused()
    {
        StreamLag.Clear();

        var container = new ContainerBuilder()
            .WithImage(RedisStreamsFixture.Image)
            .WithPortBinding(6379, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(6379))
            .Build();

        await container.StartAsync();

        try
        {
            var endpoint = $"{container.Hostname}:{container.GetMappedPublicPort(6379)}";
            var connectionString = $"{endpoint},abortConnect=false";

            await using var side = await ConnectionMultiplexer.ConnectAsync(connectionString);

            var topic = fixture.NewTopic();
            var consumer = fixture.NewConsumer();
            var topicOptions = new TopicOptions { Partitions = 1 };
            var publisher = new StreamPublisher(side.GetDatabase(), topic, topicOptions);

            var handler = new TestHandler();

            for (var i = 0; i < 20; i++)
            {
                await publisher.PublishAsync("k", Utf8($"pre-{i}"), MessageType, new PublishOptions(Partition: 0));
            }

            await using var rig = this.Build(
                topic,
                topicOptions,
                new ConsumerOptions
                {
                    Topic = topic,
                    BatchSize = 10,

                    // A short block keeps the client-side timeout (BlockMs + 5s) short too, so the
                    // outage is detected in seconds rather than in the default's tens of them.
                    BlockMs = 500,
                    Persist = PersistMode.SyncBatch,
                },
                consumer,
                handler,
                connectionString);

            await rig.Host.StartAsync(CancellationToken.None);
            await handler.WaitForAsync(20, TimeSpan.FromSeconds(30));

            await container.PauseAsync();

            // The reader's in-flight XREAD times out, and the loop treats it as an outage to wait
            // out rather than as a fault that ends the partition.
            await RedisStreamsFixture.WaitUntilAsync(
                () => rig.Log.Entries(LogLevel.Error, "transient outage").Count > 0,
                TimeSpan.FromSeconds(60),
                "the reader to report the outage rather than dying of it");

            (await rig.CheckHealthAsync()).Status.Should()
                .NotBe(HealthStatus.Healthy, "a consumer that cannot reach Redis is not healthy");

            await container.UnpauseAsync();

            await RedisStreamsFixture.WaitUntilAsync(
                () => rig.Log.Entries(LogLevel.Information, "recovered; the read resumed").Count > 0,
                TimeSpan.FromSeconds(60),
                "the reader to reconnect");

            for (var i = 0; i < 20; i++)
            {
                await publisher.PublishAsync("k", Utf8($"post-{i}"), MessageType, new PublishOptions(Partition: 0));
            }

            await handler.WaitForAsync(40, TimeSpan.FromSeconds(60));

            handler.Bodies.Should().OnlyHaveUniqueItems("resuming from the held cursor redelivers nothing already advanced past");
            handler.Bodies.Should().BeEquivalentTo(
                Enumerable.Range(0, 20).Select(static i => $"pre-{i}")
                    .Concat(Enumerable.Range(0, 20).Select(static i => $"post-{i}")),
                "nothing published across the outage was lost");

            var positions = await new RedisPositionStore(side).LoadAsync(topic, consumer, CancellationToken.None);
            positions.Should().ContainKey(0);

            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await rig.CheckHealthAsync()).Data["partitionsBlocked"] is 0,
                TimeSpan.FromSeconds(30),
                "health to clear the block once the reader is reading again");

            (await rig.CheckHealthAsync()).Status.Should().NotBe(HealthStatus.Unhealthy);
        }
        finally
        {
            await container.DisposeAsync();
        }
    }

    // --------------------------------------------------------------- helpers

    private static ReadOnlyMemory<byte> Utf8(string text) => Encoding.UTF8.GetBytes(text);

    private static string[] Bodies(ReadOnlyMemory<StreamMsg> batch)
    {
        var span = batch.Span;
        var bodies = new string[span.Length];

        for (var i = 0; i < span.Length; i++)
        {
            bodies[i] = Encoding.UTF8.GetString(span[i].Body.Span);
        }

        return bodies;
    }

    /// <summary>
    /// A clean Redis and a clean monitor registry. The second half matters as much as the first:
    /// <see cref="StreamsHealthCheck"/> answers for every partition monitor in the process, so a
    /// monitor left behind by an earlier test would be counted in this one's health.
    /// </summary>
    private async Task ResetAsync()
    {
        await fixture.FlushAllAsync();
        StreamLag.Clear();
    }

    private Rig Build(
        string topic,
        TopicOptions topicOptions,
        ConsumerOptions consumer,
        string consumerName,
        TestHandler handler,
        string? connectionString = null)
    {
        var options = new StreamOptions
        {
            ConnectionString = connectionString ?? fixture.ConnectionString,
            Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
        };

        var log = new CapturingLogger();
        var connection = new StreamsConnectionProvider(options, services: null, log);

        var host = new StreamConsumerHost(
            options,
            consumer with { Topic = topic },
            consumerName,
            ((IBatchHandler)handler).HandleAsync,
            connection,
            log);

        return new Rig(host, connection, log);
    }

    /// <summary>A service's own un-skippable failure, which is the whole point of S12b.</summary>
    private sealed class PaymentsUnavailableException(string message) : DontIgnoreException(message);

    /// <summary>One started consumer and everything a test needs to interrogate it.</summary>
    private sealed class Rig(
        StreamConsumerHost host,
        StreamsConnectionProvider connection,
        CapturingLogger log) : IAsyncDisposable
    {
        internal StreamConsumerHost Host { get; } = host;

        internal CapturingLogger Log { get; } = log;

        /// <summary>Runs the readiness check exactly as the framework would, over this host.</summary>
        internal async Task<HealthCheckResult> CheckHealthAsync()
            => await new StreamsHealthCheck(connection, [this.Host])
                .CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        public async ValueTask DisposeAsync()
        {
            try
            {
                await this.Host.DisposeAsync();
            }
            finally
            {
                await connection.DisposeAsync();
            }
        }
    }

    /// <summary>One captured log line.</summary>
    private sealed record LogEntry(LogLevel Level, string Message, Exception? Exception);

    /// <summary>
    /// The logger the consumer host is built with, so a test can assert on what an operator would
    /// actually see — which for the error paths is the behaviour, not just a side effect of it.
    /// </summary>
    private sealed class CapturingLogger : ILogger
    {
        private readonly ConcurrentQueue<LogEntry> entries = new();

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);
            this.entries.Enqueue(new LogEntry(logLevel, formatter(state, exception), exception));
        }

        /// <summary>Every captured line at <paramref name="level"/> whose text contains <paramref name="contains"/>.</summary>
        internal IReadOnlyList<LogEntry> Entries(LogLevel level, string contains)
            => this.entries
                .Where(e => e.Level == level && e.Message.Contains(contains, StringComparison.Ordinal))
                .ToArray();

        private sealed class NullScope : IDisposable
        {
            internal static readonly NullScope Instance = new();

            public void Dispose()
            {
            }
        }
    }
}
