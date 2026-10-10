using System.Diagnostics;

using FluentAssertions;

using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Diagnostics;
using RedisEvents.Extensions;
using RedisEvents.Ownership;
using RedisEvents.Producer;
using RedisEvents.Wire;

namespace RedisEvents.Tests;

/// <summary>
/// <c>UnhealthyBehindSeconds</c> against a real Redis and a real <see cref="StreamConsumerHost"/>
/// under Lease ownership: the tail comes from an actual <c>XINFO STREAM</c>, the position from an
/// actual processing loop, and the verdict from <see cref="StreamHealth"/> over the public
/// <see cref="StreamStatus"/> snapshot — the path a worker's watchdog takes.
/// </summary>
/// <remarks>
/// <para>
/// The four cases are the ones the rule exists to tell apart: held behind a moving tail (Unhealthy
/// after the window), behind but advancing (never), caught up and idle (never), and a partition this
/// instance does not own (never — somebody else reads it).
/// </para>
/// <para>
/// <b>The sampler is driven by hand.</b> The host's own sampler reads the tail every 15 seconds,
/// which would make each case a minute long; <see cref="StreamLagSampler.SampleOnceAsync"/> is the
/// same pass, taken when the test asks. The window is seconds for the same reason. The registry is
/// process-wide, so every assertion filters the snapshot to this test's own topic.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class BehindTailHealthServiceTests(RedisStreamsFixture fixture)
{
    private const int WindowSeconds = 3;

    private static readonly TimeSpan Window = TimeSpan.FromSeconds(WindowSeconds);

    [Fact]
    public async Task held_behind_a_moving_tail_is_Unhealthy_after_the_window_and_recovers_when_it_moves()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scenario = await Scenario.StartAsync(fixture, "behind-held", ct);

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        scenario.Handler.OnBatch = async (_, token) => await gate.Task.WaitAsync(token);

        await scenario.PublishAsync(1);
        await scenario.Handler.WaitForAsync(1, TimeSpan.FromSeconds(30), ct);

        // The handler has the first entry and is not returning. The tail keeps moving behind it.
        var first = await scenario.SampleAsync(ct);
        first.Status.Should().NotBe(StreamHealthStatus.Unhealthy, "the partition has only just been seen behind");
        scenario.Partition().IsBehindTail.Should().BeTrue("the tail is an entry the handler has not finished");

        var held = Stopwatch.StartNew();
        StreamHealthReport report = first;

        while (report.Status != StreamHealthStatus.Unhealthy && held.Elapsed < TimeSpan.FromSeconds(30))
        {
            await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
            await scenario.PublishAsync(1);
            report = await scenario.SampleAsync(ct);
        }

        report.Status.Should().Be(StreamHealthStatus.Unhealthy);
        report.Rule.Should().Be(StreamHealthRule.BehindTail);
        report.Description.Should().Contain("behind the tail").And.Contain($"UnhealthyBehindSeconds of {WindowSeconds}");
        held.Elapsed.Should().BeGreaterThanOrEqualTo(Window - TimeSpan.FromMilliseconds(300), "not before the window has passed");
        held.Elapsed.Should().BeLessThan(Window + TimeSpan.FromSeconds(10), "and promptly once it has");

        var frozen = scenario.Partition();
        frozen.TailId.Should().NotBeNull();
        frozen.TailId!.Value.Should().BeGreaterThan(frozen.Position);
        frozen.BehindMs.Should().BeGreaterThanOrEqualTo(Window.TotalMilliseconds);

        // Let it go: the position moves, and the verdict goes with it without anything being reset.
        var published = scenario.Published;
        gate.SetResult();
        await scenario.Handler.WaitForAsync(published, TimeSpan.FromSeconds(30), ct);
        await scenario.WaitUntilCaughtUpAsync(ct);

        (await scenario.SampleAsync(ct)).Status.Should().NotBe(StreamHealthStatus.Unhealthy);
        scenario.Partition().IsBehindTail.Should().BeFalse();
    }

    [Fact]
    public async Task behind_but_advancing_is_never_Unhealthy()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scenario = await Scenario.StartAsync(fixture, "behind-advancing", ct);

        // One entry at a time, 150 ms each: a backlog that takes three windows to drain, during
        // which the consumer is behind the tail the whole time and never once still.
        const int Backlog = 60;
        scenario.Handler.Delay = TimeSpan.FromMilliseconds(150);

        await scenario.PublishAsync(Backlog);

        var draining = Stopwatch.StartNew();
        var sawBehind = 0;

        while (scenario.Handler.Count < Backlog)
        {
            draining.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(90), "the backlog should drain");

            var report = await scenario.SampleAsync(ct);
            report.Status.Should().NotBe(
                StreamHealthStatus.Unhealthy,
                $"a consumer that is advancing is never failed, however far behind ({report.Description})");

            if (scenario.Partition().IsBehindTail)
            {
                sawBehind++;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
        }

        draining.Elapsed.Should().BeGreaterThan(Window, "the case only means something if the backlog outlasted the window");
        sawBehind.Should().BeGreaterThan(4, "and if the partition really was behind the tail while it drained");
    }

    [Fact]
    public async Task caught_up_and_idle_is_never_Unhealthy()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scenario = await Scenario.StartAsync(fixture, "behind-idle", ct);

        await scenario.PublishAsync(3);
        await scenario.Handler.WaitForAsync(3, TimeSpan.FromSeconds(30), ct);
        await scenario.WaitUntilCaughtUpAsync(ct);

        var idle = Stopwatch.StartNew();

        while (idle.Elapsed < Window + TimeSpan.FromSeconds(2))
        {
            var report = await scenario.SampleAsync(ct);
            report.Status.Should().NotBe(StreamHealthStatus.Unhealthy, report.Description);
            scenario.Partition().IsBehindTail.Should().BeFalse("the tail is the entry it last processed");

            await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
        }

        var status = scenario.Partition();
        status.TailId.Should().Be(status.Position);
        status.PositionUnchangedMs.Should().BeGreaterThanOrEqualTo(Window.TotalMilliseconds, "the position really did stand still past the window");
        status.TailUnchangedMs.Should().BeGreaterThanOrEqualTo(Window.TotalMilliseconds, "and so did the tail, which is what a host judging silence reads");
        status.BehindMs.Should().Be(0);
    }

    [Fact]
    public async Task a_partition_leased_by_another_instance_is_never_graded_behind()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic("behind-nonowner");
        var consumer = fixture.NewConsumer("behind-nonowner");

        // Another pod holds the only partition's lease and — being a registry with no reader behind
        // it — never reads a thing, so the stream genuinely runs away from every consumer position.
        await using var otherPod = await fixture.ConnectInstanceAsync("svc-7d4f8b-x2k9p");
        await using var other = new OwnershipRegistry(
            otherPod.Redis,
            new OwnershipRegistryOptions
            {
                Topic = topic,
                Consumer = consumer,
                Partitions = 1,
                PodName = otherPod.PodName,
                InstanceId = otherPod.InstanceId,
                Mode = InstanceMode.Lease,
                TtlSeconds = 30,
                RenewSeconds = 1,
            },
            logger: null);

        await other.RefreshAsync(ct);
        other.Held.Should().Equal(0);

        await using var scenario = await Scenario.StartAsync(fixture, topic, consumer, ct);
        scenario.Host.OwnedPartitions.Should().BeEmpty("the lease is exclusive and somebody else holds it");

        var waiting = Stopwatch.StartNew();

        while (waiting.Elapsed < Window + TimeSpan.FromSeconds(2))
        {
            await scenario.PublishAsync(1);
            await other.RefreshAsync(ct);

            var report = await scenario.SampleAsync(ct);
            report.Status.Should().NotBe(StreamHealthStatus.Unhealthy, report.Description);
            scenario.Partitions().Should().BeEmpty("a partition this instance does not own has no monitor here to grade");

            await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
        }

        scenario.Handler.Count.Should().Be(0, "this instance read nothing — the entries are the owner's to read");
        scenario.Host.OwnedPartitions.Should().BeEmpty();
    }

    [Fact]
    public async Task a_restarted_caught_up_consumer_reports_no_backlog_and_is_not_behind_before_its_first_batch()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic("behind-restart");
        var consumer = fixture.NewConsumer("behind-restart");

        // First life: read everything, persist the position, stop.
        await using (var first = await Scenario.StartAsync(fixture, topic, consumer, ct))
        {
            await first.PublishAsync(5);
            await first.Handler.WaitForAsync(5, TimeSpan.FromSeconds(30), ct);
            await first.WaitUntilCaughtUpAsync(ct);
            await first.Host.StopAsync(ct);
        }

        // Second life: nothing arrives, so nothing is processed and LastProcessed stays at 0-0.
        await using var second = await Scenario.StartAsync(fixture, topic, consumer, ct);
        var report = await second.SampleAsync(ct);

        var status = second.Partition();
        status.LastProcessed.Should().Be(StreamId.Min);
        status.TailId.Should().Be(status.Position, "it resumed from its stored position, which is the last entry");
        status.IsBehindTail.Should().BeFalse("0-0 would be behind every non-empty stream; the resume position is not");
        status.LagEntries.Should().Be(0, "and the five entries it already processed are not a backlog");
        report.Status.Should().NotBe(StreamHealthStatus.Unhealthy);
        second.Handler.Count.Should().Be(0);
    }

    /// <summary>One topic of one partition, one Lease-mode host over it, and a sampler to drive.</summary>
    private sealed class Scenario : IAsyncDisposable
    {
        private readonly RedisStreamsFixture fixture;
        private readonly StreamsConnectionProvider connection;
        private readonly StreamPublisher publisher;
        private readonly StreamLagSampler sampler;
        private readonly string topic;

        private Scenario(
            RedisStreamsFixture fixture,
            string topic,
            StreamsConnectionProvider connection,
            StreamConsumerHost host,
            StreamPublisher publisher,
            TestHandler handler)
        {
            this.fixture = fixture;
            this.topic = topic;
            this.connection = connection;
            this.Host = host;
            this.publisher = publisher;
            this.Handler = handler;
            this.sampler = new StreamLagSampler(fixture.Redis);
        }

        public StreamConsumerHost Host { get; }

        public TestHandler Handler { get; }

        public int Published { get; private set; }

        public static Task<Scenario> StartAsync(RedisStreamsFixture fixture, string hint, CancellationToken ct)
            => StartAsync(fixture, fixture.NewTopic(hint), fixture.NewConsumer(hint), ct);

        public static async Task<Scenario> StartAsync(
            RedisStreamsFixture fixture,
            string topic,
            string consumer,
            CancellationToken ct)
        {
            var topicOptions = new TopicOptions { Partitions = 1 };
            var consumerOptions = new ConsumerOptions
            {
                Topic = topic,
                BatchSize = 1,
                Persist = PersistMode.SyncBatch,
                UnhealthyBehindSeconds = WindowSeconds,
                Instances = new InstanceOptions
                {
                    Mode = InstanceMode.Lease,
                    LeaseTtlSeconds = 30,
                    LeaseRenewSeconds = 1,
                },
            };

            var options = new StreamOptions
            {
                ConnectionString = fixture.ConnectionString,
                Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
            };

            var handler = new TestHandler();
            var connection = new StreamsConnectionProvider(options, services: null, logger: null);
            var host = new StreamConsumerHost(
                options,
                consumerOptions,
                consumer,
                ((IBatchHandler)handler).HandleAsync,
                connection);

            await host.StartAsync(ct);

            return new Scenario(fixture, topic, connection, host, new StreamPublisher(fixture.Db, topic, topicOptions), handler);
        }

        public async Task PublishAsync(int count)
        {
            for (var i = 0; i < count; i++)
            {
                await this.publisher.PublishAsync(
                    "k",
                    System.Text.Encoding.UTF8.GetBytes($"e{this.Published}"),
                    "test.event",
                    new PublishOptions(Partition: 0));

                this.Published++;
            }
        }

        /// <summary>Reads the tail from Redis, as the host's sampler does every 15 seconds, then grades.</summary>
        public async Task<StreamHealthReport> SampleAsync(CancellationToken ct)
        {
            await this.sampler.SampleOnceAsync(ct);

            return StreamHealth.Evaluate(this.Partitions());
        }

        public StreamPartitionStatus[] Partitions()
            => StreamStatus.Partitions().Where(p => p.Topic == this.topic).ToArray();

        public StreamPartitionStatus Partition() => this.Partitions().Should().ContainSingle().Subject;

        /// <summary>Waits for the processed position to reach the stream's real last entry.</summary>
        public async Task WaitUntilCaughtUpAsync(CancellationToken ct)
        {
            var key = StreamKeys.Stream(this.topic, 0, new TopicOptions().CoLocatePartitions);

            await RedisStreamsFixture.WaitUntilAsync(
                async () =>
                {
                    var info = await this.fixture.Db.StreamInfoAsync(key);

                    return StreamId.TryParse(((string?)info.LastEntry.Id).AsSpan(), out var last)
                        && this.Partition().Position == last;
                },
                TimeSpan.FromSeconds(30),
                "the partition's position to reach the last entry in its stream");
        }

        public async ValueTask DisposeAsync()
        {
            await this.Host.DisposeAsync();
            await this.connection.DisposeAsync();
        }
    }
}
