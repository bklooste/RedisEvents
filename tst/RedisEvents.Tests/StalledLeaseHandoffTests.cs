using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Diagnostics;
using RedisEvents.Extensions;
using RedisEvents.Ownership;
using RedisEvents.Producer;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>
/// The 2026-10-25 offer-odds-projection incident against a real Redis: a <c>Lease</c>-mode owner
/// that cannot make progress must give its partitions up so another instance reads them, and must
/// report Unhealthy so it is restarted.
/// </summary>
/// <remarks>
/// <para>
/// What happened: after an out-of-memory crash, one pod owned both <c>mult_feed_odds</c> partitions
/// and processed nothing for four hours. Its ownership registry kept renewing both leases — the
/// lease script only has to reach Redis, it never asks whether anything behind it is reading — so
/// no other replica could claim them, and the health check said Degraded, so nothing restarted it.
/// </para>
/// <para>
/// Both tests use one real <see cref="StreamConsumerHost"/> or registry for the stuck instance and a
/// second <see cref="OwnershipRegistry"/> with its own instance id, cycled by hand, for the replica
/// that should take over — two hosts in one process would share
/// <see cref="OwnershipRegistry.StableInstanceId"/> and be one claimant (see
/// <see cref="OwnershipLeaseTests"/>).
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class StalledLeaseHandoffTests(RedisStreamsFixture fixture)
{
    private const int LeaseTtlSeconds = 3;

    /// <summary>
    /// The registry half. A quarantined partition is given back at once, never re-claimed while the
    /// quarantine lasts, and the quarantining instance stops counting towards the fair share — or
    /// the replica taking over would stay capped at <c>ceil(partitions / members)</c> and leave the
    /// rest unowned. When the quarantine ends the two settle back to an even split.
    /// </summary>
    [Fact]
    public async Task A_quarantined_lease_is_handed_over_whole_and_not_reclaimed_until_the_quarantine_ends()
    {
        var topic = fixture.NewTopic("stall-quarantine");
        var consumer = fixture.NewConsumer("stall-quarantine");
        var key = StreamKeys.Ownership(topic, consumer);
        var ct = TestContext.Current.CancellationToken;

        await using var podA = await fixture.ConnectInstanceAsync("offer-odds-projection-76d86fc5f8-vr5zj");
        await using var podB = await fixture.ConnectInstanceAsync("offer-odds-projection-76d86fc5f8-q2m7t");

        await using var stuck = Registry(podA, topic, consumer);
        await using var rival = Registry(podB, topic, consumer);

        await stuck.RefreshAsync(ct);
        stuck.Held.Should().Equal([0, 1], "the first instance up claims every free partition");

        // Without a quarantine the stuck instance renews what it holds for as long as it lives; the
        // rival never gets more than the fair share, however long it waits.
        for (var i = 0; i < 3; i++)
        {
            await rival.RefreshAsync(ct);
            await stuck.RefreshAsync(ct);
        }

        rival.Held.Count.Should().BeLessThanOrEqualTo(1, "the stuck instance still counts as a member and keeps its share");
        stuck.Held.Should().NotBeEmpty();

        stuck.Quarantine([0, 1], TimeSpan.FromSeconds(4));
        await stuck.RefreshAsync(ct);

        stuck.Held.Should().BeEmpty("a quarantined partition is given back on the next cycle");
        (await fixture.Db.HashExistsAsync(key, OwnershipRegistry.PresenceField(stuck.InstanceId)))
            .Should().BeFalse("an instance that cannot read must not count towards the fair share");

        await rival.RefreshAsync(ct);
        rival.Held.Should().Equal([0, 1], "with the stuck instance out of the member count, the rival takes the whole topic");

        // Still quarantined: cycling claims nothing back.
        await stuck.RefreshAsync(ct);
        stuck.Held.Should().BeEmpty();

        var owners = OwnershipRegistry.ReadOwners(await fixture.Db.HashGetAllAsync(key));
        owners.Values.Should().AllSatisfy(o => o.InstanceId.Should().Be(rival.InstanceId));

        // The quarantine lapses on its own; the two settle back to one partition each.
        await RedisStreamsFixture.WaitUntilAsync(
            async () =>
            {
                await stuck.RefreshAsync(ct);
                await rival.RefreshAsync(ct);
                return stuck.Held.Count == 1 && rival.Held.Count == 1;
            },
            TimeSpan.FromSeconds(30),
            "the instances to rebalance after the quarantine ends",
            ct);
    }

    /// <summary>
    /// The host, end to end. A <c>Lease</c> consumer whose handler never returns — the processor is
    /// wedged, while its reader keeps completing empty reads on a now-quiet topic — stalls past
    /// <c>UnhealthyStalledSeconds</c>, gives its leases up, and the rival ends up with both
    /// partitions. Its health check reports Unhealthy (rule Stalled), which is what a liveness probe
    /// or a watchdog restarts on. Before the fix the host renewed its leases indefinitely and the
    /// rival never got more than the one partition the fair share hands it.
    /// </summary>
    [Fact]
    public async Task A_lease_owner_that_stops_making_progress_hands_its_partitions_over_and_reports_Unhealthy()
    {
        var topic = fixture.NewTopic("stall-handoff");
        var consumer = fixture.NewConsumer("stall-handoff");
        var key = StreamKeys.Ownership(topic, consumer);
        var ct = TestContext.Current.CancellationToken;

        var topicOptions = new TopicOptions { Partitions = 2, CoLocatePartitions = false };
        var consumerOptions = new ConsumerOptions
        {
            Topic = topic,
            BatchSize = 10,
            UnhealthyStalledSeconds = 3,
            ShutdownTimeoutSeconds = 1,
            Instances = new InstanceOptions
            {
                Mode = InstanceMode.Lease,
                LeaseTtlSeconds = LeaseTtlSeconds,
                LeaseRenewSeconds = 1,
            },
        };

        var options = new StreamOptions
        {
            ConnectionString = fixture.ConnectionString,
            Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
        };

        // The wedge: a handler that never returns and ignores its token, like one stuck on a Redis
        // call that will not complete. Released at the end so the test can stop the host.
        var wedge = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;

        async ValueTask Handler(ReadOnlyMemory<StreamMsg> batch, CancellationToken _)
        {
            Interlocked.Increment(ref entered);
            await wedge.Task.ConfigureAwait(false);
        }

        var log = new CaptureLogger();

        await using var connection = new StreamsConnectionProvider(options, services: null, logger: null);
        var host = new StreamConsumerHost(options, consumerOptions, consumer, Handler, connection, log);

        await using var rivalPod = await fixture.ConnectInstanceAsync("offer-odds-projection-76d86fc5f8-q2m7t");
        await using var rival = Registry(rivalPod, topic, consumer);

        try
        {
            await host.StartAsync(ct);
            host.OwnedPartitions.Should().Equal([0, 1], "the only instance up claims both partitions");

            var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);

            for (var partition = 0; partition < topicOptions.Partitions; partition++)
            {
                await publisher.PublishAsync(
                    "k",
                    Encoding.UTF8.GetBytes($"price p{partition}"),
                    "Orange.Models.Integration.MOddsDelta",
                    new PublishOptions(Partition: partition));
            }

            await RedisStreamsFixture.WaitUntilAsync(
                () => Volatile.Read(ref entered) == topicOptions.Partitions,
                TimeSpan.FromSeconds(10),
                "both partitions' handlers to wedge",
                ct);

            // The other replica keeps cycling, as its renewal loop would. It must end up with both.
            await RedisStreamsFixture.WaitUntilAsync(
                async () =>
                {
                    await rival.RefreshAsync(ct);
                    return rival.Held.Count == topicOptions.Partitions;
                },
                TimeSpan.FromSeconds(30),
                "the rival to take over both partitions from the stalled owner",
                ct);

            var owners = OwnershipRegistry.ReadOwners(await fixture.Db.HashGetAllAsync(key));
            owners.Values.Should().AllSatisfy(
                o => o.InstanceId.Should().Be(rival.InstanceId),
                "the stalled owner gave both leases up");

            (await fixture.Db.HashExistsAsync(key, OwnershipRegistry.PresenceField(OwnershipRegistry.StableInstanceId)))
                .Should().BeFalse("while it is quarantined the stalled owner does not count towards the fair share");

            var mine = StreamStatus.Partitions()
                .Where(p => p.Topic == topic && p.Consumer == consumer)
                .ToArray();

            // Once the rival is cycling, the ordinary fair share moves one partition over by itself;
            // the one the stalled owner still held is the one only the stall rule frees — before the
            // fix it stayed with the stalled owner for as long as that process lived. Whatever the
            // stalled owner held when it stalled stays visible to the health check after the lease
            // has gone, and nothing it still reports is anything but stalled out.
            mine.Should().NotBeEmpty("the stalled partitions stay visible to the health check after the leases are gone");
            mine.Should().AllSatisfy(p => p.StoodDownStalled.Should().BeTrue());

            var report = StreamHealth.Evaluate(mine, new StreamHealthContext { Consumers = 1, ConsumersStarted = 1 });
            report.Status.Should().Be(StreamHealthStatus.Unhealthy);
            report.Rule.Should().Be(StreamHealthRule.Stalled);

            log.Messages(LogLevel.Error).Should().Contain(
                m => m.Contains("giving up its lease", StringComparison.Ordinal),
                "an operator has to be able to see why the partitions moved");

            // It does not take them back while quarantined, however many cycles run.
            await Task.Delay(TimeSpan.FromSeconds(LeaseTtlSeconds), ct);
            await rival.RefreshAsync(ct);
            rival.Held.Should().Equal([0, 1]);
            host.OwnedPartitions.Should().BeEmpty("its read side follows the lease it gave up");
        }
        finally
        {
            wedge.TrySetResult();
            await host.StopAsync(CancellationToken.None);
            await host.DisposeAsync();
        }

        StreamStatus.Partitions().Should().NotContain(
            p => p.Topic == topic && p.Consumer == consumer,
            "a stopped host takes its stalled-out partitions out of the health check with it");
    }

    private static OwnershipRegistry Registry(RedisStreamsFixture.StreamsInstance pod, string topic, string consumer)
        => new(
            pod.Redis,
            new OwnershipRegistryOptions
            {
                Topic = topic,
                Consumer = consumer,
                Partitions = 2,
                PodName = pod.PodName,
                InstanceId = pod.InstanceId,
                Mode = InstanceMode.Lease,
                TtlSeconds = LeaseTtlSeconds,
                RenewSeconds = 1,
            });

    /// <summary>An <see cref="ILogger"/> that keeps every formatted message for assertion.</summary>
    private sealed class CaptureLogger : ILogger
    {
        private readonly Lock gate = new();
        private readonly List<(LogLevel Level, string Message)> entries = [];

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
            ArgumentNullException.ThrowIfNull(formatter);

            lock (this.gate)
            {
                this.entries.Add((logLevel, formatter(state, exception)));
            }
        }

        internal IReadOnlyList<string> Messages(LogLevel level)
        {
            lock (this.gate)
            {
                return this.entries.Where(e => e.Level == level).Select(e => e.Message).ToArray();
            }
        }
    }
}
