using System.Diagnostics;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Ownership;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;

using StackExchange.Redis;

namespace Orange.Lib.Streams.Tests;

/// <summary>
/// S6f — <c>Instances.Mode = Lease</c> against a real Redis: three instances with no <c>Count</c>
/// anywhere claim disjoint partitions, one is hard-killed, and the survivors pick its partitions up
/// within <c>LeaseTtlSeconds</c> without any partition ever being claimed twice.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the registry directly, and not three <see cref="StreamConsumerHost"/>s.</b> Lease mode
/// tells instances apart by <see cref="OwnershipRegistry.StableInstanceId"/> — one identity per
/// pod, derived from <c>POD_NAME</c>. Three hosts built inside one test process would share that
/// identity, and the lease script would then read all three as the same claimant: each would find
/// its own value in every field, renew all of it, and the split under test could not happen at all.
/// <see cref="OwnershipRegistryOptions.InstanceId"/> plus the fixture's per-pod connections are what
/// give this test three genuinely separate claimants. The host's half of the wiring is covered by
/// <see cref="Lease_mode_host_starts_on_the_partitions_it_claims_and_hands_them_back_on_stop"/>,
/// which needs only one identity.
/// </para>
/// <para>
/// <b>Why the cycles are driven by hand.</b> Every claim, renewal and expiry here is a step in a
/// state machine, and <see cref="OwnershipRegistry.RefreshAsync"/> is exactly one step of it.
/// Driving the steps rather than starting the renewal loops makes "kill an instance" mean something
/// precise — stop stepping it, never release — and removes every sleep from the test: the only wall
/// clock that matters is the field TTL, which is what the failover assertion is about.
/// </para>
/// <para>
/// <b>Pod names are Deployment-shaped on purpose</b> (<c>svc-7d4f8b-x2k9p</c>): no StatefulSet
/// ordinal to derive, no <c>STREAMS_INSTANCE_COUNT</c> to declare. Under <c>Static</c> those three
/// pods would each fall back to <c>Index=0, Count=1</c> and consume every partition — the drift the
/// registry can only report after the fact. Lease mode is the mode where it cannot happen.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class OwnershipLeaseTests(RedisStreamsFixture fixture)
{
    private const int Partitions = 8;

    /// <summary>Short enough that the failover is a test rather than a coffee break; the renewals stay well inside it.</summary>
    private const int LeaseTtlSeconds = 3;

    /// <summary>
    /// S6f — three <c>Lease</c> instances, no <c>Count</c> configured anywhere: they settle on
    /// disjoint partitions covering the topic, one is killed, and its partitions are claimed by the
    /// survivors within the TTL. No partition is held by two instances at any point.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S6f_three_lease_instances_split_the_topic_and_cover_a_dead_instance_within_the_ttl()
    {
        var topic = fixture.NewTopic("s6f-lease");
        var consumer = fixture.NewConsumer("s6f-lease");
        var key = StreamKeys.Ownership(topic, consumer);

        // Nothing declares a pool: no Instances:Count, no STREAMS_INSTANCE_COUNT, and no ordinal in
        // any of the three pod names. Static resolution would hand every one of them all 8
        // partitions; Lease mode observes the count instead of being told it.
        var identity = InstanceResolver.Resolve(new InstanceOptions { Mode = InstanceMode.Lease }, _ => null);
        identity.Count.Should().Be(1, "nothing declares a pool size");
        identity.CountSource.Should().Be(InstanceSource.Fallback);
        InstanceResolver.TryParsePodOrdinal("svc-7d4f8b-x2k9p", out _)
            .Should().BeFalse("a Deployment pod name carries no ordinal to derive an index from");

        await using var podA = await fixture.ConnectInstanceAsync("svc-7d4f8b-x2k9p");
        await using var podB = await fixture.ConnectInstanceAsync("svc-7d4f8b-q7m1t");
        await using var podC = await fixture.ConnectInstanceAsync("svc-7d4f8b-z4v8r");

        var logA = new CaptureLogger();

        await using var a = Registry(podA, topic, consumer, logA);
        await using var b = Registry(podB, topic, consumer, logger: null);
        await using var c = Registry(podC, topic, consumer, logger: null);

        a.Held.Should().BeEmpty("nothing is claimed before the first cycle");

        // Phase 1 — the three instances converge on a split. Every cycle is checked for a partition
        // held by two instances at once, so the invariant is asserted throughout the run and not
        // only at the end.
        var live = new[] { a, b, c };

        await RedisStreamsFixture.WaitUntilAsync(
            async () =>
            {
                foreach (var instance in live)
                {
                    await instance.RefreshAsync(TestContext.Current.CancellationToken);
                    AssertNothingHeldTwice(live);
                }

                // Settled means both halves: the topic is covered, and nobody is over the fair
                // share of ceil(8 / 3) = 3. The first instance to run does claim all eight — nothing
                // else was there to claim them — and gives the surplus back on its next cycle, once
                // the others' presence fields have made the member count three.
                return Union(live).Count == Partitions && live.All(i => i.Held.Count <= 3);
            },
            TimeSpan.FromSeconds(30),
            "three lease instances to settle on disjoint partitions covering the topic");

        // Three caps of three cover eight partitions with one to spare, so with all eight covered
        // nobody can be left with fewer than two either.
        foreach (var instance in live)
        {
            instance.Held.Should().HaveCountGreaterThanOrEqualTo(2)
                .And.HaveCountLessThanOrEqualTo(3, "a lease instance never claims past ceil(partitions / live instances)");
        }

        Union(live).Should().BeEquivalentTo(Enumerable.Range(0, Partitions), "the three leases tile [0,8) exactly once");

        var owners = OwnershipRegistry.ReadOwners(await fixture.Db.HashGetAllAsync(key));
        owners.Should().HaveCount(Partitions);

        foreach (var instance in live)
        {
            foreach (var partition in instance.Held)
            {
                owners[partition].InstanceId.Should().Be(instance.InstanceId, "the hash agrees with what each instance believes it holds");
            }
        }

        logA.Messages(LogLevel.Information).Should()
            .Contain(m => m.Contains("leases partitions", StringComparison.Ordinal), "an operator has to be able to see who leases what");

        // Phase 2 — a hard kill. Instance C stops cycling and never releases, which is precisely what
        // a SIGKILLed pod leaves behind: its last claims, with a TTL and nobody to renew them.
        // Releasing (StopAsync) would model a graceful stop and would never exercise the expiry.
        var orphaned = c.Held.ToArray();
        orphaned.Should().NotBeEmpty();

        var survivors = new[] { a, b };
        var clock = Stopwatch.StartNew();

        await RedisStreamsFixture.WaitUntilAsync(
            async () =>
            {
                foreach (var instance in survivors)
                {
                    await instance.RefreshAsync(TestContext.Current.CancellationToken);
                    AssertNothingHeldTwice(survivors);
                }

                return Union(survivors).Count == Partitions;
            },
            TimeSpan.FromSeconds(30),
            "the surviving instances to pick up the dead instance's partitions");

        clock.Stop();

        clock.Elapsed.Should().BeLessThan(
            TimeSpan.FromSeconds(LeaseTtlSeconds * 4),
            "a lease that stops being renewed is free within its TTL, and the next cycle of any live instance claims it");

        Union(survivors).Should().Contain(orphaned, "every partition the dead instance held is being consumed again");
        Union(survivors).Should().BeEquivalentTo(Enumerable.Range(0, Partitions));

        // Two live members now, so the fair share is ceil(8 / 2) = 4 and both survivors are at it.
        a.Held.Should().HaveCount(4);
        b.Held.Should().HaveCount(4);

        var after = OwnershipRegistry.ReadOwners(await fixture.Db.HashGetAllAsync(key));
        after.Should().HaveCount(Partitions);
        after.Values.Should().NotContain(o => o.InstanceId == c.InstanceId, "nothing the killed instance claimed survives it");
        (await fixture.Db.HashExistsAsync(key, OwnershipRegistry.PresenceField(c.InstanceId)))
            .Should().BeFalse("the killed instance's presence lapses with its claims, which is how the member count corrects itself");
    }

    /// <summary>
    /// The host half of P4-20: a consumer with <c>Instances:Mode = Lease</c> and no <c>Count</c>
    /// starts its workers on the partitions the registry claimed for it, consumes them, and hands
    /// the leases straight back when it stops rather than leaving them to expire.
    /// </summary>
    /// <remarks>
    /// One host, not two: two hosts in one process would share
    /// <see cref="OwnershipRegistry.StableInstanceId"/> and so be one claimant, which is what
    /// <see cref="S6f_three_lease_instances_split_the_topic_and_cover_a_dead_instance_within_the_ttl"/>
    /// uses the registry directly to get around. What this covers is the wiring the registry test
    /// cannot see: that the host asks the registry what it owns instead of the static arithmetic,
    /// and that the read side follows.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Lease_mode_host_starts_on_the_partitions_it_claims_and_hands_them_back_on_stop()
    {
        var topic = fixture.NewTopic("lease-host");
        var consumer = fixture.NewConsumer("lease-host");
        var key = StreamKeys.Ownership(topic, consumer);

        var topicOptions = new TopicOptions { Partitions = 4 };
        var consumerOptions = new ConsumerOptions
        {
            Topic = topic,
            BatchSize = 1,
            Persist = PersistMode.SyncBatch,
            Instances = new InstanceOptions
            {
                Mode = InstanceMode.Lease,
                LeaseTtlSeconds = LeaseTtlSeconds,
                LeaseRenewSeconds = 1,
            },
        };

        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);

        for (var partition = 0; partition < topicOptions.Partitions; partition++)
        {
            await publisher.PublishAsync(
                "k",
                System.Text.Encoding.UTF8.GetBytes($"p{partition}"),
                "test.event",
                new PublishOptions(Partition: partition));
        }

        var handler = new TestHandler();
        var options = new StreamOptions
        {
            ConnectionString = fixture.ConnectionString,
            Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
        };

        await using var connection = new StreamsConnectionProvider(options, services: null, logger: null);
        await using var host = new StreamConsumerHost(
            options,
            consumerOptions,
            consumer,
            ((IBatchHandler)handler).HandleAsync,
            connection);

        await host.StartAsync(TestContext.Current.CancellationToken);

        // Nothing assigned it these: the sole instance claimed every free partition, which is what
        // "Count is observed, not declared" means when the observed count is one.
        host.OwnedPartitions.Should().Equal(0, 1, 2, 3);

        await handler.WaitForAsync(topicOptions.Partitions, TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        var claimed = OwnershipRegistry.ReadOwners(await fixture.Db.HashGetAllAsync(key));
        claimed.Should().HaveCount(topicOptions.Partitions);
        claimed.Values.Should().AllSatisfy(o => o.InstanceId.Should().Be(OwnershipRegistry.StableInstanceId));

        await host.StopAsync(TestContext.Current.CancellationToken);

        // A graceful stop gives the leases back rather than leaving them to time out: the next
        // instance picks them up immediately instead of after a whole TTL of nobody consuming.
        (await fixture.Db.HashLengthAsync(key)).Should().Be(
            0,
            "a stopping lease instance releases both its claims and its presence field");
    }

    /// <summary>Every partition held by any of the given instances, as a set.</summary>
    private static HashSet<int> Union(OwnershipRegistry[] instances)
    {
        var union = new HashSet<int>();

        foreach (var instance in instances)
        {
            foreach (var partition in instance.Held)
            {
                union.Add(partition);
            }
        }

        return union;
    }

    /// <summary>
    /// The invariant the whole mode rests on: two instances never hold one partition. Asserted after
    /// every cycle rather than once at the end, because a claim that overlapped for one cycle and
    /// then resolved would be exactly the bug worth catching.
    /// </summary>
    private static void AssertNothingHeldTwice(OwnershipRegistry[] instances)
    {
        var held = new List<int>();

        foreach (var instance in instances)
        {
            held.AddRange(instance.Held);
        }

        held.Should().OnlyHaveUniqueItems("a partition claimed with HSETNX cannot be held by two instances at once");
    }

    private static OwnershipRegistry Registry(
        RedisStreamsFixture.StreamsInstance pod,
        string topic,
        string consumer,
        ILogger? logger)
        => new(
            pod.Redis,
            new OwnershipRegistryOptions
            {
                Topic = topic,
                Consumer = consumer,
                Partitions = Partitions,
                PodName = pod.PodName,
                InstanceId = pod.InstanceId,
                Mode = InstanceMode.Lease,
                TtlSeconds = LeaseTtlSeconds,
                RenewSeconds = 1,
            },
            logger);

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

        /// <summary>Every message logged at <paramref name="level"/>, in order.</summary>
        internal IReadOnlyList<string> Messages(LogLevel level)
        {
            lock (this.gate)
            {
                return this.entries.Where(e => e.Level == level).Select(e => e.Message).ToArray();
            }
        }
    }
}
