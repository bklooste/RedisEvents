using System.Globalization;
using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using RedisEvents.Admin;
using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Extensions;
using RedisEvents.Ownership;
using RedisEvents.Positions;
using RedisEvents.Producer;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>
/// The cross-process half of the second-writer protocol (remediation finding P0-1): S6e, a pod
/// restart, and a reset issued from a different process.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why these tests exist.</b> Every restart and reset test in this suite ran the consumer and the
/// admin call inside one process, so both stamped the same
/// <see cref="OwnershipRegistry.ProcessInstanceId"/> and the position flusher's "somebody else's id
/// is in my field" check could never fire on a case it got wrong. Three cases it got wrong:
/// </para>
/// <list type="number">
///   <item><description>A pod restart — the predecessor's id is in the field, so the first flush of
///   every new process stood its own partitions down.</description></item>
///   <item><description>A reset issued from an admin CLI or API pod — a different id again, so a
///   reset looked exactly like a rival.</description></item>
///   <item><description>Two genuine contenders — both saw a foreign id, so <em>both</em> stood down
///   and the partition stopped being consumed at all.</description></item>
/// </list>
/// <para>
/// <see cref="RedisStreamsFixture.StreamsInstance"/> is what makes the difference visible: it hands
/// a test the identity a named pod would run under, derived the way the host derives it.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class WriterIdentityTests(RedisStreamsFixture fixture)
{
    /// <summary>The type string every seeded entry carries.</summary>
    private const string MessageType = "identity-test";

    /// <summary>Flush period for the hand-driven flushers; they are pumped explicitly, never timed.</summary>
    private static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// S6e — two live pods writing one partition's position: exactly one stands down, and it is the
    /// one the tiebreak names. Both standing down would leave the partition consumed by nobody.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S6e_two_live_writers_stand_exactly_one_of_themselves_down()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic("s6e");
        var consumer = fixture.NewConsumer("s6e");

        await using var podA = fixture.AsInstance("svc-a-0");
        await using var podB = fixture.AsInstance("svc-b-0");

        podA.InstanceId.Should().NotBe(podB.InstanceId, "two different pods are two different identities");

        // Both pods believe they own partition 0 — the Deployment-instead-of-StatefulSet shape the
        // README warns about. Both hold a live ownership claim, which is what makes each a genuine
        // contender rather than a ghost.
        await using var ownA = this.Registry(topic, consumer, podA);
        await using var ownB = this.Registry(topic, consumer, podB);
        await ownA.StartAsync(ct);
        await ownB.StartAsync(ct);

        var contestedA = new List<int>();
        var contestedB = new List<int>();

        await using var flushA = this.Flusher(topic, consumer, podA, (p, _, _) => contestedA.Add(p));
        await using var flushB = this.Flusher(topic, consumer, podB, (p, _, _) => contestedB.Add(p));

        // A writes first, then B over the top of it, then A again: each side has now seen the other's
        // id sitting in the field it was about to write.
        flushA.Record(0, new StreamId(1000, 0));
        await flushA.FlushAsync(ct);

        flushB.Record(0, new StreamId(1001, 0));
        await flushB.FlushAsync(ct);

        flushA.Record(0, new StreamId(1002, 0));
        await flushA.FlushAsync(ct);

        var loser = podA.InstanceId.CompareTo(podB.InstanceId) < 0 ? podB : podA;
        var winner = ReferenceEquals(loser, podA) ? podB : podA;
        var loserFlusher = ReferenceEquals(loser, podA) ? flushA : flushB;
        var winnerFlusher = ReferenceEquals(loser, podA) ? flushB : flushA;
        var loserCalls = ReferenceEquals(loser, podA) ? contestedA : contestedB;
        var winnerCalls = ReferenceEquals(loser, podA) ? contestedB : contestedA;

        loserFlusher.ContestedCount.Should().Be(
            1,
            "the higher identity of two genuine contenders is the one that stands down");
        loserCalls.Should().Equal(new[] { 0 }, "the host is asked to stop exactly the contested partition, once");

        winnerFlusher.ContestedCount.Should().Be(
            0,
            "if both contenders stood down the partition would be consumed by nobody — which is worse " +
            "than either of them keeping it");
        winnerCalls.Should().BeEmpty();

        // Both sides still report the overlap: it is a misconfiguration from either end, and the
        // gauge the health check reads must not go quiet just because one of them kept going.
        loserFlusher.ContentionCount.Should().Be(1);
        winnerFlusher.ContentionCount.Should().Be(1);

        // The winner is still a live writer: its next record reaches Redis, the loser's does not.
        winnerFlusher.Record(0, new StreamId(2000, 0));
        loserFlusher.Record(0, new StreamId(3000, 0));
        await winnerFlusher.FlushAsync(ct);
        await loserFlusher.FlushAsync(ct);

        var stored = await new RedisPositionStore(fixture.Redis).LoadRecordsAsync(topic, consumer, ct);
        stored.Should().ContainKey(0);
        stored[0].Id.Should().Be(new StreamId(2000, 0), "only the winner is still writing");
        stored[0].InstanceId.Should().Be(winner.InstanceId);
    }

    /// <summary>
    /// The scenario customer-wallet-views' appsettings.json comment describes trying and abandoning
    /// on <c>Instances:Mode = Lease</c>: pod A gracefully releases the only partition, pod B claims
    /// it, and then A's last (delayed) position flush lands in the field <em>after</em> B has already
    /// taken over. The comment says the new owner reads that stale write as a live rival and stands
    /// itself down — which R-01's live-presence check (see <see cref="PositionFlusher.InspectAsync"/>)
    /// should already prevent, since A's presence claim is deleted atomically with its partition claim
    /// on release, before its late flush can land. This proves it against a real Redis rather than by
    /// reading the source.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_lease_handoff_does_not_falsely_stand_down_the_new_owner()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic("lease-handoff");
        var consumer = fixture.NewConsumer("lease-handoff");

        await using var podA = fixture.AsInstance("svc-7d4f8b-aaaaa");
        await using var podB = fixture.AsInstance("svc-7d4f8b-bbbbb");

        var ownA = this.LeaseRegistry(topic, consumer, podA);
        await ownA.StartAsync(ct);
        ownA.Held.Should().Equal(new[] { 0 }, "the only instance alive claims the only partition");

        var contestedA = new List<int>();
        var contestedB = new List<int>();

        await using var flushA = this.Flusher(topic, consumer, podA, (p, _, _) => contestedA.Add(p));
        await using var flushB = this.Flusher(topic, consumer, podB, (p, _, _) => contestedB.Add(p));

        flushA.Record(0, new StreamId(1000, 0));
        await flushA.FlushAsync(ct);

        // Graceful release: a draining pod's StopAsync, which deletes the partition claim and the
        // presence field together in one script call — not a TTL lapse.
        await ownA.StopAsync(ct);

        await using var ownB = this.LeaseRegistry(topic, consumer, podB);
        await ownB.StartAsync(ct);
        ownB.Held.Should().Equal(new[] { 0 }, "the partition was free the moment A released it");

        // The delayed async flush: A writes again even though it has already released ownership,
        // landing after B has already claimed the partition.
        flushA.Record(0, new StreamId(1001, 0));
        await flushA.FlushAsync(ct);

        // Prove the race precondition actually happened, so a pass below cannot be vacuous: the
        // field really does hold A's stale id when B is about to inspect it.
        var beforeB = await new RedisPositionStore(fixture.Redis).LoadRecordsAsync(topic, consumer, ct);
        beforeB[0].InstanceId.Should().Be(podA.InstanceId, "A's late write really did land after B claimed the partition");

        // B's own flush now finds A's id sitting in the field it is about to overwrite.
        flushB.Record(0, new StreamId(2000, 0));
        await flushB.FlushAsync(ct);

        flushB.ContestedCount.Should().Be(
            0,
            "A released its ownership claim before this flush landed, so its stale write must not be read as a live rival");
        contestedB.Should().BeEmpty();

        var stored = await new RedisPositionStore(fixture.Redis).LoadRecordsAsync(topic, consumer, ct);
        stored.Should().ContainKey(0);
        stored[0].Id.Should().Be(new StreamId(2000, 0), "B keeps writing undisturbed");
        stored[0].InstanceId.Should().Be(podB.InstanceId);

        await flushA.DisposeAsync();
        await ownB.StopAsync(ct);
    }

    /// <summary>
    /// The ungraceful sibling of the test above, and what actually happened to offer-odds on a
    /// rolling deploy: pod A keeps running — it still holds partition 0 and so its presence field is
    /// live — but gives partition 1 up to a newly arrived pod B on its next lease cycle. A's last
    /// async flush for partition 1 then lands after B has claimed it. The live-presence check alone
    /// reads that as a genuine rival (A is alive), B has the higher id, B stands down, keeps the
    /// lease, and nobody reads partition 1 until B restarts. The claim field is what tells a late
    /// write from a rival, so B must ask it.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_late_flush_from_a_live_peer_that_gave_the_partition_up_does_not_stand_the_new_owner_down()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic("lease-late-flush");
        var consumer = fixture.NewConsumer("lease-late-flush");

        await using var podA = fixture.AsInstance("svc-7d4f8b-aaaaa");
        await using var podB = fixture.AsInstance("svc-7d4f8b-bbbbb");

        // Alone, A claims both partitions. RefreshAsync rather than StartAsync: the cycles are driven
        // by hand so the handoff below happens in a known order, with no renewal timer racing it.
        await using var ownA = this.LeaseRegistry(topic, consumer, podA, partitions: 2);
        (await ownA.RefreshAsync(ct)).Owners.Keys.Should().BeEquivalentTo(new[] { 0, 1 });

        var contestedA = new List<int>();
        var contestedB = new List<int>();

        // exclusiveClaims: what the host passes under Instances:Mode = Lease, where HSETNX makes the
        // claim field name exactly one holder. Static mode keeps the id tiebreak (see S6e).
        await using var flushA = this.Flusher(topic, consumer, podA, (p, _, _) => contestedA.Add(p), partitions: 2, exclusiveClaims: true);
        await using var flushB = this.Flusher(topic, consumer, podB, (p, _, _) => contestedB.Add(p), partitions: 2, exclusiveClaims: true);

        flushA.Record(1, new StreamId(1000, 0));
        await flushA.FlushAsync(ct);

        // B arrives: with two members the fair share is one partition each. B's first cycle finds
        // nothing free; A's next cycle releases its surplus (partition 1); B's next cycle claims it.
        // A is still alive and still holds partition 0 throughout — this is not a release of A.
        await using var ownB = this.LeaseRegistry(topic, consumer, podB, partitions: 2);
        await ownB.RefreshAsync(ct);
        await ownA.RefreshAsync(ct);
        await ownB.RefreshAsync(ct);

        ownA.Held.Should().Equal(new[] { 0 }, "A gave up its surplus partition and kept the other");
        ownB.Held.Should().Equal(new[] { 1 }, "B claimed the partition A released");

        // A's delayed async flush for the partition it no longer holds lands after B's claim.
        flushA.Record(1, new StreamId(1001, 0));
        await flushA.FlushAsync(ct);

        var beforeB = await new RedisPositionStore(fixture.Redis).LoadRecordsAsync(topic, consumer, ct);
        beforeB[1].InstanceId.Should().Be(podA.InstanceId, "A's late write really did land after B claimed the partition");

        flushB.Record(1, new StreamId(2000, 0));
        await flushB.FlushAsync(ct);

        flushB.ContestedCount.Should().Be(
            0,
            "A is alive but no longer holds partition 1's claim — B does — so A's write is a late flush, not a rival");
        contestedB.Should().BeEmpty("nothing may stop the new owner's worker");
        flushB.ContentionCount.Should().Be(0, "a late write is not an overlap and must not raise the contention gauge");

        // Both sides keep writing what they own: B partition 1, A partition 0.
        flushB.Record(1, new StreamId(2001, 0));
        flushA.Record(0, new StreamId(500, 0));
        await flushB.FlushAsync(ct);
        await flushA.FlushAsync(ct);

        var stored = await new RedisPositionStore(fixture.Redis).LoadRecordsAsync(topic, consumer, ct);
        stored[1].Id.Should().Be(new StreamId(2001, 0), "B keeps writing partition 1 undisturbed");
        stored[1].InstanceId.Should().Be(podB.InstanceId);
        stored[0].Id.Should().Be(new StreamId(500, 0), "A keeps writing the partition it still holds");
        stored[0].InstanceId.Should().Be(podA.InstanceId);
        contestedA.Should().BeEmpty();

        await ownB.StopAsync(ct);
        await ownA.StopAsync(ct);
    }

    /// <summary>
    /// A StatefulSet pod bounces back into its own ordinal. The predecessor's position is sitting in
    /// the field, so the first flush of the new process must recognise it as its own history rather
    /// than as a rival and stand itself down.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_restarted_pod_does_not_contest_its_own_predecessors_position()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic("restart");
        var consumer = fixture.NewConsumer("restart");

        // Two lives of one pod. The identity comes from the pod name, exactly as it does in the
        // cluster, so "same ordinal" and "same writer" are the same statement.
        await using var life1 = fixture.AsInstance("svc-1");
        await using var life2 = fixture.AsInstance("svc-1");

        life2.InstanceId.Should().Be(
            life1.InstanceId,
            "a restart of the same pod is the same logical writer — a fresh per-process GUID here is " +
            "precisely finding P0-1");

        // First life: claims, writes a position, then is hard-killed. RefreshAsync-without-StartAsync
        // and no release is what a SIGKILLed pod leaves in Redis.
        var own1 = this.Registry(topic, consumer, life1);
        await own1.RefreshAsync(ct);

        var flush1 = this.Flusher(topic, consumer, life1, null);
        flush1.Record(0, new StreamId(1000, 0));
        await flush1.FlushAsync(ct);
        await flush1.DisposeAsync();
        await own1.DisposeAsync();

        // Second life: same ordinal, same claims, and a position of its own to write.
        var contested = new List<int>();
        await using var own2 = this.Registry(topic, consumer, life2);
        await own2.StartAsync(ct);

        own2.Latest.Should().NotBeNull();
        own2.Latest!.Contested.Should().BeEmpty("a pod reclaiming its own ordinal is not an overlap");

        await using var flush2 = this.Flusher(topic, consumer, life2, (p, _, _) => contested.Add(p));
        flush2.Record(0, new StreamId(2000, 0));
        await flush2.FlushAsync(ct);

        flush2.ContestedCount.Should().Be(
            0,
            "the id in the field belongs to this pod's previous life, not to a second live writer");
        contested.Should().BeEmpty("nothing may stop a partition worker on a plain restart");

        var stored = await new RedisPositionStore(fixture.Redis).LoadRecordsAsync(topic, consumer, ct);
        stored[0].Id.Should().Be(new StreamId(2000, 0), "the restarted pod kept writing");
    }

    /// <summary>
    /// A reset issued from another process — an admin CLI or the admin API pod — must rewind the
    /// running consumer, not be mistaken for a rival writer and stop it.
    /// </summary>
    /// <remarks>
    /// This is the live-reset case S10 already covers, with the one thing S10 could not vary: the
    /// process that issues the reset. S10's admin call runs inside the test process and therefore
    /// stamps the consumer's own instance id, so the flusher sees its own id come back and is happy.
    /// Out of process it sees a stranger.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_reset_issued_from_another_process_rewinds_instead_of_standing_the_partition_down()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic("oop-reset");
        var consumer = fixture.NewConsumer("oop-reset");

        var topicOptions = new TopicOptions { Partitions = 1, Trim = TrimMode.None };
        var consumerOptions = new ConsumerOptions
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

        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, topicOptions, NullLogger.Instance, ct);

        const int Total = 20;
        const int RewindTo = 9;

        var log = new CaptureLogger();
        var handler = new TestHandler();
        await using var run = await this.StartConsumerAsync(topic, consumer, topicOptions, consumerOptions, handler, log);

        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);
        var ids = new StreamId[Total];
        for (var i = 0; i < Total; i++)
        {
            ids[i] = await publisher.PublishAsync("identity", Encoding.UTF8.GetBytes($"msg-{i:D3}"), MessageType, ct: ct);
        }

        await handler.WaitForAsync(Total, TimeSpan.FromSeconds(30), ct);

        var positions = new RedisPositionStore(fixture.Redis);
        await RedisStreamsFixture.WaitUntilAsync(
            async () => (await positions.LoadAsync(topic, consumer, ct)).TryGetValue(0, out var at) && at == ids[^1],
            TimeSpan.FromSeconds(15),
            "the running consumer to flush its position at the tail",
            ct);

        // The reset, issued by a different process on its own connection: marker first, then the
        // position, which is exactly what StreamAdmin.ResetPositionAsync does — only stamped with the
        // admin's identity rather than the consumer's, because it is not the consumer.
        await using var admin = await fixture.ConnectInstanceAsync("admin-cli-0");
        admin.InstanceId.Should().NotBe(
            OwnershipRegistry.InstanceIdFor(Environment.MachineName),
            "the admin process is not the consumer process");

        var positionsKey = StreamKeys.Positions(topic, consumer);
        await admin.Db.HashSetAsync(
            positionsKey,
            [new HashEntry(StreamAdmin.ResetMarkerField(0), StreamAdmin.FormatResetMarker(ids[RewindTo], DateTimeOffset.UtcNow))]);

        await new RedisPositionStore(admin.Redis, admin.InstanceId)
            .SaveAsync(topic, consumer, new[] { (0, ids[RewindTo]) }.AsSpan(), ct);

        // The rewind lands and the whole tail is delivered again.
        await handler.WaitForAsync(Total + (Total - 1 - RewindTo), TimeSpan.FromSeconds(30), ct);

        // Until the consumer has flushed its own position over the admin's, the second-writer check
        // has not run at all and asserting anything about it would prove nothing.
        //
        // Getting there needs nudging, because a reset deliberately drops the replay's own pending
        // position — flushing it would overwrite the target the operator just wrote — and the drop
        // lands on whichever flush tick clears the marker, so the first message after the rewind can
        // be swallowed with it. One message per attempt, until the stamp changes.
        var nudged = false;
        for (var attempt = 0; attempt < 40 && !nudged; attempt++)
        {
            await publisher.PublishAsync(
                "identity",
                Encoding.UTF8.GetBytes($"nudge-{attempt.ToString(CultureInfo.InvariantCulture)}"),
                MessageType,
                ct: ct);

            await Task.Delay(TimeSpan.FromMilliseconds(250), ct);

            nudged = (await positions.LoadRecordsAsync(topic, consumer, ct)).TryGetValue(0, out var at)
                && at.InstanceId != admin.InstanceId;
        }

        nudged.Should().BeTrue("the consumer must flush its own position over the one the admin wrote");

        // And — the part that fails without the fix — the partition is still being consumed after
        // that flush. The write and the check are one pipelined round trip, so the stand-down lands
        // after the position has already moved; only a message published later proves it is alive.
        await publisher.PublishAsync("identity", Encoding.UTF8.GetBytes("msg-last"), MessageType, ct: ct);

        await RedisStreamsFixture.WaitUntilAsync(
            () => handler.Bodies.Contains("msg-last", StringComparer.Ordinal),
            TimeSpan.FromSeconds(30),
            "a message published after the reset was digested to be delivered",
            ct);

        run.Host.IsRunning.Should().BeTrue();
        log.Messages(LogLevel.Warning).Should().NotContain(
            m => m.Contains("standing partition", StringComparison.Ordinal),
            "an out-of-process reset is an operator rewinding the consumer, not a second pod fighting it");
    }

    private OwnershipRegistry Registry(string topic, string consumer, RedisStreamsFixture.StreamsInstance pod)
        => new(
            pod.Redis,
            new OwnershipRegistryOptions
            {
                Topic = topic,
                Consumer = consumer,
                Partitions = 1,
                OwnedPartitions = [0],
                PodName = pod.PodName,
                InstanceId = pod.InstanceId,
                TtlSeconds = 30,
                RenewSeconds = 10,
            },
            NullLogger.Instance);

    private OwnershipRegistry LeaseRegistry(string topic, string consumer, RedisStreamsFixture.StreamsInstance pod, int partitions = 1)
        => new(
            pod.Redis,
            new OwnershipRegistryOptions
            {
                Topic = topic,
                Consumer = consumer,
                Partitions = partitions,
                PodName = pod.PodName,
                InstanceId = pod.InstanceId,
                Mode = InstanceMode.Lease,
                TtlSeconds = 30,
                RenewSeconds = 10,
            },
            NullLogger.Instance);

    private PositionFlusher Flusher(
        string topic,
        string consumer,
        RedisStreamsFixture.StreamsInstance pod,
        PartitionContestedCallback? onContested,
        int partitions = 1,
        bool exclusiveClaims = false)
        => new(
            new RedisPositionStore(pod.Redis, pod.InstanceId),
            topic,
            consumer,
            partitionCount: partitions,
            Interval,
            NullLogger.Instance,
            pod.Redis,
            pod.InstanceId,
            onContested,
            exclusiveClaims: exclusiveClaims);

    private async Task<RunningConsumer> StartConsumerAsync(
        string topic,
        string consumer,
        TopicOptions topicOptions,
        ConsumerOptions consumerOptions,
        TestHandler handler,
        ILogger logger)
    {
        var options = new StreamOptions
        {
            ConnectionString = fixture.ConnectionString,
            Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
        };

        var provider = new StreamsConnectionProvider(options, services: null, logger: null);

        var host = new StreamConsumerHost(
            options,
            consumerOptions,
            consumer,
            ((IBatchHandler)handler).HandleAsync,
            provider,
            logger);

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
    private sealed class RunningConsumer(StreamConsumerHost host, StreamsConnectionProvider provider) : IAsyncDisposable
    {
        /// <summary>The running host.</summary>
        internal StreamConsumerHost Host { get; } = host;

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            await this.Host.DisposeAsync();
            await provider.DisposeAsync();
        }
    }

    /// <summary>An <see cref="ILogger"/> that keeps every formatted message for later assertion.</summary>
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
        public IReadOnlyList<string> Messages(LogLevel level)
        {
            lock (this.gate)
            {
                return this.entries.Where(e => e.Level == level).Select(e => e.Message).ToArray();
            }
        }
    }
}
