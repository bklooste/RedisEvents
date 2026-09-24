using System.Globalization;

using FluentAssertions;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using RedisEvents.Config;
using RedisEvents.Diagnostics;
using RedisEvents.Ownership;
using RedisEvents.Wire;
using RedisEvents.Web;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>
/// S6, S6b, S6c and S6d — partition ownership against a real Redis, including the two drift cases
/// that are otherwise completely silent in production.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why these tests use <see cref="OwnershipRegistry"/> directly rather than two
/// <c>StreamConsumerHost</c>s.</b> The whole point of the registry is to tell two <em>processes</em>
/// apart, and it does that with <see cref="OwnershipRegistry.StableInstanceId"/> — one identity per
/// pod. Two hosts constructed inside one test process share that identity, so a second "pod" built
/// that way would be indistinguishable from the first and the overlap check (S6c) could not fire at
/// all. <see cref="OwnershipRegistryOptions.InstanceId"/> exists precisely so a test can mint a
/// second identity; the partition arithmetic each simulated pod is given still comes from the real
/// <see cref="InstanceResolver"/>, so nothing about the ownership rule is being faked here — only
/// the process boundary.
/// </para>
/// <para>
/// <b>Every test uses its own topic.</b> Ownership lives in <c>o:{topic}:&lt;consumer&gt;</c>, so a
/// unique topic per test is what makes <c>HLEN</c> and "exactly these owners" assertions meaningful
/// while other test classes share the same server. Nothing here calls <c>FLUSHALL</c> for the same
/// reason — it would be a live grenade for anything running alongside.
/// </para>
/// <para>
/// <b>Registries are always disposed.</b> <see cref="OwnershipRegistry"/> publishes into a static
/// per-process map that <see cref="StreamsHealthCheck"/> reads, and
/// <see cref="OwnershipRegistry.DisposeAsync"/> is what removes the entry again. Leaking one would
/// leave the next test's health assertion reading a previous test's degradation.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class OwnershipTests(RedisStreamsFixture fixture)
{
    private const int Partitions = 8;

    /// <summary>
    /// S6 — two instances of a <c>Count=2</c> pool own contiguous halves of an 8-partition topic:
    /// [0–3] and [4–7], with no overlap and no gap, and each logs the full ownership map.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S6_two_instances_own_contiguous_halves_with_no_overlap_and_no_gaps()
    {
        var topic = fixture.NewTopic("s6-split");
        var consumer = fixture.NewConsumer("s6-split");

        // The ownership arithmetic itself, before Redis is involved at all.
        var range0 = OwnedRange(index: 0, count: 2);
        var range1 = OwnedRange(index: 1, count: 2);

        range0.Should().Be(new PartitionRange(0, 4), "instance 0 of 2 owns the first half of 8 partitions");
        range1.Should().Be(new PartitionRange(4, 4), "instance 1 of 2 owns the second half");
        range0.ToArray().Should().Equal(0, 1, 2, 3);
        range1.ToArray().Should().Equal(4, 5, 6, 7);

        // Contiguous: instance 1 starts exactly where instance 0 stops.
        range1.Start.Should().Be(range0.End, "the ranges must abut — a gap or an overlap here is a partition consumed by nobody or by everybody");
        range0.ToArray().Concat(range1.ToArray()).Should()
            .BeInAscendingOrder().And
            .OnlyHaveUniqueItems().And
            .Equal(Enumerable.Range(0, Partitions), "the two ranges must tile [0,8) exactly once");

        var log0 = new CaptureLogger();
        var log1 = new CaptureLogger();

        await using var pod0 = this.Registry(topic, consumer, range0, "svc-0", log0);
        await using var pod1 = this.Registry(topic, consumer, range1, "svc-1", log1);

        await pod0.StartAsync(TestContext.Current.CancellationToken);
        await pod1.StartAsync(TestContext.Current.CancellationToken);

        // Each instance logged the whole map, not just its own share of it — the line an operator
        // greps to answer "which pod runs partition 5?".
        log0.Messages(LogLevel.Information).Should().ContainSingle(m => m.Contains("full map:", StringComparison.Ordinal))
            .Which.Should().Contain("claims partitions 0,1,2,3");
        var pod1Map = log1.Messages(LogLevel.Information)
            .Should().ContainSingle(m => m.Contains("full map:", StringComparison.Ordinal)).Subject;
        pod1Map.Should().Contain("claims partitions 4,5,6,7");

        // pod1 started once pod0's claims were already in place, so the map it printed is complete.
        pod1Map.Should().NotContain("(none)", "every partition had an owner by the time instance 1 printed the map");
        for (var p = 0; p < Partitions; p++)
        {
            pod1Map.Should().Contain(
                $"{p.ToString(CultureInfo.InvariantCulture)}->svc-{(p < 4 ? 0 : 1)}",
                $"partition {p} belongs to instance {(p < 4 ? 0 : 1)}");
        }

        // Steady state: from here on neither instance may report anything wrong. (Instance 0 does
        // log a gap at startup — partitions 4-7 genuinely had no owner in the moment before
        // instance 1 claimed them — which is the registry being right, not a defect.)
        log0.Clear();
        log1.Clear();

        var map0 = await pod0.RefreshAsync(TestContext.Current.CancellationToken);
        var map1 = await pod1.RefreshAsync(TestContext.Current.CancellationToken);

        foreach (var map in new[] { map0, map1 })
        {
            map.Partitions.Should().Be(Partitions);
            map.Unowned.Should().BeEmpty("every partition in [0,8) is claimed by one of the two instances");
            map.Contested.Should().BeEmpty("the ranges do not overlap");
            map.IsDegraded.Should().BeFalse();
            map.Owners.Should().HaveCount(Partitions);

            for (var p = 0; p < Partitions; p++)
            {
                map.Owners[p].PodName.Should().Be(p < 4 ? "svc-0" : "svc-1");
                map.Owners[p].InstanceId.Should().Be(p < 4 ? pod0.InstanceId : pod1.InstanceId);
            }

            map.Describe().Should().NotContain("(none)");
        }

        log0.Messages(LogLevel.Error).Should().BeEmpty("a correctly split pool logs no ownership error");
        log1.Messages(LogLevel.Error).Should().BeEmpty();

        // And the hash on the server holds exactly one claim per partition. Counted through
        // ReadOwners rather than HLEN: the hash also carries one i:<instanceId> presence field per
        // live instance, which is what the position flusher's liveness cross-check reads (R-01).
        OwnershipRegistry.ReadOwners(await this.Db.HashGetAllAsync(Key(topic, consumer)))
            .Should().HaveCount(Partitions);
    }

    /// <summary>
    /// S6b — the scale-up drift case: <c>Count</c> says 3, only instances 0 and 1 are running, so
    /// partitions 6 and 7 are consumed by nobody. Reported as unowned, logged at Error, health
    /// Degraded.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S6b_a_missing_instance_leaves_its_partitions_unowned_and_health_degraded()
    {
        var topic = fixture.NewTopic("s6b-gap");
        var consumer = fixture.NewConsumer("s6b-gap");

        var range0 = OwnedRange(index: 0, count: 3);
        var range1 = OwnedRange(index: 1, count: 3);
        var missing = OwnedRange(index: 2, count: 3);

        range0.ToArray().Should().Equal(0, 1, 2);
        range1.ToArray().Should().Equal(3, 4, 5);
        missing.ToArray().Should().Equal(new[] { 6, 7 }, "instance 2 is the one that was never started");

        var log0 = new CaptureLogger();
        var log1 = new CaptureLogger();

        await using var pod0 = this.Registry(topic, consumer, range0, "svc-0", log0);
        await using var pod1 = this.Registry(topic, consumer, range1, "svc-1", log1);

        await pod0.StartAsync(TestContext.Current.CancellationToken);
        await pod1.StartAsync(TestContext.Current.CancellationToken);

        log0.Clear();
        log1.Clear();

        var map0 = await pod0.RefreshAsync(TestContext.Current.CancellationToken);
        var map = await pod1.RefreshAsync(TestContext.Current.CancellationToken);

        map0.Unowned.Should().Equal(new[] { 6, 7 }, "the gap is a property of the whole map, so every live instance sees it");
        map.Unowned.Should().Equal(new[] { 6, 7 }, "instance 2 of the declared Count=3 is not running, so nobody claims its range");
        map.Contested.Should().BeEmpty("the two running instances do not overlap — the fault is a gap, not a clash");
        map.IsDegraded.Should().BeTrue();
        map.Owners.Should().HaveCount(6);
        map.Describe().Should().Contain("6->(none)").And.Contain("7->(none)");

        // The Error an operator is paged on, naming both the missing partitions and the variable to
        // check. Both live instances see it — the gap is a property of the map, not of one pod.
        foreach (var log in new[] { log0, log1 })
        {
            var error = log.Messages(LogLevel.Error).Should()
                .ContainSingle(m => m.Contains("ownership gap", StringComparison.Ordinal)).Subject;

            error.Should().Contain("partitions 6,7 of 8")
                 .And.Contain("consumed by nobody")
                 .And.Contain("STREAMS_INSTANCE_COUNT");
        }

        OwnershipRegistry.TryGetSnapshot(topic, consumer, out var snapshot).Should().BeTrue();
        snapshot!.IsDegraded.Should().BeTrue();

        await AssertHealthDegradedAsync(topic, consumer);
    }

    /// <summary>
    /// S6c — the Deployment-with-replicas case: two pods both resolve to <c>Index=0, Count=1</c> and
    /// therefore both claim every partition. Reported as contested, logged at Error, health Degraded.
    /// </summary>
    /// <remarks>
    /// This is the drift that costs money rather than data: nothing errors anywhere else, every
    /// message is simply processed twice.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S6c_two_instances_claiming_index_zero_are_reported_as_contested()
    {
        var topic = fixture.NewTopic("s6c-overlap");
        var consumer = fixture.NewConsumer("s6c-overlap");

        // Both pods of a Deployment fall back to Count=1, Index=0 — so both own everything.
        var everything = OwnedRange(index: 0, count: 1);
        everything.ToArray().Should().Equal(Enumerable.Range(0, Partitions));

        var logA = new CaptureLogger();
        var logB = new CaptureLogger();

        await using var podA = this.Registry(topic, consumer, everything, "deploy-abc12-x2k9p", logA);
        await using var podB = this.Registry(topic, consumer, everything, "deploy-abc12-7f4dq", logB);

        podA.InstanceId.Should().NotBe(podB.InstanceId, "two pods are two processes with two GUIDs — that is the only thing that tells them apart");

        await podA.StartAsync(TestContext.Current.CancellationToken);
        await podB.StartAsync(TestContext.Current.CancellationToken);

        // podB saw podA's claim on every field before stamping its own over it.
        var mapB = podB.Latest;
        mapB.Should().NotBeNull();
        mapB!.Unowned.Should().BeEmpty("both pods claim everything, so nothing is unowned — the fault is duplication, not a gap");
        mapB.Contested.Should().HaveCount(Partitions);
        mapB.Contested.Select(c => c.Partition).Should().Equal(Enumerable.Range(0, Partitions));
        mapB.Contested.Should().AllSatisfy(c =>
        {
            c.Expected.Should().Be(podB.InstanceId);
            c.Actual.InstanceId.Should().Be(podA.InstanceId);
            c.Actual.PodName.Should().Be("deploy-abc12-x2k9p");
        });
        mapB.IsDegraded.Should().BeTrue();

        var overlap = logB.Messages(LogLevel.Error)
            .Where(m => m.Contains("ownership overlap", StringComparison.Ordinal))
            .ToArray();

        overlap.Should().HaveCount(Partitions, "one Error per contested partition");
        overlap[0].Should().Contain("processed twice")
                  .And.Contain("deploy-abc12-x2k9p")
                  .And.Contain("StatefulSet, not a Deployment");

        // And it is symmetric: podA notices too on its next renew, now that podB has stamped the hash.
        logA.Clear();
        var mapA = await podA.RefreshAsync(TestContext.Current.CancellationToken);

        mapA.Contested.Should().HaveCount(Partitions);
        mapA.Contested.Should().AllSatisfy(c => c.Actual.InstanceId.Should().Be(podB.InstanceId));
        logA.Messages(LogLevel.Error).Should().Contain(m => m.Contains("ownership overlap", StringComparison.Ordinal));

        // The hash still has one claim per partition — an overlap is a fight over fields, not extra ones.
        OwnershipRegistry.ReadOwners(await this.Db.HashGetAllAsync(Key(topic, consumer)))
            .Should().HaveCount(Partitions);

        await AssertHealthDegradedAsync(topic, consumer);
    }

    /// <summary>
    /// S6d — a StatefulSet ordinal survives a bounce: the restarted instance 1 derives the same
    /// index from <c>POD_NAME</c>, reclaims exactly [4–7], and once the hard-killed process's field
    /// TTLs lapse the hash holds no stale duplicate of its claims.
    /// </summary>
    /// <remarks>
    /// The kill is modelled by claiming once through <see cref="OwnershipRegistry.RefreshAsync"/>
    /// and never starting the renewal loop — which is exactly what a <c>SIGKILL</c>ed pod leaves
    /// behind: its last claims, with a TTL and nobody to renew them. Calling
    /// <see cref="OwnershipRegistry.StopAsync"/> instead would model a <em>graceful</em> stop, which
    /// deletes the fields immediately and would never exercise the expiry at all.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S6d_a_restarted_instance_reclaims_its_ordinal_range_with_no_stale_duplicate()
    {
        var topic = fixture.NewTopic("s6d-bounce");
        var consumer = fixture.NewConsumer("s6d-bounce");
        var key = Key(topic, consumer);

        // The pod's identity comes from the StatefulSet ordinal, not from explicit config — that is
        // the property under test, so it is resolved the way a real pod resolves it.
        static InstanceIdentity ResolveInstanceOne(ILogger? logger = null)
            => InstanceResolver.Resolve(
                options: null,
                environment: name => name switch
                {
                    InstanceResolver.PodNameVariable => "svc-1",
                    InstanceResolver.CountVariable => "2",
                    _ => null,
                },
                logger);

        var before = ResolveInstanceOne();
        before.Index.Should().Be(1);
        before.Count.Should().Be(2);
        before.IndexSource.Should().Be(InstanceSource.PodOrdinal, "a StatefulSet pod name carries its ordinal");

        var rangeOne = InstanceResolver.Owned(Partitions, before);
        rangeOne.ToArray().Should().Equal(4, 5, 6, 7);

        var log0 = new CaptureLogger();
        await using var pod0 = this.Registry(topic, consumer, OwnedRange(0, 2), "svc-0", log0);
        await pod0.StartAsync(TestContext.Current.CancellationToken);

        // Instance 1, first life: claims [4-7] with a two-second field TTL and no renewal loop, then
        // is hard-killed. Disposing it clears its process-local bookkeeping without releasing the
        // Redis fields, which is what a SIGKILL looks like from Redis's side.
        var killedId = Guid.NewGuid();
        var killed = this.Registry(topic, consumer, rangeOne, "svc-1", logger: null, instanceId: killedId, ttlSeconds: 2, renewSeconds: 1);
        var killedMap = await killed.RefreshAsync(TestContext.Current.CancellationToken);
        await killed.DisposeAsync();

        killedMap.Unowned.Should().BeEmpty();
        killedMap.Contested.Should().BeEmpty();
        OwnershipRegistry.ReadOwners(await this.Db.HashGetAllAsync(key))
            .Should().HaveCount(Partitions, "both instances have claimed while instance 1 is alive");

        // The departed instance's claims expire on their own — no heartbeat parsing, no read-side
        // staleness filter. Polled, never slept on.
        await RedisStreamsFixture.WaitUntilAsync(
            async () => OwnershipRegistry.ReadOwners(await this.Db.HashGetAllAsync(key)).Count == 4,
            TimeSpan.FromSeconds(15),
            "the hard-killed instance's HEXPIRE'd claims to lapse");

        var owners = OwnershipRegistry.ReadOwners(await this.Db.HashGetAllAsync(key));
        owners.Keys.Should().BeEquivalentTo(new[] { 0, 1, 2, 3 }, "only the surviving instance 0's claims remain");
        owners.Values.Should().AllSatisfy(o => o.InstanceId.Should().Be(pod0.InstanceId));

        // The surviving instance now reports the gap, which is the correct reading: 4-7 really are
        // being consumed by nobody until the pod comes back.
        log0.Clear();
        var gap = await pod0.RefreshAsync(TestContext.Current.CancellationToken);
        gap.Unowned.Should().Equal(4, 5, 6, 7);
        gap.IsDegraded.Should().BeTrue();
        log0.Messages(LogLevel.Error).Should().Contain(m => m.Contains("ownership gap", StringComparison.Ordinal));

        // The bounce: same pod name, same ordinal, brand new process GUID.
        var restartLog = new CaptureLogger();
        var after = ResolveInstanceOne(restartLog);
        after.Index.Should().Be(before.Index, "a StatefulSet pod keeps its ordinal across a restart");

        var reclaimed = InstanceResolver.Owned(Partitions, after);
        reclaimed.Should().Be(rangeOne, "the restarted instance recomputes exactly the range it had");

        var restartedId = Guid.NewGuid();
        restartedId.Should().NotBe(killedId);

        await using var restarted = this.Registry(topic, consumer, reclaimed, "svc-1", restartLog, restartedId);
        await restarted.StartAsync(TestContext.Current.CancellationToken);
        var map = restarted.Latest;
        map.Should().NotBeNull();

        map!.Unowned.Should().BeEmpty("the restarted instance covers exactly the partitions that were unowned");
        map.Contested.Should().BeEmpty("the killed instance's claims had already expired, so nothing was taken from anybody");
        map.IsDegraded.Should().BeFalse();

        OwnershipRegistry.ReadOwners(await this.Db.HashGetAllAsync(key)).Should().HaveCount(
            Partitions,
            "one claim per partition — the old instance left no stale duplicate behind");

        var final = OwnershipRegistry.ReadOwners(await this.Db.HashGetAllAsync(key));
        final.Should().HaveCount(Partitions);
        for (var p = 0; p < Partitions; p++)
        {
            final[p].PodName.Should().Be(p < 4 ? "svc-0" : "svc-1");
            final[p].InstanceId.Should().Be(p < 4 ? pod0.InstanceId : restartedId);
            final[p].InstanceId.Should().NotBe(killedId, "no claim from the killed process survives");
        }

        restartLog.Messages(LogLevel.Error).Should().BeEmpty("a clean bounce back into the same ordinal is not an ownership fault");

        var restartMap = restartLog.Messages(LogLevel.Information)
            .Should().ContainSingle(m => m.Contains("full map:", StringComparison.Ordinal)).Subject;
        restartMap.Should().Contain("claims partitions 4,5,6,7").And.NotContain("(none)");
    }

    /// <summary>
    /// R-15 — a departing instance releases only the claims it still holds. Deleting the fields
    /// unconditionally took the <em>other</em> pod's live claims with it in exactly the contested
    /// case the registry exists to report, turning an overlap into an ownership gap for a whole TTL.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Release_deletes_only_the_claims_this_instance_still_holds()
    {
        var topic = fixture.NewTopic("release-cas");
        var consumer = fixture.NewConsumer("release-cas");
        var key = Key(topic, consumer);

        var everything = OwnedRange(index: 0, count: 1);

        var leaving = this.Registry(topic, consumer, everything, "deploy-abc12-leaving", logger: null);
        await using var staying = this.Registry(topic, consumer, everything, "deploy-abc12-staying", logger: null);

        await leaving.StartAsync(TestContext.Current.CancellationToken);

        // The staying pod stamps its own id over every field, so the leaving pod holds none of them
        // any more — only its own presence field.
        await staying.StartAsync(TestContext.Current.CancellationToken);

        var claimed = OwnershipRegistry.ReadOwners(await this.Db.HashGetAllAsync(key));
        claimed.Should().HaveCount(Partitions);
        claimed.Values.Should().AllSatisfy(o => o.InstanceId.Should().Be(staying.InstanceId));

        await leaving.StopAsync(TestContext.Current.CancellationToken);

        var after = OwnershipRegistry.ReadOwners(await this.Db.HashGetAllAsync(key));
        after.Should().HaveCount(
            Partitions,
            "a pod that no longer holds a field must not delete what overwrote it — that would report a gap where there is a live owner");
        after.Values.Should().AllSatisfy(o => o.InstanceId.Should().Be(staying.InstanceId));

        (await this.Db.HashExistsAsync(key, OwnershipRegistry.PresenceField(leaving.InstanceId)))
            .Should().BeFalse("the departing instance does drop its own presence field");
        (await this.Db.HashExistsAsync(key, OwnershipRegistry.PresenceField(staying.InstanceId)))
            .Should().BeTrue("the surviving instance is still here");

        await leaving.DisposeAsync();
    }

    /// <summary>
    /// R-31 — a pod shut down on an already-cancelled token still releases. The release used to
    /// early-return on a cancelled token, which is exactly the token a host stopping under SIGTERM
    /// hands it: the claim and the presence field then survived until the TTL lapsed, and a
    /// successor pod read the departed instance as a live second writer for that whole window.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_stop_on_an_already_cancelled_token_still_drops_the_claims_and_the_presence_field()
    {
        var topic = fixture.NewTopic("release-cancelled");
        var consumer = fixture.NewConsumer("release-cancelled");
        var key = Key(topic, consumer);

        var leaving = this.Registry(topic, consumer, OwnedRange(index: 0, count: 1), "deploy-abc12-leaving", logger: null);
        await leaving.StartAsync(TestContext.Current.CancellationToken);

        (await this.Db.HashExistsAsync(key, OwnershipRegistry.PresenceField(leaving.InstanceId)))
            .Should().BeTrue("the instance is running, so it is present");

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await leaving.StopAsync(cancelled.Token);

        (await this.Db.HashExistsAsync(key, OwnershipRegistry.PresenceField(leaving.InstanceId)))
            .Should().BeFalse(
                "a departing pod that stays 'present' for a whole TTL is read as a live rival by the pod replacing it");

        OwnershipRegistry.ReadOwners(await this.Db.HashGetAllAsync(key))
            .Should().BeEmpty("its partition claims go with it, so the successor does not wait out the TTL to take them");

        await leaving.DisposeAsync();
    }

    private IDatabase Db => fixture.Db;

    private static RedisKey Key(string topic, string consumer) => StreamKeys.Ownership(topic, consumer);

    /// <summary>The range instance <paramref name="index"/> of <paramref name="count"/> owns, via the real resolver.</summary>
    private static PartitionRange OwnedRange(int index, int count)
        => InstanceResolver.Owned(
            Partitions,
            InstanceResolver.Resolve(new InstanceOptions { Count = count, Index = index }, _ => null));

    /// <summary>
    /// Asserts the process-wide health check reports Degraded because of ownership.
    /// </summary>
    /// <remarks>
    /// The check short-circuits to Healthy when nothing is being consumed at all, so a partition
    /// monitor is registered for the duration — otherwise the ownership branch is never reached and
    /// the test would pass for the wrong reason.
    /// </remarks>
    private static async Task AssertHealthDegradedAsync(string topic, string consumer)
    {
        var monitor = StreamLag.Track(topic, consumer, 0, StreamKeys.Stream(topic, 0));
        monitor.MarkRunning();

        try
        {
            OwnershipRegistry.AnyDegraded().Should().BeTrue();

            var result = await new StreamsHealthCheck().CheckHealthAsync(
                new HealthCheckContext(),
                TestContext.Current.CancellationToken);

            result.Status.Should().Be(
                HealthStatus.Degraded,
                "an ownership gap or overlap is a configuration fault a restart cannot fix, so it must not fail readiness either");
            result.Description.Should().Contain("ownership registry shows a gap or an overlap");
            result.Data["ownershipDegraded"].Should().Be(true);
        }
        finally
        {
            StreamLag.Forget(monitor);
        }
    }

    private OwnershipRegistry Registry(
        string topic,
        string consumer,
        PartitionRange range,
        string podName,
        ILogger? logger,
        Guid? instanceId = null,
        int ttlSeconds = 30,
        int renewSeconds = 10)
        => new(
            fixture.Redis,
            new OwnershipRegistryOptions
            {
                Topic = topic,
                Consumer = consumer,
                Partitions = Partitions,
                OwnedPartitions = range.ToArray(),
                PodName = podName,
                InstanceId = instanceId ?? Guid.NewGuid(),
                TtlSeconds = ttlSeconds,
                RenewSeconds = renewSeconds,
            },
            logger);

    /// <summary>
    /// An <see cref="ILogger"/> that keeps every formatted message, so a test can assert on the
    /// Error the plan requires rather than on a side effect that happens to correlate with it.
    /// </summary>
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

        /// <summary>Forgets everything so far, so the next assertion is about the next phase only.</summary>
        public void Clear()
        {
            lock (this.gate)
            {
                this.entries.Clear();
            }
        }
    }
}
