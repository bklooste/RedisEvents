using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using RedisEvents.Diagnostics;
using RedisEvents.Web;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// The rule that is allowed to restart a consumer: behind the tail of its stream, with a position
/// that has not changed, for <c>UnhealthyBehindSeconds</c>. These pin the grading matrix on a fake
/// clock — what fires it, and every neighbouring condition that must not: a backlog that is
/// draining, an idle topic, an unknown or stale tail, a trimmed stream, a fresh acquisition, and the
/// states that have thresholds of their own.
/// </summary>
/// <remarks>
/// Monitors are constructed directly rather than through <see cref="StreamLag.Track"/> wherever the
/// test does not need the process-wide registry, so these do not share state with the other health
/// tests running beside them.
/// </remarks>
[Trait("TestType", "UnitTest")]
public sealed class BehindTailHealthTests
{
    private const int Window = 300;

    private static readonly StreamId Tail = new(1_700_000_000_500, 0);
    private static readonly StreamId Earlier = new(1_700_000_000_100, 0);

    private readonly ManualClock clock = new();

    private StreamPartitionMonitor Monitor(
        int unhealthyBehindSeconds = Window,
        StreamId startPosition = default,
        int unhealthyBlockSeconds = 300,
        int unhealthyStoppedSeconds = 300,
        int partition = 0)
    {
        var monitor = new StreamPartitionMonitor(
            "orders",
            "billing",
            partition,
            $"s:{{orders}}:{partition}",
            unhealthyLagMs: 120_000,
            unhealthyBlockSeconds,
            unhealthyStoppedSeconds,
            unhealthyBehindSeconds,
            startPosition,
            this.clock);

        monitor.MarkRunning();
        return monitor;
    }

    private static StreamHealthReport Grade(params StreamPartitionMonitor[] monitors)
        => StreamHealth.Evaluate(monitors.Select(StreamStatus.Snapshot).ToArray());

    /// <summary>Advances the clock in sampler-sized steps, re-sampling the same tail each time.</summary>
    private void Hold(StreamPartitionMonitor monitor, StreamId tail, TimeSpan duration)
    {
        var step = TimeSpan.FromSeconds(15);

        for (var elapsed = TimeSpan.Zero; elapsed < duration; elapsed += step)
        {
            this.clock.Advance(step);
            monitor.SetTail(tail);
        }
    }

    [Fact]
    public void behind_the_tail_with_a_frozen_position_is_Unhealthy_once_the_window_has_passed()
    {
        var monitor = this.Monitor(startPosition: Earlier);
        monitor.SetTail(Tail);

        this.Hold(monitor, Tail, TimeSpan.FromSeconds(Window - 15));
        Grade(monitor).Status.Should().Be(StreamHealthStatus.Healthy, "the window has not passed yet");

        this.Hold(monitor, Tail, TimeSpan.FromSeconds(15));
        var report = Grade(monitor);

        report.Status.Should().Be(StreamHealthStatus.Unhealthy);
        report.Rule.Should().Be(StreamHealthRule.BehindTail);
        report.Partition!.Value.Partition.Should().Be(0);
        report.Description.Should()
            .Contain("orders[0]/billing")
            .And.Contain("behind the tail")
            .And.Contain(Earlier.Format())
            .And.Contain(Tail.Format())
            .And.Contain("UnhealthyBehindSeconds of 300");
        report.Data["partitionsBehind"].Should().Be(1);
        report.Data["behindPartition"].Should().Be("orders[0]/billing");
    }

    [Fact]
    public void a_moving_tail_does_not_restart_the_clock_only_the_position_does()
    {
        var monitor = this.Monitor(startPosition: Earlier);

        for (var i = 0; i <= Window / 15; i++)
        {
            monitor.SetTail(new StreamId(Tail.Ms + i, 0));
            this.clock.Advance(TimeSpan.FromSeconds(15));
        }

        Grade(monitor).Rule.Should().Be(
            StreamHealthRule.BehindTail,
            "entries arriving behind a consumer that is not reading them is the fault, not a reason to wait longer");
    }

    [Fact]
    public void behind_but_advancing_is_never_Unhealthy_however_large_the_backlog()
    {
        var monitor = this.Monitor(startPosition: Earlier);
        var farAhead = new StreamId(Tail.Ms + 86_400_000, 0);
        monitor.SetTail(farAhead);

        // An hour of a consumer that manages one entry every 100 seconds — far slower than its
        // backlog, and never once frozen for the whole 300-second window.
        for (var i = 1; i <= 36; i++)
        {
            this.Hold(monitor, farAhead, TimeSpan.FromSeconds(90));
            monitor.Observe(new StreamId(Earlier.Ms + i, 0));
            this.clock.Advance(TimeSpan.FromSeconds(10));

            var status = StreamStatus.Snapshot(monitor);
            status.IsBehindTail.Should().BeTrue();
            status.BehindMs.Should().BeLessThan(Window * 1000d);
            Grade(monitor).Status.Should().NotBe(StreamHealthStatus.Unhealthy, "any change of position restarts the clock");
        }
    }

    [Fact]
    public void a_reset_that_moves_the_position_backwards_restarts_the_clock_too()
    {
        var monitor = this.Monitor(startPosition: Earlier);
        monitor.SetTail(Tail);
        this.Hold(monitor, Tail, TimeSpan.FromSeconds(Window - 30));

        monitor.Observe(new StreamId(Earlier.Ms - 50, 0));
        this.Hold(monitor, Tail, TimeSpan.FromSeconds(60));

        Grade(monitor).Status.Should().NotBe(
            StreamHealthStatus.Unhealthy,
            "a change is a change; the rule is about a position that does not move at all");
    }

    [Fact]
    public void caught_up_and_idle_is_never_Unhealthy()
    {
        var monitor = this.Monitor(startPosition: Tail);
        monitor.SetTail(Tail);

        this.Hold(monitor, Tail, TimeSpan.FromHours(24));

        var status = StreamStatus.Snapshot(monitor);
        status.IsBehindTail.Should().BeFalse("the tail equals the position: there is nothing unread");
        status.BehindMs.Should().Be(0);
        status.PositionUnchangedMs.Should().BeGreaterThanOrEqualTo(TimeSpan.FromHours(24).TotalMilliseconds);
        Grade(monitor).Status.Should().Be(StreamHealthStatus.Healthy);
    }

    [Fact]
    public void an_entry_landing_after_a_long_idle_starts_the_clock_then_rather_than_inheriting_the_idle_time()
    {
        var monitor = this.Monitor(startPosition: Tail);
        monitor.SetTail(Tail);
        this.Hold(monitor, Tail, TimeSpan.FromHours(24));

        var next = new StreamId(Tail.Ms + 1, 0);
        monitor.SetTail(next);

        var status = StreamStatus.Snapshot(monitor);
        status.IsBehindTail.Should().BeTrue();
        status.BehindMs.Should().Be(0, "the partition has only just been seen behind");
        Grade(monitor).Status.Should().Be(
            StreamHealthStatus.Healthy,
            "a day of silence followed by one entry must give the consumer the whole window to read it");

        this.Hold(monitor, next, TimeSpan.FromSeconds(Window));
        Grade(monitor).Rule.Should().Be(StreamHealthRule.BehindTail, "and if it does not, that is the fault");
    }

    [Fact]
    public void the_rule_is_off_by_default()
    {
        var monitor = this.Monitor(unhealthyBehindSeconds: 0, startPosition: Earlier);
        monitor.SetTail(Tail);
        this.Hold(monitor, Tail, TimeSpan.FromHours(24));

        var status = StreamStatus.Snapshot(monitor);
        status.IsBehindTail.Should().BeTrue("the signal is reported whatever the threshold");
        status.BehindMs.Should().BeGreaterThan(0);
        Grade(monitor).Status.Should().Be(StreamHealthStatus.Healthy, "0 means no existing consumer changes behaviour");
        new ConsumerOptionsProbe().DefaultBehindSeconds.Should().Be(0);
    }

    [Fact]
    public void an_unsampled_tail_is_unknown_and_unknown_is_not_Unhealthy()
    {
        var monitor = this.Monitor(startPosition: Earlier);
        this.clock.Advance(TimeSpan.FromHours(1));

        var status = StreamStatus.Snapshot(monitor);
        status.TailId.Should().BeNull();
        status.TailSampleAgeMs.Should().Be(-1);
        status.TailUnchangedMs.Should().Be(-1);
        status.IsBehindTail.Should().BeFalse();
        Grade(monitor).Status.Should().Be(StreamHealthStatus.Healthy);
    }

    [Fact]
    public void a_tail_sample_that_has_gone_stale_stops_counting_until_sampling_resumes()
    {
        var monitor = this.Monitor(startPosition: Earlier);
        monitor.SetTail(Tail);
        this.Hold(monitor, Tail, TimeSpan.FromSeconds(Window));
        Grade(monitor).Rule.Should().Be(StreamHealthRule.BehindTail);

        // The sampler stops answering. One missed sample is tolerated...
        this.clock.Advance(StreamLag.TailStaleAfter);
        StreamStatus.Snapshot(monitor).IsBehindTail.Should().BeTrue("a sample inside the staleness bound still counts");

        // ...but past four of them the tail is no longer something we know.
        this.clock.Advance(TimeSpan.FromSeconds(1));
        var stale = StreamStatus.Snapshot(monitor);
        stale.TailId.Should().Be(Tail, "the last value is still reported, with its age");
        stale.TailSampleAgeMs.Should().BeGreaterThan(StreamLag.TailStaleAfter.TotalMilliseconds);
        stale.IsBehindTail.Should().BeFalse();
        stale.BehindMs.Should().Be(0);
        Grade(monitor).Status.Should().Be(StreamHealthStatus.Healthy, "a failed sample must never be what restarts a pod");

        monitor.SetTail(Tail);
        Grade(monitor).Rule.Should().Be(
            StreamHealthRule.BehindTail,
            "the position never moved, so once the tail is confirmed again the partition was behind all along");
    }

    [Fact]
    public void a_position_past_the_tail_after_a_trim_or_a_recreated_stream_is_not_behind()
    {
        var monitor = this.Monitor(startPosition: Tail);
        monitor.SetTail(Earlier);
        this.Hold(monitor, Earlier, TimeSpan.FromHours(1));

        StreamStatus.Snapshot(monitor).IsBehindTail.Should().BeFalse();
        Grade(monitor).Status.Should().Be(StreamHealthStatus.Healthy);
    }

    [Fact]
    public void an_empty_or_missing_stream_is_not_behind_even_at_position_zero()
    {
        var monitor = this.Monitor();
        monitor.SetTail(StreamId.Min);
        this.Hold(monitor, StreamId.Min, TimeSpan.FromHours(1));

        var status = StreamStatus.Snapshot(monitor);
        status.Position.Should().Be(StreamId.Min);
        status.TailId.Should().Be(StreamId.Min, "an empty stream is a known tail, distinct from an unsampled one");
        status.IsBehindTail.Should().BeFalse("0-0 is not strictly greater than 0-0");
        Grade(monitor).Status.Should().Be(StreamHealthStatus.Healthy);
    }

    [Fact]
    public void a_stream_trimmed_to_empty_behind_a_frozen_position_stops_being_behind()
    {
        var monitor = this.Monitor(startPosition: Earlier);
        monitor.SetTail(Tail);
        this.Hold(monitor, Tail, TimeSpan.FromSeconds(Window));
        Grade(monitor).Rule.Should().Be(StreamHealthRule.BehindTail);

        monitor.SetTail(StreamId.Min);

        Grade(monitor).Status.Should().Be(StreamHealthStatus.Healthy, "there is no longer an entry to read");
    }

    [Fact]
    public void a_restarted_consumer_that_has_processed_nothing_is_measured_from_its_stored_position()
    {
        // The stored position is the tail: caught up, restarted, and the topic stays quiet.
        var monitor = this.Monitor(startPosition: Tail);
        monitor.SetTail(Tail);
        this.Hold(monitor, Tail, TimeSpan.FromHours(1));

        var status = StreamStatus.Snapshot(monitor);
        status.LastProcessed.Should().Be(StreamId.Min, "nothing has been processed since the restart");
        status.Position.Should().Be(Tail, "but the partition is not at 0-0, it is where it resumed from");
        status.IsBehindTail.Should().BeFalse();
        Grade(monitor).Status.Should().Be(StreamHealthStatus.Healthy);
    }

    [Fact]
    public void the_clock_starts_when_the_partition_is_acquired_not_when_the_process_started()
    {
        // The process has been up for a day before this instance takes the partition over.
        this.clock.Advance(TimeSpan.FromHours(24));

        var monitor = this.Monitor(startPosition: Earlier);
        monitor.SetTail(Tail);

        var status = StreamStatus.Snapshot(monitor);
        status.PositionUnchangedMs.Should().Be(0);
        status.BehindMs.Should().Be(0);
        Grade(monitor).Status.Should().Be(StreamHealthStatus.Healthy, "a new owner inherits a backlog, not the old owner's stall time");

        this.Hold(monitor, Tail, TimeSpan.FromSeconds(Window));
        Grade(monitor).Rule.Should().Be(StreamHealthRule.BehindTail, "and gets exactly one window to start moving");
    }

    [Fact]
    public void a_blocked_partition_answers_to_its_block_threshold_not_to_the_behind_window()
    {
        var monitor = this.Monitor(unhealthyBehindSeconds: 60, startPosition: Earlier, unhealthyBlockSeconds: 3600);
        monitor.SetTail(Tail);
        monitor.MarkBlocked();

        this.Hold(monitor, Tail, TimeSpan.FromSeconds(600));

        var inside = Grade(monitor);
        inside.Status.Should().Be(StreamHealthStatus.Degraded, "an hour of retrying was configured; the shorter behind window must not cut it to a minute");
        inside.Rule.Should().Be(StreamHealthRule.Blocked);
        StreamStatus.Snapshot(monitor).IsBehindTail.Should().BeTrue("the signal itself is still reported");

        this.Hold(monitor, Tail, TimeSpan.FromSeconds(3600));

        var past = Grade(monitor);
        past.Rule.Should().Be(StreamHealthRule.BlockedTooLong);
        past.Description.Should().Contain("blocked").And.Contain("UnhealthyBlockSeconds of 3600");
    }

    [Fact]
    public void a_policy_stop_stays_Degraded_however_long_it_sits_behind_the_tail()
    {
        var running = this.Monitor(startPosition: Tail, partition: 1);
        running.SetTail(Tail);

        var monitor = this.Monitor(unhealthyBehindSeconds: 60, startPosition: Earlier, unhealthyStoppedSeconds: 0);
        monitor.SetTail(Tail);
        monitor.SetLagEntries(1_000_000);
        monitor.MarkStopped("ErrorPolicy.StopPartition after InvalidOperationException: poison");

        this.Hold(monitor, Tail, TimeSpan.FromHours(1));
        running.SetTail(Tail);

        var status = StreamStatus.Snapshot(monitor);
        status.Escalates.Should().BeFalse();

        var report = Grade(monitor, running);
        report.Status.Should().Be(StreamHealthStatus.Degraded, "restarting would only replay the poison entry the operator chose to stop on");
        report.Rule.Should().Be(StreamHealthRule.Stopped);
    }

    [Fact]
    public void an_escalating_stop_is_reported_as_the_stop_it_is()
    {
        var running = this.Monitor(startPosition: Tail, partition: 1);
        running.SetTail(Tail);

        var monitor = this.Monitor(unhealthyBehindSeconds: 60, startPosition: Earlier, unhealthyStoppedSeconds: 120);
        monitor.SetTail(Tail);
        monitor.SetLagEntries(42);
        monitor.MarkStopped("contested position: another instance was also writing it", escalate: true);

        this.Hold(monitor, Tail, TimeSpan.FromSeconds(90));
        running.SetTail(Tail);

        StreamStatus.Snapshot(monitor).Escalates.Should().BeTrue();
        Grade(monitor, running).Rule.Should().Be(
            StreamHealthRule.Stopped,
            "a stopped partition waits for UnhealthyStoppedSeconds, not the shorter behind window");

        this.Hold(monitor, Tail, TimeSpan.FromSeconds(60));
        running.SetTail(Tail);

        var report = Grade(monitor, running);
        report.Rule.Should().Be(StreamHealthRule.StoppedTooLong);
        report.Description.Should().Contain("stood down").And.Contain("contested position");
    }

    [Fact]
    public void escalates_reads_false_on_a_partition_that_is_no_longer_stopped()
    {
        var monitor = this.Monitor();
        monitor.MarkStopped("contested position", escalate: true);
        StreamStatus.Snapshot(monitor).Escalates.Should().BeTrue();

        monitor.MarkRunning();
        StreamStatus.Snapshot(monitor).Escalates.Should().BeFalse();
    }

    [Fact]
    public void a_partition_that_never_gets_past_starting_is_caught_by_the_same_rule()
    {
        var monitor = new StreamPartitionMonitor(
            "orders", "billing", 0, "s:{orders}:0", 120_000, 300, 300, Window, Earlier, this.clock);
        monitor.SetTail(Tail);

        this.Hold(monitor, Tail, TimeSpan.FromSeconds(Window));

        var report = Grade(monitor);
        report.Rule.Should().Be(StreamHealthRule.BehindTail);
        report.Description.Should().Contain("has not started reading");
    }

    [Fact]
    public void a_blocked_partition_past_its_threshold_is_named_ahead_of_a_behind_one()
    {
        var blocked = this.Monitor(startPosition: Earlier, unhealthyBlockSeconds: 60, partition: 0);
        var frozen = this.Monitor(startPosition: Earlier, partition: 1);
        blocked.MarkBlocked();
        blocked.SetTail(Tail);
        frozen.SetTail(Tail);

        this.clock.Advance(TimeSpan.FromSeconds(Window));
        blocked.SetTail(Tail);
        frozen.SetTail(Tail);

        var report = Grade(frozen, blocked);
        report.Rule.Should().Be(StreamHealthRule.BlockedTooLong, "the declared state is the more specific diagnosis");
        report.Data["partitionsBehind"].Should().Be(2);
    }

    [Fact]
    public void the_time_the_tail_last_changed_ignores_a_trim_and_follows_new_entries()
    {
        var monitor = this.Monitor(startPosition: Tail);
        monitor.SetTail(Tail);
        this.Hold(monitor, Tail, TimeSpan.FromSeconds(600));
        StreamStatus.Snapshot(monitor).TailUnchangedMs.Should().Be(600_000);

        monitor.SetTail(StreamId.Min);
        this.clock.Advance(TimeSpan.FromSeconds(15));
        StreamStatus.Snapshot(monitor).TailUnchangedMs.Should().Be(615_000, "emptying the stream is not something being written");

        monitor.SetTail(new StreamId(Tail.Ms + 1, 0));
        var status = StreamStatus.Snapshot(monitor);
        status.TailUnchangedMs.Should().Be(0);
        status.TailSampleAgeMs.Should().Be(0);
    }

    [Fact]
    public void the_tail_unchanged_clock_counts_from_the_first_sample_on_a_stream_that_has_never_had_an_entry()
    {
        var monitor = this.Monitor();
        this.clock.Advance(TimeSpan.FromHours(1));
        monitor.SetTail(StreamId.Min);
        this.Hold(monitor, StreamId.Min, TimeSpan.FromSeconds(120));

        var status = StreamStatus.Snapshot(monitor);
        status.TailUnchangedMs.Should().Be(120_000);
        status.PositionUnchangedMs.Should().Be(3_720_000, "the position clock has run since the partition was acquired");
    }

    [Fact]
    public void the_sampler_tick_notices_a_position_change_without_a_sample()
    {
        var monitor = this.Monitor(startPosition: Earlier);
        monitor.SetTail(Tail);
        this.clock.Advance(TimeSpan.FromSeconds(200));

        monitor.Observe(new StreamId(Earlier.Ms + 1, 0));
        monitor.RefreshProgress();
        this.clock.Advance(TimeSpan.FromSeconds(200));
        monitor.SetTail(Tail);

        var status = StreamStatus.Snapshot(monitor);
        status.PositionUnchangedMs.Should().Be(200_000, "the change was stamped when the tick saw it, not when the snapshot was taken");
        status.BehindMs.Should().Be(200_000);
        Grade(monitor).Status.Should().NotBe(StreamHealthStatus.Unhealthy);
    }

    [Fact]
    public void a_hand_built_snapshot_grades_without_the_new_members_and_is_never_behind()
    {
        var status = new StreamPartitionStatus
        {
            Topic = "orders",
            Consumer = "billing",
            Partition = 0,
            State = StreamPartitionRunState.Running,
            LastProcessed = Earlier,
            IsCaughtUp = false,
            LagMs = 0,
            LagEntries = 5,
            BlockedMs = 0,
            StoppedMs = 0,
            UnhealthyLagMs = 120_000,
            UnhealthyBlockSeconds = 300,
            UnhealthyStoppedSeconds = 300,
        };

        StreamHealth.Grade(in status).Should().Be(new StreamPartitionHealth(StreamHealthStatus.Healthy, StreamHealthRule.None));

        // A duration alone, without the tail comparison behind it, is not a verdict.
        var durationOnly = status with { UnhealthyBehindSeconds = 1, BehindMs = 10_000_000 };
        StreamHealth.Grade(in durationOnly).Status.Should().Be(StreamHealthStatus.Healthy);

        var behind = durationOnly with { IsBehindTail = true };
        StreamHealth.Grade(in behind).Should().Be(new StreamPartitionHealth(StreamHealthStatus.Unhealthy, StreamHealthRule.BehindTail));
    }

    [Fact]
    public void process_level_facts_come_through_the_context()
    {
        var monitor = this.Monitor(startPosition: Tail);
        monitor.SetTail(Tail);
        var snapshot = new[] { StreamStatus.Snapshot(monitor) };

        var down = StreamHealth.Evaluate(snapshot, new StreamHealthContext { ConnectionFault = "redis:6379" });
        down.Status.Should().Be(StreamHealthStatus.Unhealthy);
        down.Rule.Should().Be(StreamHealthRule.ConnectionDown);
        down.Description.Should().Contain("redis:6379");

        var neverStarted = StreamHealth.Evaluate(snapshot, new StreamHealthContext { Consumers = 2, ConsumersStarted = 0 });
        neverStarted.Rule.Should().Be(StreamHealthRule.NoConsumerStarted);

        var ownership = StreamHealth.Evaluate(snapshot, new StreamHealthContext { OwnershipDegraded = true });
        ownership.Status.Should().Be(StreamHealthStatus.Degraded);
        ownership.Rule.Should().Be(StreamHealthRule.OwnershipDegraded);

        StreamHealth.Evaluate([]).Status.Should().Be(StreamHealthStatus.Healthy, "a producer-only process has nothing to grade");
    }

    /// <summary>Reads the option's default without tying the test above to the options type's other members.</summary>
    private sealed class ConsumerOptionsProbe
    {
        public int DefaultBehindSeconds { get; } = new RedisEvents.Config.ConsumerOptions().UnhealthyBehindSeconds;
    }
}

/// <summary>
/// <see cref="StreamsHealthCheck"/> is an adapter over <see cref="StreamHealth"/>: the same rule has
/// to come out of <c>/health</c> as comes out of the core grading, with the status mapped by name.
/// </summary>
/// <remarks>
/// Uses the process-wide registry, because that is what the health check reads — so it clears it
/// like the other health-check tests do.
/// </remarks>
[Trait("TestType", "UnitTest")]
[Collection(StreamMonitorCollection.Name)]
public sealed class BehindTailHealthCheckTests : IDisposable
{
    private readonly ManualClock clock = new();

    public BehindTailHealthCheckTests() => StreamLag.Clear();

    public void Dispose() => StreamLag.Clear();

    [Fact]
    public async Task the_health_check_reports_what_the_core_grading_decided()
    {
        var position = new StreamId(1_700_000_000_100, 0);
        var tail = new StreamId(1_700_000_000_500, 0);

        var monitor = StreamLag.Track(
            "orders", "billing", 0, "s:{orders}:0", unhealthyBehindSeconds: 30, startPosition: position, clock: this.clock);
        monitor.MarkRunning();
        monitor.SetTail(tail);

        var before = await new StreamsHealthCheck().CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);
        before.Status.Should().NotBe(HealthStatus.Unhealthy);

        this.clock.Advance(TimeSpan.FromSeconds(30));
        monitor.SetTail(tail);

        var core = StreamHealth.Evaluate(StreamStatus.Partitions());
        var result = await new StreamsHealthCheck().CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        core.Rule.Should().Be(StreamHealthRule.BehindTail);
        result.Status.Should().Be(HealthStatus.Unhealthy, "Unhealthy maps to Unhealthy by name; the two enums number in opposite directions");
        result.Description.Should().Be(core.Description);
        result.Data["behindPartition"].Should().Be("orders[0]/billing");
    }
}

/// <summary>A clock that moves only when told to, for both wall time and elapsed-time stamps.</summary>
internal sealed class ManualClock : TimeProvider
{
    // Not zero: the monitor's older fields use a zero timestamp to mean "not blocked / not stopped".
    private long ticks = TimeSpan.FromDays(1).Ticks;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp() => Interlocked.Read(ref this.ticks);

    public override DateTimeOffset GetUtcNow()
        => new DateTimeOffset(2026, 10, 10, 0, 0, 0, TimeSpan.Zero) + TimeSpan.FromTicks(this.GetTimestamp());

    public void Advance(TimeSpan by) => Interlocked.Add(ref this.ticks, by.Ticks);
}
