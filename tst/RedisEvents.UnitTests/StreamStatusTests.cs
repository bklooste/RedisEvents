using FluentAssertions;
using RedisEvents.Diagnostics;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// <see cref="StreamStatus"/> is the public reading of the per-partition monitors, for hosts that
/// cannot use <c>StreamsHealthCheck</c> — a Generic Host worker has no <c>/health</c> endpoint, so
/// a watchdog that wants to stop a process whose partition has died has to read the signals itself.
/// These pin the snapshot's contract: every field reaches the caller, a stalled partition is
/// distinguishable from an idle one, and the snapshot does not move after it is taken.
/// </summary>
[Collection(StreamMonitorCollection.Name)]
public sealed class StreamStatusTests : IDisposable
{
    public StreamStatusTests() => StreamLag.Clear();

    public void Dispose() => StreamLag.Clear();

    [Fact]
    public void a_producer_only_process_reports_no_partitions()
    {
        StreamStatus.Any.Should().BeFalse();
        StreamStatus.Partitions().Should().BeEmpty();
    }

    [Fact]
    public void a_tracked_partition_reports_its_identity_and_its_configured_thresholds()
    {
        StreamLag.Track("orders", "billing", 3, "s:{orders}:3", unhealthyLagMs: 45_000, unhealthyBlockSeconds: 90, unhealthyStoppedSeconds: 30);

        var status = StreamStatus.Partitions().Should().ContainSingle().Subject;

        status.Topic.Should().Be("orders");
        status.Consumer.Should().Be("billing");
        status.Partition.Should().Be(3);
        status.UnhealthyLagMs.Should().Be(45_000);
        status.UnhealthyBlockSeconds.Should().Be(90);
        status.UnhealthyStoppedSeconds.Should().Be(30);
    }

    [Fact]
    public void a_partition_that_has_processed_nothing_reports_the_minimum_position()
    {
        StreamLag.Track("orders", "billing", 0, "s:{orders}:0").MarkRunning();

        var status = StreamStatus.Partitions().Single();

        status.State.Should().Be(StreamPartitionRunState.Running);
        status.LastProcessed.Should().Be(StreamId.Min);
        status.LagMs.Should().Be(0, "there is no measurement yet, and reporting a number would invent one");
    }

    [Fact]
    public void the_position_advances_as_the_partition_processes()
    {
        var monitor = StreamLag.Track("orders", "billing", 0, "s:{orders}:0");
        monitor.MarkRunning();
        monitor.Observe(new StreamId(1_700_000_000_000, 4));

        var first = StreamStatus.Partitions().Single().LastProcessed;

        monitor.Observe(new StreamId(1_700_000_000_000, 9));

        var second = StreamStatus.Partitions().Single().LastProcessed;

        first.Should().Be(new StreamId(1_700_000_000_000, 4));
        second.Should().BeGreaterThan(first, "comparing two snapshots is how a watchdog tells a stalled partition from a busy one");
    }

    [Fact]
    public void an_idle_partition_at_the_tail_is_caught_up_and_reports_no_lag()
    {
        var monitor = StreamLag.Track("orders", "billing", 0, "s:{orders}:0");
        monitor.MarkRunning();
        monitor.Observe(new StreamId(1_700_000_000_000, 1));
        monitor.MarkCaughtUp();

        var status = StreamStatus.Partitions().Single();

        status.IsCaughtUp.Should().BeTrue();
        status.LagMs.Should().Be(0, "a quiet topic must not read as lag that climbs overnight");
    }

    [Fact]
    public void a_partition_behind_the_tail_is_not_caught_up_and_reports_its_backlog()
    {
        var monitor = StreamLag.Track("orders", "billing", 0, "s:{orders}:0");
        monitor.MarkRunning();
        monitor.Observe(new StreamId(1_700_000_000_000, 1));
        monitor.SetLagEntries(42);

        var status = StreamStatus.Partitions().Single();

        status.IsCaughtUp.Should().BeFalse();
        status.LagEntries.Should().Be(42);
        status.LagMs.Should().BeGreaterThan(0);
    }

    [Fact]
    public void an_unsampled_backlog_is_reported_as_unknown_rather_than_zero()
    {
        StreamLag.Track("orders", "billing", 0, "s:{orders}:0").MarkRunning();

        StreamStatus.Partitions().Single().LagEntries
            .Should().Be(-1, "zero would claim the partition is caught up when the sampler has simply not run");
    }

    [Fact]
    public void a_stopped_partition_reports_the_state_and_the_reason()
    {
        var monitor = StreamLag.Track("orders", "billing", 0, "s:{orders}:0");
        monitor.MarkRunning();
        monitor.MarkStopped("contested position", escalate: true);

        var status = StreamStatus.Partitions().Single();

        status.State.Should().Be(StreamPartitionRunState.Stopped);
        status.StopReason.Should().Be("contested position");
        status.StoppedMs.Should().BeGreaterThanOrEqualTo(0);
    }

    [Fact]
    public void a_snapshot_does_not_change_when_the_partition_does()
    {
        var monitor = StreamLag.Track("orders", "billing", 0, "s:{orders}:0");
        monitor.MarkRunning();

        var before = StreamStatus.Partitions().Single();

        monitor.MarkStopped("contested position", escalate: true);
        monitor.SetLagEntries(7);

        before.State.Should().Be(StreamPartitionRunState.Running, "a snapshot is a copy, not a handle on a live object");
        before.StopReason.Should().BeNull();
        before.LagEntries.Should().Be(-1);
        StreamStatus.Partitions().Single().State.Should().Be(StreamPartitionRunState.Stopped);
    }

    [Fact]
    public void every_partition_of_every_consumer_in_the_process_is_reported()
    {
        StreamLag.Track("orders", "billing", 0, "s:{orders}:0").MarkRunning();
        StreamLag.Track("orders", "billing", 1, "s:{orders}:1").MarkRunning();
        StreamLag.Track("shipments", "fulfilment", 0, "s:{shipments}:0").MarkRunning();

        StreamStatus.Partitions().Should().HaveCount(3);
        StreamStatus.Any.Should().BeTrue();
    }
}
