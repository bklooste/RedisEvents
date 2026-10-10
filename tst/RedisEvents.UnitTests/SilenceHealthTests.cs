using FluentAssertions;
using RedisEvents.Diagnostics;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// The other half of a frozen position: caught up with the tail and receiving nothing for
/// <c>DegradedSilenceSeconds</c>. These pin that it is Degraded and never Unhealthy, that it is
/// judged per consumer over all of its running partitions, and every neighbour that must not fire
/// it: the rule switched off, a window not yet reached, an unknown or stale tail, a partition that
/// is behind, and one busy sibling.
/// </summary>
[Trait("TestType", "UnitTest")]
public sealed class SilenceHealthTests
{
    private const int Window = 600;

    private static readonly StreamId Tail = new(1_700_000_000_500, 0);
    private static readonly StreamId Earlier = new(1_700_000_000_100, 0);

    private readonly ManualClock clock = new();

    private StreamPartitionMonitor Monitor(
        int degradedSilenceSeconds = Window,
        StreamId? startPosition = null,
        int partition = 0,
        string consumer = "billing",
        int unhealthyBehindSeconds = 0)
    {
        var monitor = new StreamPartitionMonitor(
            "orders",
            consumer,
            partition,
            $"s:{{orders}}:{partition}",
            unhealthyLagMs: 120_000,
            unhealthyBlockSeconds: 300,
            unhealthyStoppedSeconds: 300,
            unhealthyBehindSeconds,
            startPosition ?? Tail,
            this.clock,
            degradedSilenceSeconds);

        monitor.MarkRunning();
        return monitor;
    }

    private static StreamHealthReport Grade(params StreamPartitionMonitor[] monitors)
        => StreamHealth.Evaluate(monitors.Select(StreamStatus.Snapshot).ToArray());

    /// <summary>Advances the clock in sampler-sized steps, re-sampling each monitor's tail each time.</summary>
    private void Hold(TimeSpan duration, params (StreamPartitionMonitor Monitor, StreamId Tail)[] held)
    {
        var step = TimeSpan.FromSeconds(15);

        for (var elapsed = TimeSpan.Zero; elapsed < duration; elapsed += step)
        {
            this.clock.Advance(step);

            foreach (var (monitor, tail) in held)
            {
                monitor.SetTail(tail);
            }
        }
    }

    [Fact]
    public void caught_up_and_unmoved_past_the_window_is_Degraded_and_names_the_rule()
    {
        var monitor = this.Monitor();
        monitor.SetTail(Tail);

        this.Hold(TimeSpan.FromSeconds(Window + 15), (monitor, Tail));

        var report = Grade(monitor);
        report.Status.Should().Be(StreamHealthStatus.Degraded);
        report.Rule.Should().Be(StreamHealthRule.Silent);
        report.Description.Should().Contain("billing").And.Contain("DegradedSilenceSeconds of 600");
        report.Data["silentConsumer"].Should().Be("orders/billing");
    }

    [Fact]
    public void silence_is_never_Unhealthy_however_long_it_lasts()
    {
        var monitor = this.Monitor();
        monitor.SetTail(Tail);

        this.Hold(TimeSpan.FromHours(24), (monitor, Tail));

        Grade(monitor).Status.Should().Be(StreamHealthStatus.Degraded);
    }

    [Fact]
    public void the_rule_is_off_by_default()
    {
        var monitor = this.Monitor(degradedSilenceSeconds: 0);
        monitor.SetTail(Tail);

        this.Hold(TimeSpan.FromHours(24), (monitor, Tail));

        Grade(monitor).Status.Should().Be(StreamHealthStatus.Healthy);
    }

    [Fact]
    public void inside_the_window_is_Healthy()
    {
        var monitor = this.Monitor();
        monitor.SetTail(Tail);

        this.Hold(TimeSpan.FromSeconds(Window - 30), (monitor, Tail));

        Grade(monitor).Status.Should().Be(StreamHealthStatus.Healthy);
    }

    [Fact]
    public void a_tail_that_was_never_sampled_is_unknown_and_unknown_is_not_silent()
    {
        var monitor = this.Monitor();

        this.clock.Advance(TimeSpan.FromHours(1));

        StreamHealth.FindSilent([StreamStatus.Snapshot(monitor)]).Should().BeNull();
    }

    [Fact]
    public void a_stale_tail_sample_is_not_silent()
    {
        var monitor = this.Monitor();
        monitor.SetTail(Tail);
        this.Hold(TimeSpan.FromSeconds(Window + 15), (monitor, Tail));

        // The sampler stops: past four missed samples the tail is no longer known.
        this.clock.Advance(TimeSpan.FromSeconds(90));

        StreamHealth.FindSilent([StreamStatus.Snapshot(monitor)]).Should().BeNull();
    }

    [Fact]
    public void a_partition_behind_its_tail_is_not_silent()
    {
        var monitor = this.Monitor(startPosition: Earlier);
        monitor.SetTail(Tail);

        this.Hold(TimeSpan.FromSeconds(Window + 15), (monitor, Tail));

        StreamStatus.Snapshot(monitor).IsBehindTail.Should().BeTrue();
        Grade(monitor).Rule.Should().NotBe(StreamHealthRule.Silent);
    }

    [Fact]
    public void one_busy_sibling_partition_keeps_the_consumer_from_being_silent()
    {
        var idle = this.Monitor(partition: 0);
        idle.SetTail(Tail);
        this.Hold(TimeSpan.FromSeconds(Window + 15), (idle, Tail));

        // A partition whose position clock started just now: it has not been unmoved for the window.
        var busy = this.Monitor(partition: 1);
        busy.SetTail(Tail);

        Grade(idle, busy).Status.Should().Be(StreamHealthStatus.Healthy);
    }

    [Fact]
    public void every_partition_idle_reports_the_shortest_silence_and_the_partition_count()
    {
        var first = this.Monitor(partition: 0);
        first.SetTail(Tail);
        this.Hold(TimeSpan.FromSeconds(300), (first, Tail));

        var second = this.Monitor(partition: 1);
        second.SetTail(Tail);
        this.Hold(TimeSpan.FromSeconds(Window + 15), (first, Tail), (second, Tail));

        var silent = StreamHealth.FindSilent([StreamStatus.Snapshot(first), StreamStatus.Snapshot(second)]);
        silent.Should().NotBeNull();
        silent!.Value.Partitions.Should().Be(2);
        silent.Value.SilentMs.Should().BeApproximately((Window + 15) * 1000d, 1000d);
    }

    [Fact]
    public void the_window_is_per_consumer_so_a_quiet_consumer_without_one_is_left_alone()
    {
        var feed = this.Monitor(consumer: "feed");
        var internalTopic = this.Monitor(consumer: "internal", degradedSilenceSeconds: 0);
        feed.SetTail(Tail);
        internalTopic.SetTail(Tail);

        this.Hold(TimeSpan.FromSeconds(Window + 15), (feed, Tail), (internalTopic, Tail));

        var silent = StreamHealth.FindSilent([StreamStatus.Snapshot(internalTopic), StreamStatus.Snapshot(feed)]);
        silent!.Value.Consumer.Should().Be("feed");
    }

    [Fact]
    public void a_partition_that_is_not_running_is_left_to_its_own_rule()
    {
        var monitor = this.Monitor();
        monitor.SetTail(Tail);
        this.Hold(TimeSpan.FromSeconds(Window + 15), (monitor, Tail));
        monitor.MarkStopped("policy");

        StreamHealth.FindSilent([StreamStatus.Snapshot(monitor)]).Should().BeNull();
    }

    [Fact]
    public void behind_and_frozen_outranks_silence_on_another_consumer()
    {
        var silent = this.Monitor(consumer: "feed");
        var frozen = this.Monitor(consumer: "billing", startPosition: Earlier, degradedSilenceSeconds: 0, unhealthyBehindSeconds: 300);
        silent.SetTail(Tail);
        frozen.SetTail(Tail);

        this.Hold(TimeSpan.FromSeconds(Window + 15), (silent, Tail), (frozen, Tail));

        var report = Grade(silent, frozen);
        report.Status.Should().Be(StreamHealthStatus.Unhealthy);
        report.Rule.Should().Be(StreamHealthRule.BehindTail);
    }
}
