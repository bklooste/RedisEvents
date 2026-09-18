using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using RedisEvents.Diagnostics;
using RedisEvents.Web;

namespace RedisEvents.UnitTests;

/// <summary>
/// A partition that stood down outside any error policy — a contested position, a retired
/// co-located slot — is read by nobody until the process restarts, and until this change the
/// contested path did not even mark its monitor, so the health check said Healthy over a dead
/// partition for eight hours. These pin down the watchdog: such a stop is Degraded at once and
/// Unhealthy once it has outlasted <c>UnhealthyStoppedSeconds</c> with entries behind it, while an
/// <c>ErrorPolicy.StopPartition</c> stop — an operator's decision — stays Degraded.
/// </summary>
public sealed class StoppedPartitionHealthTests : IDisposable
{
    public StoppedPartitionHealthTests() => StreamLag.Clear();

    public void Dispose() => StreamLag.Clear();

    private static Task<HealthCheckResult> CheckAsync()
        => new StreamsHealthCheck().CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

    /// <summary>
    /// The partition under test plus a running sibling. Without the sibling every case below would
    /// hit the older "all partition workers have stopped" rule and be Unhealthy for that reason,
    /// which is not the rule these tests are about — offer-odds had a healthy partition 0 beside
    /// its dead partition 1, and that is the shape that hid it.
    /// </summary>
    private static StreamPartitionMonitor Stopped(string reason, bool escalate, int unhealthyStoppedSeconds)
    {
        StreamLag.Track("orders", "billing", 0, "s:{orders}:0").MarkRunning();

        var monitor = StreamLag.Track("orders", "billing", 1, "s:{orders}:1", unhealthyStoppedSeconds: unhealthyStoppedSeconds);
        monitor.MarkRunning();
        monitor.MarkStopped(reason, escalate);
        return monitor;
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_escalating_stop_that_is_behind_its_stream_past_the_threshold_is_Unhealthy()
    {
        var monitor = Stopped("contested position: another instance was also writing it", escalate: true, unhealthyStoppedSeconds: 0);
        monitor.SetLagEntries(42);

        // StoppedMs must exceed 0 × 1000; a stopwatch tick has certainly passed, but say so.
        await Task.Delay(TimeSpan.FromMilliseconds(5));

        var result = await CheckAsync();

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("stood down").And.Contain("42 entries behind").And.Contain("UnhealthyStoppedSeconds");
        result.Data["partitionsStopped"].Should().Be(1);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_escalating_stop_is_only_Degraded_while_inside_the_threshold()
    {
        var monitor = Stopped("contested position", escalate: true, unhealthyStoppedSeconds: 3600);
        monitor.SetLagEntries(42);

        var result = await CheckAsync();

        result.Status.Should().Be(HealthStatus.Degraded, "a stop that is still inside its grace period should alert, not restart");
        result.Description.Should().Contain("stopped");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_escalating_stop_at_the_head_of_its_stream_is_only_Degraded()
    {
        var monitor = Stopped("contested position", escalate: true, unhealthyStoppedSeconds: 0);
        monitor.SetLagEntries(0);

        await Task.Delay(TimeSpan.FromMilliseconds(5));

        var result = await CheckAsync();

        result.Status.Should().Be(HealthStatus.Degraded, "nothing is piling up behind a partition at its tail; a restart would fix nothing yet");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_unsampled_stop_is_only_Degraded()
    {
        var monitor = Stopped("contested position", escalate: true, unhealthyStoppedSeconds: 0);

        await Task.Delay(TimeSpan.FromMilliseconds(5));

        var result = await CheckAsync();

        monitor.LagEntries.Should().Be(-1, "the sampler has not run");
        result.Status.Should().Be(HealthStatus.Degraded, "unknown is not evidence that entries are being left behind");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_policy_stop_never_escalates_however_far_behind()
    {
        var monitor = Stopped("ErrorPolicy.StopPartition after InvalidOperationException: poison", escalate: false, unhealthyStoppedSeconds: 0);
        monitor.SetLagEntries(1_000_000);

        await Task.Delay(TimeSpan.FromMilliseconds(5));

        var result = await CheckAsync();

        monitor.Escalates.Should().BeFalse();
        result.Status.Should().Be(HealthStatus.Degraded, "StopPartition is the operator asking for exactly this state; restarting would only replay the poison batch");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_first_stop_decides_whether_it_escalates()
    {
        var monitor = StreamLag.Track("orders", "billing", 1, "s:{orders}:1");
        monitor.MarkRunning();

        monitor.MarkStopped("ErrorPolicy.StopPartition after poison");
        monitor.MarkStopped("the partition was stood down while its co-located siblings kept reading", escalate: true);

        monitor.Escalates.Should().BeFalse("the co-located reader retiring a slot its policy already stopped must not upgrade an operator's decision");
        monitor.StopReason.Should().Contain("co-located", "the latest reason is still the one surfaced");

        monitor.MarkRunning();
        monitor.MarkStopped("contested position", escalate: true);

        monitor.Escalates.Should().BeTrue("a fresh stop after a run is judged on its own");
    }
}
