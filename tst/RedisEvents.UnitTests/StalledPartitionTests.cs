using System.Text;

using FluentAssertions;

using RedisEvents.Consumer;
using RedisEvents.Diagnostics;
using RedisEvents.Errors;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// The 2026-10-25 offer-odds-projection incident, at unit scale. After an out-of-memory crash one
/// pod held both <c>mult_feed_odds</c> partitions for four hours without processing a message: its
/// Redis calls timed out or threw <c>InvalidCastException: Cannot convert to RedisValue: Array</c>,
/// its health check said only Degraded, and nothing ever handed the partitions on.
/// </summary>
/// <remarks>
/// <para>
/// These pin the three local pieces of the fix. <b>The stall clock</b>: a partition that makes no
/// progress — no read returning, no batch completing, no position write landing — is graded
/// Unhealthy on this process's own clock, where the behind-the-tail rule needed a tail sample from
/// the same broken connection and stood down without one. <b>Unreadable replies</b>: a reply the
/// client cannot parse is a transport failure the read loop waits out, not a fault that ends the
/// partition while its lease stays renewed. <b>The parser</b>: an array where <c>XREAD</c> has a
/// scalar is reported as a malformed reply instead of escaping as a bare cast exception.
/// </para>
/// <para>
/// The lease hand-off itself needs a real Redis and is in <c>StalledLeaseHandoffTests</c>.
/// </para>
/// </remarks>
[Trait("TestType", "UnitTest")]
public sealed class StalledPartitionGradingTests
{
    private const int Window = 300;

    private readonly ManualClock clock = new();

    private StreamPartitionMonitor Monitor(int unhealthyStalledSeconds = Window, int partition = 0)
    {
        var monitor = new StreamPartitionMonitor(
            "mult_feed_odds",
            "offer_odds_proj",
            partition,
            $"s:{{mult_feed_odds}}:{partition}",
            unhealthyLagMs: 120_000,
            unhealthyBlockSeconds: 300,
            unhealthyStoppedSeconds: 300,
            unhealthyBehindSeconds: 300,
            startPosition: new StreamId(1_700_000_000_000, 0),
            clock: this.clock,
            unhealthyStalledSeconds: unhealthyStalledSeconds);

        monitor.MarkRunning();
        return monitor;
    }

    private static StreamHealthReport Evaluate(params StreamPartitionMonitor[] monitors)
        => StreamHealth.Evaluate(
            [.. monitors.Select(StreamStatus.Snapshot)],
            new StreamHealthContext { Consumers = 1, ConsumersStarted = 1 });

    /// <summary>
    /// The incident, graded. A partition that processed a batch, then stopped making progress, with
    /// every tail sample failing — the behind-the-tail rule has nothing fresh to compare against, so
    /// before this change the best the check could say was Degraded (lagging). The stall rule says
    /// Unhealthy, which is what restarts the pod.
    /// </summary>
    [Fact]
    public void A_partition_with_no_progress_past_the_window_is_Unhealthy_even_with_no_tail_sample()
    {
        var monitor = this.Monitor();
        var now = this.clock.GetUtcNow().ToUnixTimeMilliseconds();
        monitor.Observe(new StreamId(now, 0));

        // The last sample before the connection went bad: behind, with entries waiting. Every
        // sample after it fails, so it goes stale and behind-the-tail stands down.
        monitor.SetTail(new StreamId(now + 10_000, 0));
        monitor.SetLagEntries(4_000);

        this.clock.Advance(TimeSpan.FromSeconds(Window + 1));

        var status = StreamStatus.Snapshot(monitor);
        status.IsBehindTail.Should().BeFalse("a tail sample this old is not knowledge of the tail");
        status.StalledMs.Should().BeGreaterThanOrEqualTo(Window * 1000d);

        var report = Evaluate(monitor);

        report.Status.Should().Be(StreamHealthStatus.Unhealthy);
        report.Rule.Should().Be(StreamHealthRule.Stalled);
        report.Description.Should().Contain("no progress").And.Contain($"UnhealthyStalledSeconds of {Window}");
        report.Partition!.Value.Partition.Should().Be(0);
    }

    /// <summary>The same partition with the rule off: the gap the incident fell through.</summary>
    [Fact]
    public void With_the_stall_rule_off_the_same_partition_is_only_Degraded()
    {
        var monitor = this.Monitor(unhealthyStalledSeconds: 0);
        var now = this.clock.GetUtcNow().ToUnixTimeMilliseconds();
        monitor.Observe(new StreamId(now, 0));
        monitor.SetTail(new StreamId(now + 10_000, 0));
        monitor.SetLagEntries(4_000);

        this.clock.Advance(TimeSpan.FromSeconds(Window + 1));

        var report = Evaluate(monitor);

        report.Status.Should().Be(StreamHealthStatus.Degraded, "this is all the health check could say about the stuck pod");
        report.Rule.Should().Be(StreamHealthRule.Lagging);
    }

    /// <summary>
    /// An idle consumer is never stalled: every empty read is progress, and in <c>ReadMode.Block</c>
    /// one completes every <c>BlockMs</c> whether or not anything is published.
    /// </summary>
    [Fact]
    public void An_idle_consumer_completing_empty_reads_never_stalls()
    {
        var monitor = this.Monitor();

        for (var i = 0; i < Window * 3; i++)
        {
            this.clock.Advance(TimeSpan.FromSeconds(1));
            monitor.MarkCaughtUp();
        }

        StreamStatus.Snapshot(monitor).StalledMs.Should().BeLessThan(2_000);
        Evaluate(monitor).Status.Should().Be(StreamHealthStatus.Healthy);
    }

    /// <summary>
    /// A slow handler that still completes batches is not stalled, however long the backlog: each
    /// completed batch is progress.
    /// </summary>
    [Fact]
    public void A_slow_handler_that_completes_batches_never_stalls()
    {
        var monitor = this.Monitor();

        for (var i = 1; i <= 10; i++)
        {
            this.clock.Advance(TimeSpan.FromSeconds(Window - 10));
            monitor.Observe(new StreamId(1_700_000_000_000 + i, 0));
        }

        Grade(monitor).Rule.Should().NotBe(StreamHealthRule.Stalled);
    }

    /// <summary>
    /// A handler that never returns stalls its partition even while the reader, running ahead of the
    /// processor, keeps completing empty reads on a quiet topic.
    /// </summary>
    [Fact]
    public void A_handler_that_never_returns_stalls_even_while_empty_reads_complete()
    {
        var monitor = this.Monitor();

        monitor.MarkBusy();

        for (var i = 0; i < Window + 1; i++)
        {
            this.clock.Advance(TimeSpan.FromSeconds(1));
            monitor.MarkCaughtUp();
        }

        Grade(monitor).Rule.Should().Be(StreamHealthRule.Stalled);

        monitor.MarkIdle();
        Grade(monitor).Rule.Should().NotBe(StreamHealthRule.Stalled, "the batch finishing is progress");
    }

    /// <summary>
    /// Reads that keep failing on the transport — timeouts, a garbled reply — are the outage the rule
    /// exists for: Blocked, but the stall clock runs, and past the window it is Unhealthy. Before,
    /// a block that flapped back to Running on the odd successful read never reached
    /// <c>UnhealthyBlockSeconds</c> either.
    /// </summary>
    [Fact]
    public void A_partition_whose_reads_keep_failing_stalls()
    {
        var monitor = this.Monitor();

        monitor.MarkTransportBlocked();
        this.clock.Advance(TimeSpan.FromSeconds(Window + 1));

        monitor.State.Should().Be(PartitionRunState.Blocked);
        Grade(monitor).Should().Be(new StreamPartitionHealth(StreamHealthStatus.Unhealthy, StreamHealthRule.Stalled));
    }

    /// <summary>
    /// A partition blocked retrying a <c>DontIgnoreException</c> is doing what its handler asked; it
    /// answers to <c>UnhealthyBlockSeconds</c>, not to the stall rule. The error contract is unchanged.
    /// </summary>
    [Fact]
    public void A_handler_block_is_graded_by_the_block_rule_not_the_stall_rule()
    {
        var monitor = this.Monitor(unhealthyStalledSeconds: 10);

        monitor.MarkBlocked();
        this.clock.Advance(TimeSpan.FromSeconds(60));

        StreamStatus.Snapshot(monitor).StalledMs.Should().Be(0);
        Grade(monitor).Should().Be(new StreamPartitionHealth(StreamHealthStatus.Degraded, StreamHealthRule.Blocked));

        monitor.MarkUnblocked();
        StreamStatus.Snapshot(monitor).StalledMs.Should().BeLessThan(1_000, "the end of a block is progress");
    }

    /// <summary>
    /// Reading and handling, but every position write failing: not durable progress. The host feeds
    /// the flusher's failure duration in; past the window the partition is stalled.
    /// </summary>
    [Fact]
    public void Position_writes_that_keep_failing_stall_a_partition_that_is_still_reading()
    {
        var monitor = this.Monitor();

        monitor.MarkCaughtUp();
        monitor.SetPersistFailing(TimeSpan.FromSeconds(Window + 5));

        Grade(monitor).Rule.Should().Be(StreamHealthRule.Stalled);

        monitor.SetPersistFailing(TimeSpan.Zero);
        Grade(monitor).Rule.Should().NotBe(StreamHealthRule.Stalled, "a write that lands clears it");
    }

    /// <summary>
    /// A partition the host stood down for stalling stays Unhealthy after its lease has gone — at
    /// once, not after another window — until this instance reads it again.
    /// </summary>
    [Fact]
    public void A_stalled_out_partition_is_Unhealthy_until_it_is_read_again()
    {
        var sibling = this.Monitor(partition: 1);
        sibling.MarkCaughtUp();

        var monitor = this.Monitor();
        monitor.MarkStalledOut("stalled: no progress for 300s — no read returned and no batch completed");

        var status = StreamStatus.Snapshot(monitor);
        status.StoodDownStalled.Should().BeTrue();
        status.State.Should().Be(StreamPartitionRunState.Stopped);

        var report = Evaluate(sibling, monitor);
        report.Status.Should().Be(StreamHealthStatus.Unhealthy);
        report.Rule.Should().Be(StreamHealthRule.Stalled);
        report.Description.Should().Contain("stood down").And.Contain("restart");

        monitor.MarkRunning();
        Grade(monitor).Rule.Should().NotBe(StreamHealthRule.Stalled);
    }

    /// <summary>An operator's <c>ErrorPolicy.StopPartition</c> stop is not a stall.</summary>
    [Fact]
    public void A_policy_stop_is_not_a_stall()
    {
        var monitor = this.Monitor(unhealthyStalledSeconds: 10);

        monitor.MarkStopped("ErrorPolicy.StopPartition after InvalidOperationException: boom");
        this.clock.Advance(TimeSpan.FromSeconds(60));

        StreamStatus.Snapshot(monitor).StalledMs.Should().Be(0);
        Grade(monitor).Rule.Should().Be(StreamHealthRule.Stopped);
    }

    private static StreamPartitionHealth Grade(StreamPartitionMonitor monitor)
    {
        var status = StreamStatus.Snapshot(monitor);
        return StreamHealth.Grade(in status);
    }
}

/// <summary>
/// A reply the client could not read is a transport failure: the read loop waits it out instead of
/// ending the partition. Before, an <see cref="InvalidCastException"/> out of StackExchange.Redis —
/// how a desynchronised connection shows up — faulted the read loop, and with the lease renewed by
/// a script that never looks at the read side, nobody read the partition again.
/// </summary>
[Collection(StreamMonitorCollection.Name)]
[Trait("TestType", "UnitTest")]
public sealed class UnreadableReplyTests : IDisposable
{
    public UnreadableReplyTests() => StreamLag.Clear();

    public void Dispose()
    {
        PartitionWorker.BlockDelay = null;
        StreamLag.Clear();
    }

    /// <summary>The cast StackExchange.Redis raises when a reply lands on the wrong command.</summary>
    private static InvalidCastException DesyncCast()
    {
        var ex = new InvalidCastException("Cannot convert to RedisValue: Array");

        // What Message.ComputeResult attaches before failing the command with it.
        ex.Data.Add("got", "*2\r\n...");
        return ex;
    }

    /// <summary>A genuine cast failure from inside StackExchange.Redis, for the Source check.</summary>
    private static InvalidCastException RealStackExchangeCast()
    {
        try
        {
            _ = (RedisValue)RedisResult.Create([RedisResult.Create((RedisValue)"a"), RedisResult.Create((RedisValue)"b")]);
        }
        catch (InvalidCastException ex)
        {
            return ex;
        }

        throw new InvalidOperationException("Casting a two-element array result to a value was expected to throw.");
    }

    [Fact]
    public void Unreadable_replies_are_recognised_and_ordinary_failures_are_not()
    {
        PartitionWorker.IsUnreadableReply(DesyncCast()).Should().BeTrue();
        PartitionWorker.IsUnreadableReply(RealStackExchangeCast()).Should().BeTrue();
        PartitionWorker.IsUnreadableReply(new StreamTransportException("the XREAD reply was not in the expected shape")).Should().BeTrue();
        PartitionWorker.IsUnreadableReply(new InvalidOperationException("High-integrity mode detected possible protocol de-sync")).Should().BeTrue();

        PartitionWorker.IsUnreadableReply(new InvalidCastException("a handler's own bug")).Should().BeFalse();
        PartitionWorker.IsUnreadableReply(new InvalidOperationException("something else")).Should().BeFalse();
        PartitionWorker.IsUnreadableReply(new NotSupportedException()).Should().BeFalse();
    }

    /// <summary>
    /// The read loop survives both shapes of unreadable reply and delivers the batch that follows;
    /// the partition reads Blocked meanwhile, with the stall clock running.
    /// </summary>
    [Theory]
    [InlineData("desync")]
    [InlineData("malformed")]
    public async Task The_read_loop_waits_out_an_unreadable_reply_instead_of_ending(string shape)
    {
        var gate = new SemaphoreSlim(0);
        PartitionWorker.BlockDelay = async (_, ct) => await gate.WaitAsync(ct).ConfigureAwait(false);

        var monitor = StreamLag.Track("t", "c", 0, (RedisKey)"s:t:0", unhealthyStalledSeconds: 300);
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = Pipeline.Context(channel.Writer) with { Monitor = monitor };
        var calls = 0;

        ValueTask<StreamEntryBatch> Fetch(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            return Interlocked.Increment(ref calls) switch
            {
                1 => throw (shape == "desync" ? DesyncCast() : new StreamTransportException("an entry id arrived as an array")),
                2 => new ValueTask<StreamEntryBatch>(Pipeline.Batch(Pipeline.Entry(1, 0, "Keep", "after-the-garble"))),
                _ => Park(ct),
            };
        }

        using var cts = new CancellationTokenSource();
        var read = PartitionWorker.ReadLoopAsync(ctx, StreamId.Min, Fetch, cts.Token);

        await Pipeline.WaitFor(() => monitor.State == PartitionRunState.Blocked, "the unreadable reply to block the partition");

        read.IsFaulted.Should().BeFalse("an unreadable reply must not end the partition");
        StreamStatus.Snapshot(monitor).StalledMs.Should().BeGreaterThanOrEqualTo(0, "the stall clock is running, not suspended");

        gate.Release();

        var batch = await channel.Reader.ReadAsync(TestContext.Current.CancellationToken);
        Encoding.UTF8.GetString(batch.AsSpan()[0].Body.Span).Should().Be("after-the-garble");
        batch.Return();

        monitor.State.Should().Be(PartitionRunState.Running, "the next good read ends the outage");

        await cts.CancelAsync();
        await read;
    }

    /// <summary>The stall clock keeps running through a transport block, unlike a handler block.</summary>
    [Fact]
    public async Task A_transport_block_does_not_suspend_the_stall_clock()
    {
        var monitor = StreamLag.Track("t", "c", 0, (RedisKey)"s:t:0", unhealthyStalledSeconds: 300);
        monitor.MarkRunning();

        monitor.MarkTransportBlocked();
        await Task.Delay(20);
        StreamStatus.Snapshot(monitor).StalledMs.Should().BeGreaterThan(0);

        monitor.MarkBlocked();
        StreamStatus.Snapshot(monitor).StalledMs.Should().Be(0);
    }

    private static async ValueTask<StreamEntryBatch> Park(CancellationToken ct)
    {
        await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
        return StreamEntryBatch.Empty;
    }
}

/// <summary>
/// <see cref="BlockFetch"/>'s parser claimed that anything not in the <c>XREAD</c> shape throws a
/// <see cref="StreamTransportException"/>, but an array in a scalar slot escaped through a plain
/// <c>(RedisValue)</c> cast as a bare <see cref="InvalidCastException"/> — not a transport failure
/// to the read loop, so one garbled reply ended the partition for good.
/// </summary>
[Trait("TestType", "UnitTest")]
public sealed class BlockFetchGarbledReplyTests
{
    private static byte[] Bytes(string key) => (byte[]?)(RedisValue)key ?? [];

    private static RedisResult TwoItems() => Reply.Array(Reply.Value("x"), Reply.Value("y"));

    [Fact]
    public void An_array_where_the_stream_key_belongs_is_a_malformed_reply()
    {
        var reply = Reply.Array(Reply.Array(TwoItems(), Reply.Array(Reply.Entry("100-0", ("b", "one")))));

        var parse = () => BlockFetch.Parse(reply, Bytes("s:t:0"));

        parse.Should().Throw<StreamTransportException>().WithMessage("*stream key arrived as an array*");
    }

    [Fact]
    public void An_array_where_the_entry_id_belongs_is_a_malformed_reply()
    {
        var entry = Reply.Array(TwoItems(), Reply.Array(Reply.Value("b"), Reply.Value("one")));
        var reply = Reply.Array(Reply.Resp2Stream("s:t:0", Reply.Array(entry)));

        var parse = () => BlockFetch.Parse(reply, Bytes("s:t:0"));

        parse.Should().Throw<StreamTransportException>().WithMessage("*entry id arrived as an array*");
    }

    [Fact]
    public void An_array_where_a_field_value_belongs_is_a_malformed_reply()
    {
        var entry = Reply.Array(Reply.Value("100-0"), Reply.Array(Reply.Value("b"), TwoItems()));
        var reply = Reply.Array(Reply.Resp2Stream("s:t:0", Reply.Array(entry)));

        var parse = () => BlockFetch.Parse(reply, Bytes("s:t:0"));

        parse.Should().Throw<StreamTransportException>().WithMessage("*field value arrived as an array*");
    }

    [Fact]
    public void The_multi_stream_parse_reports_a_garbled_key_the_same_way()
    {
        RedisKey[] keys = ["s:t:0", "s:t:1"];
        byte[][] keyBytes = [Bytes("s:t:0"), Bytes("s:t:1")];
        var reply = Reply.Array(Reply.Array(TwoItems(), Reply.Array(Reply.Entry("100-0", ("b", "one")))));

        var parse = () => BlockFetch.ParseAll(reply, keys, keyBytes);

        parse.Should().Throw<StreamTransportException>();
    }
}
