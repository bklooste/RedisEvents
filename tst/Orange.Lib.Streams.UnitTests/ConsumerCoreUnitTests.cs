using System.Diagnostics.Metrics;
using System.Text;
using System.Threading.Channels;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Diagnostics;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Wire;

using StackExchange.Redis;

namespace Orange.Lib.Streams.UnitTests;

/// <summary>
/// The R-gate unit tests for the consumer core: the co-located inline loop's missing transport
/// handling and monitor updates (R-06), the shutdown <see cref="ChannelClosedException"/> race
/// (R-09), and the two <c>ProcessBatchAsync</c> defects — a filtered-empty batch advancing outside
/// the error handling, and a blocked batch's retry period landing in <c>streams.batch.duration</c>
/// and in the <c>streams.process</c> span (R-10).
/// </summary>
/// <remarks>
/// No Redis: every one of these drives the loops through their fetch seam, exactly as
/// <c>ConsumerPipelineTests</c> and <c>ErrorHandlingTests</c> do, and the backoff runs through
/// <see cref="PartitionWorker.BlockDelay"/> so a 1 s → 30 s ladder costs milliseconds.
/// </remarks>
public static class CoLocatedInline
{
    /// <summary>A tracked monitor for one partition of the shared test topic.</summary>
    internal static StreamPartitionMonitor Monitor(string topic, int partition)
        => StreamLag.Track(topic, "c", partition, (RedisKey)$"s:t:{partition}");

    /// <summary>One partition's context for the inline group loop: no writer, and a live monitor.</summary>
    internal static PartitionContext Context(StreamPartitionMonitor monitor, int partition)
        => Pipeline.Context(writer: null, partition: partition) with { Monitor = monitor };

    /// <summary>A slice carrying one entry for the given partition's stream key.</summary>
    internal static StreamSlice Slice(int partition, long ms, long seq, string body)
        => new((RedisKey)$"s:t:{partition}", [Pipeline.Entry(ms, seq, "Keep", body)]);

    /// <summary>A fetch that never returns until the token is cancelled.</summary>
    internal static async ValueTask<StreamSlice[]> ParkAsync(CancellationToken ct)
    {
        await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);

        return [];
    }
}

/// <summary>R-06 — the co-located inline loop's transport handling and monitor updates.</summary>
public class CoLocatedInlineLoopTests
{
    /// <summary>
    /// A dropped connection is an outage to be waited out, not the end of the group.
    /// </summary>
    /// <remarks>
    /// The loop awaited its fetch bare, so one <see cref="RedisConnectionException"/> — a failover, a
    /// rolling Redis restart — faulted it permanently and took every partition the worker owned with
    /// it until somebody restarted the pod. The other three loops have always retried; this asserts
    /// the fourth does too, and that the messages after the outage are still delivered.
    /// </remarks>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_transient_transport_failure_is_retried_rather_than_fatal()
    {
        StreamLag.Clear();
        PartitionWorker.BlockDelay = static (_, ct) => Task.Delay(5, ct);

        var monitors = new[] { CoLocatedInline.Monitor("t", 0), CoLocatedInline.Monitor("t", 1) };
        var partitions = new[]
        {
            CoLocatedInline.Context(monitors[0], 0),
            CoLocatedInline.Context(monitors[1], 1),
        };

        var seen = new List<string>();
        var calls = 0;

        ValueTask<StreamSlice[]> Fetch(StreamPosition[] positions, int count, CancellationToken ct)
        {
            _ = positions;
            _ = count;
            ct.ThrowIfCancellationRequested();

            return Interlocked.Increment(ref calls) switch
            {
                1 => throw new RedisConnectionException(ConnectionFailureType.SocketFailure, "the primary went away"),
                2 => new ValueTask<StreamSlice[]>([CoLocatedInline.Slice(0, 1, 0, "after-the-outage")]),
                _ => CoLocatedInline.ParkAsync(ct),
            };
        }

        using var cts = new CancellationTokenSource();

        var loop = PartitionWorker.ReadGroupInlineLoopAsync(
            partitions,
            [StreamId.Min, StreamId.Min],
            Fetch,
            (batch, _) =>
            {
                lock (seen)
                {
                    seen.Add(Encoding.UTF8.GetString(batch.Span[0].Body.Span));
                }

                return ValueTask.CompletedTask;
            },
            static (_, _) => { },
            cts.Token);

        await Pipeline.WaitFor(
            () =>
            {
                lock (seen)
                {
                    return seen.Count == 1;
                }
            },
            "the loop to survive the connection failure and deliver the next batch");

        loop.IsFaulted.Should().BeFalse("a transport outage must not end the group");

        monitors[0].State.Should().Be(PartitionRunState.Running, "the recovery clears the block");
        monitors[1].State.Should().Be(PartitionRunState.Running, "every partition of the group recovers together");

        await cts.CancelAsync();
        await loop;

        PartitionWorker.BlockDelay = null;
        StreamLag.Clear();
    }

    /// <summary>
    /// The loop marks its partitions running when it starts and caught up when a round returns
    /// nothing.
    /// </summary>
    /// <remarks>
    /// Without <c>MarkRunning</c> every partition stayed <see cref="PartitionRunState.Starting"/> for
    /// the life of the process, which the health check reads as "not yet reading". Without
    /// <c>MarkCaughtUp</c>, <c>lag.ms</c> is computed off the age of the last processed entry, so on a
    /// quiet topic it climbs forever and a consumer sitting at the tail with nothing to do pages
    /// overnight. The assertion below is exactly that: an old entry, then an idle round, then zero.
    /// </remarks>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_loop_marks_its_partitions_running_and_caught_up()
    {
        StreamLag.Clear();

        var monitors = new[] { CoLocatedInline.Monitor("t", 0), CoLocatedInline.Monitor("t", 1) };
        var partitions = new[]
        {
            CoLocatedInline.Context(monitors[0], 0),
            CoLocatedInline.Context(monitors[1], 1),
        };

        var handled = 0;
        var calls = 0;

        // One entry stamped in 1970, then idle forever: the lag is enormous until the loop says it is
        // at the tail. The idle round yields, exactly as both real read modes do — Block parks
        // server-side, Poll applies its backoff — because a fetch that completes synchronously with
        // nothing would spin this loop on its caller's thread and never let the assertions run.
        async ValueTask<StreamSlice[]> Fetch(StreamPosition[] positions, int count, CancellationToken ct)
        {
            _ = positions;
            _ = count;
            ct.ThrowIfCancellationRequested();

            if (Interlocked.Increment(ref calls) == 1)
            {
                return [CoLocatedInline.Slice(0, 1, 0, "ancient")];
            }

            await Task.Delay(2, ct).ConfigureAwait(false);

            return [];
        }

        using var cts = new CancellationTokenSource();

        var loop = PartitionWorker.ReadGroupInlineLoopAsync(
            partitions,
            [StreamId.Min, StreamId.Min],
            Fetch,
            (_, _) =>
            {
                Interlocked.Increment(ref handled);
                return ValueTask.CompletedTask;
            },
            static (_, _) => { },
            cts.Token);

        await Pipeline.WaitFor(() => Volatile.Read(ref handled) == 1, "the first batch to be handled");

        monitors[0].State.Should().Be(PartitionRunState.Running);
        monitors[1].State.Should().Be(PartitionRunState.Running);

        await Pipeline.WaitFor(
            () => monitors[0].LagMs == 0 && monitors[1].LagMs == 0,
            "an idle round to mark every partition caught up rather than leaving lag.ms climbing");

        await cts.CancelAsync();
        await loop;

        StreamLag.Clear();
    }
}

/// <summary>R-09 — completing a writer under a parked reader is a stop, not a fault.</summary>
public class ReaderShutdownRaceTests
{
    /// <summary>
    /// The host completes every writer on shutdown, and the partition's own end-of-processor
    /// continuation completes this one when the partition stands down. Either can land while the
    /// reader is parked inside <c>WriteAsync</c>, and the <see cref="ChannelClosedException"/> that
    /// results was not swallowed: the worker faulted and the drain logged a spurious "worker fail
    /// during shutdown" on what was an ordinary stop.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Completing_the_writer_under_a_parked_reader_ends_the_loop_cleanly()
    {
        var channel = PartitionWorker.CreateChannel(1);
        var ctx = Pipeline.Context(channel.Writer);
        var fetches = 0;

        // Nothing reads the channel, so the reader fills it and parks in WriteAsync.
        var fetch = Pipeline.Script(
            [.. Enumerable.Range(0, 16).Select(i => Pipeline.Batch(Pipeline.Entry(1, i, "Keep")))],
            () => Interlocked.Increment(ref fetches));

        using var cts = new CancellationTokenSource();

        var read = PartitionWorker.ReadLoopAsync(ctx, StreamId.Min, fetch, cts.Token);

        // Capacity 1: the first batch goes in, the second fetch's write is the one that parks.
        await Pipeline.WaitFor(() => Volatile.Read(ref fetches) >= 2, "the reader to fill the channel");
        await Task.Delay(50);

        channel.Writer.TryComplete();

        await read.WaitAsync(TimeSpan.FromSeconds(5));

        read.IsFaulted.Should().BeFalse("a completed channel means nobody is left to hand a batch to, which is a stop");

        // Whatever is still queued is the host's to drop; what matters is that the reader did not
        // walk away holding a rented array.
        while (channel.Reader.TryRead(out var queued))
        {
            queued.Return();
        }
    }
}

/// <summary>R-10 — the two <c>ProcessBatchAsync</c> defects.</summary>
public class ProcessBatchAccountingTests
{
    /// <summary>
    /// A batch the filter emptied still advances, and a <see cref="DontIgnoreException"/> from that
    /// advance blocks the partition instead of faulting the worker.
    /// </summary>
    /// <remarks>
    /// The advance for a filtered-to-empty batch used to sit outside the try, so under
    /// <see cref="PersistMode.SyncBatch"/> — where the advance is a Redis write — a
    /// <see cref="DontIgnoreException"/> from the flush killed the worker outright. That is the exact
    /// opposite of what the exception means: block and retry, never skip and never die.
    /// </remarks>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_filtered_empty_batch_blocks_on_a_failing_flush_instead_of_faulting()
    {
        PartitionWorker.BlockDelay = static (_, ct) => Task.Delay(5, ct);

        var channel = PartitionWorker.CreateChannel(4);
        var log = new CapturingLog();
        var ctx = ErrorHandling.Context(channel.Writer, log);

        var flushes = 0;
        var recorded = new List<StreamId>();

        // Everything in the read was filtered out: no messages, but a real Last to checkpoint.
        var empty = new StreamBatch(StreamBatch.Rent(1), 0, new StreamId(5, 0));
        channel.Writer.TryWrite(empty).Should().BeTrue();

        using var cts = new CancellationTokenSource();

        var loop = PartitionWorker.ProcessLoopAsync(
            ctx,
            static (_, _) => ValueTask.CompletedTask,
            (_, id) =>
            {
                lock (recorded)
                {
                    recorded.Add(id);
                }
            },
            cts.Token,
            channel.Reader,
            PersistMode.SyncBatch,
            _ => Interlocked.Increment(ref flushes) <= 2
                ? throw new MandatoryDownstreamDownException("the position store is unreachable")
                : default);

        await Pipeline.WaitFor(() => Volatile.Read(ref flushes) >= 3, "the blocked advance to be retried");

        loop.IsFaulted.Should().BeFalse("a DontIgnoreException blocks the partition; it never faults the worker");

        log.At(LogLevel.Error).Should()
            .Contain(
                m => m.Contains("now BLOCKED", StringComparison.Ordinal),
                "the block is the documented behaviour and has to be visible");

        lock (recorded)
        {
            recorded.Should().Contain(new StreamId(5, 0), "the position still advances past what was read");
        }

        channel.Writer.TryComplete();
        await loop.WaitAsync(TimeSpan.FromSeconds(5));

        PartitionWorker.BlockDelay = null;
    }

    /// <summary>
    /// A blocked batch's retry period stays out of <c>streams.batch.duration</c>.
    /// </summary>
    /// <remarks>
    /// The histogram was recorded once, in the <c>finally</c>, after the blocking-retry loop had
    /// returned — so a partition blocked through a four-hour outage recorded one four-hour sample and
    /// held one four-hour <c>streams.process</c> span open. The p99 of the handler then measured the
    /// outage, and the trace was unrenderable. Each attempt now gets its own sample.
    /// </remarks>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_blocked_batch_does_not_record_its_retry_period_as_batch_duration()
    {
        const int delayMs = 300;

        PartitionWorker.BlockDelay = static (_, ct) => Task.Delay(delayMs, ct);

        var channel = PartitionWorker.CreateChannel(4);
        var log = new CapturingLog();
        var ctx = ErrorHandling.Context(channel.Writer, log);
        var attempts = 0;

        using var durations = new HistogramReader("streams.batch.duration");

        ErrorHandling.Queue(channel, 5, 0);

        using var cts = new CancellationTokenSource();

        var loop = PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) => Interlocked.Increment(ref attempts) <= 2
                ? throw new MandatoryDownstreamDownException("the mandatory downstream is down")
                : ValueTask.CompletedTask,
            static (_, _) => { },
            cts.Token,
            channel.Reader);

        await Pipeline.WaitFor(() => Volatile.Read(ref attempts) >= 3, "the blocked batch to succeed on its third attempt");

        channel.Writer.TryComplete();
        await loop.WaitAsync(TimeSpan.FromSeconds(5));

        durations.Recorded.Should().NotBeEmpty("every attempt records its own duration");
        durations.Recorded.Should().OnlyContain(
            ms => ms < delayMs,
            "the block's backoff belongs to streams.block.duration_ms, not to the handler's histogram");

        PartitionWorker.BlockDelay = null;
    }

    /// <summary>Collects one histogram's measurements the way an exporter would.</summary>
    private sealed class HistogramReader : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly List<double> recorded = [];
        private readonly string instrument;

        internal HistogramReader(string instrument)
        {
            this.instrument = instrument;

            this.listener.InstrumentPublished = (published, l) =>
            {
                if (published.Meter.Name == StreamsDiagnostics.SourceName && published.Name == this.instrument)
                {
                    l.EnableMeasurementEvents(published);
                }
            };

            this.listener.SetMeasurementEventCallback<double>((_, measurement, _, _) =>
            {
                lock (this.recorded)
                {
                    this.recorded.Add(measurement);
                }
            });

            this.listener.Start();
        }

        internal IReadOnlyList<double> Recorded
        {
            get
            {
                lock (this.recorded)
                {
                    return this.recorded.ToArray();
                }
            }
        }

        public void Dispose() => this.listener.Dispose();
    }
}

/// <summary>R-10 — the multi-stream reply's slot matching.</summary>
public class BlockFetchSlotTests
{
    /// <summary>
    /// A reply carrying a stream this worker never asked for is never attributed to a requested slot
    /// when the reply holds other streams too.
    /// </summary>
    /// <remarks>
    /// <c>SlotOf</c> ended with "one key was requested, so an unmatched key must be it" — a rule that
    /// is only sound for a <em>single-stream</em> reply (a key prefix on the connection rewrites the
    /// key text, which is why the rule exists at all). Applied per stream of a multi-stream reply it
    /// records a foreign stream's last id as this partition's position, which silently skips
    /// everything between. The single-stream case still resolves, because that one really is ours.
    /// </remarks>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_unrequested_key_is_only_claimed_when_it_is_the_only_stream_in_the_reply()
    {
        RedisKey[] keys = [(RedisKey)"s:t:0"];
        byte[][] keyBytes = [(byte[])(RedisValue)"s:t:0"];

        var mine = BlockFetchReply.Stream("prefixed:s:t:0", "1-0");
        var foreign = BlockFetchReply.Stream("s:other:9", "500-0");

        var alone = BlockFetch.ParseAll(RedisResult.Create([mine]), keys, keyBytes);

        alone.Should().HaveCount(1);
        alone[0].Key.Should().Be(keys[0], "a single-stream reply to a single-key read is that key, prefix or not");

        var both = BlockFetch.ParseAll(RedisResult.Create([mine, foreign]), keys, keyBytes);

        both.Should().HaveCount(2);
        both.Should().OnlyContain(
            slice => slice.Key != keys[0],
            "neither of two unmatched streams can be claimed as the requested one");
    }
}

/// <summary>Builds <c>XREAD</c> replies in the RESP2 shape the parser walks.</summary>
internal static class BlockFetchReply
{
    /// <summary>One stream: <c>[key, [[id, [field, value]]]]</c>.</summary>
    internal static RedisResult Stream(string key, string id)
        => RedisResult.Create(
        [
            RedisResult.Create((RedisValue)key),
            RedisResult.Create(
            [
                RedisResult.Create(
                [
                    RedisResult.Create((RedisValue)id),
                    RedisResult.Create([RedisResult.Create((RedisValue)"b"), RedisResult.Create((RedisValue)"x")]),
                ]),
            ]),
        ]);
}
