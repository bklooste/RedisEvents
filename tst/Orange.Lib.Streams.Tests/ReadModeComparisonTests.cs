using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;

using FluentAssertions;

using Microsoft.Extensions.Logging.Abstractions;

using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;

using StackExchange.Redis;

namespace Orange.Lib.Streams.Tests;

/// <summary>
/// S19, S22 and S22b — the evidence for keeping <see cref="ReadMode.Block"/> as the shipped default.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why these three and not a throughput number.</b> <c>PerfBenchmarkTests</c> measures throughput
/// against a pre-published backlog, and there <c>Poll</c> wins — which is exactly what the mechanism
/// predicts and says nothing about the choice of default. Under saturation there is always an entry
/// waiting, so <see cref="PollFetch"/> never reaches its idle delay and degenerates into a tight
/// loop of plain <c>XREAD</c>s, while <see cref="BlockFetch"/> pays the extra <c>BLOCK</c> argument
/// and the hand-rolled RESP parse on every call. A saturated benchmark structurally cannot measure
/// the thing <c>Block</c> exists for, so reading it as a verdict would revert the default on the
/// wrong evidence.
/// </para>
/// <para>
/// What <c>Block</c> is actually for is the <em>quiet</em> topic: the cost of noticing that a message
/// arrived after a period of silence, and the cost of the silence itself. Those are the two numbers
/// here — <see cref="Idle_command_cost_is_an_order_of_magnitude_lower_under_block"/> for the cost of
/// waiting and <see cref="Publish_to_handler_latency_is_materially_lower_under_block"/> for the cost
/// of noticing — measured on identical topics with identical consumer options, the read mode being
/// the only difference. <see cref="Block_drains_a_backlog_far_faster_than_any_per_batch_sleep_allows"/>
/// then closes the obvious follow-up question: whether buying idle latency costs anything under
/// load, i.e. whether a hybrid "poll while busy, block while idle" fetch loop would be worth
/// building. It would not, because <c>BLOCK</c> already returns the instant one entry exists.
/// </para>
/// <para>
/// <b>Every threshold here is loose, and deliberately so.</b> The failure modes being guarded against
/// are order-of-magnitude ones — a blocking read that quietly became a poll, or a poll that lost its
/// backoff and started hard-spinning — so the assertions are set an order of magnitude away from the
/// measured values rather than at them. A test that pins <c>p50 &lt; 1.2 ms</c> would fail on a busy
/// CI agent while a genuinely broken default sailed through elsewhere.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class ReadModeComparisonTests(RedisStreamsFixture fixture, ITestOutputHelper output)
{
    private const string MessageType = "readmode";

    /// <summary>The library default, and the divisor in the poll-cost arithmetic.</summary>
    private const int MaxIdleDelayMs = 50;

    /// <summary>The library default. A blocking reader wakes at most this often on a silent topic.</summary>
    private const int BlockMs = 1000;

    /// <summary>How long each mode is left with nothing to read in S19.</summary>
    private const int IdleSeconds = 10;

    /// <summary>
    /// S19 — the cost of silence. With no traffic at all, a polling reader must issue about
    /// <c>window / MaxIdleDelayMs</c> reads and a blocking one about <c>window / BlockMs</c>, and the
    /// commands are counted at the server rather than inferred.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the "no hard spin" requirement being <em>verified</em> instead of asserted. Redis
    /// itself does the counting: <c>CONFIG RESETSTAT</c> zeroes the per-command table, the consumer
    /// is left alone for <see cref="IdleSeconds"/>, and <c>INFO commandstats</c> is read back for
    /// <c>cmdstat_xread</c>. Both modes issue <c>XREAD</c> and nothing else in the library does, so
    /// the counter is unambiguous — and the count is taken as a delta across the window as well as
    /// from a reset, so a stray command from the fixture could not be mistaken for a read.
    /// </para>
    /// <para>
    /// <b>One partition on purpose.</b> With the default two, <c>CoLocatePartitions</c> folds both
    /// streams into a single multi-stream <c>XREAD</c> and the count would be per-loop rather than
    /// per-partition — still correct, but no longer the arithmetic the plan states. One partition
    /// makes "per worker" and "in total" the same number.
    /// </para>
    /// <para>
    /// <b>What breaks it.</b> Give <see cref="BlockFetch"/> a backoff sleep, or default
    /// <see cref="ConsumerOptions.ReadMode"/> to <see cref="ReadMode.Poll"/>, and the block count
    /// moves from ~10 to ~200 and fails the upper bound. Remove the cap from
    /// <see cref="Backoff.Next"/> — or the <c>await</c> in <c>PollFetch.WaitIdleAsync</c> — and the
    /// poll count runs to tens of thousands and fails its upper bound. Neither is a number that can
    /// drift into range by accident.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Idle_command_cost_is_an_order_of_magnitude_lower_under_block()
    {
        await fixture.FlushAllAsync();

        var blockCalls = await this.MeasureIdleXReadsAsync(ReadMode.Block);
        var pollCalls = await this.MeasureIdleXReadsAsync(ReadMode.Poll);

        var predictedPoll = IdleSeconds * 1000 / MaxIdleDelayMs;
        var predictedBlock = IdleSeconds * 1000 / BlockMs;

        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"S19 idle XREADs over {IdleSeconds}s, 1 partition, no traffic:"));
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"  Block (BlockMs={BlockMs}):            {blockCalls} (arithmetic prediction {predictedBlock})"));
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"  Poll  (MaxIdleDelayMs={MaxIdleDelayMs}):        {pollCalls} (arithmetic prediction {predictedPoll})"));
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"  ratio poll/block:                {(double)pollCalls / Math.Max(blockCalls, 1):0.0}x"));

        // Poll has to land near its arithmetic prediction in both directions. The upper bound is the
        // no-hard-spin requirement (a lost backoff shows up as thousands); the lower bound stops the
        // test passing because the consumer never started and issued nothing at all.
        pollCalls.Should().BeInRange(
            predictedPoll / 3,
            predictedPoll * 2,
            "a polling reader idle for {0}s at MaxIdleDelayMs={1} must issue about {2} XREADs — far more and the idle backoff is not being applied, far fewer and it never ran",
            IdleSeconds,
            MaxIdleDelayMs,
            predictedPoll);

        // Block cannot wake more often than BlockMs allows. Doubling the prediction and adding a few
        // absorbs the partial block either side of the window and the connection handshake.
        blockCalls.Should().BeInRange(
            Math.Max(1, predictedBlock / 2),
            (predictedBlock * 2) + 5,
            "a blocking reader idle for {0}s at BlockMs={1} wakes about {2} times — many more means the read is not actually blocking",
            IdleSeconds,
            BlockMs,
            predictedBlock);

        // The comparison itself. The two range assertions above already pin each mode to its own
        // arithmetic, so this only has to establish the gap is real and large; 5x is asserted rather
        // than the ~20x measured so that the worst legal value of each bound still passes.
        pollCalls.Should().BeGreaterThan(
            blockCalls * 5,
            "blocking must cost an order of magnitude fewer idle commands than polling — that is the whole reason it is the default");
    }

    /// <summary>
    /// S22 — the gate. Publish-to-handler latency at a rate low enough that the consumer goes idle
    /// between messages, which is the only condition under which the read modes differ.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why one message every ~200 ms.</b> The whole difference between the modes is what happens
    /// when the stream is empty. Publish faster than the reader drains and both modes always find
    /// data waiting, the poller never sleeps, and the measurement collapses into the throughput
    /// benchmark that cannot answer this question. Spacing the publishes past
    /// <see cref="MaxIdleDelayMs"/> forces the poller into its idle delay before every single
    /// message, which is exactly the state a real low-volume topic sits in almost all the time.
    /// </para>
    /// <para>
    /// The spacing is jittered rather than fixed. At a fixed period the poller's sleep schedule can
    /// phase-lock against the publisher and produce a latency that is an artefact of the resonance
    /// rather than of the mode — flattering or damning by luck. The jitter is seeded, so the run is
    /// still reproducible.
    /// </para>
    /// <para>
    /// The publish timestamp travels in the message body as <see cref="Stopwatch"/> ticks. That is
    /// deliberate: <c>StreamMsg.EnqueuedTime</c> comes from the Redis-assigned id and so measures
    /// from the server's clock in whole milliseconds, which is both a different clock from the
    /// handler's and too coarse for a sub-millisecond p50.
    /// </para>
    /// <para>
    /// <b>What breaks it.</b> Switching the default to <see cref="ReadMode.Poll"/>, or making
    /// <see cref="BlockFetch"/> return without actually passing <c>BLOCK</c> to Redis, moves the
    /// block p50 from around a millisecond to around half of <see cref="MaxIdleDelayMs"/> and fails
    /// both the absolute bound and the ratio.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Publish_to_handler_latency_is_materially_lower_under_block()
    {
        await fixture.FlushAllAsync();

        const int samples = 50;

        var block = await this.MeasureIdlePublishLatencyAsync(ReadMode.Block, samples);
        var poll = await this.MeasureIdlePublishLatencyAsync(ReadMode.Poll, samples);

        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"S22 publish -> handler latency, {samples} messages ~200ms apart (consumer idle between each):"));
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"  Block: p50 {block.P50:0.000} ms  p90 {block.P90:0.000} ms  p99 {block.P99:0.000} ms  max {block.Max:0.000} ms"));
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"  Poll : p50 {poll.P50:0.000} ms  p90 {poll.P90:0.000} ms  p99 {poll.P99:0.000} ms  max {poll.Max:0.000} ms   (MaxIdleDelayMs={MaxIdleDelayMs})"));
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"  p50 ratio poll/block: {poll.P50 / Math.Max(block.P50, 0.001):0.0}x"));

        // Poll's p50 is a property of the idle backoff, not of Redis: a message lands uniformly
        // inside a sleep of MaxIdleDelayMs, so it waits about half of one on average. Bounding it
        // both ways keeps the comparison honest — if the poller were not sleeping at all this range
        // would fail and the test would not silently become a comparison of two blocking readers.
        poll.P50.Should().BeInRange(
            MaxIdleDelayMs * 0.15,
            MaxIdleDelayMs * 0.95,
            "a message published into an idle poller waits for the remainder of a MaxIdleDelayMs={0} sleep, so the median should sit near half of it",
            MaxIdleDelayMs);

        // Block's own bound, generous by an order of magnitude against the ~1 ms measured. XREAD
        // BLOCK is woken by the XADD itself, so this number is a socket round trip plus a channel
        // hop and has nothing to do with any delay constant.
        block.P50.Should().BeLessThan(
            MaxIdleDelayMs / 5.0,
            "a blocking reader is woken by the publish itself, so its median latency is a round trip and must not resemble a polling interval");

        // The gate. Asserted at 3x against a measured ~20x.
        block.P50.Should().BeLessThan(
            poll.P50 / 3.0,
            "ReadMode.Block ships as the default because it notices a message on a quiet topic materially sooner than ReadMode.Poll; if this fails, the default is wrong");

        // The tail matters more than the median for anything user-facing. Asserted on p90 rather
        // than the p99 that is reported: with 50 samples the 99th percentile *is* the maximum, so a
        // single GC pause on a loaded agent would decide it, and a flaky gate is worse than none.
        // p90 still discriminates — a poller's p90 sits just under its full interval.
        block.P90.Should().BeLessThan(
            MaxIdleDelayMs / 2.0,
            "nine in ten messages should reach a blocking handler in well under half a poll interval; a poller's p90 sits just below a whole one");
    }

    /// <summary>
    /// S22b — <c>BLOCK</c> is self-adapting, so no hybrid fetch loop is needed. Under sustained load
    /// the blocking loop returns immediately and comes straight back around; it never falls into a
    /// sleep.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The design question this closes: if blocking buys idle latency, does it cost throughput
    /// because the loop "waits" on every iteration? No — Redis returns from <c>XREAD BLOCK</c> the
    /// moment one entry exists and does not wait for <c>COUNT</c> to fill, so with a backlog present
    /// the call is a plain read and the loop is a drain loop. That is why <see cref="BlockFetch"/>
    /// has no backoff and why a "poll while busy, block while idle" mode would be redundant.
    /// </para>
    /// <para>
    /// <b>Why a rate rather than a timing.</b> Asserting "the loop did not sleep" directly would mean
    /// instrumenting the loop. Instead the test makes the claim falsifiable arithmetically: the
    /// backlog is sized so it cannot be drained in fewer than a couple of hundred batches, and the
    /// consumer is timed. If the loop paused for even one <see cref="MaxIdleDelayMs"/> per batch the
    /// run could not finish in under <c>batches × 50 ms</c>, so finishing in a small fraction of that
    /// is proof no such pause happened. The bound is expressed against the measured batch count, so
    /// it stays valid if <c>BatchSize</c> or the message count is ever changed.
    /// </para>
    /// <para>
    /// <b>What breaks it.</b> Adding a <c>Backoff</c> await to the block read loop, or blocking for
    /// <c>BlockMs</c> even when entries are available (an <c>XREAD BLOCK</c> issued with a
    /// <c>$</c> cursor would do exactly that), pushes the elapsed time past the bound by more than
    /// an order of magnitude.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Block_drains_a_backlog_far_faster_than_any_per_batch_sleep_allows()
    {
        await fixture.FlushAllAsync();

        const int total = 20_000;
        const int batchSize = 100;

        var topic = fixture.NewTopic();
        var topicOptions = Untrimmed(partitions: 1);
        var root = this.Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        await PublishBacklogAsync(publisher, total);

        var handler = new TestHandler();

        var host = new StreamConsumerHost(
            root,
            Options(topic, ReadMode.Block, batchSize),
            fixture.NewConsumer(),
            handler.HandleAsync,
            connection,
            NullLogger.Instance);

        var sw = Stopwatch.StartNew();
        await host.StartAsync(CancellationToken.None);

        try
        {
            await handler.WaitForAsync(total, TimeSpan.FromSeconds(60));
        }
        finally
        {
            sw.Stop();
            await host.StopAsync(CancellationToken.None);
            await host.DisposeAsync();
        }

        var batches = handler.Batches;
        var elapsedMs = sw.Elapsed.TotalMilliseconds;

        // One sleep of MaxIdleDelayMs per batch is the cheapest possible sleeping loop; anything that
        // backs off costs at least this much.
        var sleepingLoopFloorMs = (double)batches * MaxIdleDelayMs;

        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"S22b block drain of a {total} message backlog, 1 partition, BatchSize={batchSize}:"));
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"  batches {batches}, elapsed {elapsedMs:0} ms = {batches / Math.Max(sw.Elapsed.TotalSeconds, 0.0001):0} batches/s, {total / Math.Max(sw.Elapsed.TotalSeconds, 0.0001):0} msg/s"));
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"  a loop sleeping MaxIdleDelayMs={MaxIdleDelayMs} once per batch could not finish under {sleepingLoopFloorMs:0} ms"));

        // Without enough batches the timing proves nothing: a single COUNT-sized read could have
        // swallowed the backlog whole.
        batches.Should().BeGreaterThanOrEqualTo(
            total / batchSize / 2,
            "the backlog must take many fetches to drain or the elapsed time says nothing about per-fetch behaviour");

        handler.Count.Should().Be(total, "the drain must be complete before its rate means anything");

        elapsedMs.Should().BeLessThan(
            sleepingLoopFloorMs / 4.0,
            "ReadMode.Block drained {0} batches in {1:0} ms, which is incompatible with any per-batch sleep — XREAD BLOCK returns as soon as one entry exists, so the loop is already a drain loop and a separate hybrid fetch mode would be redundant",
            batches,
            elapsedMs);
    }

    /// <summary>
    /// Runs a consumer against a topic that never receives a message and returns the number of
    /// <c>XREAD</c>s Redis saw during the idle window.
    /// </summary>
    private async Task<long> MeasureIdleXReadsAsync(ReadMode mode)
    {
        var topic = fixture.NewTopic($"idle-{mode}");
        var topicOptions = Untrimmed(partitions: 1);
        var root = this.Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);

        var handler = new TestHandler();

        var host = new StreamConsumerHost(
            root,
            Options(topic, mode, batchSize: 100) with { Persist = PersistMode.None, StartFrom = StartFrom.Beginning },
            fixture.NewConsumer(),
            handler.HandleAsync,
            connection,
            NullLogger.Instance);

        await host.StartAsync(CancellationToken.None);

        try
        {
            // Do not start counting until the reader has demonstrably issued its first read, or a
            // slow connection handshake would be measured as idle time the consumer never spent.
            await RedisStreamsFixture.WaitUntilAsync(
                async () => await this.XReadCallsAsync() > 0,
                TimeSpan.FromSeconds(30),
                $"the {mode} reader to issue its first XREAD");

            await this.ResetCommandStatsAsync();
            var before = await this.XReadCallsAsync();

            await Task.Delay(TimeSpan.FromSeconds(IdleSeconds));

            var after = await this.XReadCallsAsync();

            handler.Count.Should().Be(0, "the topic was never published to, so an idle-cost measurement must have consumed nothing");

            return after - before;
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            await host.DisposeAsync();
        }
    }

    /// <summary>
    /// Publishes <paramref name="samples"/> messages one at a time, spaced far enough apart that the
    /// consumer is idle before each, and returns the publish-to-handler latency distribution.
    /// </summary>
    private async Task<Latency> MeasureIdlePublishLatencyAsync(ReadMode mode, int samples)
    {
        var topic = fixture.NewTopic($"latency-{mode}");
        var topicOptions = Untrimmed(partitions: 1);
        var root = this.Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        var gate = new Lock();
        var latencies = new List<double>(samples + 1);
        var seen = 0;

        ValueTask Handle(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            var arrived = Stopwatch.GetTimestamp();

            var span = batch.Span;
            for (var i = 0; i < span.Length; i++)
            {
                var sent = BinaryPrimitives.ReadInt64LittleEndian(span[i].Body.Span);
                var ms = (arrived - sent) * 1000.0 / Stopwatch.Frequency;

                lock (gate)
                {
                    latencies.Add(ms);
                }
            }

            _ = Interlocked.Add(ref seen, span.Length);
            return default;
        }

        var host = new StreamConsumerHost(
            root,
            Options(topic, mode, batchSize: 100),
            fixture.NewConsumer(),
            Handle,
            connection,
            NullLogger.Instance);

        await host.StartAsync(CancellationToken.None);

        try
        {
            // Warm-up: prove the pipeline is live and pay the JIT and first-connection costs on a
            // message whose latency is then thrown away.
            await PublishStampedAsync(publisher);
            await RedisStreamsFixture.WaitUntilAsync(
                () => Volatile.Read(ref seen) >= 1,
                TimeSpan.FromSeconds(30),
                $"the {mode} consumer to deliver its warm-up message");

            lock (gate)
            {
                latencies.Clear();
            }

            // Seeded jitter: at a fixed period the poller's sleep schedule can phase-lock with the
            // publisher and produce a latency that is an artefact of the resonance.
            var rng = new Random(20260906);

            for (var i = 0; i < samples; i++)
            {
                await Task.Delay(180 + rng.Next(0, 41));
                await PublishStampedAsync(publisher);
            }

            await RedisStreamsFixture.WaitUntilAsync(
                () => Volatile.Read(ref seen) >= samples + 1,
                TimeSpan.FromSeconds(30),
                $"the {mode} consumer to deliver all {samples} measured messages");
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            await host.DisposeAsync();
        }

        double[] sorted;
        lock (gate)
        {
            sorted = [.. latencies];
        }

        Array.Sort(sorted);

        sorted.Should().HaveCount(samples, "every published message must be accounted for or the percentiles are drawn from a partial sample");

        return new Latency(
            Percentile(sorted, 0.50),
            Percentile(sorted, 0.90),
            Percentile(sorted, 0.99),
            sorted[^1]);
    }

    private static async Task PublishStampedAsync(StreamPublisher publisher)
    {
        var body = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(body, Stopwatch.GetTimestamp());
        _ = await publisher.PublishAsync("k", body, MessageType);
    }

    private static async Task PublishBacklogAsync(StreamPublisher publisher, int total, int chunk = 1_000)
    {
        var body = new byte[64];
        var bodies = new ReadOnlyMemory<byte>[chunk];
        for (var i = 0; i < bodies.Length; i++)
        {
            bodies[i] = body;
        }

        for (var sent = 0; sent < total; sent += chunk)
        {
            var count = Math.Min(chunk, total - sent);
            await publisher.PublishBatchAsync("k", bodies.AsMemory(0, count), MessageType);
        }
    }

    private static double Percentile(double[] sorted, double q)
    {
        if (sorted.Length == 0)
        {
            return 0;
        }

        var rank = (int)Math.Ceiling(q * sorted.Length) - 1;
        return sorted[Math.Clamp(rank, 0, sorted.Length - 1)];
    }

    private IServer Server()
    {
        var endpoints = fixture.Redis.GetEndPoints();
        return fixture.Redis.GetServer(endpoints[0]);
    }

    private async Task ResetCommandStatsAsync() =>
        _ = await this.Server().ExecuteAsync("CONFIG", "RESETSTAT");

    /// <summary>Reads <c>calls=</c> out of the <c>cmdstat_xread</c> line of <c>INFO commandstats</c>.</summary>
    private async Task<long> XReadCallsAsync()
    {
        var info = await this.Server().InfoAsync("commandstats");

        foreach (var group in info)
        {
            foreach (var entry in group)
            {
                if (!entry.Key.Equals("cmdstat_xread", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                // "calls=91,usec=1234,usec_per_call=13.56,..."
                foreach (var field in entry.Value.Split(','))
                {
                    if (field.StartsWith("calls=", StringComparison.Ordinal)
                        && long.TryParse(field.AsSpan(6), CultureInfo.InvariantCulture, out var calls))
                    {
                        return calls;
                    }
                }
            }
        }

        return 0;
    }

    private static ConsumerOptions Options(string topic, ReadMode mode, int batchSize) => new()
    {
        Topic = topic,
        BatchSize = batchSize,
        ReadMode = mode,
        BlockMs = BlockMs,
        MaxIdleDelayMs = MaxIdleDelayMs,
        StartFrom = StartFrom.Beginning,
        Persist = PersistMode.AsyncBatch,
        PersistIntervalMs = 1_000,
        Backpressure = new BackpressureOptions { Enabled = true, Capacity = 4 },
    };

    private static TopicOptions Untrimmed(int partitions) => new()
    {
        Partitions = partitions,
        Trim = TrimMode.None,
        MaxLen = long.MaxValue,
        BackgroundTrimIntervalSeconds = 0,
    };

    private StreamOptions Root(string topic, TopicOptions topicOptions) => new()
    {
        ConnectionString = fixture.ConnectionString,
        Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
    };

    private readonly record struct Latency(double P50, double P90, double P99, double Max);
}
