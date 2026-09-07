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
/// Connection isolation and starvation survival against real Redis — plan rows S21 and S21f.
/// </summary>
/// <remarks>
/// <para>
/// <b>What S21 is actually about.</b> StackExchange.Redis multiplexes every command a process issues
/// over one connection per endpoint. A blocking <c>XREAD</c> parks that connection <em>server-side</em>
/// for up to <c>BLOCK</c> milliseconds, and Redis serves one client's commands strictly in order, so
/// anything queued behind the block waits for it. Sharing the multiplexer with a blocking reader would
/// therefore add up to a whole <c>BlockMs</c> to every unrelated <c>GET</c>, <c>XADD</c> and position
/// flush in the process. That is the entire justification for
/// <see cref="StreamReaderConnection"/> giving each blocking reader its own
/// <see cref="ConnectionMultiplexer"/>, and these tests are what make the claim falsifiable.
/// </para>
/// <para>
/// <b>The shared multiplexer under test is the library's own.</b> Measuring latency on some third
/// connection would prove nothing — it is isolated no matter what the library does. So the fixture's
/// multiplexer is handed to <see cref="StreamsConnectionProvider"/> through a one-entry
/// <see cref="IServiceProvider"/>, which reuses it because the endpoint matches. It is then literally
/// the connection the library uses for writes, positions, ownership and admin: the connection a
/// blocking read would stall if the reader were not separate.
/// </para>
/// <para>
/// <b>Every measurement carries a positive control.</b> "The p99 stayed flat" is only evidence if the
/// same measurement would have moved had the behaviour regressed, and "the pool was starved" is only
/// evidence if the pool really was starved. So <see cref="S21_a_parked_blocking_read_does_not_delay_the_shared_multiplexer"/>
/// ends by issuing the blocking <c>XREAD</c> on the very connection it is timing and asserting the p99
/// explodes, and both starvation tests assert that a probe work item queued behind the hogs does
/// <em>not</em> get to run. Without those controls both tests would pass on a library that had thrown
/// the design away.
/// </para>
/// <para>
/// <b>Reconnects are detected server-side, not client-side.</b> The reader's multiplexer is
/// deliberately unreachable — <see cref="StreamReaderConnection"/> hands it out to nobody — so there
/// is no <c>ConnectionFailed</c> event to subscribe to. <c>CLIENT LIST</c> is better evidence anyway:
/// a reconnect means a new TCP connection and therefore a new client id, so "the same client ids are
/// still there, and their age has grown by the length of the window" is a direct observation that the
/// socket was never torn down.
/// </para>
/// <para>
/// <b>Honesty about S21f.</b> The plan row promises that the reader "keeps consuming into the
/// channel" while the ThreadPool is saturated. Measured against real Redis on StackExchange.Redis
/// 2.10.1, it does not: with the pool full of blocking work, replies stop being parsed — the timeout
/// report shows kilobytes sitting unread in the inbound buffer while the reader's own socket-manager
/// worker is idle — and reads through the dedicated connection go to zero for the length of the
/// window. Writing a test that asserted otherwise would have meant either asserting something false
/// or loosening the bound until it asserted nothing, so these tests assert the two things the
/// measurement does support, which are also the two that matter in production: <b>the connection is
/// never torn down and rebuilt</b>, and <b>nothing is lost</b> — reads resume at full rate the
/// instant the pool clears, the only failure raised is the timeout the read loop already treats as
/// transient, and the whole backlog drains. Throughput during starvation is reported and deliberately
/// not asserted on: handlers and the read loop's own continuations run on the standard ThreadPool, so
/// during the window the channel fills and the reader parks on <c>WriteAsync</c>, which is
/// backpressure behaving correctly rather than a fault.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class ReaderIsolationTests(RedisStreamsFixture fixture, ITestOutputHelper output)
{
    /// <summary>The type string every message in this file carries.</summary>
    private const string MessageType = "isolation";

    /// <summary>The prefix <see cref="Diagnostics.StreamNames.ReaderClientName"/> gives every dedicated reader connection.</summary>
    private const string ReaderClientPrefix = "streams-rd:";

    /// <summary>
    /// How long a latency window runs. Long enough to span several <c>BLOCK</c> intervals — a window
    /// shorter than one block could miss the stall it is looking for entirely.
    /// </summary>
    private static readonly TimeSpan LatencyWindow = TimeSpan.FromSeconds(4);

    /// <summary>
    /// The <c>BLOCK</c> interval used by the latency test, in milliseconds. Deliberately larger than
    /// the default so that a shared connection would be unmissable: every queued command would wait a
    /// uniform 0–2 s, and the assertion floor is 250 ms.
    /// </summary>
    private const int LatencyBlockMs = 2_000;

    /// <summary>
    /// The p99 ceiling for the shared multiplexer while a reader is parked. Eight times below the
    /// block interval, so it cannot be reached by a stall and cannot be tripped by a slow CI box:
    /// with thousands of samples in the window, twenty of them would have to exceed a quarter of a
    /// second before this fires.
    /// </summary>
    private static readonly TimeSpan IsolatedP99Ceiling = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// How long the idle-survival test leaves the reader parked with nothing to read.
    /// </summary>
    /// <remarks>
    /// <b>65 seconds, not 60, and not less.</b> StackExchange.Redis's keep-alive and configuration
    /// check both default to 60 seconds, and the failure mode this test exists to catch is a PING
    /// queued behind a long <c>BLOCK</c> tripping the unhealthy-connection detection. A window under
    /// 60 s would never issue that PING and the test would assert nothing at all. This is the single
    /// most expensive test in the file and it is deliberate; shortening it below 60 s would make it
    /// cheap and worthless rather than cheap and useful.
    /// </remarks>
    private static readonly TimeSpan IdleWindow = TimeSpan.FromSeconds(65);

    /// <summary>
    /// How long the ThreadPool is held saturated.
    /// </summary>
    /// <remarks>
    /// Above the reader connection's client-side timeout — <c>BlockMs</c> plus the five-second
    /// margin <see cref="StreamReaderConnection"/> adds — so that a connection whose socket work had
    /// been pushed onto the starved pool would have had time to be declared unhealthy and torn down.
    /// Below that, "no reconnect" would be a tautology rather than an observation.
    /// </remarks>
    private static readonly TimeSpan StarvationWindow = TimeSpan.FromSeconds(8);

    /// <summary>
    /// <c>BlockMs</c> for the starvation tests: with the +5 s margin
    /// <see cref="StreamReaderConnection"/> adds, the reader connection's client-side timeout is
    /// 5.5 s — comfortably inside <see cref="StarvationWindow"/>.
    /// </summary>
    private const int StarvationBlockMs = 500;

    // ---------------------------------------------------------------------------------------
    // S21 — a parked blocking read is invisible to the rest of the process
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// S21. A consumer parked in <c>XREAD BLOCK 2000</c> on an idle topic does not move the p99 of a
    /// tight <c>GET</c> loop running on the shared multiplexer — and the same loop, timed against a
    /// blocking read issued on its <em>own</em> connection, blows out by an order of magnitude.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Three phases, deliberately in this order. <b>Baseline</b>: the loop with nothing else running,
    /// which is what "flat" is measured against. <b>Isolated</b>: the same loop with a real
    /// <see cref="StreamConsumerHost"/> parked in a block, on the multiplexer that host uses for
    /// everything except reading. <b>Control</b>: the same loop on a private connection that is
    /// itself issuing the blocking read, which is the arrangement the dedicated reader exists to
    /// avoid.
    /// </para>
    /// <para>
    /// The control is not decoration. Without it, a p99 of 0.4 ms in phase two is consistent both with
    /// "the reader is isolated" and with "this measurement cannot see head-of-line blocking" — and a
    /// test that cannot tell those apart is not evidence of anything. The control runs last so that
    /// the timeouts it deliberately provokes cannot perturb the phases before it, and on its own
    /// throwaway multiplexer so it cannot perturb anything after it either.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S21_a_parked_blocking_read_does_not_delay_the_shared_multiplexer()
    {
        var topic = fixture.NewTopic();
        var consumerName = fixture.NewConsumer();
        var probeKey = (RedisKey)$"isolation-probe:{topic}";

        var topicOptions = Quiet(partitions: 2);
        var root = this.Root(topic, topicOptions);

        // The provider reuses the fixture's multiplexer (the endpoint matches), so the connection the
        // GET loop is timing IS the library's shared connection — the one a blocking read would stall.
        await using var connection = new StreamsConnectionProvider(root, new OneService(fixture.Redis), logger: null);
        connection.Source.Should().Be(
            StreamsConnectionSource.Reused,
            "the test must measure the library's own shared multiplexer, not an unrelated one");

        var shared = connection.Connection.GetDatabase();
        _ = await shared.StringSetAsync(probeKey, "v");

        // Phase 1 — baseline, nothing else running.
        var baseline = await SampleGetLatencyAsync(shared, probeKey, LatencyWindow);

        // Phase 2 — a real consumer parked in a block on its own dedicated connection.
        var handler = new TestHandler();
        var host = new StreamConsumerHost(root, LatencyConsumer(topic), consumerName, handler.HandleAsync, connection, NullLogger.Instance);

        List<double> isolated;
        IReadOnlyList<ClientInfo> readers;

        try
        {
            await host.StartAsync(CancellationToken.None);

            // Wait for the dedicated connection to exist before timing anything: a window that ran
            // before the reader connected would be a second baseline wearing a disguise.
            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await this.ReaderClientsAsync(consumerName)).Count > 0,
                TimeSpan.FromSeconds(30),
                "the dedicated reader connection to appear in CLIENT LIST");

            isolated = await SampleGetLatencyAsync(shared, probeKey, LatencyWindow);
            readers = await this.ReaderClientsAsync(consumerName);
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        // Phase 3 — the control: the same loop, with the block on the very connection being timed.
        await using var control = await fixture.ConnectAsync();
        var controlDb = control.GetDatabase();
        _ = await controlDb.StringSetAsync(probeKey, "v");

        using var stopBlocking = new CancellationTokenSource();
        var blocker = BlockForeverAsync(controlDb, $"s:{{{topic}}}:0", stopBlocking.Token);

        List<double> stalled;
        try
        {
            stalled = await SampleGetLatencyAsync(controlDb, probeKey, LatencyWindow);
        }
        finally
        {
            await stopBlocking.CancelAsync();
            await blocker;
        }

        var baselineP99 = Percentile(baseline, 0.99);
        var isolatedP99 = Percentile(isolated, 0.99);
        var stalledP99 = Percentile(stalled, 0.99);

        output.WriteLine(
            $"GET p99 over {LatencyWindow.TotalSeconds:0}s windows, BLOCK={LatencyBlockMs}ms:\n" +
            $"  baseline (no reader)          {baselineP99.TotalMilliseconds,9:0.00} ms  over {baseline.Count,6} samples\n" +
            $"  dedicated reader parked       {isolatedP99.TotalMilliseconds,9:0.00} ms  over {isolated.Count,6} samples\n" +
            $"  CONTROL: block on this conn   {stalledP99.TotalMilliseconds,9:0.00} ms  over {stalled.Count,6} samples");

        // The rig itself has to have worked.
        readers.Should().NotBeEmpty("ReadMode.Block must open a dedicated reader connection");
        baseline.Should().HaveCountGreaterThan(100, "the baseline window must have collected enough samples for a p99 to mean anything");
        isolated.Should().HaveCountGreaterThan(100, "the measured window must have collected enough samples for a p99 to mean anything");

        // The reader is on a different connection from the one being timed — asked of Redis itself,
        // so this cannot quietly become a comparison of two empty sets.
        var timedClientId = (long)await shared.ExecuteAsync("CLIENT", "ID");
        readers.Select(static c => c.Id).Should().NotContain(
            timedClientId,
            "the dedicated reader must not be the connection the GET loop was timed on");
        readers.Should().OnlyContain(
            c => c.Name!.StartsWith(ReaderClientPrefix, StringComparison.Ordinal),
            "a dedicated reader connection names itself so it is identifiable in CLIENT LIST");

        // The claim.
        isolatedP99.Should().BeLessThan(
            IsolatedP99Ceiling,
            "a blocking XREAD on the reader's own connection must not appear in the shared multiplexer's latency at all; " +
            "if it shared the connection every GET would queue behind a {0} ms block",
            LatencyBlockMs);

        isolatedP99.Should().BeLessThan(
            baselineP99 + IsolatedP99Ceiling,
            "the p99 must stay flat relative to the same loop with no reader running");

        // The control: the measurement can see head-of-line blocking when it is there.
        stalled.Should().HaveCountGreaterThanOrEqualTo(
            2, "CONTROL — the stalled window must have produced at least a couple of measurements");

        stalledP99.Should().BeGreaterThan(
            TimeSpan.FromMilliseconds(500),
            "CONTROL — a blocking XREAD on the connection being timed must stall it; if this does not fire, " +
            "the isolated measurement above proves nothing");

        stalledP99.Should().BeGreaterThan(
            isolatedP99 * 5,
            "CONTROL — sharing the connection must cost orders of magnitude more than not sharing it");
    }

    // ---------------------------------------------------------------------------------------
    // S21 — the reader connection survives a long idle
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// S21. Left parked on an empty topic for 65 seconds, the dedicated reader connection is never
    /// torn down and re-established — the same Redis client ids are still there, 65 seconds older —
    /// and the consumer picks up the first message published after the idle within milliseconds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The failure this catches is subtle and would never show up in a short test: StackExchange.Redis
    /// heartbeats an idle connection, and a PING that lands behind a parked <c>BLOCK</c> can look like
    /// an unresponsive server. Trip that and the multiplexer drops and rebuilds a connection that was
    /// perfectly healthy — every reconnect losing the parked read and, with it, up to a whole block
    /// interval of latency. The 65-second window is chosen to cross the 60-second keep-alive boundary
    /// where that PING is actually issued; see <see cref="IdleWindow"/>.
    /// </para>
    /// <para>
    /// <c>CLIENT LIST</c> is sampled throughout rather than only at the ends, so a connection that
    /// churned and happened to land on the same count would still be caught: the assertion is that the
    /// <em>set of ids</em> observed over the whole window never grew.
    /// </para>
    /// <para>
    /// The resume assertion is what proves the surviving connection is still <em>useful</em> rather
    /// than merely still listed. A parked <c>XREAD</c> wakes the instant an entry is added, so the
    /// wake is a few milliseconds; a reconnect-and-retry would be hundreds. Two seconds is a ceiling
    /// no healthy path approaches and no broken one meets.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S21_reader_connection_survives_a_long_idle_and_resumes_immediately()
    {
        var topic = fixture.NewTopic();
        var consumerName = fixture.NewConsumer();

        var topicOptions = Quiet(partitions: 1);
        var root = this.Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, new OneService(fixture.Redis), logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        var handler = new TestHandler();
        var consumer = new ConsumerOptions
        {
            Topic = topic,
            ReadMode = ReadMode.Block,
            // The shipped default. The point of the test is the connection's behaviour under the
            // configuration real services run, not under one tuned to make the test pass.
            BlockMs = 1_000,
            BatchSize = 16,
            Persist = PersistMode.AsyncBatch,
            PersistIntervalMs = 500,
            StartFrom = StartFrom.Beginning,
        };

        var host = new StreamConsumerHost(root, consumer, consumerName, handler.HandleAsync, connection, NullLogger.Instance);

        try
        {
            await host.StartAsync(CancellationToken.None);

            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await this.ReaderClientsAsync(consumerName)).Count > 0,
                TimeSpan.FromSeconds(30),
                "the dedicated reader connection to appear in CLIENT LIST");

            var initial = await this.ReaderClientsAsync(consumerName);
            var initialIds = initial.Select(static c => c.Id).OrderBy(static id => id).ToArray();

            // Sample across the whole idle window, not just at the ends: a connection that churned and
            // came back to the same count would otherwise slip through.
            var seenIds = new HashSet<long>(initialIds);
            var samples = 0;
            var idle = Stopwatch.StartNew();

            while (idle.Elapsed < IdleWindow)
            {
                await Task.Delay(TimeSpan.FromSeconds(5));

                var now = await this.ReaderClientsAsync(consumerName);
                foreach (var client in now)
                {
                    _ = seenIds.Add(client.Id);
                }

                samples++;
            }

            var after = await this.ReaderClientsAsync(consumerName);
            var afterIds = after.Select(static c => c.Id).OrderBy(static id => id).ToArray();
            var oldest = after.Count == 0 ? 0 : after.Max(static c => c.AgeSeconds);

            output.WriteLine(
                $"Idle window {IdleWindow.TotalSeconds:0}s, BlockMs={consumer.BlockMs}: " +
                $"{initialIds.Length} reader client(s) at start, {afterIds.Length} at end, " +
                $"{seenIds.Count} distinct id(s) seen across {samples} samples, oldest age {oldest}s.");

            handler.Count.Should().Be(0, "nothing was published, so nothing may have been delivered");

            afterIds.Should().Equal(
                initialIds,
                "the dedicated reader connection must survive a {0}s idle block without being torn down and rebuilt",
                IdleWindow.TotalSeconds);

            seenIds.Should().HaveCount(
                initialIds.Length,
                "no reader connection may have appeared and vanished mid-window — that is reconnect churn");

            oldest.Should().BeGreaterThanOrEqualTo(
                (int)IdleWindow.TotalSeconds - 5,
                "the surviving connection must actually be the original one, aged by the whole window");

            // Still usable, and still parked where it can see a new entry.
            var wake = Stopwatch.StartNew();
            _ = await publisher.PublishAsync("wake", new byte[8], MessageType);
            await handler.WaitForAsync(1, TimeSpan.FromSeconds(15));
            wake.Stop();

            output.WriteLine($"First message after the idle window was handled in {wake.Elapsed.TotalMilliseconds:0} ms.");

            wake.Elapsed.Should().BeLessThan(
                TimeSpan.FromSeconds(2),
                "a parked XREAD wakes the instant an entry is added; a connection that had to be re-established would not");

            var finalIds = (await this.ReaderClientsAsync(consumerName)).Select(static c => c.Id).OrderBy(static id => id);
            finalIds.Should().Equal(initialIds, "delivering after the idle must not have needed a new connection either");
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    // ---------------------------------------------------------------------------------------
    // S21f — ThreadPool starvation
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// S21f. A ThreadPool saturated with blocking work does not cost the dedicated reader connection
    /// its socket: the same Redis client ids are still there afterwards, the connection is still
    /// established, nothing worse than a retryable timeout was raised, and reads resume at full rate
    /// the moment the pool clears.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This test says less than the plan row's wording promises, because the measurement said so.</b>
    /// <see cref="StreamReaderConnection"/>'s remarks argue that a dedicated socket thread keeps the
    /// connection serviced when the pool is saturated. Run against real Redis on
    /// StackExchange.Redis 2.10.1, that is only half true: under full starvation the reply <em>does</em>
    /// stop being parsed — the timeout report shows several kilobytes sitting unread in the inbound
    /// buffer while the socket manager's own worker is idle — and an in-flight <c>XREAD</c> eventually
    /// raises <see cref="RedisTimeoutException"/> at <c>BlockMs</c> + 5 s. Asserting "reads keep
    /// flowing" would therefore be asserting something false, and asserting it loosely enough to pass
    /// would be asserting nothing. What survives the measurement, and is asserted here, is the part
    /// that actually matters operationally: <b>the connection is never torn down and rebuilt</b>, the
    /// only failure is the one the read loop already classifies as transient and retries, and full
    /// read throughput returns as soon as the pool does. The end-to-end consequence — no message lost,
    /// no reconnect, complete drain — is
    /// <see cref="S21f_consumer_survives_threadpool_starvation_without_reconnecting_or_losing_messages"/>.
    /// </para>
    /// <para>
    /// The reads are driven from a dedicated <see cref="Thread"/> waiting on each fetch synchronously,
    /// so that the observation itself never needs a pool thread and cannot be what stalled. The fetch
    /// is the library's real <see cref="BlockFetch"/> — cursor advance and reply parsing included —
    /// against a real backlog, not a hand-rolled command.
    /// </para>
    /// <para>
    /// The whole starvation section is written without a single <c>await</c>. A continuation inside it
    /// would need the very pool thread the test has taken away, and the test would hang on its own
    /// setup rather than on anything it is measuring.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S21f_threadpool_starvation_does_not_reconnect_the_reader_connection()
    {
        const int backlog = 20_000;

        var topic = fixture.NewTopic();
        var consumerName = fixture.NewConsumer();

        var topicOptions = Quiet(partitions: 1);
        var root = this.Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, new OneService(fixture.Redis), logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        await PublishManyAsync(publisher, backlog);

        var consumer = new ConsumerOptions
        {
            Topic = topic,
            ReadMode = ReadMode.Block,
            BlockMs = StarvationBlockMs,
            BatchSize = 50,
        };

        await using var reader = await StreamReaderConnection.CreateAsync(root, consumer, consumerName, 0, logger: null);

        await RedisStreamsFixture.WaitUntilAsync(
            async () => (await this.ReaderClientsAsync(consumerName)).Count > 0,
            TimeSpan.FromSeconds(30),
            "the dedicated reader connection to appear in CLIENT LIST");

        var before = await this.ReaderClientsAsync(consumerName);
        before.Should().NotBeEmpty("the reader connection must be visible in CLIENT LIST for the reconnect check to mean anything");
        var beforeIds = before.Select(static c => c.Id).OrderBy(static id => id).ToArray();

        var fetch = new BlockFetch(reader, StreamKey(topic, 0), StreamId.Min, consumer.BatchSize);

        var entries = 0L;
        var rounds = 0;
        var stop = false;
        var faults = new List<string>();
        var faultGate = new Lock();

        // Rewound well before the end of the backlog. Reaching the tail would park the read for a
        // whole BlockMs, and a progress slice that happened to land inside one would read zero entries
        // for a reason that has nothing to do with the ThreadPool.
        var roundsPerLap = (backlog / consumer.BatchSize) - 50;

        var pump = new Thread(() =>
        {
            var lap = 0;

            while (!Volatile.Read(ref stop))
            {
                try
                {
                    var batch = fetch.FetchAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
                    _ = Interlocked.Add(ref entries, batch.Count);
                    _ = Interlocked.Increment(ref rounds);

                    if (++lap >= roundsPerLap || batch.IsEmpty)
                    {
                        fetch.SeekTo(StreamId.Min);
                        lap = 0;
                    }
                }
                catch (Exception ex)
                {
                    lock (faultGate)
                    {
                        faults.Add($"{ex.GetType().Name}");
                    }
                }
            }
        })
        {
            IsBackground = true,
            Name = "s21f-pump",
        };

        pump.Start();

        long healthy;
        long duringStarvation;
        long afterRecovery;
        int[] slices;
        TimeSpan probeDelay;
        TimeSpan recovery;

        // ---- no awaits from here until the pool is released ----
        using (var starver = new PoolStarver(hold: false))
        {
            // Rig control, taken before the hogs are queued: the pump must be able to read at all.
            Thread.Sleep(750);
            healthy = Interlocked.Read(ref entries);

            starver.Start();
            probeDelay = starver.MeasureQueueDelay(TimeSpan.FromSeconds(2));

            var at = Interlocked.Read(ref entries);
            var measured = new List<int>();
            var window = Stopwatch.StartNew();

            while (window.Elapsed < StarvationWindow)
            {
                Thread.Sleep(500);

                var now = Interlocked.Read(ref entries);
                measured.Add((int)Math.Min(int.MaxValue, now - at));
                at = now;
            }

            slices = [.. measured];
            duringStarvation = slices.Sum(static x => (long)x);
        }
        // ---- pool released ----

        // Reads must come back on their own, on the same connection, without anyone restarting anything.
        var resumeFrom = Interlocked.Read(ref entries);
        var resumeClock = Stopwatch.StartNew();

        await RedisStreamsFixture.WaitUntilAsync(
            () => Interlocked.Read(ref entries) - resumeFrom >= 1_000,
            TimeSpan.FromSeconds(30),
            "the reader connection to resume reading once the ThreadPool recovers");

        recovery = resumeClock.Elapsed;
        afterRecovery = Interlocked.Read(ref entries) - resumeFrom;

        var after = await this.ReaderClientsAsync(consumerName);
        var afterIds = after.Select(static c => c.Id).OrderBy(static id => id).ToArray();

        Volatile.Write(ref stop, true);
        pump.Join(TimeSpan.FromSeconds(30)).Should().BeTrue("the read pump must not be wedged");

        string[] faultTypes;
        lock (faultGate)
        {
            faultTypes = [.. faults];
        }

        output.WriteLine(
            $"Reader connection through a {StarvationWindow.TotalSeconds:0}s ThreadPool starvation " +
            $"(a probe work item queued behind the hogs waited >{probeDelay.TotalMilliseconds:0} ms without running):\n" +
            $"  entries read in 750 ms with a healthy pool: {healthy}\n" +
            $"  entries read during the starvation window:  {duringStarvation}  (observed, not asserted)\n" +
            $"  per 500 ms slice: [{string.Join(", ", slices)}]\n" +
            $"  entries read after release: {afterRecovery} in {recovery.TotalMilliseconds:0} ms\n" +
            $"  round trips {Volatile.Read(ref rounds)}, faults [{string.Join(", ", faultTypes.Distinct())}] x{faultTypes.Length}\n" +
            $"  reader client ids before/after: [{string.Join(",", beforeIds)}] / [{string.Join(",", afterIds)}]");

        // Rig control — a pump that could not read even on a healthy pool would prove nothing below.
        healthy.Should().BeGreaterThan(
            1_000,
            "the read pump must be reading at full rate before the ThreadPool is taken away");

        // CONTROL — the pool really was starved; without this the test passes on a healthy machine
        // no matter what the reader connection does.
        probeDelay.Should().BeGreaterThanOrEqualTo(
            TimeSpan.FromMilliseconds(1_900),
            "CONTROL — a work item queued behind the hogs must not have run; otherwise nothing was starved");

        // The claims that survive measurement.
        afterIds.Should().Equal(
            beforeIds,
            "a saturated ThreadPool must not cost the reader its socket — these are the same Redis client ids, so it was never torn down and rebuilt");

        reader.IsConnected.Should().BeTrue("the reader connection must still be established");

        faultTypes.Should().OnlyContain(
            name => name == nameof(RedisTimeoutException),
            "the only failure a starved pool may produce is the retryable timeout the read loop already classifies as a transient transport failure — " +
            "a RedisConnectionException would mean the connection actually dropped");

        afterRecovery.Should().BeGreaterThanOrEqualTo(
            1_000,
            "reads must resume on the same connection once the pool clears, with no reconnect and no restart");

        _ = await connection.Connection.GetDatabase().KeyDeleteAsync(StreamKey(topic, 0));
    }

    /// <summary>
    /// S21f. A full <see cref="StreamConsumerHost"/> ridden through a ThreadPool starvation window
    /// keeps its reader connection — no reconnect — and drains the whole backlog once the pool frees
    /// up, with nothing lost.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The previous test isolates the connection; this one runs the shipping pipeline over it. The
    /// assertions are deliberately the two that hold regardless of scheduling luck: the reader's Redis
    /// client ids are unchanged, and every message published before the starvation is eventually
    /// delivered exactly once.
    /// </para>
    /// <para>
    /// <b>No throughput assertion, on purpose.</b> The handler runs on the standard ThreadPool and the
    /// read loop's continuation after each fetch does too, so during starvation the channel fills and
    /// the reader parks on <c>WriteAsync</c>. Delivery during the window is therefore whatever the
    /// pool's thread injection happens to allow, and asserting on it would be asserting on the .NET
    /// hill-climbing heuristic. What must not happen — and what is asserted — is a connection tear-down
    /// or a lost message.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S21f_consumer_survives_threadpool_starvation_without_reconnecting_or_losing_messages()
    {
        const int backlog = 5_000;

        var topic = fixture.NewTopic();
        var consumerName = fixture.NewConsumer();

        var topicOptions = Quiet(partitions: 2);
        var root = this.Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, new OneService(fixture.Redis), logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        await PublishManyAsync(publisher, backlog);

        var handler = new TestHandler();
        var consumer = new ConsumerOptions
        {
            Topic = topic,
            ReadMode = ReadMode.Block,
            BlockMs = StarvationBlockMs,
            BatchSize = 100,
            Persist = PersistMode.AsyncBatch,
            PersistIntervalMs = 250,
            StartFrom = StartFrom.Beginning,
            Backpressure = new BackpressureOptions { Enabled = true, Capacity = 4 },
        };

        var host = new StreamConsumerHost(root, consumer, consumerName, handler.HandleAsync, connection, NullLogger.Instance);

        try
        {
            await host.StartAsync(CancellationToken.None);

            // Let the pipeline reach steady state before the pool is taken away, so the starvation
            // window lands on a running consumer rather than on one still starting up.
            await handler.WaitForAsync(500, TimeSpan.FromSeconds(60));

            var before = await this.ReaderClientsAsync(consumerName);
            before.Should().NotBeEmpty("ReadMode.Block must open a dedicated reader connection");
            var beforeIds = before.Select(static c => c.Id).OrderBy(static id => id).ToArray();

            var deliveredBefore = handler.Count;
            TimeSpan probeDelay;

            // ---- no awaits from here until the pool is released ----
            using (var starver = new PoolStarver())
            {
                probeDelay = starver.MeasureQueueDelay(TimeSpan.FromSeconds(2));
                Thread.Sleep(StarvationWindow);
            }
            // ---- pool released ----

            var duringStarvation = handler.Count - deliveredBefore;

            var midIds = (await this.ReaderClientsAsync(consumerName)).Select(static c => c.Id).OrderBy(static id => id).ToArray();

            await handler.WaitForAsync(backlog, TimeSpan.FromSeconds(120));

            var afterIds = (await this.ReaderClientsAsync(consumerName)).Select(static c => c.Id).OrderBy(static id => id).ToArray();

            output.WriteLine(
                $"Consumer through a {StarvationWindow.TotalSeconds:0}s starvation window " +
                $"(probe work item waited >{probeDelay.TotalMilliseconds:0} ms without running):\n" +
                $"  delivered before {deliveredBefore}, during the window {duringStarvation} (not asserted on), total {handler.Count}\n" +
                $"  reader client ids before/during/after: {beforeIds.Length}/{midIds.Length}/{afterIds.Length}");

            // CONTROL — the pool really was starved.
            probeDelay.Should().BeGreaterThanOrEqualTo(
                TimeSpan.FromMilliseconds(1_900),
                "CONTROL — the ThreadPool must genuinely be starved for the rest of this test to mean anything");

            midIds.Should().Equal(
                beforeIds,
                "a saturated ThreadPool must not delay the reader connection's heartbeats into a reconnect");

            afterIds.Should().Equal(
                beforeIds,
                "and it must still be the same connection once the pool recovers");

            handler.Count.Should().Be(
                backlog,
                "every message published before the starvation must be delivered exactly once once the pool frees up");
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    // ---------------------------------------------------------------------------------------
    // Rig
    // ---------------------------------------------------------------------------------------

    /// <summary>Topic options for a test that publishes little and must never lose what it does publish.</summary>
    private static TopicOptions Quiet(int partitions) => new()
    {
        Partitions = partitions,
        Trim = TrimMode.None,
        MaxLen = long.MaxValue,
        BackgroundTrimIntervalSeconds = 0,
    };

    private static ConsumerOptions LatencyConsumer(string topic) => new()
    {
        Topic = topic,
        ReadMode = ReadMode.Block,
        BlockMs = LatencyBlockMs,
        BatchSize = 16,
        // Nothing is published in that test, so the flusher would have nothing to write; a long
        // interval keeps even that off the connection being timed.
        Persist = PersistMode.AsyncBatch,
        PersistIntervalMs = 5_000,
        StartFrom = StartFrom.Beginning,
    };

    private static RedisKey StreamKey(string topic, int partition)
        => (RedisKey)string.Create(CultureInfo.InvariantCulture, $"s:{{{topic}}}:{partition}");

    private StreamOptions Root(string topic, TopicOptions topicOptions) => new()
    {
        ConnectionString = fixture.ConnectionString,
        Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
    };

    /// <summary>Publishes <paramref name="count"/> small messages, spread across keys, in pipelined chunks.</summary>
    private static async Task PublishManyAsync(StreamPublisher publisher, int count, int chunk = 500)
    {
        var buffers = new byte[Math.Min(chunk, count)][];
        for (var i = 0; i < buffers.Length; i++)
        {
            buffers[i] = new byte[32];
        }

        var bodies = new ReadOnlyMemory<byte>[buffers.Length];
        for (var i = 0; i < buffers.Length; i++)
        {
            bodies[i] = buffers[i];
        }

        for (var sent = 0; sent < count; sent += buffers.Length)
        {
            var take = Math.Min(buffers.Length, count - sent);
            await publisher.PublishBatchAsync("starve", bodies.AsMemory(0, take), MessageType);
        }
    }

    /// <summary>
    /// Times a tight sequential <c>GET</c> loop for <paramref name="window"/> and returns every
    /// latency in milliseconds.
    /// </summary>
    /// <remarks>
    /// Sequential on purpose: a parallel loop would hide a stall behind its own queueing, and the
    /// question here is what one command's latency looks like, not what the connection's throughput
    /// is. A timeout is recorded at its elapsed time rather than thrown — in the control phase a
    /// timeout <em>is</em> the result.
    /// </remarks>
    private static async Task<List<double>> SampleGetLatencyAsync(IDatabase db, RedisKey key, TimeSpan window)
    {
        var samples = new List<double>(8192);
        var clock = Stopwatch.StartNew();

        while (clock.Elapsed < window)
        {
            var start = Stopwatch.GetTimestamp();
            try
            {
                _ = await db.StringGetAsync(key).ConfigureAwait(false);
            }
            catch (RedisTimeoutException)
            {
                // Recorded, not swallowed: the elapsed time below is the measurement.
            }

            samples.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
        }

        return samples;
    }

    /// <summary>
    /// Issues back-to-back blocking <c>XREAD</c>s on <paramref name="db"/> until cancelled — the
    /// control arrangement, where the block is on the same connection everything else is using.
    /// </summary>
    private static async Task BlockForeverAsync(IDatabase db, string key, CancellationToken ct)
    {
        object[] args = ["BLOCK", LatencyBlockMs, "COUNT", 1, "STREAMS", (RedisKey)key, "$"];

        while (!ct.IsCancellationRequested)
        {
            try
            {
                _ = await db.ExecuteAsync("XREAD", args).ConfigureAwait(false);
            }
            catch (RedisException)
            {
                // The control connection is expected to misbehave; that is the point of it.
            }
        }
    }

    /// <summary>The percentile of <paramref name="samples"/>, nearest-rank.</summary>
    private static TimeSpan Percentile(List<double> samples, double percentile)
    {
        if (samples.Count == 0)
        {
            return TimeSpan.Zero;
        }

        var sorted = samples.ToArray();
        Array.Sort(sorted);

        var rank = (int)Math.Ceiling(percentile * sorted.Length) - 1;
        return TimeSpan.FromMilliseconds(sorted[Math.Clamp(rank, 0, sorted.Length - 1)]);
    }

    /// <summary>
    /// The Redis clients belonging to <paramref name="consumerName"/>'s dedicated reader connection.
    /// </summary>
    /// <remarks>
    /// A reconnect means a new TCP connection and therefore a new client id, so the ids these carry
    /// are the reconnect detector. The consumer name is unique per test, so this cannot pick up
    /// another test's reader.
    /// </remarks>
    private async Task<IReadOnlyList<ClientInfo>> ReaderClientsAsync(string consumerName)
    {
        var clients = await this.ClientsAsync();

        return [.. clients.Where(c =>
            c.Name is { } name &&
            name.StartsWith(ReaderClientPrefix, StringComparison.Ordinal) &&
            name.Contains(consumerName, StringComparison.Ordinal))];
    }

    private async Task<ClientInfo[]> ClientsAsync()
    {
        var endpoint = fixture.Redis.GetEndPoints()[0];
        return await fixture.Redis.GetServer(endpoint).ClientListAsync();
    }

    /// <summary>
    /// The one-entry container that makes <see cref="StreamsConnectionProvider"/> reuse the fixture's
    /// multiplexer, so the connection these tests time is the library's own shared connection.
    /// </summary>
    private sealed class OneService(IConnectionMultiplexer multiplexer) : IServiceProvider
    {
        public object? GetService(Type serviceType) =>
            serviceType == typeof(IConnectionMultiplexer) ? multiplexer : null;
    }

    /// <summary>
    /// Saturates the ThreadPool with blocking work items for as long as it is alive.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>MinThreads</c> is dropped to one first so the pool cannot simply absorb the hogs with
    /// threads it was already entitled to create, and the hogs are queued to the global queue where
    /// they are served roughly in order. Beyond the threads that already exist, the pool then injects
    /// new ones only at its hill-climbing rate of one or two a second, which is what keeps the queue
    /// backed up for the length of a window measured in seconds.
    /// </para>
    /// <para>
    /// The hogs block on a <see cref="ManualResetEventSlim"/> created with a spin count of zero: the
    /// default spin would burn a core per hog before parking, which on a CI box would starve the
    /// machine rather than the pool and make everything else in the process — Redis's own threads
    /// included — look pathologically slow.
    /// </para>
    /// <para>
    /// Everything is restored on <see cref="Dispose"/>, including the original <c>MinThreads</c>.
    /// </para>
    /// </remarks>
    private sealed class PoolStarver : IDisposable
    {
        private readonly ManualResetEventSlim release = new(false, 0);
        private readonly int minWorkers;
        private readonly int minIo;
        private bool started;

        internal PoolStarver(bool hold = true)
        {
            ThreadPool.GetMinThreads(out this.minWorkers, out this.minIo);

            if (hold)
            {
                this.Start();
            }
        }

        /// <summary>
        /// Queues the hogs. Separate from the constructor so a test can take a rig-control measurement
        /// on a healthy pool first, under the same <c>using</c> that guarantees the release.
        /// </summary>
        internal void Start()
        {
            if (this.started)
            {
                return;
            }

            this.started = true;
            _ = ThreadPool.SetMinThreads(1, this.minIo);

            var hogs = (Environment.ProcessorCount * 8) + 128;
            for (var i = 0; i < hogs; i++)
            {
                // preferLocal: false puts every hog on the global queue, where they are served in
                // order — which is what makes the probe below a reliable starvation detector.
                ThreadPool.UnsafeQueueUserWorkItem(static gate => gate.Wait(), this.release, preferLocal: false);
            }
        }

        /// <summary>
        /// Queues one probe work item behind the hogs and returns how long it waited without running,
        /// up to <paramref name="limit"/>. A value at the limit is the positive control: the pool is
        /// genuinely starved, and any liveness observed while this object is alive is meaningful.
        /// </summary>
        internal TimeSpan MeasureQueueDelay(TimeSpan limit)
        {
            // Not disposed: it stays queued behind the hogs and is set long after this returns.
            var started = new ManualResetEventSlim(false, 0);
            ThreadPool.UnsafeQueueUserWorkItem(static gate => gate.Set(), started, preferLocal: false);

            var clock = Stopwatch.StartNew();
            _ = started.Wait(limit);
            return clock.Elapsed;
        }

        public void Dispose()
        {
            // Released but not disposed: the hogs are still inside Wait() and disposing the event out
            // from under them is how a starvation helper turns into an ObjectDisposedException storm.
            this.release.Set();
            _ = ThreadPool.SetMinThreads(this.minWorkers, this.minIo);
        }
    }
}
