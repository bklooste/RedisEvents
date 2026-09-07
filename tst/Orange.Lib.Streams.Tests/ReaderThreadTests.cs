using System.Diagnostics;
using System.Globalization;
using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Logging.Abstractions;

using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Diagnostics;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;

namespace Orange.Lib.Streams.Tests;

/// <summary>
/// What a blocking reader costs in threads, and where handler code is allowed to run — plan rows
/// S21c and S21d.
/// </summary>
/// <remarks>
/// <para>
/// <b>S21c — the cost of a reader is one thread, and it is one thread on every size of box.</b> Each
/// <see cref="ReadMode.Block"/> consumer opens its own multiplexer with its own
/// <see cref="StackExchange.Redis.SocketManager"/> constructed at
/// <c>workerCount: <see cref="ConsumerOptions.ReaderThreads"/></c> (default 1). The default
/// <c>SocketManager</c> sizes its pool off <see cref="Environment.ProcessorCount"/> instead — this
/// process's shared multiplexer runs one of those, and the test prints its size next to the reader's
/// so the two numbers can be compared in the log. On the machine this was written on that is
/// <b>10 threads for the shared default pool against 1 per reader on 12 cores</b>: eight consumers
/// would cost eighty threads rather than eight if the explicit <c>workerCount</c> were ever dropped.
/// </para>
/// <para>
/// <b>Counting by thread name, not by process thread count.</b> A raw
/// <see cref="Process.Threads"/> delta is not a stable measurement: the ThreadPool injects and
/// retires threads on its own schedule, so the number moves under a test that never asked it to.
/// The reader threads are named — <see cref="StreamNames.ReaderThreadName"/> builds
/// <c>streams-rd:{consumer}:i{index}</c> — and .NET pushes a managed thread's name down to the OS,
/// so Linux exposes them one file at a time under <c>/proc/self/task/*/comm</c>. That count is
/// exact, attributable to <em>this</em> test's consumers, and unaffected by ThreadPool weather. The
/// process-wide delta is still measured and printed, and asserted only as a loose sanity bound.
/// The kernel truncates <c>comm</c> to 15 characters, which is why every consumer name here starts
/// with a marker that survives inside <c>streams-rd:</c> + 4 characters.
/// </para>
/// <para>
/// <b>S21d — the handler never runs on a reader thread.</b> With one socket worker for the whole
/// connection, application code executing there would stop that consumer reading for exactly as long
/// as the handler took. Both backpressure modes are covered, because they reach the handler by
/// different routes: the channel path starts its processor with <c>Task.Run</c> (structural), while
/// the inline path calls the handler from the read loop itself and has to force the hop
/// deliberately — the more fragile of the two, and the one
/// <see cref="S21d_inline_block_handler_does_not_occupy_the_reader_connection"/> holds a stopwatch to.
/// </para>
/// <para>
/// <b>Measured while writing these:</b> StackExchange.Redis 2.10.1 already completes the
/// <c>XREAD</c> task on the ThreadPool rather than on the socket worker, so today the forced hop is
/// the second of two defences rather than the only one. That is a reason to keep the assertion, not
/// to weaken it: the completion behaviour is the client library's business and can change under us,
/// while the hop is ours. The tests below assert the outcome — where the handler actually ran — so
/// they hold whichever of the two mechanisms is doing the work.
/// </para>
/// <para>
/// <b>Every test here uses its own topic and its own consumer names</b>, and the thread-name
/// counters match a per-test marker, so nothing in this file can see another test's readers.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class ReaderThreadTests(RedisStreamsFixture fixture, ITestOutputHelper output)
{
    /// <summary>The prefix every reader thread name carries.</summary>
    private const string ReaderPrefix = "streams-rd:";

    /// <summary>How long the OS thread names are given to appear or disappear.</summary>
    private static readonly TimeSpan ThreadWait = TimeSpan.FromSeconds(15);

    /// <summary>Long enough that a reader stalled behind it is unmistakable, short enough to run in CI.</summary>
    private const int HandlerBlockMs = 4_000;

    // -------------------------------------------------------------------------------------------
    // S21c — thread cost
    // -------------------------------------------------------------------------------------------

    /// <summary>
    /// S21c. One, four and eight blocking consumers cost one named reader thread each — not one pool
    /// per multiplexer sized off <see cref="Environment.ProcessorCount"/> — and the threads are gone
    /// again once the hosts are disposed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The assertion that matters is the equality: <c>N</c> consumers, <c>N</c> reader threads,
    /// whatever the core count of the box running it. A per-multiplexer default pool would make this
    /// <c>N × ProcessorCount</c> — 96 threads for the eight-consumer case on the 12-core machine this
    /// was written on, 16 on the smallest plausible CI node — so the check fails loudly the moment
    /// the explicit <c>workerCount</c> stops being passed, on any hardware. It is an equality rather
    /// than a range because the count is genuinely deterministic: a <c>SocketManager</c> starts its
    /// workers in its constructor, so there is no window in which some of them have not appeared.
    /// </para>
    /// <para>
    /// The teardown half is not filler. The <c>SocketManager</c> is not owned by the multiplexer it
    /// is handed to, so <see cref="StreamReaderConnection.Dispose"/> has to close it explicitly; if
    /// that were dropped, a service that restarts consumers on rebalance would leak a thread per
    /// restart and nothing else in the suite would notice.
    /// </para>
    /// </remarks>
    /// <param name="consumers">How many blocking consumers to run at once.</param>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(8)]
    [Trait("TestType", "ServiceTest")]
    public async Task S21c_each_block_consumer_costs_exactly_one_named_reader_thread(int consumers)
    {
        Assert.SkipUnless(NamedThreadsVisible(), "Thread names are only readable through /proc/self/task on Linux.");

        // 15-character comm truncation: "streams-rd:" + this marker is exactly what the kernel keeps.
        var marker = $"c{consumers.ToString(CultureInfo.InvariantCulture)}s";
        var prefix = ReaderPrefix + marker;

        var topic = fixture.NewTopic();
        var root = this.Root(topic, Partitioned(1));

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);

        // A previous case's readers may still be shutting down; start from a clean count.
        await RedisStreamsFixture.WaitUntilAsync(
            () => CountNamed(prefix) == 0,
            ThreadWait,
            $"no leftover '{prefix}' reader threads before the measurement");

        var processBefore = ProcessThreads();
        var defaultPool = CountNamed("DefaultSocketMa");

        var hosts = new List<StreamConsumerHost>(consumers);

        try
        {
            for (var i = 0; i < consumers; i++)
            {
                var host = new StreamConsumerHost(
                    root,
                    BlockingConsumer(topic),
                    $"{marker}-{i.ToString(CultureInfo.InvariantCulture)}",
                    static (_, _) => default,
                    connection,
                    NullLogger.Instance);

                hosts.Add(host);
                await host.StartAsync(CancellationToken.None);
            }

            await RedisStreamsFixture.WaitUntilAsync(
                () => CountNamed(prefix) >= consumers,
                ThreadWait,
                $"{consumers} reader thread(s) named '{prefix}…' to appear");

            // Settle, then take the worst of several samples: if anything were creating threads
            // lazily or in bursts, the maximum is where it would show up.
            await Task.Delay(TimeSpan.FromMilliseconds(750));

            var peak = 0;
            for (var sample = 0; sample < 5; sample++)
            {
                peak = Math.Max(peak, CountNamed(prefix));
                await Task.Delay(TimeSpan.FromMilliseconds(100));
            }

            var processAfter = ProcessThreads();
            var delta = processAfter - processBefore;

            output.WriteLine(
                $"consumers={consumers} readerThreads={peak} (expected {consumers}) " +
                $"processorCount={Environment.ProcessorCount} " +
                $"defaultSocketManagerThreads={defaultPool} " +
                $"processThreads {processBefore}->{processAfter} (delta {delta})");

            peak.Should().Be(
                consumers,
                "each Block consumer must cost exactly one reader thread — a SocketManager left at its " +
                "default worker count would size off ProcessorCount ({0} here), making this {1} rather than {2}",
                Environment.ProcessorCount,
                consumers * Environment.ProcessorCount,
                consumers);

            // Loose by design: the ThreadPool grows and shrinks underneath any process-wide count, so
            // this is a sanity bound rather than the measurement. It is still enough to catch
            // per-core scaling on any box with four or more cores.
            delta.Should().BeLessThanOrEqualTo(
                consumers + 16,
                "starting {0} blocking consumers must not add a pool of threads per connection",
                consumers);
        }
        finally
        {
            foreach (var host in hosts)
            {
                await host.StopAsync(CancellationToken.None);
                await host.DisposeAsync();
            }
        }

        await RedisStreamsFixture.WaitUntilAsync(
            () => CountNamed(prefix) == 0,
            ThreadWait,
            "every reader thread to exit once its host is disposed (the SocketManager is not owned by the multiplexer)");
    }

    /// <summary>
    /// S21c, the control. The reader thread count follows
    /// <see cref="ConsumerOptions.ReaderThreads"/> and nothing else: asking for three gives three,
    /// which is what proves the "exactly one" measurement above is a measurement rather than a
    /// counter that can only ever return one.
    /// </summary>
    /// <remarks>
    /// It also pins the default. <c>ReaderThreads</c> defaulting to anything but 1 — or being ignored
    /// in favour of the <c>SocketManager</c> default — is precisely the regression S21c exists to
    /// catch, and here it is stated twice: once as the documented default value, once as an observed
    /// thread count.
    /// </remarks>
    /// <param name="readerThreads">The configured worker count.</param>
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [Trait("TestType", "ServiceTest")]
    public async Task S21c_reader_thread_count_follows_the_configured_worker_count(int readerThreads)
    {
        Assert.SkipUnless(NamedThreadsVisible(), "Thread names are only readable through /proc/self/task on Linux.");

        new ConsumerOptions().ReaderThreads.Should().Be(
            1,
            "the documented default is one dedicated thread per reader, not one pool per connection");

        var marker = $"w{readerThreads.ToString(CultureInfo.InvariantCulture)}s";
        var prefix = ReaderPrefix + marker;

        var topic = fixture.NewTopic();
        var root = this.Root(topic, Partitioned(1));

        await RedisStreamsFixture.WaitUntilAsync(
            () => CountNamed(prefix) == 0,
            ThreadWait,
            $"no leftover '{prefix}' reader threads before the measurement");

        var consumer = BlockingConsumer(topic) with { ReaderThreads = readerThreads };

        var reader = await StreamReaderConnection.CreateAsync(
            root,
            consumer,
            $"{marker}-only",
            index: 0,
            NullLogger.Instance);

        try
        {
            reader.ReaderThreads.Should().Be(readerThreads);
            reader.ThreadName.Should().StartWith(prefix, "the reader thread name is what this test counts");

            await RedisStreamsFixture.WaitUntilAsync(
                () => CountNamed(prefix) >= readerThreads,
                ThreadWait,
                $"{readerThreads} reader thread(s) named '{prefix}…' to appear");

            await Task.Delay(TimeSpan.FromMilliseconds(500));

            var counted = CountNamed(prefix);
            output.WriteLine($"ReaderThreads={readerThreads} counted={counted} processorCount={Environment.ProcessorCount}");

            counted.Should().Be(
                readerThreads,
                "the socket manager is built at workerCount: ReaderThreads, so the thread count is the configured one — " +
                "not ProcessorCount ({0})",
                Environment.ProcessorCount);
        }
        finally
        {
            await reader.DisposeAsync();
        }

        await RedisStreamsFixture.WaitUntilAsync(
            () => CountNamed(prefix) == 0,
            ThreadWait,
            "the reader's threads to exit when the connection is disposed");
    }

    // -------------------------------------------------------------------------------------------
    // S21d — thread placement
    // -------------------------------------------------------------------------------------------

    /// <summary>
    /// S21d. Every batch is handed to the handler on a ThreadPool thread and never on one of the
    /// consumer's dedicated reader threads — with backpressure enabled (the channel path) and
    /// disabled (the inline path, where the hop onto the pool is forced rather than structural).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Two things are asserted per delivery: the thread is a ThreadPool thread, and its name is not a
    /// reader thread name. Both matter — the pool check catches handler work moved onto any dedicated
    /// thread, the name check catches it landing on <em>this</em> consumer's socket worker, which is
    /// the one that would stop the consumer reading.
    /// </para>
    /// <para>
    /// The test first waits for a reader thread to actually exist. Without that, "no sample was a
    /// reader thread" would also pass in a world where <see cref="ReadMode.Block"/> had quietly
    /// stopped opening a dedicated connection at all — the exclusion set has to be non-empty for the
    /// exclusion to mean anything.
    /// </para>
    /// </remarks>
    /// <param name="backpressure">Whether the channel path or the inline path is under test.</param>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("TestType", "ServiceTest")]
    public async Task S21d_handler_runs_on_the_thread_pool_never_on_a_reader_thread(bool backpressure)
    {
        const int total = 40;

        var marker = backpressure ? "dbps" : "dinl";
        var prefix = ReaderPrefix + marker;

        var topic = fixture.NewTopic();
        var topicOptions = Partitioned(2);
        var root = this.Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        var gate = new Lock();
        var samples = new List<Sample>();

        var handler = new TestHandler
        {
            OnBatch = (_, _) =>
            {
                var current = Thread.CurrentThread;
                lock (gate)
                {
                    samples.Add(new Sample(
                        Environment.CurrentManagedThreadId,
                        current.Name,
                        current.IsThreadPoolThread));
                }

                return default;
            },
        };

        var consumer = BlockingConsumer(topic) with
        {
            BatchSize = 8,
            Backpressure = new BackpressureOptions { Enabled = backpressure, Capacity = 4 },
        };

        var host = new StreamConsumerHost(
            root,
            consumer,
            $"{marker}-{Guid.NewGuid().ToString("N")[..6]}",
            handler.HandleAsync,
            connection,
            NullLogger.Instance);

        await host.StartAsync(CancellationToken.None);

        try
        {
            if (NamedThreadsVisible())
            {
                await RedisStreamsFixture.WaitUntilAsync(
                    () => CountNamed(prefix) >= 1,
                    ThreadWait,
                    "the consumer's dedicated reader thread to exist — otherwise 'not a reader thread' asserts nothing");
            }

            // Published in bursts so the handler is entered many times over many read rounds,
            // rather than once on whichever thread happened to drain a single backlog.
            for (var burst = 0; burst < 8; burst++)
            {
                for (var i = 0; i < total / 8; i++)
                {
                    var key = $"k{((burst * 5) + i).ToString(CultureInfo.InvariantCulture)}";
                    _ = await publisher.PublishAsync(key, Encoding.UTF8.GetBytes(key), "placement");
                }

                await Task.Delay(TimeSpan.FromMilliseconds(40));
            }

            await handler.WaitForAsync(total, TimeSpan.FromSeconds(30));
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            await host.DisposeAsync();
        }

        Sample[] observed;
        lock (gate)
        {
            observed = [.. samples];
        }

        output.WriteLine(
            $"backpressure={backpressure} batches={observed.Length} " +
            $"distinctThreads={observed.Select(s => s.ThreadId).Distinct().Count()} " +
            $"names=[{string.Join(", ", observed.Select(s => s.Name ?? "<null>").Distinct())}]");

        observed.Should().NotBeEmpty("the handler must have run for this test to say anything");
        handler.ThreadIds.Should().NotBeEmpty();

        observed.Should().OnlyContain(
            s => s.IsThreadPool,
            "handler code must start on the standard ThreadPool; a dedicated reader thread running it " +
            "would stall this consumer's XREADs for as long as the handler took");

        observed.Should().NotContain(
            s => s.Name != null && s.Name.StartsWith(ReaderPrefix, StringComparison.Ordinal),
            "no batch may be handled on a reader connection's socket thread");
    }

    /// <summary>
    /// S21d, the one with teeth: with <see cref="ReadMode.Block"/> and backpressure <b>off</b>, a
    /// handler that blocks its thread for four seconds does not stop the same consumer reading a
    /// different partition over the same dedicated connection.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the inline path — the read loop calls the handler itself, and a forced
    /// <c>Task.Run</c> is what keeps that call off the connection's single socket worker. The topic is
    /// configured with <c>CoLocatePartitions = false</c> on purpose: that gives one read loop per
    /// partition, both queued on the one reader connection, so the two partitions share exactly the
    /// resource the handler must not occupy. Partition A's handler blocks; partition B's message is
    /// published afterwards and must still arrive while A is still blocked.
    /// </para>
    /// <para>
    /// If handler code ran on the reader's socket thread, B's reply could not be processed until A
    /// returned four seconds later, and the wait would time out. The measured gap is ~1 ms against a
    /// 2 s bound and a 4 s block, so the tolerance is two thousand times the observed value in the
    /// healthy case and still an order of magnitude below the broken one — the assertion is
    /// structural (arrived-while-blocked) rather than a stopwatch race.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S21d_inline_block_handler_does_not_occupy_the_reader_connection()
    {
        const int partitions = 2;

        var topic = fixture.NewTopic();
        var topicOptions = Partitioned(partitions) with { CoLocatePartitions = false };
        var root = this.Root(topic, topicOptions);

        var keyA = KeyForPartition(0, partitions);
        var keyB = KeyForPartition(1, partitions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        var blocking = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var gate = new Lock();
        var samples = new List<Sample>();

        ValueTask Handle(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            var current = Thread.CurrentThread;
            lock (gate)
            {
                samples.Add(new Sample(Environment.CurrentManagedThreadId, current.Name, current.IsThreadPoolThread));
            }

            if (Encoding.UTF8.GetString(batch.Span[0].Body.Span) == keyA)
            {
                blocking.TrySetResult();

                // Deliberately synchronous: the point is to occupy whatever thread the handler was
                // given, which an await would not do.
                Thread.Sleep(HandlerBlockMs);
                released.TrySetResult();
            }
            else
            {
                second.TrySetResult();
            }

            return default;
        }

        var consumer = BlockingConsumer(topic) with
        {
            BatchSize = 8,
            Backpressure = new BackpressureOptions { Enabled = false },
        };

        var host = new StreamConsumerHost(
            root,
            consumer,
            $"dliv-{Guid.NewGuid().ToString("N")[..6]}",
            Handle,
            connection,
            NullLogger.Instance);

        await host.StartAsync(CancellationToken.None);

        try
        {
            host.OwnedPartitions.Should().HaveCount(
                partitions,
                "one host must own both partitions for their read loops to share its reader connection");

            _ = await publisher.PublishAsync(keyA, Encoding.UTF8.GetBytes(keyA), "placement");
            await blocking.Task.WaitAsync(TimeSpan.FromSeconds(20));

            var watch = Stopwatch.StartNew();
            _ = await publisher.PublishAsync(keyB, Encoding.UTF8.GetBytes(keyB), "placement");

            try
            {
                await second.Task.WaitAsync(TimeSpan.FromSeconds(2));
            }
            catch (TimeoutException ex)
            {
                throw new TimeoutException(
                    "Partition 1 was not delivered within 2 s while partition 0's handler was blocked for " +
                    $"{HandlerBlockMs} ms. The handler is occupying something the reader needs — the dedicated " +
                    "connection's socket thread, or the read loop itself.",
                    ex);
            }

            watch.Stop();

            released.Task.IsCompleted.Should().BeFalse(
                "the second partition must be served while the first partition's handler is still blocked — " +
                "if the handler held the reader connection's socket thread, nothing could be read for {0} ms",
                HandlerBlockMs);

            output.WriteLine(
                $"partition 1 delivered {watch.ElapsedMilliseconds} ms after publish, " +
                $"while partition 0's handler was {HandlerBlockMs} ms into a synchronous block");

            await released.Task.WaitAsync(TimeSpan.FromSeconds(30));
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
            await host.DisposeAsync();
        }

        Sample[] observed;
        lock (gate)
        {
            observed = [.. samples];
        }

        observed.Should().HaveCountGreaterThanOrEqualTo(2);
        observed.Should().OnlyContain(
            s => s.IsThreadPool,
            "the inline path forces a ThreadPool hop precisely so a blocking handler cannot occupy the reader thread");
        observed.Should().NotContain(
            s => s.Name != null && s.Name.StartsWith(ReaderPrefix, StringComparison.Ordinal));
    }

    // -------------------------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------------------------

    /// <summary>One handler entry: which thread it ran on and what kind of thread that was.</summary>
    private readonly record struct Sample(int ThreadId, string? Name, bool IsThreadPool);

    /// <summary>Whether OS thread names can be read — Linux only, which is what CI runs.</summary>
    private static bool NamedThreadsVisible() => Directory.Exists("/proc/self/task");

    /// <summary>
    /// How many OS threads currently carry a name starting with <paramref name="prefix"/>.
    /// </summary>
    /// <remarks>
    /// The kernel truncates <c>comm</c> to 15 characters, so callers pass a prefix that fits inside
    /// that. Threads come and go while the directory is being walked; a vanished task is simply not
    /// counted rather than an error.
    /// </remarks>
    private static int CountNamed(string prefix)
    {
        var count = 0;

        foreach (var task in Directory.EnumerateDirectories("/proc/self/task"))
        {
            try
            {
                if (File.ReadAllText(Path.Combine(task, "comm")).TrimEnd('\n').StartsWith(prefix, StringComparison.Ordinal))
                {
                    count++;
                }
            }
            catch (IOException)
            {
                // The thread exited between the enumeration and the read.
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        return count;
    }

    /// <summary>The process's current OS thread count, refreshed rather than cached.</summary>
    private static int ProcessThreads()
    {
        using var self = Process.GetCurrentProcess();
        return self.Threads.Count;
    }

    /// <summary>The first key that routes to <paramref name="partition"/>.</summary>
    private static string KeyForPartition(int partition, int partitions)
    {
        for (var i = 0; i < 10_000; i++)
        {
            var key = $"k{i.ToString(CultureInfo.InvariantCulture)}";
            if (PartitionRouter.ForKey(Encoding.UTF8.GetBytes(key), partitions) == partition)
            {
                return key;
            }
        }

        throw new InvalidOperationException($"No key in the search space routes to partition {partition}.");
    }

    /// <summary>A blocking consumer that reads from the beginning and keeps no position state.</summary>
    private static ConsumerOptions BlockingConsumer(string topic) => new()
    {
        Topic = topic,
        BatchSize = 100,
        BlockMs = 250,
        ReadMode = ReadMode.Block,
        StartFrom = StartFrom.Beginning,
        Persist = PersistMode.AsyncBatch,
        PersistIntervalMs = 250,
    };

    private static TopicOptions Partitioned(int partitions) => new()
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
}
