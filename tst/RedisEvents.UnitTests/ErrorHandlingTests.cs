using System.Diagnostics;
using System.Threading.Channels;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Errors;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// P3-10. The error-handling contract from <c>06-errors-and-observability.md</c>, asserted against
/// the real processing loop: best effort logs once and advances, a <see cref="DontIgnoreException"/>
/// blocks and never advances, the backoff walks 1→2→4→8→16→30 s and stays there, the block log is
/// rate limited, cancellation is honoured inside the backoff, the two <see cref="ErrorPolicy"/>
/// escapes behave, and a startup <see cref="StreamConfigurationException"/> fails fast.
/// </summary>
/// <remarks>
/// No Redis, no Docker, and no real sleeping: the backoff runs through
/// <c>PartitionWorker.BlockDelay</c>, the <see cref="AsyncLocal{T}"/> seam the implementation exposes
/// for exactly this, so the 30 s ceiling is asserted in milliseconds. The one test that deliberately
/// uses the real <see cref="Task.Delay(int, CancellationToken)"/> is the cancellation test, whose
/// whole point is that a real delay is abandoned rather than waited out.
/// </remarks>
public static class ErrorHandling
{
    /// <summary>A context wired to a channel writer and a logger the test can read back.</summary>
    internal static PartitionContext Context(
        ChannelWriter<StreamBatch>? writer,
        ILogger log,
        ErrorPolicy onError = ErrorPolicy.BestEffort,
        int partition = 0)
        => new(
            Db: null!,
            StreamKey: (RedisKey)$"s:t:{partition}",
            Partition: partition,
            Topic: "t",
            Consumer: "c",
            BatchSize: 100,
            Filter: null,
            OnError: onError,
            Writer: writer,
            Log: log);

    /// <summary>Queues one decoded single-message batch, exactly as the read loop would have.</summary>
    internal static StreamId Queue(Channel<StreamBatch> channel, long ms, long seq, string type = "Keep")
    {
        var entry = Pipeline.Entry(ms, seq, type);
        var items = StreamBatch.Rent(1);
        items[0] = EntryCodec.Decode(in entry, partition: 0);

        var batch = new StreamBatch(items, 1, items[0].Id);
        channel.Writer.TryWrite(batch).Should().BeTrue();

        return batch.Last;
    }
}

/// <summary>
/// A service-defined <see cref="DontIgnoreException"/>, declared in the <em>test</em> assembly on
/// purpose: the requirement is that the base is public and abstract so a service can derive its own
/// and have the library respect it, which is only actually proven from outside the library.
/// </summary>
public sealed class MandatoryDownstreamDownException : DontIgnoreException
{
    public MandatoryDownstreamDownException(string message)
        : base(message)
    {
    }
}

/// <summary>Captures log entries so the tests can assert on level, text and rate.</summary>
internal sealed class CapturingLog : ILogger
{
    private readonly List<(LogLevel Level, string Message)> entries = [];

    internal IReadOnlyList<(LogLevel Level, string Message)> Entries
    {
        get
        {
            lock (this.entries)
            {
                return this.entries.ToArray();
            }
        }
    }

    /// <summary>The formatted messages logged at one level, in order.</summary>
    internal IReadOnlyList<string> At(LogLevel level)
        => this.Entries.Where(e => e.Level == level).Select(e => e.Message).ToArray();

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
        lock (this.entries)
        {
            this.entries.Add((logLevel, formatter(state, exception)));
        }
    }
}

/// <summary>
/// The substituted backoff delay: records what the loop asked to sleep for, never sleeps, and can
/// cancel the host token after a chosen number of retries so a test terminates deterministically.
/// </summary>
internal sealed class ScriptedDelay
{
    private readonly List<int> delays = [];
    private readonly int cancelAfter;
    private readonly CancellationTokenSource? cts;

    internal ScriptedDelay(int cancelAfter = int.MaxValue, CancellationTokenSource? cts = null)
    {
        this.cancelAfter = cancelAfter;
        this.cts = cts;
    }

    /// <summary>Every delay the blocking-retry loop asked for, in order.</summary>
    internal IReadOnlyList<int> Delays
    {
        get
        {
            lock (this.delays)
            {
                return this.delays.ToArray();
            }
        }
    }

    internal int Count
    {
        get
        {
            lock (this.delays)
            {
                return this.delays.Count;
            }
        }
    }

    /// <summary>The seam itself. Honours the token, as the substitute contract requires.</summary>
    internal Task DelayAsync(int milliseconds, CancellationToken ct)
    {
        int count;

        lock (this.delays)
        {
            this.delays.Add(milliseconds);
            count = this.delays.Count;
        }

        if (count >= this.cancelAfter)
        {
            this.cts?.Cancel();
        }

        // Synchronous throw is fine: the loop awaits this inside the try that ends the block.
        ct.ThrowIfCancellationRequested();

        return Task.CompletedTask;
    }
}

/// <summary>Best effort — the default, and the behaviour a migrating handler must know about.</summary>
public class BestEffortTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_Ordinary_Exception_Logs_Once_Advances_And_The_Next_Batch_Is_Processed()
    {
        var log = new CapturingLog();
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = ErrorHandling.Context(channel.Writer, log);

        // Any retry at all would show up as a delay request, so the seam is installed even though
        // this path must never reach it.
        var delays = new ScriptedDelay();
        PartitionWorker.BlockDelay = delays.DelayAsync;

        var poison = ErrorHandling.Queue(channel, 1, 0);
        var good = ErrorHandling.Queue(channel, 1, 1);
        channel.Writer.Complete();

        var seen = new List<StreamId>();
        var recorded = new List<StreamId>();

        await PartitionWorker.ProcessLoopAsync(
            ctx,
            (batch, _) =>
            {
                var id = batch.Span[0].Id;
                seen.Add(id);

                if (id == poison)
                {
                    throw new InvalidOperationException("poison");
                }

                return default;
            },
            (_, id) => recorded.Add(id),
            CancellationToken.None,
            channel.Reader);

        // The whole contract in three assertions: invoked once, position advanced, next batch ran.
        seen.Should().Equal(poison, good);
        seen.Count(id => id == poison).Should().Be(1, "the library does not retry a best-effort failure");
        // The position advances past the poison batch, so it is never seen again.
        recorded.Should().Equal(poison, good);

        delays.Delays.Should().BeEmpty("no backoff is entered for a non-DontIgnoreException");

        var errors = log.At(LogLevel.Error);
        errors.Should().ContainSingle("one Error line per failure, not one per attempt");
        errors[0].Should().Contain("BestEffort").And.Contain("does not retry");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Shutdown_Cancellation_From_The_Handler_Is_Not_An_Error()
    {
        var log = new CapturingLog();
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = ErrorHandling.Context(channel.Writer, log);
        using var cts = new CancellationTokenSource();

        ErrorHandling.Queue(channel, 2, 0);
        channel.Writer.Complete();

        var recorded = new List<StreamId>();

        await PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, ct) =>
            {
                cts.Cancel();
                ct.ThrowIfCancellationRequested();
                return default;
            },
            (_, id) => recorded.Add(id),
            cts.Token,
            channel.Reader);

        recorded.Should().BeEmpty("nothing was processed, so nothing is checkpointed; the batch redelivers");
        log.At(LogLevel.Error).Should().BeEmpty("shutdown is not a handler failure");
        log.At(LogLevel.Critical).Should().BeEmpty();
    }
}

/// <summary>
/// <see cref="DontIgnoreException"/>: block on the batch, retry forever with capped backoff, never
/// advance.
/// </summary>
public class BlockingRetryTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_Service_Defined_Subclass_Blocks_Never_Advances_And_Re_Handles_The_Same_Batch()
    {
        var log = new CapturingLog();
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = ErrorHandling.Context(channel.Writer, log);
        using var cts = new CancellationTokenSource();

        // Eight retries then shutdown — enough to see the ladder reach and hold the cap.
        var delays = new ScriptedDelay(cancelAfter: 8, cts);
        PartitionWorker.BlockDelay = delays.DelayAsync;

        var blocked = ErrorHandling.Queue(channel, 3, 0);
        var seen = new List<StreamId>();
        var recorded = new List<StreamId>();

        await PartitionWorker.ProcessLoopAsync(
            ctx,
            (batch, _) =>
            {
                seen.Add(batch.Span[0].Id);
                throw new MandatoryDownstreamDownException("the ledger is unreachable");
            },
            (_, id) => recorded.Add(id),
            cts.Token,
            channel.Reader);

        seen.Should().HaveCount(8, "the first attempt plus one per retry, with no attempt limit reached");
        seen.Should().AllBeEquivalentTo(blocked, "the same batch is re-handled on every attempt");

        recorded.Should().BeEmpty("a blocked partition NEVER advances its position");

        log.At(LogLevel.Error)[0].Should().Contain("BLOCKED")
            .And.Contain("NOT advanced")
            .And.Contain(nameof(MandatoryDownstreamDownException));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_Backoff_Walks_1_2_4_8_16_30_And_Then_Holds_At_30_Seconds()
    {
        var log = new CapturingLog();
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = ErrorHandling.Context(channel.Writer, log);
        using var cts = new CancellationTokenSource();

        var delays = new ScriptedDelay(cancelAfter: 9, cts);
        PartitionWorker.BlockDelay = delays.DelayAsync;

        ErrorHandling.Queue(channel, 4, 0);

        await PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) => throw new StreamTransportException("XREAD failed"),
            (_, _) => { },
            cts.Token,
            channel.Reader);

        delays.Delays.Should().Equal(1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000, 30_000);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_Backoff_Schedule_Is_Capped_And_Has_No_Attempt_Limit()
    {
        var used = new List<int>();
        var current = 0;

        for (var i = 0; i < 12; i++)
        {
            current = PartitionWorker.NextBlockDelayMs(current);
            used.Add(current);
        }

        used.Should().Equal(
            1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000, 30_000, 30_000, 30_000, 30_000);

        PartitionWorker.BlockInitialDelayMs.Should().Be(1_000);
        PartitionWorker.BlockMaxDelayMs.Should().Be(30_000);

        // A huge current must clamp rather than double into a negative delay.
        PartitionWorker.NextBlockDelayMs(int.MaxValue).Should().Be(PartitionWorker.BlockMaxDelayMs);
        PartitionWorker.NextBlockDelayMs(-1).Should().Be(PartitionWorker.BlockInitialDelayMs);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_Block_Log_Is_Rate_Limited_Rather_Than_One_Line_Per_Attempt()
    {
        var log = new CapturingLog();
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = ErrorHandling.Context(channel.Writer, log);
        using var cts = new CancellationTokenSource();

        const int attempts = 200;
        var delays = new ScriptedDelay(cancelAfter: attempts, cts);
        PartitionWorker.BlockDelay = delays.DelayAsync;

        ErrorHandling.Queue(channel, 5, 0);

        var sw = Stopwatch.StartNew();

        await PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) => throw new MandatoryDownstreamDownException("still down"),
            (_, _) => { },
            cts.Token,
            channel.Reader);

        sw.Stop();

        delays.Count.Should().Be(attempts, "the retry loop really did run that many times");

        // The documented budget: the first failure, plus one line per BlockLogIntervalMs of wall
        // clock, plus one for the partial interval this run ends in. With the delay substituted the
        // 200 attempts take milliseconds, so in practice this is the single opening Error line —
        // the point being that it is emphatically not 200.
        var allowed = 2 + (int)(sw.Elapsed.TotalMilliseconds / PartitionWorker.BlockLogIntervalMs);

        var errors = log.At(LogLevel.Error);
        errors.Should().NotBeEmpty("the first failure is always logged, with the exception");
        errors.Count.Should().BeLessThanOrEqualTo(allowed);
        errors.Count.Should().BeLessThan(attempts / 10, "a Redis outage must not emit a line per retry");

        PartitionWorker.BlockLogIntervalMs.Should().Be(30_000);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Cancellation_During_The_Backoff_Returns_Promptly()
    {
        var log = new CapturingLog();
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = ErrorHandling.Context(channel.Writer, log);
        using var cts = new CancellationTokenSource();

        // Deliberately NOT substituted: this test exists to prove that a real one-second
        // Task.Delay is abandoned on the token rather than waited out.
        PartitionWorker.BlockDelay = null;

        ErrorHandling.Queue(channel, 6, 0);

        var failed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var recorded = new List<StreamId>();

        var process = Task.Run(() => PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) =>
            {
                failed.TrySetResult();
                throw new MandatoryDownstreamDownException("down");
            },
            (_, id) => recorded.Add(id),
            cts.Token,
            channel.Reader));

        await failed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var sw = Stopwatch.StartNew();
        await cts.CancelAsync();
        await process.WaitAsync(TimeSpan.FromSeconds(5));
        sw.Stop();

        sw.Elapsed.TotalMilliseconds.Should().BeLessThan(
            PartitionWorker.BlockInitialDelayMs,
            "shutdown must not wait out the current backoff delay");

        recorded.Should().BeEmpty("nothing advanced, so the batch redelivers on restart");
        log.At(LogLevel.Information).Should().Contain(m => m.Contains("shutting down while"));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_Recovered_Block_Processes_The_Same_Batch_And_Advances()
    {
        var log = new CapturingLog();
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = ErrorHandling.Context(channel.Writer, log);

        var delays = new ScriptedDelay();
        PartitionWorker.BlockDelay = delays.DelayAsync;

        var id = ErrorHandling.Queue(channel, 7, 0);
        channel.Writer.Complete();

        var calls = 0;
        var recorded = new List<StreamId>();

        await PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) =>
            {
                if (Interlocked.Increment(ref calls) <= 3)
                {
                    throw new MandatoryDownstreamDownException("still down");
                }

                return default;
            },
            (_, recordedId) => recorded.Add(recordedId),
            CancellationToken.None,
            channel.Reader);

        calls.Should().Be(4, "three failures then the attempt that succeeds");
        delays.Delays.Should().Equal(1_000, 2_000, 4_000);
        // The position advances only once the batch finally succeeds.
        recorded.Should().Equal(id);
        log.At(LogLevel.Information).Should().Contain(m => m.Contains("recovered after 4 attempts"));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_Different_Exception_During_A_Block_Hands_Back_To_The_Error_Policy()
    {
        var log = new CapturingLog();
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = ErrorHandling.Context(channel.Writer, log);

        var delays = new ScriptedDelay();
        PartitionWorker.BlockDelay = delays.DelayAsync;

        var id = ErrorHandling.Queue(channel, 8, 0);
        channel.Writer.Complete();

        var calls = 0;
        var recorded = new List<StreamId>();

        await PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) => Interlocked.Increment(ref calls) == 1
                ? throw new MandatoryDownstreamDownException("down")
                : throw new InvalidOperationException("a different failure"),
            (_, recordedId) => recorded.Add(recordedId),
            CancellationToken.None,
            channel.Reader);

        calls.Should().Be(2, "the block ends as soon as the failure is no longer un-skippable");
        // BestEffort took over and advanced past the batch.
        recorded.Should().Equal(id);
        log.At(LogLevel.Error).Should().HaveCount(2)
            .And.Contain(m => m.Contains("BLOCKED"))
            .And.Contain(m => m.Contains("BestEffort"));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_Exception_Class_Beats_The_Error_Policy()
    {
        var log = new CapturingLog();
        var channel = PartitionWorker.CreateChannel(4);

        // ErrorPolicy.Fail would fault the host for an ordinary exception; a DontIgnoreException is
        // a different mechanism and still blocks.
        var ctx = ErrorHandling.Context(channel.Writer, log, ErrorPolicy.Fail);
        using var cts = new CancellationTokenSource();

        var delays = new ScriptedDelay(cancelAfter: 4, cts);
        PartitionWorker.BlockDelay = delays.DelayAsync;

        ErrorHandling.Queue(channel, 9, 0);

        var recorded = new List<StreamId>();

        var act = () => PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) => throw new MandatoryDownstreamDownException("down"),
            (_, id) => recorded.Add(id),
            cts.Token,
            channel.Reader);

        await act.Should().NotThrowAsync("the host is not faulted by a DontIgnoreException");

        delays.Count.Should().Be(4, "it blocked and retried rather than faulting");
        recorded.Should().BeEmpty();
        log.At(LogLevel.Critical).Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_Base_Class_Is_Public_And_Abstract_So_Services_Can_Derive_Their_Own()
    {
        var type = typeof(DontIgnoreException);

        type.IsAbstract.Should().BeTrue();
        type.IsPublic.Should().BeTrue();
        typeof(MandatoryDownstreamDownException).Assembly.Should().NotBeSameAs(
            type.Assembly,
            "the subclass under test is declared outside the library, as a service's would be");

        typeof(StreamConfigurationException).Should().BeAssignableTo<DontIgnoreException>();
        typeof(StreamTransportException).Should().BeAssignableTo<DontIgnoreException>();
    }
}

/// <summary>The two non-default <see cref="ErrorPolicy"/> escapes.</summary>
public class ErrorPolicyTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task StopPartition_Does_Not_Advance_And_Stops_Only_That_Partition()
    {
        var stoppingLog = new CapturingLog();
        var stopping = PartitionWorker.CreateChannel(4);
        var stoppingCtx = ErrorHandling.Context(stopping.Writer, stoppingLog, ErrorPolicy.StopPartition);

        var healthyLog = new CapturingLog();
        var healthy = PartitionWorker.CreateChannel(4);
        var healthyCtx = ErrorHandling.Context(healthy.Writer, healthyLog, partition: 1);

        var failing = ErrorHandling.Queue(stopping, 10, 0);
        ErrorHandling.Queue(stopping, 10, 1);
        stopping.Writer.Complete();

        var a = ErrorHandling.Queue(healthy, 20, 0);
        var b = ErrorHandling.Queue(healthy, 20, 1);
        healthy.Writer.Complete();

        var stoppingCalls = 0;
        var stoppingPositions = new List<StreamId>();
        var healthyPositions = new List<StreamId>();

        var stopped = PartitionWorker.ProcessLoopAsync(
            stoppingCtx,
            (_, _) =>
            {
                Interlocked.Increment(ref stoppingCalls);
                throw new InvalidOperationException("boom");
            },
            (_, id) => stoppingPositions.Add(id),
            CancellationToken.None,
            stopping.Reader);

        var kept = PartitionWorker.ProcessLoopAsync(
            healthyCtx,
            (_, _) => default,
            (_, id) => healthyPositions.Add(id),
            CancellationToken.None,
            healthy.Reader);

        await Task.WhenAll(stopped, kept).WaitAsync(TimeSpan.FromSeconds(10));

        stoppingCalls.Should().Be(1, "the partition stands down after the first failure — it does not retry");
        stoppingPositions.Should().BeEmpty("StopPartition does not advance");
        stopping.Reader.Count.Should().Be(0, "the queued batch was drained back to the pool, not processed");

        // Another partition is entirely unaffected by the one that stopped.
        healthyPositions.Should().Equal(a, b);

        stoppingLog.At(LogLevel.Error).Should().ContainSingle()
            .Which.Should().Contain("StopPartition").And.Contain("NOT advanced").And.Contain(failing.Format());
        healthyLog.At(LogLevel.Error).Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Fail_Faults_The_Host_With_The_Original_Exception_And_Does_Not_Advance()
    {
        var log = new CapturingLog();
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = ErrorHandling.Context(channel.Writer, log, ErrorPolicy.Fail);

        ErrorHandling.Queue(channel, 11, 0);
        channel.Writer.Complete();

        var recorded = new List<StreamId>();

        var act = () => PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) => throw new InvalidOperationException("boom"),
            (_, id) => recorded.Add(id),
            CancellationToken.None,
            channel.Reader);

        (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("boom");

        recorded.Should().BeEmpty("a faulting host must not checkpoint the batch that killed it");
        log.At(LogLevel.Critical).Should().ContainSingle()
            .Which.Should().Contain("ErrorPolicy.Fail");
    }
}

/// <summary>
/// Startup validation is the exception to the exception: a <see cref="StreamConfigurationException"/>
/// raised there fails fast instead of entering the blocking-retry loop.
/// </summary>
public class StartupFailFastTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_Startup_Configuration_Error_Fails_Fast_Rather_Than_Retrying()
    {
        var log = new CapturingLog();
        var channel = PartitionWorker.CreateChannel(1);
        var ctx = ErrorHandling.Context(channel.Writer, log);

        var delays = new ScriptedDelay();
        PartitionWorker.BlockDelay = delays.DelayAsync;

        var handlerCalls = 0;

        var act = () => PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return default;
            },
            (_, _) => { },
            CancellationToken.None,
            reader: null);

        var thrown = await act.Should().ThrowAsync<StreamConfigurationException>();

        // It IS a DontIgnoreException — the point is that the startup path does not retry it anyway.
        thrown.Which.Should().BeAssignableTo<DontIgnoreException>();

        delays.Delays.Should().BeEmpty("no backoff, no retry: no amount of waiting fixes a config file");
        handlerCalls.Should().Be(0);
    }

    [Theory]
    [InlineData(PersistMode.SyncBatch)]
    [InlineData(PersistMode.SyncMessage)]
    [Trait("TestType", "UnitTest")]
    public async Task A_Sync_Persist_Mode_Without_A_Flush_Fails_Fast_Too(PersistMode persist)
    {
        var log = new CapturingLog();
        var channel = PartitionWorker.CreateChannel(1);
        var ctx = ErrorHandling.Context(channel.Writer, log);

        var delays = new ScriptedDelay();
        PartitionWorker.BlockDelay = delays.DelayAsync;

        var act = () => PartitionWorker.ProcessLoopAsync(
            ctx, (_, _) => default, (_, _) => { }, CancellationToken.None, channel.Reader, persist, flush: null);

        (await act.Should().ThrowAsync<StreamConfigurationException>())
            .WithMessage("*no synchronous position flush*");

        delays.Delays.Should().BeEmpty();
    }
}
