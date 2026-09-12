using System.Buffers;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Channels;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Errors;
using RedisEvents.Wire;
using StackExchange.Redis;
using StreamTypeFilter = RedisEvents.Consumer.TypeFilter;

namespace RedisEvents.UnitTests;

/// <summary>
/// Consumer-pipeline unit tests (P1-24). Everything here drives the real read and process loops
/// through the fetch-delegate seam — a scripted <c>Func&lt;CancellationToken, ValueTask&lt;StreamEntryBatch&gt;&gt;</c>
/// — so there is no Redis, no <see cref="IDatabase"/> mock and no Docker anywhere in the file.
/// </summary>
public static class Pipeline
{
    /// <summary>Builds a raw entry exactly as <c>XREAD</c> would have handed it back.</summary>
    internal static StreamEntry Entry(long ms, long seq, string type, string body = "x")
        => new(new StreamId(ms, seq).Format(), EntryCodec.Encode(Encoding.UTF8.GetBytes(body), type));

    internal static StreamEntryBatch Batch(params StreamEntry[] entries) => new(entries);

    /// <summary>A context wired to a channel writer; <c>Db</c> is never touched by the loops.</summary>
    internal static PartitionContext Context(
        ChannelWriter<StreamBatch>? writer,
        string[]? filter = null,
        ErrorPolicy onError = ErrorPolicy.BestEffort,
        int partition = 0,
        int batchSize = 100)
        => new(
            Db: null!,
            StreamKey: (RedisKey)$"s:t:{partition}",
            Partition: partition,
            Topic: "t",
            Consumer: "c",
            BatchSize: batchSize,
            Filter: filter,
            OnError: onError,
            Writer: writer,
            Log: NullLogger.Instance);

    /// <summary>
    /// A fetch seam that hands back a scripted sequence and then parks until the token is cancelled.
    /// </summary>
    internal static Func<CancellationToken, ValueTask<StreamEntryBatch>> Script(
        IReadOnlyList<StreamEntryBatch> script,
        Action? onFetch = null)
    {
        var at = 0;

        return async ct =>
        {
            ct.ThrowIfCancellationRequested();
            onFetch?.Invoke();

            var index = Interlocked.Increment(ref at) - 1;

            if (index < script.Count)
            {
                return script[index];
            }

            // Exhausted: behave like an idle stream that never gets another entry.
            await Task.Delay(Timeout.Infinite, ct).ConfigureAwait(false);
            return StreamEntryBatch.Empty;
        };
    }

    /// <summary>Waits for a condition, or fails the test rather than hanging the run.</summary>
    internal static async Task WaitFor(Func<bool> condition, string because, int timeoutMs = 5_000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;

        while (!condition())
        {
            if (Environment.TickCount64 > deadline)
            {
                throw new TimeoutException($"Timed out after {timeoutMs} ms waiting: {because}");
            }

            await Task.Delay(5).ConfigureAwait(false);
        }
    }
}

/// <summary>The idle backoff schedule used by <c>ReadMode.Poll</c>.</summary>
public class BackoffTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Backoff_Walks_The_Exact_Documented_Sequence()
    {
        // The delay actually used after each consecutive empty read, starting from "no empty read yet".
        var used = new List<int>();
        var current = 0;

        for (var i = 0; i < 10; i++)
        {
            used.Add(current);
            current = Backoff.Next(current, 50);
        }

        used.Should().Equal(0, 1, 2, 4, 8, 16, 32, 50, 50, 50);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Backoff_Doubles_Cleanly_When_The_Cap_Is_A_Power_Of_Two()
    {
        var used = new List<int>();
        var current = 0;

        for (var i = 0; i < 9; i++)
        {
            used.Add(current);
            current = Backoff.Next(current, 64);
        }

        used.Should().Equal(0, 1, 2, 4, 8, 16, 32, 64, 64);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(7)]
    [InlineData(50)]
    [InlineData(1_000)]
    [InlineData(int.MaxValue)]
    [Trait("TestType", "UnitTest")]
    public void Backoff_Never_Exceeds_MaxIdleDelayMs(int max)
    {
        var current = 0;

        for (var i = 0; i < 200; i++)
        {
            current = Backoff.Next(current, max);
            current.Should().BeGreaterThan(0).And.BeLessThanOrEqualTo(max);
        }

        // And it settles on the cap rather than oscillating below it.
        current.Should().Be(max);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Backoff_Resets_To_Zero_On_A_NonEmpty_Read()
    {
        // Walk up the ladder...
        var current = 0;
        for (var i = 0; i < 6; i++)
        {
            current = Backoff.Next(current, 50);
        }

        current.Should().Be(32);

        // ...then a non-empty read resets the caller's state, and the ladder starts from the floor.
        current = 0;
        Backoff.Next(current, 50).Should().Be(1);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    [Trait("TestType", "UnitTest")]
    public void Backoff_With_A_NonPositive_Cap_Is_A_Pure_Yield_Loop(int max)
    {
        Backoff.Next(0, max).Should().Be(0);
        Backoff.Next(32, max).Should().Be(0);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Backoff_Does_Not_Overflow_On_An_Absurd_Current()
    {
        Backoff.Next(int.MaxValue, 50).Should().Be(50);
        Backoff.Next(int.MaxValue, int.MaxValue).Should().Be(int.MaxValue);
    }
}

/// <summary>The consumer's type filter: the linear scan and the frozen-set path must agree.</summary>
public class TypeFilterTests
{
    private static readonly string[] Universe =
    [
        "A.One", "A.Two", "A.Three", "A.Four", "A.Five", "A.Six", "A.Seven", "A.Eight", "A.Nine",
    ];

    private static ReadOnlySpan<NameValueEntry> Values(string type)
        => EntryCodec.Encode(Encoding.UTF8.GetBytes("body"), type);

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]  // last size on the linear-scan path
    [InlineData(5)]  // first size on the FrozenSet path
    [InlineData(6)]
    [InlineData(9)]
    [Trait("TestType", "UnitTest")]
    public void Filter_Paths_Agree_With_A_Naive_Oracle(int size)
    {
        var configured = Universe.Take(size).ToArray();
        var oracle = new HashSet<string>(configured, StringComparer.Ordinal);
        var filter = StreamTypeFilter.Create(configured);

        size.Should().BeLessThanOrEqualTo(StreamTypeFilter.LinearScanLimit * 3);
        filter.KeepsEverything.Should().BeFalse();

        foreach (var candidate in Universe.Concat(["A.On", "A.Ones", "", "a.one"]))
        {
            filter.Matches(Values(candidate))
                .Should().Be(oracle.Contains(candidate), "type '{0}' with a filter of {1}", candidate, size);
        }
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Filter_Boundary_Is_Four_Types()
    {
        // Not an implementation detail worth hiding: the two paths are only equivalent because both
        // are exercised, so the boundary itself is asserted.
        StreamTypeFilter.LinearScanLimit.Should().Be(4);

        var linear = StreamTypeFilter.Create(Universe.Take(4).ToArray());
        var frozen = StreamTypeFilter.Create(Universe.Take(5).ToArray());

        linear.Matches(Values("A.Four")).Should().BeTrue();
        frozen.Matches(Values("A.Four")).Should().BeTrue();
        linear.Matches(Values("A.Five")).Should().BeFalse();
        frozen.Matches(Values("A.Five")).Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_Null_Filter_Keeps_Everything() => KeepsEverything(null);

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_Empty_Filter_Keeps_Everything() => KeepsEverything([]);

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_Filter_Of_Blanks_Keeps_Everything() => KeepsEverything([string.Empty, "  ", "\t"]);

    private static void KeepsEverything(string[]? configured)
    {
        var filter = StreamTypeFilter.Create(configured);

        filter.KeepsEverything.Should().BeTrue();
        filter.Matches(Values("anything")).Should().BeTrue();
        filter.Matches([]).Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Filter_Collapses_Duplicates_Onto_The_Linear_Path()
    {
        // Six configured entries, three distinct — still the linear path, and still correct.
        var filter = StreamTypeFilter.Create(["A.One", "A.One", "A.Two", "A.Two", "A.Three", "  "]);

        filter.Matches(Values("A.One")).Should().BeTrue();
        filter.Matches(Values("A.Three")).Should().BeTrue();
        filter.Matches(Values("A.Four")).Should().BeFalse();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Filter_Drops_An_Entry_With_No_Type_Field()
    {
        var filter = StreamTypeFilter.Create(["A.One"]);

        NameValueEntry[] noType = [new("b", "body"), new("k", "key")];

        filter.Matches(noType).Should().BeFalse();
    }
}

/// <summary>The reader-to-processor pipeline, driven end to end through the fetch seam.</summary>
public class ConsumerPipelineTests
{
    private static readonly string[] KeepOnlyKeep = ["Keep"];

    // ------------------------------------------------------------------ happy path

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Pipeline_Delivers_Every_Message_In_Order_And_Advances_The_Position()
    {
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = Pipeline.Context(channel.Writer);
        using var cts = new CancellationTokenSource();

        var fetch = Pipeline.Script(
        [
            Pipeline.Batch(Pipeline.Entry(1, 0, "Keep", "a"), Pipeline.Entry(1, 1, "Keep", "b")),
            StreamEntryBatch.Empty,
            Pipeline.Batch(Pipeline.Entry(2, 0, "Keep", "c")),
        ]);

        var seen = new List<string>();
        var recorded = new List<(int Partition, StreamId Id)>();

        var read = PartitionWorker.ReadLoopAsync(ctx, StreamId.Min, fetch, cts.Token);
        var process = Task.Run(() => PartitionWorker.ProcessLoopAsync(
            ctx,
            (batch, _) =>
            {
                foreach (var msg in batch.Span)
                {
                    seen.Add(Encoding.UTF8.GetString(msg.Body.Span));
                }

                return default;
            },
            (partition, id) => recorded.Add((partition, id)),
            cts.Token,
            channel.Reader));

        await Pipeline.WaitFor(() => seen.Count == 3, "all three messages handled");

        await cts.CancelAsync();
        await read;
        await process;

        seen.Should().Equal("a", "b", "c");
        recorded.Should().Equal((0, new StreamId(1, 1)), (0, new StreamId(2, 0)));
    }

    // ------------------------------------------------------------------ filtering

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_Batch_Filtered_To_Zero_Still_Advances_The_Position()
    {
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = Pipeline.Context(channel.Writer, filter: KeepOnlyKeep);
        using var cts = new CancellationTokenSource();

        var fetch = Pipeline.Script(
        [
            // Nothing in this read survives the filter — the classic "1 % of a busy topic" case.
            Pipeline.Batch(Pipeline.Entry(5, 0, "Drop"), Pipeline.Entry(5, 1, "Drop"), Pipeline.Entry(5, 2, "Drop")),
        ]);

        var handlerCalls = 0;
        var recorded = new List<StreamId>();

        var read = PartitionWorker.ReadLoopAsync(ctx, StreamId.Min, fetch, cts.Token);
        var process = Task.Run(() => PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return default;
            },
            (_, id) => recorded.Add(id),
            cts.Token,
            channel.Reader));

        await Pipeline.WaitFor(() => recorded.Count == 1, "the filtered-to-zero batch checkpointed");

        await cts.CancelAsync();
        await read;
        await process;

        handlerCalls.Should().Be(0, "there was nothing to hand a handler");
        recorded.Should().Equal([new StreamId(5, 2)], "the position advances past every entry READ, not every entry kept");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_Partially_Filtered_Batch_Advances_Past_The_Dropped_Tail()
    {
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = Pipeline.Context(channel.Writer, filter: KeepOnlyKeep);
        using var cts = new CancellationTokenSource();

        var fetch = Pipeline.Script(
        [
            Pipeline.Batch(
                Pipeline.Entry(7, 0, "Drop"),
                Pipeline.Entry(7, 1, "Keep", "kept"),
                Pipeline.Entry(7, 2, "Drop"),
                Pipeline.Entry(7, 3, "Drop")),
        ]);

        var seen = new List<string>();
        var recorded = new List<StreamId>();

        var read = PartitionWorker.ReadLoopAsync(ctx, StreamId.Min, fetch, cts.Token);
        var process = Task.Run(() => PartitionWorker.ProcessLoopAsync(
            ctx,
            (batch, _) =>
            {
                foreach (var msg in batch.Span)
                {
                    seen.Add(Encoding.UTF8.GetString(msg.Body.Span));
                }

                return default;
            },
            (_, id) => recorded.Add(id),
            cts.Token,
            channel.Reader));

        await Pipeline.WaitFor(() => recorded.Count == 1, "the batch was handled");

        await cts.CancelAsync();
        await read;
        await process;

        seen.Should().Equal("kept");
        recorded.Should().Equal(new StreamId(7, 3));
    }

    // ------------------------------------------------------------------ backpressure

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [Trait("TestType", "UnitTest")]
    public async Task A_Full_Channel_Genuinely_Blocks_The_Reader(int capacity)
    {
        var channel = PartitionWorker.CreateChannel(capacity);
        var ctx = Pipeline.Context(channel.Writer);
        using var cts = new CancellationTokenSource();

        var fetches = 0;
        var entry = 0;

        // An endlessly hot stream: every fetch returns a full batch immediately, so the ONLY thing
        // that can ever stop this reader is the channel write in front of the next fetch.
        async ValueTask<StreamEntryBatch> Fetch(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            Interlocked.Increment(ref fetches);
            await Task.Yield();
            return Pipeline.Batch(Pipeline.Entry(1, Interlocked.Increment(ref entry), "Keep"));
        }

        // No processor at all: nothing drains the channel.
        var read = PartitionWorker.ReadLoopAsync(ctx, StreamId.Min, Fetch, cts.Token);

        // capacity batches sit in the channel, one more is parked inside WriteAsync.
        var expected = capacity + 1;
        await Pipeline.WaitFor(() => Volatile.Read(ref fetches) >= expected, $"{expected} fetches issued");

        await Task.Delay(150);
        Volatile.Read(ref fetches).Should().Be(expected, "the reader must be blocked on the channel write, not spinning");

        // Draining one batch releases exactly one more round trip.
        (await channel.Reader.ReadAsync(cts.Token)).Return();

        await Pipeline.WaitFor(() => Volatile.Read(ref fetches) >= expected + 1, "the freed slot let one more fetch through");
        await Task.Delay(150);
        Volatile.Read(ref fetches).Should().Be(expected + 1);

        await cts.CancelAsync();
        await read;

        // Give the pool back what the test consumed itself.
        while (channel.Reader.TryRead(out var abandoned))
        {
            abandoned.Return();
        }
    }

    // ------------------------------------------------------------------ pooling

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Every_Pooled_Array_Is_Returned_Exactly_Once()
    {
        using var pool = CountingArrayPool.Install();

        var channel = PartitionWorker.CreateChannel(2);
        var ctx = Pipeline.Context(channel.Writer);
        using var cts = new CancellationTokenSource();

        var script = new List<StreamEntryBatch>();
        for (var i = 0; i < 8; i++)
        {
            script.Add(Pipeline.Batch(Pipeline.Entry(1, i, "Keep")));
        }

        var read = PartitionWorker.ReadLoopAsync(ctx, StreamId.Min, Pipeline.Script(script), cts.Token);
        var process = Task.Run(() => PartitionWorker.ProcessLoopAsync(
            ctx,
            (batch, _) =>
            {
                // Asserts, inside the handler, that this array has NOT been returned yet.
                pool.Observe(batch);
                return default;
            },
            (_, _) => { },
            cts.Token,
            channel.Reader));

        await Pipeline.WaitFor(() => pool.Observed == script.Count, "every batch reached the handler");

        await cts.CancelAsync();
        await read;
        await process;

        pool.Observed.Should().Be(8);

        // Exactly once: never zero (a leaked array pins the Redis read buffers it aliases) and never
        // twice (two future rentals would share one array and corrupt each other).
        pool.ReturnsForObserved().Should().AllSatisfy(count => count.Should().Be(1));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_Double_Return_Is_Caught()
    {
        var items = StreamBatch.Rent(4);
        var batch = new StreamBatch(items, 0, StreamId.Min);

        batch.Return();

#if DEBUG
        // DEBUG tracks rentals by reference, so the second return throws at the point of the mistake
        // rather than corrupting an unrelated batch later.
        var again = () => batch.Return();
        again.Should().Throw<InvalidOperationException>()
            .WithMessage("*not currently rented*");
#endif
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Returning_A_Default_Batch_Is_Caught()
    {
        var batch = default(StreamBatch);

        var act = () => batch.Return();

        act.Should().Throw<InvalidOperationException>().WithMessage("*default StreamBatch*");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Batches_Abandoned_In_The_Channel_Are_Returned_To_The_Pool()
    {
        using var pool = CountingArrayPool.Install();

        var channel = PartitionWorker.CreateChannel(4);
        var ctx = Pipeline.Context(channel.Writer, onError: ErrorPolicy.StopPartition);

        // Four batches queued with arrays this test knows the identity of, so the accounting is
        // exact rather than inferred.
        var arrays = new List<StreamMsg[]>();

        for (var i = 0; i < 4; i++)
        {
            var entry = Pipeline.Entry(1, i, "Keep");
            var items = StreamBatch.Rent(1);
            items[0] = EntryCodec.Decode(in entry, partition: 0);
            arrays.Add(items);

            channel.Writer.TryWrite(new StreamBatch(items, 1, items[0].Id)).Should().BeTrue();
        }

        channel.Writer.Complete();

        var handled = 0;

        // The first batch stops the partition under StopPartition; the other three never reach a
        // handler at all — and must still go back to the pool.
        await PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) =>
            {
                Interlocked.Increment(ref handled);
                throw new InvalidOperationException("boom");
            },
            (_, _) => { },
            CancellationToken.None,
            channel.Reader);

        handled.Should().Be(1, "StopPartition stands the partition down after the first failure");
        channel.Reader.Count.Should().Be(0, "the drain in the finally emptied the channel");
        arrays.Should().AllSatisfy(array => pool.ReturnsFor(array).Should().Be(1));
    }

    // ------------------------------------------------------------------ shutdown

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Shutdown_Drains_The_Batches_Already_In_Flight()
    {
        var channel = PartitionWorker.CreateChannel(8);
        var ctx = Pipeline.Context(channel.Writer);
        using var cts = new CancellationTokenSource();

        var script = new List<StreamEntryBatch>();
        for (var i = 0; i < 3; i++)
        {
            script.Add(Pipeline.Batch(Pipeline.Entry(9, i, "Keep")));
        }

        var read = PartitionWorker.ReadLoopAsync(ctx, StreamId.Min, Pipeline.Script(script), cts.Token);

        await Pipeline.WaitFor(() => channel.Reader.Count == 3, "three batches are in flight");

        // Shut down with the channel full of unprocessed work.
        await cts.CancelAsync();
        await read;
        channel.Writer.TryComplete().Should().BeFalse("the read loop already completed the writer on shutdown");
        channel.Reader.Count.Should().Be(3, "completing the writer does not discard queued batches");

        var handled = 0;
        var recorded = new List<StreamId>();

        // The processor is started with an ALREADY-cancelled token: the drain must still happen,
        // because it waits on CancellationToken.None and ends when the writer completes.
        await PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) =>
            {
                Interlocked.Increment(ref handled);
                return default;
            },
            (_, id) => recorded.Add(id),
            cts.Token,
            channel.Reader);

        handled.Should().Be(3, "in-flight batches finish rather than being dropped");
        recorded.Should().Equal(new StreamId(9, 0), new StreamId(9, 1), new StreamId(9, 2));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Shutdown_Is_Bounded_By_A_Timeout_When_A_Handler_Will_Not_Return()
    {
        var channel = PartitionWorker.CreateChannel(4);
        var ctx = Pipeline.Context(channel.Writer);
        using var cts = new CancellationTokenSource();

        var read = PartitionWorker.ReadLoopAsync(
            ctx,
            StreamId.Min,
            Pipeline.Script([Pipeline.Batch(Pipeline.Entry(11, 0, "Keep"))]),
            cts.Token);

        var stuck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // A handler that ignores the token entirely — the case the drain budget exists for.
        var process = Task.Run(() => PartitionWorker.ProcessLoopAsync(
            ctx,
            async (_, _) =>
            {
                entered.TrySetResult();
                await stuck.Task.ConfigureAwait(false);
            },
            (_, _) => { },
            cts.Token,
            channel.Reader));

        await entered.Task;
        await cts.CancelAsync();
        await read;

        // The drain budget, exactly as StreamConsumerHost.DrainAsync spends it.
        var budget = Task.Delay(250);
        var winner = await Task.WhenAny(process, budget);

        winner.Should().BeSameAs(budget, "a wedged handler must not extend shutdown past its budget");
        process.IsCompleted.Should().BeFalse();

        // ...and once the handler does come back, the loop finishes on its own.
        stuck.SetResult();
        await process.WaitAsync(TimeSpan.FromSeconds(5));
    }

    // ------------------------------------------------------------------ guards

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_Processor_Started_Without_A_Channel_Reader_Refuses_To_Run()
    {
        var channel = PartitionWorker.CreateChannel(1);
        var ctx = Pipeline.Context(channel.Writer);

        var act = () => PartitionWorker.ProcessLoopAsync(
            ctx, (_, _) => default, (_, _) => { }, CancellationToken.None, reader: null);

        (await act.Should().ThrowAsync<StreamConfigurationException>())
            .WithMessage("*channel reader*");
    }

    [Theory]
    [InlineData(PersistMode.SyncBatch)]
    [InlineData(PersistMode.SyncMessage)]
    [Trait("TestType", "UnitTest")]
    public async Task A_Sync_Persist_Mode_Without_A_Flush_Refuses_To_Run(PersistMode persist)
    {
        var channel = PartitionWorker.CreateChannel(1);
        var ctx = Pipeline.Context(channel.Writer);

        var act = () => PartitionWorker.ProcessLoopAsync(
            ctx, (_, _) => default, (_, _) => { }, CancellationToken.None, channel.Reader, persist, flush: null);

        (await act.Should().ThrowAsync<StreamConfigurationException>())
            .WithMessage("*no synchronous position flush*");
    }

    /// <summary>
    /// A channel-backed read loop with no writer still refuses to run — and R-18: it must no longer
    /// say inline mode is unimplemented. Inline mode <em>is</em> implemented
    /// (<c>PartitionWorker.RunInlineAsync</c>) and the host dispatches to it before this loop is
    /// reached, so the old message sent whoever hit this straight to the wrong fix: turning
    /// <c>Backpressure.Enabled</c> back on, which is not what a null writer here means.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_Reader_Without_A_Channel_Writer_Refuses_To_Run()
    {
        var ctx = Pipeline.Context(writer: null);

        var act = () => PartitionWorker.ReadLoopAsync(
            ctx, StreamId.Min, _ => new ValueTask<StreamEntryBatch>(StreamEntryBatch.Empty), CancellationToken.None);

        var thrown = (await act.Should().ThrowAsync<NotSupportedException>()).Which;

        thrown.Message.Should().NotContain(
            "not implemented yet",
            "inline mode is implemented; the message must not send the reader to re-enable backpressure");
        thrown.Message.Should().Contain("RunInlineAsync", "the message must name where inline mode actually lives");
    }
}

/// <summary>
/// The read-only invariant on <see cref="StreamReaderConnection"/>, asserted structurally: the type
/// must not merely avoid writing, it must offer no way to write.
/// </summary>
public class ReaderConnectionSurfaceTests
{
    private static readonly Type[] Forbidden =
    [
        typeof(IDatabase), typeof(IDatabaseAsync), typeof(IConnectionMultiplexer), typeof(ConnectionMultiplexer),
        typeof(IServer), typeof(ISubscriber), typeof(IBatch), typeof(ITransaction),
    ];

    private static bool IsVisibleOutside(MethodBase? method)
        => method is not null && (method.IsPublic || method.IsFamily || method.IsFamilyOrAssembly || method.IsAssembly);

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_Reader_Connection_Hands_Out_No_Way_To_Issue_A_Command()
    {
        var type = typeof(StreamReaderConnection);
        const BindingFlags All = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;

        var leaks = new List<string>();

        foreach (var method in type.GetMethods(All))
        {
            if (!IsVisibleOutside(method))
            {
                continue;
            }

            if (Forbidden.Contains(method.ReturnType) || method.GetParameters().Any(p => Forbidden.Contains(p.ParameterType)))
            {
                leaks.Add(method.Name);
            }
        }

        foreach (var property in type.GetProperties(All))
        {
            if (IsVisibleOutside(property.GetMethod) && Forbidden.Contains(property.PropertyType))
            {
                leaks.Add(property.Name);
            }
        }

        foreach (var field in type.GetFields(All).Where(f => f.IsPublic || f.IsFamily || f.IsAssembly))
        {
            if (Forbidden.Contains(field.FieldType))
            {
                leaks.Add(field.Name);
            }
        }

        leaks.Should().BeEmpty(
            "a write moved onto the blocking reader connection would sit behind an XREAD parked for BlockMs, " +
            "and MULTI/EXEC cannot span two multiplexers, so the outbox would break");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_Reader_Connection_Exposes_Exactly_One_Command_Method()
    {
        var methods = typeof(StreamReaderConnection)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(m => !m.IsSpecialName)
            .Select(m => m.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        methods.Should().BeEquivalentTo(["Create", "CreateAsync", "Dispose", "DisposeAsync", "ExecuteXReadAsync"]);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_Reader_Connection_Is_Not_A_Multiplexer()
    {
        // It must be impossible to register in DI as IConnectionMultiplexer and be reused for writes.
        typeof(IConnectionMultiplexer).IsAssignableFrom(typeof(StreamReaderConnection)).Should().BeFalse();
        typeof(IDatabase).IsAssignableFrom(typeof(StreamReaderConnection)).Should().BeFalse();
    }
}

/// <summary>
/// A counting <see cref="System.Buffers.ArrayPool{T}"/> shim installed over
/// <c>StreamBatch.Pool</c> for the duration of a test, so "every rented array is returned exactly
/// once" is an exact count rather than an inference from recycled memory.
/// </summary>
/// <remarks>
/// It never recycles: every <see cref="Rent"/> allocates. That is deliberate — an array that can be
/// handed out twice cannot have its returns counted unambiguously, and other test classes running in
/// parallel rent from this same static seam while it is installed.
/// </remarks>
internal sealed class CountingArrayPool : ArrayPool<StreamMsg>, IDisposable
{
    private readonly Dictionary<StreamMsg[], int> returns = new(ReferenceEqualityComparer.Instance as IEqualityComparer<StreamMsg[]>);
    private readonly List<StreamMsg[]> observed = [];
    private readonly ArrayPool<StreamMsg> previous;

    private CountingArrayPool(ArrayPool<StreamMsg> previous) => this.previous = previous;

    /// <summary>Installs the shim; disposing restores whatever was there before.</summary>
    internal static CountingArrayPool Install()
    {
        var shim = new CountingArrayPool(StreamBatch.Pool);
        StreamBatch.Pool = shim;
        return shim;
    }

    /// <summary>How many pooled arrays the handler was handed.</summary>
    internal int Observed
    {
        get
        {
            lock (this.returns)
            {
                return this.observed.Count;
            }
        }
    }

    /// <summary>The pooled array behind a handler's batch memory.</summary>
    internal static StreamMsg[] ArrayOf(ReadOnlyMemory<StreamMsg> batch)
    {
        MemoryMarshal.TryGetArray(batch, out var segment).Should().BeTrue("a batch is always backed by a pooled array");
        return segment.Array!;
    }

    public override StreamMsg[] Rent(int minimumLength)
    {
        var array = new StreamMsg[Math.Max(minimumLength, 1)];

        lock (this.returns)
        {
            this.returns[array] = 0;
        }

        return array;
    }

    public override void Return(StreamMsg[] array, bool clearArray = false)
    {
        lock (this.returns)
        {
            this.returns.TryGetValue(array, out var count);
            this.returns[array] = count + 1;
        }
    }

    /// <summary>Records the array behind a batch the handler is holding right now.</summary>
    internal void Observe(ReadOnlyMemory<StreamMsg> batch)
    {
        var array = ArrayOf(batch);

        lock (this.returns)
        {
            this.observed.Add(array);
            this.returns.TryGetValue(array, out var count);
            count.Should().Be(0, "the handler is still holding this array, so it cannot have been returned yet");
        }
    }

    /// <summary>How many times the array behind a batch has been returned to the pool.</summary>
    internal int ReturnsFor(StreamMsg[] array)
    {
        lock (this.returns)
        {
            return this.returns.TryGetValue(array, out var count) ? count : 0;
        }
    }

    /// <summary>The return count for every array the handler saw, in order.</summary>
    internal int[] ReturnsForObserved()
    {
        lock (this.returns)
        {
            return this.observed.Select(a => this.returns.TryGetValue(a, out var count) ? count : 0).ToArray();
        }
    }

    public void Dispose() => StreamBatch.Pool = this.previous;
}
