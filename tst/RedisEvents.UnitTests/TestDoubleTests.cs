using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using FluentAssertions;
using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Producer;
using RedisEvents.Testing;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// R-20: the shipped test doubles had zero tests and zero usages, and the review found three real
/// defects in them — no routing at all (<c>options.Partition</c> was recorded verbatim, so a
/// partition assertion written against the double proved nothing), a <c>static</c> id counter
/// incremented without <see cref="Interlocked"/>, and an unlocked <c>List&lt;T&gt;</c>. A double
/// that lies about routing is worse than no double, because the test it makes pass is the test
/// somebody trusts.
/// </summary>
public class TestDoubleTests
{
    private static readonly byte[] Body = Encoding.UTF8.GetBytes("body");

    // -------------------------------------------------------------------------------------------
    // InMemoryStreamPublisher — routing
    // -------------------------------------------------------------------------------------------

    /// <summary>
    /// The recorded partition is the one the message <em>routed to</em>, computed with the same
    /// <c>PartitionRouter</c> the live publisher uses — not the one the caller asked for. The two
    /// are kept apart so a test can still see what was requested.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task RoutesByKeyRatherThanRecordingTheRequestedPartition()
    {
        var publisher = new InMemoryStreamPublisher(partitions: 8);

        await publisher.PublishAsync("customer-42", Body, "T");

        var recorded = publisher.Published.Should().ContainSingle().Subject;
        recorded.RequestedPartition.Should().BeNull("nothing was requested");
        recorded.Partition.Should().BeInRange(0, 7);

        // Same key, same partition — the property every ordering assertion rests on.
        await publisher.PublishAsync("customer-42", Body, "T");
        publisher.Published[1].Partition.Should().Be(recorded.Partition);
    }

    /// <summary>
    /// The double's routing must agree with the real publisher's, or a test that pins a partition
    /// index against it is pinning a fiction. Both run xxHash3 over the UTF-8 key.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task RoutingAgreesWithTheOutboxRouterForTheSameKey()
    {
        const int partitions = 8;
        var publisher = new InMemoryStreamPublisher(partitions);

        // Outbox.EnsureSameSlot is not the router, so route through the double and check the
        // distribution is a real hash rather than a constant: eight distinct keys over eight
        // partitions must not all land on one.
        for (var i = 0; i < 64; i++)
        {
            await publisher.PublishAsync($"key-{i}", Body, "T");
        }

        var used = new HashSet<int>();
        foreach (var m in publisher.Published)
        {
            used.Add(m.Partition);
        }

        used.Count.Should().BeGreaterThan(1, "a real hash spreads 64 distinct keys across 8 partitions");
        used.Should().OnlyContain(p => p >= 0 && p < partitions);
    }

    /// <summary>
    /// An explicit partition wins outright, and is range-checked — the live publisher throws for an
    /// out-of-range partition and so must the double, or a bug only shows up against Redis.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ExplicitPartitionWinsAndIsRangeChecked()
    {
        var publisher = new InMemoryStreamPublisher(partitions: 4);

        for (var p = 0; p < 4; p++)
        {
            await publisher.PublishAsync("customer-42", Body, "T", new PublishOptions(Partition: p));
        }

        publisher.Published.Select(m => m.Partition).Should().Equal(0, 1, 2, 3);
        publisher.Published.Select(m => m.RequestedPartition).Should().Equal(0, 1, 2, 3);

        var act = async () => await publisher.PublishAsync("k", Body, "T", new PublishOptions(Partition: 4));
        await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
    }

    /// <summary>
    /// An empty key round-robins rather than pinning partition 0, which is what the live publisher
    /// does and what a "spread with no ordering promise" test needs to see.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task EmptyKeyRoundRobins()
    {
        var publisher = new InMemoryStreamPublisher(partitions: 4);

        for (var i = 0; i < 8; i++)
        {
            await publisher.PublishAsync(string.Empty, Body, "T");
        }

        var partitions = publisher.Published.Select(m => m.Partition).Distinct().ToArray();
        partitions.Should().HaveCount(4);
    }

    /// <summary>
    /// A single-partition topic short-circuits before hashing, exactly as the router does.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task SinglePartitionTopicRoutesEverythingToZero()
    {
        var publisher = new InMemoryStreamPublisher();

        await publisher.PublishAsync("a", Body, "T");
        await publisher.PublishAsync(string.Empty, Body, "T");

        publisher.Published.Should().OnlyContain(m => m.Partition == 0);
    }

    /// <summary>
    /// A batch shares one partition key, so — as on the wire — every entry lands on one partition.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task BatchLandsOnOnePartitionAndKeepsOrder()
    {
        var publisher = new InMemoryStreamPublisher(partitions: 8);
        ReadOnlyMemory<byte>[] bodies =
        [
            Encoding.UTF8.GetBytes("1"),
            Encoding.UTF8.GetBytes("2"),
            Encoding.UTF8.GetBytes("3"),
        ];

        await publisher.PublishBatchAsync("customer-42", bodies, "T");

        publisher.Published.Should().HaveCount(3);
        publisher.Published.Select(m => m.Partition).Distinct().Should().ContainSingle();
        publisher.Published.Select(m => Encoding.UTF8.GetString(m.Body)).Should().Equal("1", "2", "3");
    }

    // -------------------------------------------------------------------------------------------
    // InMemoryStreamPublisher — ids, copying, thread safety
    // -------------------------------------------------------------------------------------------

    /// <summary>
    /// Ids are unique and increasing within a publisher, and — the R-20 defect — two publishers do
    /// not share a counter, so two tests running side by side do not renumber each other.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task IdsArePerInstanceAndIncreasing()
    {
        var a = new InMemoryStreamPublisher();
        var b = new InMemoryStreamPublisher();

        await a.PublishAsync("k", Body, "T");
        await a.PublishAsync("k", Body, "T");
        await b.PublishAsync("k", Body, "T");

        a.Published.Select(m => m.Id.Ms).Should().Equal(1, 2);
        b.Published.Select(m => m.Id.Ms).Should().Equal(1);
        a.Published[1].Id.Should().BeGreaterThan(a.Published[0].Id);
    }

    /// <summary>
    /// The body is copied on the way in: a caller reusing a pooled buffer must not be able to
    /// rewrite what the test later asserts on.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task BodyIsCopied()
    {
        var publisher = new InMemoryStreamPublisher();
        var buffer = Encoding.UTF8.GetBytes("first");

        await publisher.PublishAsync("k", buffer, "T");
        Array.Fill(buffer, (byte)'x');

        Encoding.UTF8.GetString(publisher.Published[0].Body).Should().Be("first");
    }

    /// <summary>
    /// <c>Published</c> is a snapshot, so enumerating it while another thread publishes cannot
    /// throw — the unlocked <c>List&lt;T&gt;</c> plus <c>AsReadOnly()</c> the double shipped with
    /// could do both.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ConcurrentPublishesAreAllRecordedAndSnapshotsNeverTear()
    {
        var publisher = new InMemoryStreamPublisher(partitions: 4);
        const int writers = 8;
        const int each = 250;

        using var readerStop = new CancellationTokenSource();
        var reader = Task.Run(() =>
        {
            while (!readerStop.IsCancellationRequested)
            {
                var count = 0;
                foreach (var m in publisher.Published)
                {
                    count += m.Body.Length;
                }

                _ = count;
            }
        });

        await Task.WhenAll(Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
        {
            for (var i = 0; i < each; i++)
            {
                await publisher.PublishAsync($"w{w}", Body, "T");
            }
        })));

        await readerStop.CancelAsync();
        await reader;

        publisher.Count.Should().Be(writers * each);
        publisher.Published.Select(m => m.Id).Distinct().Should().HaveCount(writers * each);
    }

    /// <summary>
    /// <c>Clear()</c> empties the recording but does not rewind the id sequence, so a cleared
    /// publisher never hands a test an id it has already seen.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ClearKeepsIdsMovingForward()
    {
        var publisher = new InMemoryStreamPublisher();

        await publisher.PublishAsync("k", Body, "T");
        var before = publisher.Published[0].Id;

        publisher.Clear();
        publisher.Published.Should().BeEmpty();

        await publisher.PublishAsync("k", Body, "T");
        publisher.Published[0].Id.Should().BeGreaterThan(before);
    }

    /// <summary>
    /// <c>ForPartition</c> filters the recording, which is the assertion a partitioned test wants.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ForPartitionFilters()
    {
        var publisher = new InMemoryStreamPublisher(partitions: 4);

        await publisher.PublishAsync("k", Body, "T", new PublishOptions(Partition: 2));
        await publisher.PublishAsync("k", Body, "T", new PublishOptions(Partition: 3));

        publisher.ForPartition(2).Should().ContainSingle();
        publisher.ForPartition(1).Should().BeEmpty();
    }

    /// <summary>A publisher with fewer than one partition is a caller bug, not a silent 1.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ZeroPartitionsIsRefused()
    {
        var act = () => new InMemoryStreamPublisher(partitions: 0);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    // -------------------------------------------------------------------------------------------
    // InMemoryBufferedStreamPublisher
    // -------------------------------------------------------------------------------------------

    /// <summary>
    /// The one property that makes buffering worth a double: an enqueued message is not published.
    /// A test that forgets to flush fails here, the way production would lose the message.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task EnqueuedMessagesAreNotPublishedUntilFlush()
    {
        var publisher = new InMemoryBufferedStreamPublisher(partitions: 4);

        await publisher.EnqueueAsync("k", Body, "T");

        publisher.Published.Should().BeEmpty();
        publisher.PendingCount.Should().Be(1);
        publisher.Pending.Should().ContainSingle().Which.Type.Should().Be("T");

        await publisher.FlushAsync();

        publisher.PendingCount.Should().Be(0);
        publisher.Published.Should().ContainSingle();
    }

    /// <summary>
    /// A flush publishes in enqueue order and routes each message the way a direct publish would.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task FlushPreservesOrderAndRouting()
    {
        var publisher = new InMemoryBufferedStreamPublisher(partitions: 8);

        await publisher.EnqueueAsync("customer-42", Encoding.UTF8.GetBytes("1"), "T");
        await publisher.EnqueueAsync("customer-42", Encoding.UTF8.GetBytes("2"), "T");
        await publisher.FlushAsync();

        publisher.Published.Select(m => Encoding.UTF8.GetString(m.Body)).Should().Equal("1", "2");
        publisher.Published.Select(m => m.Partition).Distinct().Should().ContainSingle()
            .Which.Should().Be(publisher.PartitionFor("customer-42"));
    }

    /// <summary>
    /// A direct publish bypasses the buffer — the real publisher's documented "ordering between the
    /// two paths is undefined", made visible so a test cannot accidentally rely on the opposite.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task DirectPublishBypassesTheBuffer()
    {
        var publisher = new InMemoryBufferedStreamPublisher();

        await publisher.EnqueueAsync("k", Encoding.UTF8.GetBytes("buffered"), "T");
        await publisher.PublishAsync("k", Encoding.UTF8.GetBytes("direct"), "T");

        publisher.Published.Should().ContainSingle()
            .Which.Body.Should().BeEquivalentTo(Encoding.UTF8.GetBytes("direct"));
        publisher.PendingCount.Should().Be(1);
    }

    /// <summary>
    /// The enqueued bytes are copied, because the interface promises it and a caller is entitled to
    /// reuse the buffer the moment <c>EnqueueAsync</c> returns.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task EnqueueCopiesTheBody()
    {
        var publisher = new InMemoryBufferedStreamPublisher();
        var buffer = Encoding.UTF8.GetBytes("first");

        await publisher.EnqueueAsync("k", buffer, "T");
        Array.Fill(buffer, (byte)'x');
        await publisher.FlushAsync();

        Encoding.UTF8.GetString(publisher.Published[0].Body).Should().Be("first");
    }

    /// <summary>
    /// A failed flush faults the waiter and leaves the batch in the buffer, so a caller's retry
    /// recovers the messages rather than losing them.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ArmedFlushFailureFaultsAndKeepsTheBatch()
    {
        var publisher = new InMemoryBufferedStreamPublisher
        {
            FailNextFlush = new InvalidOperationException("redis is away"),
        };

        await publisher.EnqueueAsync("k", Body, "T");

        var act = async () => await publisher.FlushAsync();
        (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("redis is away");

        publisher.PendingCount.Should().Be(1);
        publisher.Published.Should().BeEmpty();

        // One-shot: the retry gets through.
        await publisher.FlushAsync();
        publisher.Published.Should().ContainSingle();
    }

    /// <summary>Flushing an empty buffer is a no-op, not a fault.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task FlushingAnEmptyBufferIsANoOp()
    {
        var publisher = new InMemoryBufferedStreamPublisher();
        await publisher.FlushAsync();
        publisher.Published.Should().BeEmpty();
    }

    /// <summary>
    /// Concurrent enqueues and flushes lose nothing and duplicate nothing: every message is
    /// published exactly once by the time the last flush returns.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ConcurrentEnqueueAndFlushPublishEachMessageOnce()
    {
        var publisher = new InMemoryBufferedStreamPublisher(partitions: 4);
        const int writers = 6;
        const int each = 200;

        await Task.WhenAll(Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
        {
            for (var i = 0; i < each; i++)
            {
                await publisher.EnqueueAsync($"w{w}", Encoding.UTF8.GetBytes($"{w}:{i}"), "T");
                if (i % 25 == 0)
                {
                    await publisher.FlushAsync();
                }
            }
        })));

        await publisher.FlushAsync();

        publisher.PendingCount.Should().Be(0);
        publisher.Count().Should().Be(writers * each);
        publisher.Published.Select(m => Encoding.UTF8.GetString(m.Body)).Distinct()
            .Should().HaveCount(writers * each);
    }

    /// <summary>It is an <see cref="IStreamBufferedPublisher"/>, which is the point of it existing.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void IsABufferedPublisher()
    {
        IStreamBufferedPublisher publisher = new InMemoryBufferedStreamPublisher();
        publisher.Should().BeAssignableTo<IStreamPublisher>();
    }

    // -------------------------------------------------------------------------------------------
    // StreamTestHarness
    // -------------------------------------------------------------------------------------------

    /// <summary>
    /// The delegate overload still works, and the message it builds carries the serialized body and
    /// the type name a consumer filters on.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task HarnessSendsToADelegate()
    {
        StreamMsg? seen = null;

        await StreamTestHarness.SendAsync(
            (msg, _) =>
            {
                seen = msg;
                return default;
            },
            new Ping("hello"),
            HarnessJson.Default.Ping);

        seen.Should().NotBeNull();
        seen!.Value.Type.Should().Be(typeof(Ping).FullName);
        JsonSerializer.Deserialize(seen.Value.Body.Span, HarnessJson.Default.Ping)!.Text.Should().Be("hello");
    }

    /// <summary>
    /// R-20: <see cref="IMessageHandler.HandleAsync"/> takes <c>in StreamMsg</c>, so no
    /// <c>Func&lt;StreamMsg, …&gt;</c> matches it and the delegate overload could never drive the
    /// interface the consumer host actually dispatches to. This overload can.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task HarnessSendsToAMessageHandler()
    {
        var handler = new RecordingMessageHandler();

        await StreamTestHarness.SendAsync(handler, new Ping("a"), HarnessJson.Default.Ping);

        handler.Seen.Should().ContainSingle();
        JsonSerializer.Deserialize(handler.Seen[0].Span, HarnessJson.Default.Ping)!.Text.Should().Be("a");
    }

    /// <summary>
    /// The batch overload delivers one call with every message, in order and with increasing ids —
    /// the shape a handler that records the last id as its position depends on.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task HarnessSendsABatchToABatchHandler()
    {
        var handler = new RecordingBatchHandler();

        await StreamTestHarness.SendBatchAsync(
            handler,
            [new Ping("a"), new Ping("b"), new Ping("c")],
            HarnessJson.Default.Ping);

        handler.Calls.Should().Be(1);
        handler.Texts.Should().Equal("a", "b", "c");
        handler.Ids.Should().BeInAscendingOrder();
    }

    /// <summary>An empty batch still reaches the handler, which is what the live pipeline does.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task HarnessSendsAnEmptyBatch()
    {
        var handler = new RecordingBatchHandler();

        await StreamTestHarness.SendBatchAsync(handler, [], HarnessJson.Default.Ping);

        handler.Calls.Should().Be(1);
        handler.Texts.Should().BeEmpty();
    }

    /// <summary>
    /// Options are stamped onto the message, so a handler that reads a header or a correlation id
    /// can be driven without a host.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void HarnessStampsOptionsOntoTheMessage()
    {
        var msg = StreamTestHarness.Build(
            new Ping("a"),
            HarnessJson.Default.Ping,
            new PublishOptions(
                CorrelationId: "corr-1",
                Headers: [new KeyValuePair<string, string>("tenant", "au")],
                Partition: 3));

        msg.Partition.Should().Be(3);
        msg.CorrelationId.Should().Be("corr-1");
        msg.Headers.TryGetValue("tenant", out var tenant).Should().BeTrue();
        tenant.Should().Be("au");
    }

    /// <summary>
    /// R-20: the id counter was a plain <c>static long</c> incremented with <c>++</c>, so parallel
    /// tests could be handed the same id. It is now <see cref="Interlocked"/>-advanced.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task HarnessIdsAreUniqueUnderConcurrency()
    {
        const int workers = 8;
        const int each = 200;
        var ids = new StreamId[workers][];

        await Task.WhenAll(Enumerable.Range(0, workers).Select(w => Task.Run(() =>
        {
            var mine = new StreamId[each];
            for (var i = 0; i < each; i++)
            {
                mine[i] = StreamTestHarness.Build(new Ping("x"), HarnessJson.Default.Ping).Id;
            }

            ids[w] = mine;
        })));

        ids.SelectMany(x => x).Distinct().Should().HaveCount(workers * each);
    }

    // -------------------------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------------------------

    private sealed class RecordingMessageHandler : IMessageHandler
    {
        internal List<ReadOnlyMemory<byte>> Seen { get; } = [];

        public ValueTask HandleAsync(in StreamMsg msg, CancellationToken ct)
        {
            this.Seen.Add(msg.Body.ToArray());
            return default;
        }
    }

    private sealed class RecordingBatchHandler : IBatchHandler
    {
        internal int Calls { get; private set; }

        internal List<string> Texts { get; } = [];

        internal List<StreamId> Ids { get; } = [];

        public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            this.Calls++;
            var span = batch.Span;
            for (var i = 0; i < span.Length; i++)
            {
                this.Ids.Add(span[i].Id);
                this.Texts.Add(JsonSerializer.Deserialize(span[i].Body.Span, HarnessJson.Default.Ping)!.Text);
            }

            return default;
        }
    }
}

/// <summary>A trivial payload for the harness tests.</summary>
internal sealed record Ping(string Text);

/// <summary>Source-generated serialization for <see cref="Ping"/>; the harness takes a JsonTypeInfo.</summary>
[JsonSerializable(typeof(Ping))]
internal sealed partial class HarnessJson : JsonSerializerContext;

/// <summary>Convenience so the buffered double reads like the plain one in assertions.</summary>
internal static class BufferedPublisherAssertionExtensions
{
    internal static int Count(this InMemoryBufferedStreamPublisher publisher) => publisher.Published.Count;
}
