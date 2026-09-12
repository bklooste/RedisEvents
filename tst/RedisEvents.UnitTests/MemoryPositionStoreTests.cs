using FluentAssertions;

using RedisEvents.Positions;
using RedisEvents.Testing;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// <see cref="MemoryPositionStore"/>: a real (not no-op) <see cref="IPositionStore"/>, so it is tested
/// against the same load/save/reset contract <c>RedisPositionStore</c> honours.
/// </summary>
public class MemoryPositionStoreTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task LoadAsync_withNothingSaved_returnsEmpty()
    {
        var store = new MemoryPositionStore();

        var loaded = await store.LoadAsync("orders", "svc", CancellationToken.None);

        loaded.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task SaveAsync_thenLoadAsync_roundTripsEveryPartition()
    {
        var store = new MemoryPositionStore();

        await store.SaveAsync(
            "orders",
            "svc",
            [(0, new StreamId(100, 0)), (1, new StreamId(200, 3))],
            CancellationToken.None);

        var loaded = await store.LoadAsync("orders", "svc", CancellationToken.None);

        loaded.Should().BeEquivalentTo(new Dictionary<int, StreamId>
        {
            [0] = new StreamId(100, 0),
            [1] = new StreamId(200, 3),
        });
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Positions_areScopedByTopicAndConsumer()
    {
        var store = new MemoryPositionStore();

        await store.SaveAsync("orders", "svc-a", [(0, new StreamId(1, 0))], CancellationToken.None);
        await store.SaveAsync("orders", "svc-b", [(0, new StreamId(2, 0))], CancellationToken.None);
        await store.SaveAsync("payments", "svc-a", [(0, new StreamId(3, 0))], CancellationToken.None);

        (await store.LoadAsync("orders", "svc-a", CancellationToken.None))[0].Should().Be(new StreamId(1, 0));
        (await store.LoadAsync("orders", "svc-b", CancellationToken.None))[0].Should().Be(new StreamId(2, 0));
        (await store.LoadAsync("payments", "svc-a", CancellationToken.None))[0].Should().Be(new StreamId(3, 0));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ResetAsync_withAPartition_movesOnlyThatPartition()
    {
        var store = new MemoryPositionStore();
        await store.SaveAsync(
            "orders", "svc", [(0, new StreamId(100, 0)), (1, new StreamId(100, 0))], CancellationToken.None);

        await store.ResetAsync("orders", "svc", new StreamId(1, 0), partition: 0, CancellationToken.None);

        var loaded = await store.LoadAsync("orders", "svc", CancellationToken.None);
        loaded[0].Should().Be(new StreamId(1, 0));
        loaded[1].Should().Be(new StreamId(100, 0), "resetting one partition must not touch the others");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ResetAsync_withNoPartition_movesEveryPartitionForThatConsumer()
    {
        var store = new MemoryPositionStore();
        await store.SaveAsync(
            "orders", "svc", [(0, new StreamId(100, 0)), (1, new StreamId(100, 0))], CancellationToken.None);
        await store.SaveAsync("orders", "other-svc", [(0, new StreamId(100, 0))], CancellationToken.None);

        await store.ResetAsync("orders", "svc", new StreamId(1, 0), partition: null, CancellationToken.None);

        var loaded = await store.LoadAsync("orders", "svc", CancellationToken.None);
        loaded[0].Should().Be(new StreamId(1, 0));
        loaded[1].Should().Be(new StreamId(1, 0));

        (await store.LoadAsync("orders", "other-svc", CancellationToken.None))[0]
            .Should().Be(new StreamId(100, 0), "a reset for one consumer must not touch another consumer's positions");
    }
}
