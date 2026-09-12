using System.Text.Json;

using FluentAssertions;

using RedisEvents.Consumer;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// <see cref="TypedMessageHandlerAdapter{T}"/> / <see cref="TypedBatchHandlerAdapter{T}"/>: each wraps
/// a typed handler into the corresponding non-typed <see cref="IMessageHandler"/>/<see cref="IBatchHandler"/>,
/// so these are tested the same way a hand-written implementation of those interfaces would be — no
/// DI, no host, no Redis.
/// </summary>
public class TypedHandlerAdapterTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Message_adapter_deserialises_and_hands_over_the_envelope()
    {
        var recorder = new RecordingMessageHandler();
        var adapter = new TypedMessageHandlerAdapter<TypedOrder>(recorder, TypedPublishJson.Default.TypedOrder);
        var msg = Build(new TypedOrder(7, "seven"), id: 1);

        await adapter.HandleAsync(in msg, CancellationToken.None);

        var seen = recorder.Seen.Should().ContainSingle().Subject;
        seen.Value.Should().Be(new TypedOrder(7, "seven"));
        seen.Envelope.Id.Should().Be(msg.Id);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Message_adapter_propagates_a_deserialisation_failure()
    {
        var recorder = new RecordingMessageHandler();
        var adapter = new TypedMessageHandlerAdapter<TypedOrder>(recorder, TypedPublishJson.Default.TypedOrder);
        var msg = new StreamMsg("not json"u8.ToArray(), "T", new StreamId(1, 0), 0, "k", string.Empty, null, HeaderBlock.Empty);

        var act = async () => await adapter.HandleAsync(in msg, CancellationToken.None);

        await act.Should().ThrowAsync<JsonException>();
        recorder.Seen.Should().BeEmpty("the handler is never called once deserialisation has failed");
    }

    [Theory]
    [Trait("TestType", "UnitTest")]
    [InlineData(5)]                                     // below TypedBatchHandlerAdapter<T>.ChunkSize: sequential path
    [InlineData(TypedBatchHandlerAdapter<TypedOrder>.ChunkSize)]        // exactly the cutover
    [InlineData(TypedBatchHandlerAdapter<TypedOrder>.ChunkSize + 1)]    // one past it: parallel path kicks in
    [InlineData(250)]                                   // several chunks, parallel path
    public async Task Batch_adapter_preserves_order_at_every_batch_size(int count)
    {
        var recorder = new RecordingBatchHandler();
        var adapter = new TypedBatchHandlerAdapter<TypedOrder>(recorder, TypedPublishJson.Default.TypedOrder);

        var batch = new StreamMsg[count];
        for (var i = 0; i < count; i++)
        {
            batch[i] = Build(new TypedOrder(i, $"order-{i}"), id: i + 1);
        }

        await adapter.HandleAsync(batch, CancellationToken.None);

        var seen = recorder.Seen.Should().ContainSingle().Subject;
        seen.Length.Should().Be(count);
        for (var i = 0; i < count; i++)
        {
            seen.Span[i].Value.Should().Be(new TypedOrder(i, $"order-{i}"), "chunked parallel deserialisation must not reorder results");
            seen.Span[i].Envelope.Id.Should().Be(batch[i].Id);
        }
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Batch_adapter_propagates_a_deserialisation_failure_from_the_parallel_path()
    {
        var recorder = new RecordingBatchHandler();
        var adapter = new TypedBatchHandlerAdapter<TypedOrder>(recorder, TypedPublishJson.Default.TypedOrder);

        var batch = new StreamMsg[TypedBatchHandlerAdapter<TypedOrder>.ChunkSize + 1];
        for (var i = 0; i < batch.Length; i++)
        {
            batch[i] = i == batch.Length - 1
                ? new StreamMsg("not json"u8.ToArray(), "T", new StreamId(i + 1, 0), 0, "k", string.Empty, null, HeaderBlock.Empty)
                : Build(new TypedOrder(i, $"order-{i}"), id: i + 1);
        }

        var act = async () => await adapter.HandleAsync(batch, CancellationToken.None);

        await act.Should().ThrowAsync<JsonException>();
        recorder.Seen.Should().BeEmpty("a failure while deserialising must not still call the handler");
    }

    private static StreamMsg Build(TypedOrder order, long id)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(order, TypedPublishJson.Default.TypedOrder);
        return new StreamMsg(body, typeof(TypedOrder).FullName!, new StreamId(id, 0), 0, "k", string.Empty, null, HeaderBlock.Empty);
    }

    private sealed class RecordingMessageHandler : IMessageHandler<TypedOrder>
    {
        private readonly List<StreamMsg<TypedOrder>> seen = [];

        internal IReadOnlyList<StreamMsg<TypedOrder>> Seen => this.seen;

        public ValueTask HandleAsync(in StreamMsg<TypedOrder> msg, CancellationToken ct)
        {
            this.seen.Add(msg);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RecordingBatchHandler : IBatchHandler<TypedOrder>
    {
        private readonly List<ReadOnlyMemory<StreamMsg<TypedOrder>>> seen = [];

        internal IReadOnlyList<ReadOnlyMemory<StreamMsg<TypedOrder>>> Seen => this.seen;

        public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg<TypedOrder>> batch, CancellationToken ct)
        {
            this.seen.Add(batch);
            return ValueTask.CompletedTask;
        }
    }
}
