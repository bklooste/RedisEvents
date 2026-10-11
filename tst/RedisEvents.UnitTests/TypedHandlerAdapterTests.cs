using Microsoft.Extensions.Logging;
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

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Message_adapter_warns_when_a_body_decodes_to_null_and_still_calls_the_handler()
    {
        var recorder = new RecordingMessageHandler();
        var log = new CapturingLog();
        var adapter = new TypedMessageHandlerAdapter<TypedOrder>(recorder, TypedPublishJson.Default.TypedOrder);
        adapter.Attach(log, "orders", "billing");
        var msg = new StreamMsg("null"u8.ToArray(), "Orders.TypedOrder", new StreamId(7, 3), 2, "k", string.Empty, null, HeaderBlock.Empty);

        await adapter.HandleAsync(in msg, CancellationToken.None);

        recorder.Seen.Should().ContainSingle().Which.Value.Should().BeNull();
        var warning = log.At(LogLevel.Warning).Should().ContainSingle().Subject;
        warning.Should().Contain("topic=orders").And.Contain("consumer=billing").And.Contain("partition=2")
            .And.Contain("ids=7-3..7-3").And.Contain("type=Orders.TypedOrder").And.Contain("1 of 1");
    }

    [Theory]
    [Trait("TestType", "UnitTest")]
    [InlineData(5)]                                                     // sequential path
    [InlineData(TypedBatchHandlerAdapter<TypedOrder>.ChunkSize * 3)]    // chunked parallel path
    public async Task Batch_adapter_logs_one_warning_per_batch_for_bodies_that_decode_to_null(int count)
    {
        var recorder = new RecordingBatchHandler();
        var log = new CapturingLog();
        var adapter = new TypedBatchHandlerAdapter<TypedOrder>(recorder, TypedPublishJson.Default.TypedOrder);
        adapter.Attach(log, "orders", "billing");
        var batch = new StreamMsg[count];
        for (var i = 0; i < count; i++)
        {
            var body = i is 1 or 3
                ? "null"u8.ToArray()
                : JsonSerializer.SerializeToUtf8Bytes(new TypedOrder(i, "o"), TypedPublishJson.Default.TypedOrder);
            batch[i] = new StreamMsg(body, "Orders.TypedOrder", new StreamId(10, i), 0, "k", string.Empty, null, HeaderBlock.Empty);
        }

        await adapter.HandleAsync(batch, CancellationToken.None);

        recorder.Seen.Should().ContainSingle().Which.Length.Should().Be(count, "the handler still sees every message");
        var warning = log.At(LogLevel.Warning).Should().ContainSingle().Subject;
        warning.Should().Contain($"2 of {count}").And.Contain("topic=orders").And.Contain("consumer=billing")
            .And.Contain("ids=10-1..10-3").And.Contain("type=Orders.TypedOrder");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Batch_adapter_logs_nothing_when_every_body_decodes()
    {
        var log = new CapturingLog();
        var adapter = new TypedBatchHandlerAdapter<TypedOrder>(new RecordingBatchHandler(), TypedPublishJson.Default.TypedOrder);
        adapter.Attach(log, "orders", "billing");
        var body = JsonSerializer.SerializeToUtf8Bytes(new TypedOrder(1, "o"), TypedPublishJson.Default.TypedOrder);
        var batch = new[] { new StreamMsg(body, "T", new StreamId(1, 0), 0, "k", string.Empty, null, HeaderBlock.Empty) };

        await adapter.HandleAsync(batch, CancellationToken.None);

        log.Entries.Should().BeEmpty();
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
