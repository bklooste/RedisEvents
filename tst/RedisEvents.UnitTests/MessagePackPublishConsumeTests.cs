using global::MessagePack;

using FluentAssertions;

using RedisEvents.MessagePack;
using RedisEvents.Testing;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// <see cref="MessagePackPublishExtensions"/> / <see cref="MessagePackConsumeExtensions"/>: the
/// MessagePack sibling of <c>TypedPublishTests</c>/<c>TypedConsumeTests</c>, plus the size-gated
/// auto-compression behaviour that has no JSON equivalent.
/// </summary>
public class MessagePackPublishConsumeTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task PublishAsync_roundTrips_aSmallMessage_uncompressed()
    {
        var publisher = new InMemoryStreamPublisher();
        var message = new MsgPackOrder(7, "seven");

        await publisher.PublishAsync("k", message, MessagePackSerializerOptions.Standard);

        var published = publisher.Published.Should().ContainSingle().Subject;
        published.Type.Should().Be(typeof(MsgPackOrder).FullName);

        // Below the default 1024-byte threshold: the published body is exactly the plain,
        // uncompressed encoding — no LZ4 framing at all.
        var uncompressed = MessagePackSerializer.Serialize(message, MessagePackSerializerOptions.Standard);
        published.Body.Should().Equal(uncompressed);

        Decode(published.Body).Should().Be(message);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task PublishAsync_compresses_aLargeMessage_andDecodesTransparently()
    {
        var publisher = new InMemoryStreamPublisher();

        // Highly repetitive and well over 1024 bytes uncompressed, so LZ4 shrinks it dramatically.
        var message = new MsgPackOrder(1, new string('a', 4000));

        await publisher.PublishAsync("k", message, MessagePackSerializerOptions.Standard);

        var published = publisher.Published.Should().ContainSingle().Subject;
        var uncompressed = MessagePackSerializer.Serialize(message, MessagePackSerializerOptions.Standard);

        published.Body.Length.Should().BeLessThan(uncompressed.Length, "a large, repetitive payload should compress");

        // Deserialize<T> handles this without the caller needing to know it was compressed.
        Decode(published.Body).Should().Be(message);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task PublishBatchAsync_roundTrips_everyMessage_inOrder()
    {
        var publisher = new InMemoryStreamPublisher();
        var messages = new[] { new MsgPackOrder(1, "one"), new MsgPackOrder(2, "two"), new MsgPackOrder(3, "three") };

        await publisher.PublishBatchAsync("k", messages, MessagePackSerializerOptions.Standard);

        publisher.Published.Select(p => Decode(p.Body)).Should().Equal(messages);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task EnqueueAsync_roundTrips_throughTheBufferedPublisher()
    {
        var publisher = new InMemoryBufferedStreamPublisher();
        var message = new MsgPackOrder(9, "nine");

        await publisher.EnqueueAsync("k", message, MessagePackSerializerOptions.Standard);
        await publisher.FlushAsync();

        var published = publisher.Published.Should().ContainSingle().Subject;
        Decode(published.Body).Should().Be(message);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Deserialize_batch_reads_everyMessage_inOrder()
    {
        var messages = new[] { new MsgPackOrder(1, "one"), new MsgPackOrder(2, "two") };
        ReadOnlyMemory<StreamMsg> batch = messages
            .Select(m => BuildMsg(MessagePackSerializer.Serialize(m, MessagePackSerializerOptions.Standard)))
            .ToArray();

        var decoded = batch.Deserialize<MsgPackOrder>(MessagePackSerializerOptions.Standard);

        decoded.Should().Equal(messages);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Deserialize_singleMessage_decodesACompressedBody()
    {
        var message = new MsgPackOrder(1, new string('b', 4000));
        var compressed = MessagePackSerializer.Serialize(
            message, MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4BlockArray));

        Decode(compressed).Should().Be(message);
    }

    /// <summary>Exercises the real public <see cref="MessagePackConsumeExtensions.Deserialize{T}(in StreamMsg, MessagePackSerializerOptions)"/> path.</summary>
    private static MsgPackOrder? Decode(byte[] body) => BuildMsg(body).Deserialize<MsgPackOrder>(MessagePackSerializerOptions.Standard);

    private static StreamMsg BuildMsg(byte[] body)
        => new(body, typeof(MsgPackOrder).FullName!, new StreamId(1, 0), 0, "k", string.Empty, null, HeaderBlock.Empty);
}

/// <summary>The payload the MessagePack tests serialise.</summary>
[MessagePackObject]
public sealed record MsgPackOrder([property: Key(0)] int Id, [property: Key(1)] string Name);
