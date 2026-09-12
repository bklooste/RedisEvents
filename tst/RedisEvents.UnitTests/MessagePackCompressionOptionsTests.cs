using global::MessagePack;

using FluentAssertions;

using RedisEvents.MessagePack;
using RedisEvents.Testing;

namespace RedisEvents.UnitTests;

/// <summary>
/// <see cref="MessagePackCompressionOptions"/>: the config knobs on top of the size-gated
/// auto-compression exercised in <see cref="MessagePackPublishConsumeTests"/> — disabling it
/// entirely, and moving the threshold.
/// </summary>
public class MessagePackCompressionOptionsTests
{
    private static readonly MsgPackOrder LargeMessage = new(1, new string('a', 4000));

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Disabled_neverCompresses_regardlessOfSize()
    {
        var publisher = new InMemoryStreamPublisher();

        await publisher.PublishAsync(
            "k", LargeMessage, MessagePackSerializerOptions.Standard, compression: MessagePackCompressionOptions.Disabled);

        var published = publisher.Published.Should().ContainSingle().Subject;
        var uncompressed = MessagePackSerializer.Serialize(LargeMessage, MessagePackSerializerOptions.Standard);

        published.Body.Should().Equal(uncompressed, "compression is disabled, so even a large payload ships as-is");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ALowerThreshold_compressesAMessageTheDefaultThresholdWouldLeaveUncompressed()
    {
        // Comfortably under the default 1024-byte threshold, so it proves nothing on its own —
        // this test is about moving the cutover, not about compression in general.
        var message = new MsgPackOrder(1, new string('a', 300));
        var uncompressed = MessagePackSerializer.Serialize(message, MessagePackSerializerOptions.Standard);

        var atDefaultThreshold = new InMemoryStreamPublisher();
        await atDefaultThreshold.PublishAsync("k", message, MessagePackSerializerOptions.Standard);
        atDefaultThreshold.Published.Should().ContainSingle().Subject.Body.Should().Equal(uncompressed);

        var atLoweredThreshold = new InMemoryStreamPublisher();
        await atLoweredThreshold.PublishAsync(
            "k", message, MessagePackSerializerOptions.Standard,
            compression: new MessagePackCompressionOptions { ThresholdBytes = 100 });
        atLoweredThreshold.Published.Should().ContainSingle().Subject.Body.Should().NotEqual(uncompressed);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task AHigherThreshold_leavesALargeMessageUncompressed()
    {
        var publisher = new InMemoryStreamPublisher();
        var uncompressed = MessagePackSerializer.Serialize(LargeMessage, MessagePackSerializerOptions.Standard);

        await publisher.PublishAsync(
            "k", LargeMessage, MessagePackSerializerOptions.Standard,
            compression: new MessagePackCompressionOptions { ThresholdBytes = uncompressed.Length + 1 });

        var published = publisher.Published.Should().ContainSingle().Subject;

        published.Body.Should().Equal(uncompressed, "the payload sits exactly at the raised threshold, so it should not compress");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Default_isEnabled_withA1024ByteThreshold()
    {
        MessagePackCompressionOptions.Default.Enabled.Should().BeTrue();
        MessagePackCompressionOptions.Default.ThresholdBytes.Should().Be(1024);
    }
}
