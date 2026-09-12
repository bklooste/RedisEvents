using System.Text;

using global::MessagePack;

using FluentAssertions;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RedisEvents.Consumer;
using RedisEvents.Extensions;
using RedisEvents.MessagePack;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// The MessagePack-typed <c>AddStream&lt;THandler, TMessage&gt;</c> overloads
/// (<see cref="MessagePackStreamsBuilderExtensions"/>): each is a one-line call into core's
/// serialiser-agnostic <c>AddStream&lt;THandler, TMessage&gt;(topic, Func&lt;...&gt;)</c> overload, so
/// this mirrors <c>ConsumerRegistrationTests.AddStream_typed_byTopic_carriesHandlerTypeAndTypedWrapper</c>
/// (the JSON equivalent) rather than resolving the hosted service — that would connect to Redis.
/// </summary>
public class MessagePackRegistrationTests
{
    private const string OneConsumer = """
    {
      "Streams": {
        "Consumers": [ { "Topic": "orders", "BatchSize": 250 } ]
      }
    }
    """;

    private static HostApplicationBuilder Builder()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
        {
            EnvironmentName = "Production",
        });
        builder.Configuration.AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(OneConsumer)));
        return builder;
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStream_msgPack_byTopic_carriesHandlerTypeAndTypedWrapper()
    {
        var builder = Builder();
        builder.AddStream<MsgPackOrdersHandler, MsgPackOrder>("orders", MessagePackSerializerOptions.Standard);

        using var host = builder.Build();
        var registration = host.Services.GetRequiredService<StreamRegistry>().Consumers[0];

        registration.Options.Topic.Should().Be("orders");
        registration.HandlerType.Should().Be<MsgPackOrdersHandler>();
        registration.Handler.Should().BeNull();
        registration.WrapAsTypedHandler.Should().NotBeNull();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStream_msgPack_fromConfiguration_usesTheSoleConfiguredConsumer()
    {
        var builder = Builder();
        builder.AddStream<MsgPackOrdersHandler, MsgPackOrder>(MessagePackSerializerOptions.Standard);

        using var host = builder.Build();
        var registration = host.Services.GetRequiredService<StreamRegistry>().Consumers[0];

        registration.Options.Topic.Should().Be("orders");
        registration.WrapAsTypedHandler.Should().NotBeNull();
    }

    /// <summary>A typed handler class, for the registration assertions.</summary>
    public sealed class MsgPackOrdersHandler : IBatchHandler<MsgPackOrder>
    {
        public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg<MsgPackOrder>> batch, CancellationToken ct)
            => ValueTask.CompletedTask;
    }
}
