using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Producer;

namespace Orange.Lib.Streams.UnitTests;

/// <summary>
/// R-17. <c>AddStreamPublisher(topic, buffered)</c> dropped its <c>buffered</c> argument on the
/// floor whenever the topic had a <c>Streams:Producers</c> entry, so
/// <c>AddStreamPublisher("orders", buffered: true)</c> against a configured topic quietly produced a
/// direct publisher and no pump. The argument is now nullable: unspecified defers to configuration,
/// and a contradiction is refused with both sources named.
/// </summary>
public class PublisherRegistrationTests
{
    private static HostApplicationBuilder Builder(string? json = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
        {
            EnvironmentName = "Production",
        });

        if (json is not null)
        {
            builder.Configuration.AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(json)));
        }

        return builder;
    }

    private const string BufferedOrders = """
    { "Streams": { "Producers": [ { "Topic": "orders", "Buffered": true } ] } }
    """;

    private const string DirectOrders = """
    { "Streams": { "Producers": [ { "Topic": "orders", "Buffered": false } ] } }
    """;

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamPublisher_ArgumentContradictingConfiguration_Throws()
    {
        var builder = Builder(DirectOrders);

        var act = () => builder.AddStreamPublisher("orders", buffered: true);

        act.Should().Throw<StreamConfigurationException>()
            .Which.Message.Should().Contain("Streams:Producers").And.Contain("orders");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamPublisher_ArgumentContradictingConfiguration_TheOtherWayRound_Throws()
    {
        var builder = Builder(BufferedOrders);

        var act = () => builder.AddStreamPublisher("orders", buffered: false);

        act.Should().Throw<StreamConfigurationException>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamPublisher_ArgumentAgreeingWithConfiguration_IsAccepted()
    {
        var builder = Builder(BufferedOrders);
        builder.AddStreamPublisher("orders", buffered: true);

        using var host = builder.Build();

        host.Services.GetRequiredService<StreamRegistry>().Publishers.Should().ContainSingle()
            .Which.Options.Buffered.Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamPublisher_WithoutTheArgument_TakesTheConfiguredValue()
    {
        var builder = Builder(BufferedOrders);
        builder.AddStreamPublisher("orders");

        using var host = builder.Build();

        host.Services.GetRequiredService<StreamRegistry>().Publishers[0].Options.Buffered.Should().BeTrue();

        // The buffered publisher is also its own pump, so it has to be in the hosted services.
        host.Services.GetServices<IHostedService>().OfType<IStreamBufferedPublisher>().Should().ContainSingle();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamPublisher_UnconfiguredTopic_UsesTheArgument()
    {
        var builder = Builder();
        builder.AddStreamPublisher("orders", buffered: true);

        using var host = builder.Build();

        host.Services.GetRequiredService<StreamRegistry>().Publishers[0].Options.Buffered.Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamPublisher_UnconfiguredTopicWithoutTheArgument_IsDirect()
    {
        var builder = Builder();
        builder.AddStreamPublisher("orders");

        using var host = builder.Build();

        host.Services.GetRequiredService<StreamRegistry>().Publishers[0].Options.Buffered.Should().BeFalse();
        host.Services.GetServices<IHostedService>().OfType<IStreamBufferedPublisher>().Should().BeEmpty();
    }
}
