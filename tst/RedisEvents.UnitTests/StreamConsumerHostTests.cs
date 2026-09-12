using FluentAssertions;

using Microsoft.Extensions.DependencyInjection;

using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Extensions;
using RedisEvents.Positions;
using RedisEvents.Testing;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// <see cref="StreamConsumerHost.Create"/>'s position-store resolution: it consults
/// <see cref="IPositionStore"/> from the container the same way it already does the handler, and does
/// so without ever touching <c>StreamsConnectionProvider.Connection</c> — so this is testable with no
/// real Redis, exactly like the registration tests in <c>ConsumerRegistrationTests</c>.
/// </summary>
public class StreamConsumerHostTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Create_withNoRegisteredPositionStore_leavesTheOverrideNull()
    {
        var options = new StreamOptions();
        var services = new ServiceCollection();
        services.AddSingleton(new StreamsConnectionProvider(options, null, null));
        var provider = services.BuildServiceProvider();

        var registration = new StreamConsumerRegistration(
            new ConsumerOptions { Topic = "orders" },
            "svc",
            HandlerType: null,
            Handler: (_, _) => ValueTask.CompletedTask);

        var host = StreamConsumerHost.Create(options, registration, provider);

        host.PositionStoreOverride.Should().BeNull("nothing registered IPositionStore, so the default RedisPositionStore applies");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Create_withARegisteredPositionStore_usesIt()
    {
        var options = new StreamOptions();
        var fakeStore = new MemoryPositionStore();

        var services = new ServiceCollection();
        services.AddSingleton(new StreamsConnectionProvider(options, null, null));
        services.AddSingleton<IPositionStore>(fakeStore);
        var provider = services.BuildServiceProvider();

        var registration = new StreamConsumerRegistration(
            new ConsumerOptions { Topic = "orders" },
            "svc",
            HandlerType: null,
            Handler: (_, _) => ValueTask.CompletedTask);

        var host = StreamConsumerHost.Create(options, registration, provider);

        host.PositionStoreOverride.Should().BeSameAs(fakeStore);
    }
}
