using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Extensions;
using RedisEvents.Web;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// R-04. <see cref="StreamsHealthCheck"/> was a finished class that nothing registered — the hook
/// was a live <c>TODO(P1/P3)</c> and every test that wanted an answer from it constructed it by
/// hand, which is precisely why nobody noticed it was absent from every real container. These tests
/// assert the registration through a host, so a future refactor that drops it fails here rather
/// than in a cluster.
///
/// After the R-25 headless split the registration lives in <c>RedisEvents.Web</c> and is
/// explicit: core must not reference the health-check abstractions, so it cannot register a check.
/// </summary>
public class HealthCheckRegistrationTests
{
    private static HostApplicationBuilder Builder()
        => Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
        {
            EnvironmentName = "Production",
        });

    private static HealthCheckRegistration[] Registrations(IServiceProvider services)
        => [.. services.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations];

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamsHealthCheck_RegistersTheCheckUnderTheStreamsTag()
    {
        var builder = Builder();
        builder.AddStream<Handler>("orders");
        builder.AddStreamsHealthCheck();

        using var host = builder.Build();

        var registration = Registrations(host.Services).Should().ContainSingle().Subject;

        registration.Name.Should().Be(StreamsHealthCheck.Name);
        registration.Tags.Should().Contain(StreamsHealthCheck.Name, "the plan names 'streams' as the tag to probe on");
        registration.Factory(host.Services).Should().BeOfType<StreamsHealthCheck>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamsHealthCheck_SeesTheConsumerHostsAndTheSharedConnection()
    {
        var builder = Builder();
        builder.AddStream<Handler>("orders");
        builder.AddStream<Handler>("payments");
        builder.AddStreamsHealthCheck();

        using var host = builder.Build();

        // The check reaches the consumer hosts only through IEnumerable<IHostedService> — they are
        // registered one-per-consumer rather than by type — so resolving the factory has to produce a
        // check that can actually see them.
        registrationFactoryResolves(host);

        var hosts = 0;
        foreach (var service in host.Services.GetServices<IHostedService>())
        {
            if (service is StreamConsumerHost)
                hosts++;
        }

        hosts.Should().Be(2);
        host.Services.GetService<StreamsConnectionProvider>().Should().NotBeNull();

        static void registrationFactoryResolves(IHost host)
            => Registrations(host.Services)[0].Factory(host.Services).Should().BeOfType<StreamsHealthCheck>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamsHealthCheck_CalledTwice_RegistersOnce()
    {
        var builder = Builder();
        builder.AddStream<Handler>("orders");
        builder.AddStreamsHealthCheck();
        builder.AddStreamsHealthCheck();

        using var host = builder.Build();

        // A duplicate name throws when HealthCheckService is first resolved, which would turn a
        // second call — easy to make in a service that registers streams in two places — into a
        // startup failure.
        Registrations(host.Services).Should().ContainSingle();

        var act = () => host.Services.GetRequiredService<HealthCheckService>();
        act.Should().NotThrow();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStream_AloneDoesNotRegisterTheCheck()
    {
        var builder = Builder();
        builder.AddStream<Handler>("orders");

        using var host = builder.Build();

        // Core cannot register it after R-25 (no health-check dependency), and a headless worker must
        // not be made to carry one. The registration is a deliberate opt-in.
        Registrations(host.Services).Should().BeEmpty();
    }

    /// <summary>
    /// R-17: the advisor that gives the binder's advisory rules a real logger has to be in the
    /// container, or those rules stay unprintable in production however correct they are.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStream_RegistersTheConfigurationAdvisor()
    {
        var builder = Builder();
        builder.AddStream<Handler>("orders");

        using var host = builder.Build();

        host.Services.GetServices<IHostedService>()
            .OfType<StreamsConfigurationAdvisor>()
            .Should().ContainSingle();
    }

    private sealed class Handler : IBatchHandler
    {
        public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            _ = batch;
            _ = ct;
            return ValueTask.CompletedTask;
        }
    }
}
