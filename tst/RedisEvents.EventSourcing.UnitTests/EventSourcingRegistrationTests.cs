using System.Net;
using System.Reflection;
using System.Text;
using System.Text.Json.Serialization;

using FluentAssertions;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RedisEvents.EventSourcing;
using RedisEvents.Errors;
using RedisEvents.Producer;
using RedisEvents.Projections;

using StackExchange.Redis;

namespace RedisEvents.UnitTests.EventSourcing;

/// <summary>
/// <c>AddEventStore</c>, <c>AddEventProjector</c>, <c>AddProjection</c> and
/// <c>AddRedisViewStore</c>: the registration-time wiring, none of which needs a real Redis to check.
/// </summary>
/// <remarks>
/// No Redis. The multiplexer in the container is a <see cref="DispatchProxy"/> stand-in at the
/// configured endpoint (copied from <c>StoreRegistrationTests</c>'s pattern), so the reuse path is
/// taken and nothing ever connects.
/// </remarks>
public partial class EventSourcingRegistrationTests
{
    private const string Endpoint = "redis-db.infra:6379";

    private sealed record Widget(string Id);

    [JsonSerializable(typeof(Widget))]
    private sealed partial class WidgetJson : JsonSerializerContext;

    private static void RegisterWidget(EventTypeRegistry events)
        => events.RegisterJson("widget", WidgetJson.Default.Widget);

    /// <summary>Keyed and unkeyed resolution both work for a single event store.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddEventStore_registersTheRepositoryKeyedAndUnkeyed()
    {
        var builder = Builder();
        builder.AddEventStore("inventory", RegisterWidget);

        using var host = builder.Build();

        var keyed = host.Services.GetRequiredKeyedService<IEventRepository>("inventory");
        keyed.Should().NotBeNull();

        host.Services.GetRequiredService<IEventRepository>().Should().BeSameAs(
            keyed,
            "the unkeyed resolution must hand back the keyed singleton, not a second repository");
    }

    /// <summary>A second call for the same topic is a no-op: one repository, the first registry wins.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddEventStore_twiceForOneTopic_registersOneRepository()
    {
        var builder = Builder();
        var calls = 0;

        builder.AddEventStore("inventory", _ => calls++);
        builder.AddEventStore("inventory", _ => calls++);

        using var host = builder.Build();

        calls.Should().Be(1, "the second call's registry-builder must not run once a registry exists for the topic");
        host.Services.GetRequiredService<IEventRepository>().Should().NotBeNull();
    }

    /// <summary>Two event stores: the unkeyed repository is ambiguous, but each keyed one resolves.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddEventStore_twoTopics_unkeyedRepositoryThrows_keyedResolve()
    {
        var builder = Builder();
        builder.AddEventStore("inventory", RegisterWidget);
        builder.AddEventStore("orders", RegisterWidget);

        using var host = builder.Build();

        host.Services.GetRequiredKeyedService<IEventRepository>("inventory").Should().NotBeNull();
        host.Services.GetRequiredKeyedService<IEventRepository>("orders").Should().NotBeNull();

        var act = () => host.Services.GetRequiredService<IEventRepository>();
        act.Should().Throw<StreamConfigurationException>()
            .WithMessage("*2 event stores*")
            .Which.Message.Should().Contain("inventory").And.Contain("orders");
    }

    /// <summary>No event store at all: the unkeyed repository names what to call instead.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void NoEventStore_unkeyedRepositoryThrows()
    {
        using var host = Builder().Build();

        var act = () => host.Services.GetRequiredService<IEventRepository>();
        act.Should().Throw<InvalidOperationException>();
    }

    /// <summary>
    /// The critical wiring proof: resolving the projector must run the REAL factory, not the bare
    /// <c>AddSingleton&lt;EventProjector&gt;()</c> that <c>AddStream&lt;EventProjector&gt;</c>
    /// registers first — the bare one cannot construct anything (its constructor needs a decoded
    /// projection list DI has no way to supply) and would throw if it were the one resolved.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddEventProjector_resolvesARealProjector_notTheBareRegistration()
    {
        var builder = Builder();
        builder.AddEventProjector("inventory", RegisterWidget);
        builder.AddProjection<SpyProjection>();

        using var host = builder.Build();

        var projector = host.Services.GetRequiredService<EventProjector>();
        projector.Should().NotBeNull();
    }

    /// <summary>Every <c>AddProjection&lt;T&gt;</c> instance is independently resolvable on its own.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddProjection_registersEachProjectionIndependently()
    {
        var builder = Builder();
        builder.AddEventProjector("inventory", RegisterWidget);
        builder.AddProjection<SpyProjection>();
        builder.AddProjection<OtherSpyProjection>();

        using var host = builder.Build();

        host.Services.GetRequiredService<SpyProjection>().Should().NotBeNull();
        host.Services.GetRequiredService<OtherSpyProjection>().Should().NotBeNull();
    }

    /// <summary><c>AddEventProjector</c> reuses an <c>AddEventStore</c> registry for the same topic.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddEventProjector_reusesAnExistingEventStoreRegistry()
    {
        var builder = Builder();
        var calls = 0;

        builder.AddEventStore("inventory", _ => calls++);
        builder.AddEventProjector("inventory");

        using var host = builder.Build();

        calls.Should().Be(1);
        host.Services.GetRequiredService<EventProjector>().Should().NotBeNull();
    }

    /// <summary>A projector with no registry and no <c>events</c> argument fails at call time, not lazily.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddEventProjector_noRegistryAndNoEvents_throwsImmediately()
    {
        var builder = Builder();

        var act = () => builder.AddEventProjector("inventory");

        act.Should().Throw<StreamConfigurationException>().WithMessage("*inventory*");
    }

    private static HostApplicationBuilder Builder()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
        {
            EnvironmentName = "Production",
        });

        builder.Configuration.AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(
            $$"""{ "Streams": { "ConnectionString": "{{Endpoint}}" } }""")));

        // Reused rather than connected: the endpoint matches Streams:ConnectionString.
        builder.Services.AddSingleton(FakeMultiplexer.At(Endpoint));

        return builder;
    }

    /// <summary>A no-op projection, just to prove independent resolution and non-throwing dispatch.</summary>
    private sealed class SpyProjection : IProjection<Widget>
    {
        public ValueTask HandleAsync(Widget @event, EventMeta meta, CancellationToken ct) => ValueTask.CompletedTask;
    }

    /// <summary>A second, distinct projection registered on the same builder.</summary>
    private sealed class OtherSpyProjection : IProjection<Widget>
    {
        public ValueTask HandleAsync(Widget @event, EventMeta meta, CancellationToken ct) => ValueTask.CompletedTask;
    }

    /// <summary>
    /// An <see cref="IConnectionMultiplexer"/> stand-in that answers what the reuse decision asks
    /// and hands out an <see cref="IDatabase"/> stand-in — enough to construct a store, and nothing
    /// more: any command issued on it throws. Copied from <c>StoreRegistrationTests</c>.
    /// </summary>
    private sealed class FakeMultiplexer
    {
        private readonly EndPoint[] endpoints;
        private readonly string configuration;
        private readonly IDatabase database;

        private FakeMultiplexer(EndPoint[] endpoints, string configuration)
        {
            this.endpoints = endpoints;
            this.configuration = configuration;

            var db = DispatchProxy.Create<IDatabase, Proxy>();
            ((Proxy)db).Owner = this;
            this.database = db;
        }

        internal static IConnectionMultiplexer At(string endpoint)
        {
            var parsed = ConfigurationOptions.Parse(endpoint).EndPoints.ToArray();
            var owner = new FakeMultiplexer(parsed, endpoint);

            var proxy = DispatchProxy.Create<IConnectionMultiplexer, Proxy>();
            ((Proxy)proxy).Owner = owner;

            return proxy;
        }

        private object? Invoke(MethodInfo method)
            => method.Name switch
            {
                "GetEndPoints" => this.endpoints,
                "get_Configuration" => this.configuration,
                "get_ClientName" => "an-existing-registration",
                "GetDatabase" => this.database,
                "Dispose" => null,
                _ => throw new NotSupportedException($"FakeMultiplexer does not implement {method.Name}."),
            };

        /// <summary>The generated proxy's base; public because <see cref="DispatchProxy"/> subclasses it.</summary>
        public class Proxy : DispatchProxy
        {
            internal FakeMultiplexer Owner { get; set; } = null!;

            /// <inheritdoc />
            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            {
                ArgumentNullException.ThrowIfNull(targetMethod);
                return this.Owner.Invoke(targetMethod);
            }
        }
    }
}
