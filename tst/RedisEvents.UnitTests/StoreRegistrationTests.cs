using System.Net;
using System.Reflection;
using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RedisEvents.Errors;
using RedisEvents.Extensions;
using RedisEvents.Producer;

using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// <c>AddStreamStore(topic)</c> and <see cref="StreamsConnection.GetSharedDatabase"/>: the two
/// registration-time rules an event store rests on, neither of which needs a Redis to check.
/// </summary>
/// <remarks>
/// No Redis. The multiplexer in the container is a <see cref="DispatchProxy"/> stand-in at the
/// configured endpoint, so the reuse path is taken and nothing ever connects — which also means a
/// resolved <see cref="IStreamStore"/> here proves the wiring, not the wire.
/// </remarks>
public class StoreRegistrationTests
{
    private const string Endpoint = "redis-db.infra:6379";

    private const string CrossSlotOrders = """
    { "Streams": { "Topics": { "orders": { "CoLocatePartitions": false } } } }
    """;

    /// <summary>One store: keyed by topic, and resolvable unkeyed.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamStore_registersTheStoreKeyedAndUnkeyed()
    {
        var builder = Builder();
        builder.AddStreamStore("orders");

        using var host = builder.Build();

        host.Services.GetRequiredService<StreamRegistry>().Stores.Should().ContainSingle()
            .Which.Topic.Should().Be("orders");

        host.Services.GetRequiredKeyedService<IStreamStore>("orders").Should().NotBeNull();
        host.Services.GetRequiredService<IStreamStore>().Should().BeSameAs(
            host.Services.GetRequiredKeyedService<IStreamStore>("orders"),
            "the unkeyed resolution must hand back the keyed singleton, not a second store");
    }

    /// <summary>A second call for the same topic is a no-op, exactly as it is for a publisher.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamStore_twiceForOneTopic_registersOneStore()
    {
        var builder = Builder();
        builder.AddStreamStore("orders");
        builder.AddStreamStore("orders");

        using var host = builder.Build();

        host.Services.GetRequiredService<StreamRegistry>().Stores.Should().ContainSingle();
        host.Services.GetServices<IStreamStore>().Should().ContainSingle();
    }

    /// <summary>
    /// Two stores make an unkeyed resolution ambiguous, and a silent winner there would write an
    /// aggregate's history into another topic's slot.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamStore_twoTopics_makesTheUnkeyedResolutionThrow()
    {
        var builder = Builder();
        builder.AddStreamStore("orders");
        builder.AddStreamStore("inventory");

        using var host = builder.Build();

        var act = () => host.Services.GetRequiredService<IStreamStore>();

        act.Should().Throw<StreamConfigurationException>()
            .Which.Message.Should().Contain("orders").And.Contain("inventory").And.Contain("FromKeyedServices");

        // Both are still resolvable by the name the service meant.
        host.Services.GetRequiredKeyedService<IStreamStore>("inventory").Should().NotBeNull();
    }

    /// <summary>
    /// A topic whose partitions are spread across cluster slots cannot have a state store: the
    /// append and the publish could not be one transaction. It fails at registration, before the
    /// container exists, because there is no non-transactional fallback to offer.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStreamStore_withoutCoLocatedPartitions_throwsAtRegistration()
    {
        var builder = Builder(CrossSlotOrders);

        var act = () => builder.AddStreamStore("orders");

        act.Should().Throw<StreamConfigurationException>()
            .Which.Message.Should().Contain("CoLocatePartitions").And.Contain("MULTI/EXEC");
    }

    /// <summary>The facade hands out the shared connection's database, not a second connection.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void GetSharedDatabase_returnsTheSharedStreamsDatabase()
    {
        var builder = Builder();
        builder.AddStreamStore("orders");

        using var host = builder.Build();

        var db = StreamsConnection.GetSharedDatabase(host.Services);

        db.Should().NotBeNull();
        db.Should().BeSameAs(StreamsConnection.GetSharedDatabase(host.Services), "the multiplexer is resolved once and cached");
    }

    /// <summary>
    /// Without an <c>AddStream…</c> call there is no streams connection to hand out, and saying so
    /// is better than connecting to a default endpoint nobody asked for.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void GetSharedDatabase_withoutAnyRegistration_throws()
    {
        using var host = Builder().Build();

        var act = () => StreamsConnection.GetSharedDatabase(host.Services);

        act.Should().Throw<StreamConfigurationException>()
            .Which.Message.Should().Contain("AddStreamStore");
    }

    private static HostApplicationBuilder Builder(string? json = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
        {
            EnvironmentName = "Production",
        });

        builder.Configuration.AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(
            $$"""{ "Streams": { "ConnectionString": "{{Endpoint}}" } }""")));

        if (json is not null)
        {
            builder.Configuration.AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(json)));
        }

        // Reused rather than connected: the endpoint matches Streams:ConnectionString.
        builder.Services.AddSingleton(FakeMultiplexer.At(Endpoint));

        return builder;
    }

    /// <summary>
    /// An <see cref="IConnectionMultiplexer"/> stand-in that answers what the reuse decision asks
    /// and hands out an <see cref="IDatabase"/> stand-in — enough to construct a store, and nothing
    /// more: any command issued on it throws.
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
