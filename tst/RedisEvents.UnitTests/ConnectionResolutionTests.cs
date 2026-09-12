using System.Net;
using System.Reflection;

using FluentAssertions;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using RedisEvents.Config;
using RedisEvents.Errors;
using RedisEvents.Extensions;

using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// R-21 (P3 test gap 13). <see cref="StreamsConnectionProvider"/>'s decline-to-reuse path had no
/// test, and it is the rule that keeps streams off <c>redis-cache</c>: that instance runs with
/// <c>--save "" --appendonly no</c>, so reusing its multiplexer for streams would lose every
/// stream, every position and every unprocessed message on a pod restart. The check is
/// endpoint-based and silent when it passes, which is exactly the kind of rule that rots unnoticed.
/// </summary>
/// <remarks>
/// No Redis. The registered multiplexer is a <see cref="DispatchProxy"/> stand-in that answers
/// <c>GetEndPoints</c>, <c>Configuration</c> and <c>ClientName</c> — everything the reuse decision
/// consults. The one test that lets the library connect its own multiplexer points it at a closed
/// port with a short timeout and asserts only that the candidate was not handed back.
/// </remarks>
public class ConnectionResolutionTests
{
    /// <summary>A closed port, so the library's own connect attempt fails fast rather than hanging.</summary>
    private const string Unreachable = "127.0.0.1:6399,connectTimeout=200,connectRetry=1,abortConnect=true";

    /// <summary>The endpoint match is on host and port, and a match is what permits reuse.</summary>
    [Theory]
    [Trait("TestType", "UnitTest")]
    [InlineData("redis-db.infra:6379", "redis-db.infra:6379", true)]
    [InlineData("REDIS-DB.INFRA:6379", "redis-db.infra:6379", true)]
    [InlineData("redis-cache.infra:6379", "redis-db.infra:6379", false)]
    [InlineData("redis-db.infra:6380", "redis-db.infra:6379", false)]
    public void EndpointsMatch_comparesHostAndPort(string existing, string configured, bool expected)
    {
        var candidate = FakeMultiplexer.At(existing);

        StreamsConnectionProvider.EndpointsMatch(candidate, configured).Should().Be(expected);
    }

    /// <summary>
    /// A connected multiplexer can report a resolved IP where the configuration still carries the DNS
    /// name, so the configuration string is consulted too — otherwise every reuse would be declined
    /// once the connection was up, which is the opposite failure.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void EndpointsMatch_alsoConsultsTheMultiplexersOwnConfiguration()
    {
        var resolved = FakeMultiplexer.At("10.1.2.3:6379", configuration: "redis-db.infra:6379,abortConnect=false");

        StreamsConnectionProvider.EndpointsMatch(resolved, "redis-db.infra:6379").Should().BeTrue();
        StreamsConnectionProvider.EndpointsMatch(resolved, "redis-cache.infra:6379").Should().BeFalse();
    }

    /// <summary>An unparseable configuration on the candidate contributes nothing and is not fatal.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void EndpointsMatch_survivesAnUnparseableConfigurationOnTheCandidate()
    {
        var odd = FakeMultiplexer.At("10.1.2.3:6379", configuration: "not a connection string, ,,:::");

        StreamsConnectionProvider.EndpointsMatch(odd, "redis-db.infra:6379").Should().BeFalse();
        StreamsConnectionProvider.EndpointsMatch(odd, "10.1.2.3:6379").Should().BeTrue();
    }

    /// <summary>A matching registered multiplexer is reused as-is, and is not the library's to dispose.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_matching_registered_multiplexer_is_reused()
    {
        var candidate = FakeMultiplexer.At("127.0.0.1:6399");
        var services = Container(candidate);
        var log = new Lines();

        var resolved = StreamsConnectionProvider.Resolve(
            services,
            new StreamOptions { ConnectionString = Unreachable },
            log,
            out var created);

        resolved.Should().BeSameAs(candidate);
        created.Should().BeFalse("a multiplexer from the container belongs to the container");
        log.Text.Should().Contain("reusing the IConnectionMultiplexer");
    }

    /// <summary>
    /// The finding this file exists for. A registered multiplexer pointing somewhere else is
    /// declined, both endpoints are logged so the mismatch is visible rather than silent, and the
    /// library connects its own — here, to a closed port, so the attempt fails and is wrapped.
    /// What must never happen is the candidate coming back.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_multiplexer_on_another_endpoint_is_never_reused()
    {
        var candidate = FakeMultiplexer.At("redis-cache.infra:6379");
        var services = Container(candidate);
        var log = new Lines();

        IConnectionMultiplexer? resolved = null;
        try
        {
            resolved = StreamsConnectionProvider.Resolve(
                services,
                new StreamOptions { ConnectionString = Unreachable },
                log,
                out _);
        }
        catch (StreamTransportException ex)
        {
            // The expected outcome here: nothing is listening on the configured endpoint. What the
            // test is about is which endpoint was tried, not whether it answered.
            ex.Message.Should().Contain("127.0.0.1:6399");
        }
        finally
        {
            resolved?.Dispose();
        }

        resolved.Should().NotBeSameAs(candidate, "reusing redis-cache for streams loses every stream on a restart");

        log.Text.Should().Contain("NOT reusing")
            .And.Contain("redis-cache.infra:6379")
            .And.Contain("127.0.0.1:6399", "both endpoints are named so the mismatch is diagnosable");
    }

    /// <summary>
    /// An unusable connection string is a configuration error naming the key, not a transport
    /// failure — and it is caught before any connect is attempted.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_unparseable_connection_string_is_a_configuration_error()
    {
        var act = () => StreamsConnectionProvider.Resolve(
            services: null,
            new StreamOptions { ConnectionString = "localhost:6379,syncTimeout=not-a-number" },
            logger: null,
            out _);

        act.Should().Throw<StreamConfigurationException>()
            .Which.Message.Should().Contain("Streams:ConnectionString");
    }

    private static IServiceProvider Container(IConnectionMultiplexer candidate)
    {
        var services = new ServiceCollection();
        services.AddSingleton(candidate);
        return services.BuildServiceProvider();
    }

    /// <summary>Collects the log text the resolution wrote, so the "which path was taken" lines are assertable.</summary>
    private sealed class Lines : ILogger
    {
        private readonly List<string> entries = [];

        internal string Text
        {
            get
            {
                lock (this.entries)
                {
                    return string.Join("\n", this.entries);
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (this.entries)
            {
                this.entries.Add(formatter(state, exception));
            }
        }
    }

    /// <summary>
    /// An <see cref="IConnectionMultiplexer"/> stand-in that answers only what the reuse decision
    /// asks it: its endpoints, its configuration string and its client name.
    /// </summary>
    private sealed class FakeMultiplexer
    {
        private readonly EndPoint[] endpoints;
        private readonly string configuration;

        private FakeMultiplexer(EndPoint[] endpoints, string configuration)
        {
            this.endpoints = endpoints;
            this.configuration = configuration;
        }

        /// <summary>Builds a stand-in reporting one endpoint.</summary>
        /// <param name="endpoint">The endpoint <c>GetEndPoints</c> reports, as <c>host:port</c>.</param>
        /// <param name="configuration">The multiplexer's own configuration string; defaults to the endpoint.</param>
        internal static IConnectionMultiplexer At(string endpoint, string? configuration = null)
        {
            var parsed = ConfigurationOptions.Parse(endpoint).EndPoints.ToArray();
            var owner = new FakeMultiplexer(parsed, configuration ?? endpoint);

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
