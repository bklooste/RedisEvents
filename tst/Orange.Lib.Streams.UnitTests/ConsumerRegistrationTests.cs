using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Wire;

namespace Orange.Lib.Streams.UnitTests;

/// <summary>
/// R-21 (P3 test gap 13). The <c>AddStream</c> overloads had no functional test at all — the README
/// samples compile and are never run, and the only registration tests were the publisher ones. What
/// each overload actually produces (which configured entry it picks, whether the handler is a
/// singleton, whether a delegate handler is carried instead of a type) was unasserted, so a wrong
/// entry or a dropped override would have been found by the first service to migrate.
/// </summary>
/// <remarks>
/// The assertions stop at the <see cref="StreamRegistry"/> and the service descriptors on purpose:
/// resolving the registered <see cref="IHostedService"/> would construct a
/// <c>StreamConsumerHost</c>, which connects to Redis. Registration is what this file is about.
/// </remarks>
public class ConsumerRegistrationTests
{
    private const string TwoConsumers = """
    {
      "Streams": {
        "Consumers": [
          { "Topic": "orders", "BatchSize": 250, "Consumer": "orders-svc" },
          { "Topic": "payments", "BatchSize": 7, "Consumer": "payments-svc" }
        ]
      }
    }
    """;

    private const string OneConsumer = """
    {
      "Streams": {
        "Consumers": [ { "Topic": "orders", "BatchSize": 250, "Persist": "SyncBatch" } ]
      }
    }
    """;

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

    private static StreamRegistry RegistryOf(HostApplicationBuilder builder)
    {
        using var host = builder.Build();
        return host.Services.GetRequiredService<StreamRegistry>();
    }

    /// <summary>
    /// <c>AddStream&lt;T&gt;(topic)</c> takes the matching configured entry rather than defaults —
    /// the whole point of the overload is that configuration still applies to a topic named in code.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStream_byTopic_usesTheMatchingConfiguredEntry()
    {
        var builder = Builder(TwoConsumers);
        builder.AddStream<PaymentsHandler>("payments");

        var registration = RegistryOf(builder).Consumers.Should().ContainSingle().Subject;

        registration.Options.Topic.Should().Be("payments");
        registration.Options.BatchSize.Should().Be(7, "the configured entry wins over record defaults");
        registration.Consumer.Should().Be("payments-svc");
        registration.HandlerType.Should().Be<PaymentsHandler>();
        registration.Handler.Should().BeNull();
    }

    /// <summary>A topic with no configured entry is legal and takes record defaults — zero-config registration.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStream_byTopic_withNoConfiguredEntry_takesDefaults()
    {
        var builder = Builder(TwoConsumers);
        builder.AddStream<OrdersHandler>("shipments");

        var registration = RegistryOf(builder).Consumers[0];

        registration.Options.Topic.Should().Be("shipments");
        registration.Options.BatchSize.Should().Be(new ConsumerOptions().BatchSize);
        registration.Options.ReadMode.Should().Be(ReadMode.Block, "the default read mode is the blocking one");
    }

    /// <summary>The handler class is registered as a singleton, not one per batch.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStream_registersTheHandlerAsASingleton()
    {
        var builder = Builder(OneConsumer);
        builder.AddStream<OrdersHandler>("orders");

        using var host = builder.Build();

        var first = host.Services.GetRequiredService<OrdersHandler>();
        var second = host.Services.GetRequiredService<OrdersHandler>();

        second.Should().BeSameAs(first);
    }

    /// <summary>The no-argument overload takes the topic from the single configured entry.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStream_fromConfiguration_usesTheSoleConfiguredConsumer()
    {
        var builder = Builder(OneConsumer);
        builder.AddStream<OrdersHandler>();

        var registration = RegistryOf(builder).Consumers[0];

        registration.Options.Topic.Should().Be("orders");
        registration.Options.Persist.Should().Be(PersistMode.SyncBatch);
    }

    /// <summary>With more than one — or with none — the no-argument overload cannot guess, and says so.</summary>
    [Theory]
    [Trait("TestType", "UnitTest")]
    [InlineData(null)]
    [InlineData(TwoConsumers)]
    public void AddStream_fromConfiguration_refusesAnythingButExactlyOneEntry(string? json)
    {
        var builder = Builder(json);

        var act = () => builder.AddStream<OrdersHandler>();

        act.Should().Throw<StreamConfigurationException>()
            .Which.Message.Should().Contain("Streams:Consumers").And.Contain("AddStream<OrdersHandler>");
    }

    /// <summary>The index overload picks by position — the shape the Kafka-era registration had.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStream_byIndex_picksThatEntry()
    {
        var builder = Builder(TwoConsumers);
        builder.AddStream<PaymentsHandler>(1);

        RegistryOf(builder).Consumers[0].Options.Topic.Should().Be("payments");
    }

    /// <summary>An index past the end names the count, because that is the thing the caller got wrong.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStream_byIndex_outOfRange_throwsNamingTheCount()
    {
        var builder = Builder(TwoConsumers);

        var act = () => builder.AddStream<OrdersHandler>(5);

        act.Should().Throw<StreamConfigurationException>()
            .Which.Message.Should().Contain("out of range").And.Contain("2 entries");
    }

    /// <summary>
    /// The configure overload is seeded with the single configured entry, so <c>with</c>-style
    /// modification starts from configuration rather than from defaults.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStream_configure_seedsFromConfigurationAndKeepsTheOverride()
    {
        var builder = Builder(OneConsumer);
        string? seedTopic = null;
        var seedBatchSize = 0;

        builder.AddStream<OrdersHandler>(options =>
        {
            // Read the seed before touching it: ConsumerOptions is a mutable class, so the callback
            // edits the configured instance in place rather than producing a copy of it.
            seedTopic = options.Topic;
            seedBatchSize = options.BatchSize;

            options.BatchSize = 500;
            return options;
        });

        seedTopic.Should().Be("orders", "the callback is handed the configured entry, not a blank one");
        seedBatchSize.Should().Be(250);

        RegistryOf(builder).Consumers[0].Options.BatchSize.Should().Be(500, "the callback's value is the effective one");
    }

    /// <summary>Options with no topic cannot be registered; the message says where to set it.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddStream_configure_withNoTopic_throws()
    {
        var builder = Builder();

        var act = () => builder.AddStream<OrdersHandler>(_ => new ConsumerOptions());

        act.Should().Throw<StreamConfigurationException>()
            .Which.Message.Should().Contain("no Topic");
    }

    /// <summary>The delegate overload carries the delegate itself and registers no handler type.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task AddStream_withADelegate_carriesTheDelegateAndNoHandlerType()
    {
        var builder = Builder(OneConsumer);
        var calls = 0;

        builder.AddStream("orders", (ReadOnlyMemory<StreamMsg> _, CancellationToken _) =>
        {
            calls++;
            return ValueTask.CompletedTask;
        });

        var registration = RegistryOf(builder).Consumers[0];

        registration.HandlerType.Should().BeNull();
        registration.Handler.Should().NotBeNull();
        registration.Options.BatchSize.Should().Be(250, "a delegate consumer is configured like any other");

        // The delegate really is the one that was registered, not a wrapper around something else.
        await registration.Handler!(ReadOnlyMemory<StreamMsg>.Empty, CancellationToken.None);
        calls.Should().Be(1);
    }

    /// <summary>
    /// Two registrations of the same (topic, consumer) in one process would fight over one position
    /// hash — each would flush its own idea of where the other had got to. Refused at registration,
    /// which is the only moment it is cheap.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Registering_the_same_topic_and_consumer_twice_throws()
    {
        var builder = Builder(OneConsumer);
        builder.AddStream<OrdersHandler>("orders");

        var act = () => builder.AddStream<PaymentsHandler>("orders");

        act.Should().Throw<StreamConfigurationException>()
            .Which.Message.Should().Contain("orders").And.Contain("distinct Consumer name");
    }

    /// <summary>The same topic twice is fine when the two consumers are named apart — a fan-out.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_same_topic_with_distinct_consumer_names_is_allowed()
    {
        var builder = Builder();

        builder.AddStream<OrdersHandler>(_ => new ConsumerOptions { Topic = "orders", Consumer = "projector" });
        builder.AddStream<PaymentsHandler>(_ => new ConsumerOptions { Topic = "orders", Consumer = "auditor" });

        RegistryOf(builder).Consumers.Select(c => c.Consumer).Should().Equal("projector", "auditor");
    }

    // ------------------------------------------------------------------ the registry itself

    /// <summary>The duplicate rule lives in the registry, and is asserted directly as well as through the builder.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamRegistry_refusesADuplicateTopicConsumerPair()
    {
        var registry = new StreamRegistry();
        registry.Add(new StreamConsumerRegistration(new ConsumerOptions { Topic = "orders" }, "svc", typeof(OrdersHandler), null));

        var duplicate = () => registry.Add(
            new StreamConsumerRegistration(new ConsumerOptions { Topic = "orders" }, "svc", typeof(PaymentsHandler), null));

        duplicate.Should().Throw<StreamConfigurationException>();

        var distinct = () => registry.Add(
            new StreamConsumerRegistration(new ConsumerOptions { Topic = "orders" }, "other", typeof(PaymentsHandler), null));

        distinct.Should().NotThrow();
        registry.Consumers.Should().HaveCount(2);
    }

    /// <summary>
    /// A second publisher on one topic is <em>not</em> an error — publishing twice to a topic from one
    /// process is normal — but it must not register a second producer, which for a buffered topic
    /// would mean two buffers and two pumps behind one interface.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamRegistry_deduplicatesPublishersSilently()
    {
        var registry = new StreamRegistry();

        registry.Add(new StreamPublisherRegistration(new ProducerOptions { Topic = "orders", Buffered = true }));
        registry.Add(new StreamPublisherRegistration(new ProducerOptions { Topic = "orders" }));
        registry.Add(new StreamPublisherRegistration(new ProducerOptions { Topic = "payments" }));

        registry.Publishers.Select(p => p.Options.Topic).Should().Equal("orders", "payments");
        registry.Publishers[0].Options.Buffered.Should().BeTrue("the first registration is the one that stands");
    }

    /// <summary>A handler class with no dependencies, for the registration assertions.</summary>
    public sealed class OrdersHandler;

    /// <inheritdoc cref="OrdersHandler"/>
    public sealed class PaymentsHandler;
}
