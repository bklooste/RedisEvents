using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Orange.Lib.EventHubs.Consumer.BatchConsumer;
using RedisEvents.Compat;
using RedisEvents.Config;
using RedisEvents.Errors;
using RedisEvents.Extensions;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// P4-01 / P4-02. The compatibility shim is the mechanism the whole cutover rests on: a service is
/// supposed to change one <c>.csproj</c> line plus its configuration and keep its handler code, so
/// what these assert is that a Kafka-era registration still resolves the same consumer, and that a
/// Kafka-era handler still sees the same field values it saw on the other transport.
/// </summary>
/// <remarks>
/// The registration tests stop at the <see cref="StreamRegistry"/> and the container, like
/// <c>ConsumerRegistrationTests</c>: resolving the hosted service would build a
/// <c>StreamConsumerHost</c> and connect to Redis. The delivery tests drive
/// <c>CompatBatchHandler&lt;T&gt;</c> directly with a synthesized batch, which is exactly what the
/// pipeline does to it.
/// </remarks>
public class CompatShimTests
{
    /// <summary>plat-alerts' own section, verbatim apart from the Namespace the environment supplies.</summary>
    private const string RealisticEventHubs = """
    {
      "EventHubs": {
        "common": {
          "Namespace": "kafka",
          "BatchConsumers": [
            { "EventHubName": "alerts", "ConsumerGroup": "plat_alerts", "BatchSize": 10 },
            { "EventHubName": "transactions", "ConsumerGroup": "plat_alerts", "BatchSize": 10 },
            { "EventHubName": "ebets", "ConsumerGroup": "plat_alerts", "BatchSize": 10 },
            { "EventHubName": "customerinfo", "ConsumerGroup": "plat_alerts", "BatchSize": 10 }
          ]
        }
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

    private static IConfiguration Config(string json) =>
        new ConfigurationBuilder().AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(json))).Build();

    // ---------------------------------------------------------------------------------------
    // P4-02 — reading EventHubs: and mapping it onto ConsumerOptions.
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// The mapping a migrating service depends on: four keys carry over, in the order the numbered
    /// registrations index into.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ReadBatchConsumers_mapsARealisticSection()
    {
        var consumers = EventHubsCompatConfig.ReadBatchConsumers(Config(RealisticEventHubs));

        consumers.Should().HaveCount(4);
        consumers.Select(c => c.Topic).Should().Equal("alerts", "transactions", "ebets", "customerinfo");
        consumers.Should().OnlyContain(c => c.BatchSize == 10);
        consumers.Should().OnlyContain(c => c.Consumer == "plat_alerts");
        consumers.Should().OnlyContain(c => c.Filter == null);
    }

    /// <summary>
    /// <c>EventHubName</c> → <c>Topic</c>, <c>Filter</c> → <c>Filter</c>, and an absent
    /// <c>BatchSize</c> takes the same 100 the Kafka reader defaulted to.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ReadBatchConsumers_mapsFilterAndDefaultsBatchSize()
    {
        var consumers = EventHubsCompatConfig.ReadBatchConsumers(Config("""
        {
          "EventHubs": {
            "common": {
              "Namespace": "kafka",
              "BatchConsumers": [
                {
                  "EventHubName": "odds",
                  "ConsumerGroup": "offer-odds",
                  "Filter": [ "Orange.Models.Odds.OddsUpdate", "Orange.Models.Odds.OddsRemoved" ]
                }
              ]
            }
          }
        }
        """));

        var only = consumers.Should().ContainSingle().Subject;
        only.Topic.Should().Be("odds");
        only.BatchSize.Should().Be(100, "the Kafka reader defaulted BatchSize to 100");
        only.Consumer.Should().Be("offer-odds");
        only.Filter.Should().Equal("Orange.Models.Odds.OddsUpdate", "Orange.Models.Odds.OddsRemoved");
    }

    /// <summary>
    /// <c>$Default</c> and <c>$Dummy</c> meant "no group of my own" on the Kafka side, so they must
    /// not become a literal consumer name — that name owns the stored position hash.
    /// </summary>
    [Theory]
    [InlineData("$Default")]
    [InlineData("$Dummy")]
    [InlineData("")]
    [Trait("TestType", "UnitTest")]
    public void ReadBatchConsumers_placeholderConsumerGroupsMapToNull(string group)
    {
        var consumers = EventHubsCompatConfig.ReadBatchConsumers(Config($$"""
        {
          "EventHubs": {
            "common": {
              "Namespace": "kafka",
              "BatchConsumers": [ { "EventHubName": "alerts", "ConsumerGroup": "{{group}}" } ]
            }
          }
        }
        """));

        consumers.Should().ContainSingle().Which.Consumer.Should().BeNull();
    }

    /// <summary>
    /// The index is into the flattened list, concatenated in configuration order — the consumer
    /// twin of the per-domain producer-index gotcha. Renumbering during a transport migration would
    /// wire handlers to the wrong topics with everything still compiling, so it is reproduced.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ReadBatchConsumers_flattensNamespacesInConfigurationOrder()
    {
        var consumers = EventHubsCompatConfig.ReadBatchConsumers(Config("""
        {
          "EventHubs": {
            "common": {
              "Namespace": "kafka",
              "BatchConsumers": [ { "EventHubName": "alerts" }, { "EventHubName": "transactions" } ]
            },
            "external": {
              "Namespace": "kafka-ext",
              "BatchConsumers": [ { "EventHubName": "partner" } ]
            }
          }
        }
        """));

        consumers.Select(c => c.Topic).Should().Equal("alerts", "transactions", "partner");
    }

    /// <summary>
    /// A namespace section with no <c>Namespace</c> value was invisible to the Kafka reader, and the
    /// flattened index counts what survives that. Including it here would shift every later index.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ReadBatchConsumers_skipsNamespaceSectionsWithoutANamespaceValue()
    {
        var consumers = EventHubsCompatConfig.ReadBatchConsumers(Config("""
        {
          "EventHubs": {
            "unconfigured": { "BatchConsumers": [ { "EventHubName": "ghost" } ] },
            "common": { "Namespace": "kafka", "BatchConsumers": [ { "EventHubName": "alerts" } ] }
          }
        }
        """));

        consumers.Should().ContainSingle().Which.Topic.Should().Be("alerts");
    }

    /// <summary>
    /// Services set <c>Namespace</c> from the environment, so "my consumers vanished" is the
    /// predictable way this goes wrong. The exception has to say so rather than report an empty list.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResolveByIndex_namesTheMissingNamespaceAsTheCause()
    {
        var configuration = Config("""
        {
          "EventHubs": { "common": { "BatchConsumers": [ { "EventHubName": "alerts" } ] } }
        }
        """);

        var act = () => EventHubsCompatConfig.ResolveByIndex(configuration, 0);

        act.Should().Throw<StreamConfigurationException>()
            .WithMessage("*no Namespace value*")
            .WithMessage("*DOTNET_EventHubs__<ns>__Namespace*");
    }

    /// <summary>An out-of-range index lists what was found, so the fix does not need a debugger.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResolveByIndex_outOfRangeListsTheConfiguredTopics()
    {
        var act = () => EventHubsCompatConfig.ResolveByIndex(Config(RealisticEventHubs), 4);

        act.Should().Throw<StreamConfigurationException>()
            .WithMessage("*4 entries*")
            .WithMessage("*[3] customerinfo*");
    }

    /// <summary>A topic-based registration for a topic nobody configured fails at startup, by name.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResolveByTopic_unknownTopicThrows()
    {
        var act = () => EventHubsCompatConfig.ResolveByTopic(Config(RealisticEventHubs), "nope");

        act.Should().Throw<StreamConfigurationException>().WithMessage("*\"nope\"*");
    }

    /// <summary>
    /// Half the migration checklist lives on keys <c>EventHubs:</c> cannot express, so a
    /// <c>Streams:Consumers</c> entry for the same topic replaces the mapped one outright.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamsConsumersEntry_overridesTheMappedEntryForThatTopic()
    {
        var configuration = Config("""
        {
          "EventHubs": {
            "common": {
              "Namespace": "kafka",
              "BatchConsumers": [
                { "EventHubName": "alerts", "ConsumerGroup": "plat_alerts", "BatchSize": 10 },
                { "EventHubName": "transactions", "ConsumerGroup": "plat_alerts", "BatchSize": 10 }
              ]
            }
          },
          "Streams": {
            "Consumers": [
              { "Topic": "transactions", "Consumer": "plat-alerts", "BatchSize": 250, "UseConsumerGroup": true, "OnError": "StopPartition" }
            ]
          }
        }
        """);

        var overridden = EventHubsCompatConfig.ResolveByIndex(configuration, 1);
        overridden.Topic.Should().Be("transactions");
        overridden.BatchSize.Should().Be(250);
        overridden.Consumer.Should().Be("plat-alerts");
        overridden.UseConsumerGroup.Should().BeTrue();
        overridden.OnError.Should().Be(ErrorPolicy.StopPartition);

        var untouched = EventHubsCompatConfig.ResolveByIndex(configuration, 0);
        untouched.Topic.Should().Be("alerts");
        untouched.BatchSize.Should().Be(10, "a topic with no Streams:Consumers entry keeps the mapped one");
    }

    // ---------------------------------------------------------------------------------------
    // P4-01 — registration.
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// The registration a migrating service does not change: four numbered calls, in order, each
    /// landing on the topic its index named.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddBatchConsumerHostedServiceV2_byIndex_resolvesTheConfiguredConsumer()
    {
        var builder = Builder(RealisticEventHubs);

        builder.AddBatchConsumerHostedServiceV2<FirstConsumer>(0);
        builder.AddBatchConsumerHostedServiceV2<SecondConsumer>(1);

        using var host = builder.Build();
        var registrations = host.Services.GetRequiredService<StreamRegistry>().Consumers;

        registrations.Should().HaveCount(2);
        registrations[0].Options.Topic.Should().Be("alerts");
        registrations[0].Options.BatchSize.Should().Be(10);
        registrations[0].Consumer.Should().Be("plat_alerts");
        registrations[0].HandlerType.Should().Be(typeof(CompatBatchHandler<FirstConsumer>));
        registrations[1].Options.Topic.Should().Be("transactions");
        registrations[1].HandlerType.Should().Be(typeof(CompatBatchHandler<SecondConsumer>));
    }

    /// <summary>The topic-based overload resolves by <c>EventHubName</c> rather than by position.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddBatchConsumerHostedServiceV2_byTopic_resolvesTheConfiguredConsumer()
    {
        var builder = Builder(RealisticEventHubs);
        builder.AddBatchConsumerHostedServiceV2<FirstConsumer>("ebets");

        using var host = builder.Build();
        var registration = host.Services.GetRequiredService<StreamRegistry>().Consumers.Should().ContainSingle().Subject;

        registration.Options.Topic.Should().Be("ebets");
        registration.Options.BatchSize.Should().Be(10);
        registration.HandlerType.Should().Be(typeof(CompatBatchHandler<FirstConsumer>));
    }

    /// <summary>
    /// The consumer and its adapter are both resolvable singletons, and the adapter holds the very
    /// instance the container built — the handler is resolved once at startup, never per batch.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AddBatchConsumerHostedServiceV2_registersTheConsumerAndItsAdapterAsSingletons()
    {
        var builder = Builder(RealisticEventHubs);
        builder.AddBatchConsumerHostedServiceV2<RecordingConsumer>("alerts");

        using var host = builder.Build();

        var consumer = host.Services.GetRequiredService<RecordingConsumer>();
        host.Services.GetRequiredService<RecordingConsumer>().Should().BeSameAs(consumer);

        var adapter = host.Services.GetRequiredService<CompatBatchHandler<RecordingConsumer>>();
        host.Services.GetRequiredService<CompatBatchHandler<RecordingConsumer>>().Should().BeSameAs(adapter);
    }

    // ---------------------------------------------------------------------------------------
    // P4-01 — delivery and field mapping.
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// The point of the shim: a registered <see cref="IBatchConsumer"/> is handed an
    /// <see cref="EventMsg"/><c>[]</c> whose every field carries what the handler used to read.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Shim_deliversABatchToTheConsumerWithFieldsMapped()
    {
        var builder = Builder(RealisticEventHubs);
        builder.AddBatchConsumerHostedServiceV2<RecordingConsumer>("alerts");

        using var host = builder.Build();
        var consumer = host.Services.GetRequiredService<RecordingConsumer>();
        var adapter = host.Services.GetRequiredService<CompatBatchHandler<RecordingConsumer>>();

        var body = "{\"id\":1}"u8.ToArray();
        var headers = HeaderBlock.Pack([new KeyValuePair<string, string>("brand", "orange")]);
        var msg = new StreamMsg(
            body,
            "Orange.Models.Alerts.NeoAlert",
            new StreamId(1_757_000_000_123, 4),
            Partition: 2,
            PartitionKey: "cust-7",
            CorrelationId: "corr-9",
            TraceParent: "00-trace-span-01",
            Headers: headers);

        using var cts = new CancellationTokenSource();
        await adapter.HandleAsync(new[] { msg }, cts.Token);

        var delivered = consumer.Batches.Should().ContainSingle().Subject;
        var got = delivered.Should().ContainSingle().Subject;

        got.Type.Should().Be("Orange.Models.Alerts.NeoAlert");
        got.Body.ToArray().Should().Equal(body);
        got.CorrelationId.Should().Be("corr-9");
        got.PartitionKey.Should().Be("cust-7");
        got.OffsetString.Should().Be("1757000000123-4", "the offset is now the Redis stream id");
        got.EnqueuedTime.Should().Be(DateTimeOffset.FromUnixTimeMilliseconds(1_757_000_000_123));
        got.Headers.Should().NotBeNull().And.ContainKey("brand");
        got.Headers!["brand"].Should().Be("orange");
        consumer.Token.Should().Be(cts.Token, "the handler's cancellation token is the host's");
    }

    /// <summary>
    /// A missing type stays <see langword="null"/>, not empty: handlers branch on
    /// <c>msg.Type == null</c> to pick up untyped payloads (plat-alerts' Azure alerts do), and an
    /// empty string there would route them nowhere with nothing in the log.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ToEventMsgs_leavesAnAbsentTypeNullAndAbsentHeadersNull()
    {
        var msg = new StreamMsg("x"u8.ToArray(), string.Empty, new StreamId(7, 0), 0, string.Empty, string.Empty, null, HeaderBlock.Empty);

        var mapped = CompatBatchHandler<RecordingConsumer>.ToEventMsgs([msg]).Should().ContainSingle().Subject;

        mapped.Type.Should().BeNull();
        mapped.Headers.Should().BeNull();
        mapped.CorrelationId.Should().BeEmpty();
        mapped.PartitionKey.Should().BeEmpty();
    }

    /// <summary>
    /// Bodies are copied, not aliased onto the pooled read buffer, so a handler may keep a message
    /// after <c>Consume</c> returns exactly as it could on Kafka. This is the mistake that would
    /// otherwise corrupt data silently and somewhere else entirely.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ToEventMsgs_copiesBodiesSoAConsumerCanRetainThem()
    {
        var buffer = "original"u8.ToArray();
        var msg = new StreamMsg(buffer, "T", new StreamId(1, 0), 0, string.Empty, string.Empty, null, HeaderBlock.Empty);

        var mapped = CompatBatchHandler<RecordingConsumer>.ToEventMsgs([msg])[0];

        // Whatever the transport does with its buffer afterwards must not reach the retained message.
        buffer.AsSpan().Fill((byte)'!');

        Encoding.UTF8.GetString(mapped.Body.Span).Should().Be("original");
    }

    /// <summary>An empty batch still reaches the consumer, as it did on Kafka after filtering.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Shim_deliversAnEmptyBatchRatherThanSkippingTheCall()
    {
        var consumer = new RecordingConsumer();
        var adapter = new CompatBatchHandler<RecordingConsumer>(consumer);

        await adapter.HandleAsync(ReadOnlyMemory<StreamMsg>.Empty, CancellationToken.None);

        consumer.Batches.Should().ContainSingle().Which.Should().BeEmpty();
    }

    private sealed class FirstConsumer : IBatchConsumer
    {
        public Task Consume(EventMsg[] msgs, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private sealed class SecondConsumer : IBatchConsumer
    {
        public Task Consume(EventMsg[] msgs, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    /// <summary>A handler shaped like the ones services already have: constructed by DI, keeps what it is given.</summary>
    private sealed class RecordingConsumer : IBatchConsumer
    {
        public List<EventMsg[]> Batches { get; } = [];

        public CancellationToken Token { get; private set; }

        public Task Consume(EventMsg[] msgs, CancellationToken cancellationToken = default)
        {
            this.Batches.Add(msgs);
            this.Token = cancellationToken;
            return Task.CompletedTask;
        }
    }
}
