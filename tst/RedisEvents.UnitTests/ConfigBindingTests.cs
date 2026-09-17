using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using RedisEvents.Config;
using RedisEvents.Errors;

namespace RedisEvents.UnitTests;

/// <summary>
/// R-17. Two things the existing <see cref="ConfigTests"/> could not have caught, because every one
/// of its binding tests binds an <em>empty</em> <c>Streams</c> section and every validation test
/// hand-constructs the options object:
///
/// <list type="number">
/// <item><description>
/// <b>Nothing bound at all.</b> The option records were <c>init</c>-only. The source-generated
/// configuration binder assigns members on an already-constructed instance and cannot assign an
/// <c>init</c>-only property — and it says nothing when it skips one. The generated
/// <c>BindCore(…, ref TopicOptions …)</c> was a single call that validated the key names and set
/// no value, so <c>Streams:ConnectionString</c>, every <c>Streams:Topics:*</c> entry and every
/// <c>Streams:Consumers[n]</c> field was read from appsettings and thrown away. A service could
/// configure 8 partitions, a 5-minute retention and a Poll reader and get 2 partitions, no
/// retention and a blocking reader, silently.
/// </description></item>
/// <item><description>
/// <b>Keys that bind and do nothing.</b> Each is now refused with a message naming it.
/// </description></item>
/// </list>
/// </summary>
public class ConfigBindingTests
{
    private const string Production = "Production";

    /// <summary>Every key in the section, each set to something that is not its default.</summary>
    private const string FullSection = """
    {
      "Streams": {
        "ConnectionString": "localhost:6380,syncTimeout=9000",
        "Consumer": "svc-wide-name",
        "Topics": {
          "orders": {
            "Partitions": 4,
            "MaxLen": 250000,
            "Trim": "Exact",
            "RetentionSeconds": 3600,
            "BackgroundTrimIntervalSeconds": 60,
            "CoLocatePartitions": false,
            "ClampReleaseThreshold": 0.5,
            "StateMetadata": true
          }
        },
        "Consumers": [
          {
            "Topic": "orders",
            "Consumer": "orders-reader",
            "BatchSize": 250,
            "Filter": [ "OrderPlaced", "OrderCancelled" ],
            "StartFrom": "Date",
            "StartFromDate": "2026-01-02T03:04:05+00:00",
            "Persist": "SyncBatch",
            "PersistIntervalMs": 250,
            "Backpressure": { "Enabled": false, "Capacity": 9 },
            "ReadMode": "Poll",
            "BlockMs": 750,
            "MaxIdleDelayMs": 25,
            "OnError": "StopPartition",
            "UnhealthyBlockSeconds": 90,
            "Instances": { "Count": 3, "Index": 1 },
            "ReaderThreads": 2,
            "ShutdownTimeoutSeconds": 30,
            "UnhealthyLagMs": 45000,
            "StartFromWhenMissing": "Now"
          }
        ],
        "Producers": [
          {
            "Topic": "orders",
            "Buffered": true,
            "MaxWaitMs": 5,
            "MaxBatch": 1000,
            "MaxQueue": 250,
            "DropOldest": true
          }
        ]
      }
    }
    """;

    private static IConfiguration FromJson(string json)
        => new ConfigurationBuilder()
            .AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(json)))
            .Build();

    private static StreamConfigurationException Throws(string json)
        => Assert.Throws<StreamConfigurationException>(
            () => StreamConfigBinder.Validate(StreamConfigBinder.Bind(FromJson(json)), Production, 5000, logger: null));

    // ---------------------------------------------------------------- binding actually binds

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Bind_RootKeys_AreAssigned()
    {
        var options = StreamConfigBinder.Bind(FromJson(FullSection));

        options.ConnectionString.Should().Be("localhost:6380,syncTimeout=9000");
        options.Consumer.Should().Be("svc-wide-name");
        options.Topics.Should().ContainKey("orders");
        options.Consumers.Should().HaveCount(1);
        options.Producers.Should().HaveCount(1);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Bind_TopicKeys_AreAssigned()
    {
        var topic = StreamConfigBinder.Bind(FromJson(FullSection)).Topics["orders"];

        topic.Partitions.Should().Be(4);
        topic.MaxLen.Should().Be(250_000);
        topic.Trim.Should().Be(TrimMode.Exact);
        topic.RetentionSeconds.Should().Be(3600);
        topic.BackgroundTrimIntervalSeconds.Should().Be(60);
        topic.CoLocatePartitions.Should().BeFalse();
        topic.ClampReleaseThreshold.Should().Be(0.5);
        topic.StateMetadata.Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Bind_ConsumerKeys_AreAssigned()
    {
        var consumer = StreamConfigBinder.Bind(FromJson(FullSection)).Consumers[0];

        consumer.Topic.Should().Be("orders");
        consumer.Consumer.Should().Be("orders-reader");
        consumer.BatchSize.Should().Be(250);
        consumer.Filter.Should().BeEquivalentTo(["OrderPlaced", "OrderCancelled"]);
        consumer.StartFrom.Should().Be(StartFrom.Date);
        consumer.StartFromDate.Should().Be(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero));
        consumer.Persist.Should().Be(PersistMode.SyncBatch);
        consumer.PersistIntervalMs.Should().Be(250);
        consumer.Backpressure.Enabled.Should().BeFalse();
        consumer.Backpressure.Capacity.Should().Be(9);
        consumer.ReadMode.Should().Be(ReadMode.Poll);
        consumer.BlockMs.Should().Be(750);
        consumer.MaxIdleDelayMs.Should().Be(25);
        consumer.OnError.Should().Be(ErrorPolicy.StopPartition);
        consumer.UnhealthyBlockSeconds.Should().Be(90);
        consumer.ReaderThreads.Should().Be(2);
        consumer.ShutdownTimeoutSeconds.Should().Be(30);
        consumer.UnhealthyLagMs.Should().Be(45_000);
        consumer.StartFromWhenMissing.Should().Be(StartFrom.Now);
        consumer.Instances.Should().NotBeNull();
        consumer.Instances!.Count.Should().Be(3);
        consumer.Instances.Index.Should().Be(1);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Bind_ProducerKeys_AreAssigned()
    {
        var producer = StreamConfigBinder.Bind(FromJson(FullSection)).Producers[0];

        producer.Topic.Should().Be("orders");
        producer.Buffered.Should().BeTrue();
        producer.MaxWaitMs.Should().Be(5);
        producer.MaxBatch.Should().Be(1000);
        producer.MaxQueue.Should().Be(250);
        producer.DropOldest.Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Bind_FullSection_ValidatesCleanly()
    {
        var options = StreamConfigBinder.Bind(FromJson(FullSection));

        var act = () => StreamConfigBinder.Validate(options, Production, 5000, logger: null);

        act.Should().NotThrow("a fully populated section that binds must also be a legal one");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Bind_KeysNotPresent_KeepTheirDefaults()
    {
        var options = StreamConfigBinder.Bind(FromJson("""
        { "Streams": { "Topics": { "orders": { "Partitions": 8 } } } }
        """));

        var topic = options.Topics["orders"];
        topic.Partitions.Should().Be(8);
        topic.MaxLen.Should().Be(TopicOptions.DefaultMaxLen, "an absent key must not overwrite the record default");
        topic.Trim.Should().Be(TrimMode.Approx);
        topic.CoLocatePartitions.Should().BeTrue();
    }

    // ---------------------------------------------------------------- MaxLen: configured vs defaulted

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Bind_MaxLen_TracksWhetherItWasConfigured()
    {
        var configured = StreamConfigBinder.Bind(FromJson("""
        { "Streams": { "Topics": { "orders": { "MaxLen": 10000 } } } }
        """)).Topics["orders"];

        var defaulted = StreamConfigBinder.Bind(FromJson("""
        { "Streams": { "Topics": { "orders": { "Partitions": 2 } } } }
        """)).Topics["orders"];

        configured.MaxLen.Should().Be(TopicOptions.DefaultMaxLen);
        defaulted.MaxLen.Should().Be(TopicOptions.DefaultMaxLen);

        configured.MaxLenConfigured.Should().BeTrue(
            "the two are indistinguishable by value — an operator who writes the default explicitly has still chosen a ceiling");
        defaulted.MaxLenConfigured.Should().BeFalse();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_RetentionWithoutMaxLen_ThrowsNamingBothKeys()
    {
        var message = Throws("""
        { "Streams": { "Topics": { "orders": { "RetentionSeconds": 604800 } } } }
        """).Message;

        message.Should().Contain("Streams:Topics:orders:RetentionSeconds");
        message.Should().Contain("Streams:Topics:orders:MaxLen");
        message.Should().Contain("not configured");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_RetentionWithMaxLen_IsAccepted()
    {
        var options = StreamConfigBinder.Bind(FromJson("""
        { "Streams": { "Topics": { "orders": { "RetentionSeconds": 604800, "MaxLen": 5000000 } } } }
        """));

        var act = () => StreamConfigBinder.Validate(options, Production, 5000, logger: null);

        act.Should().NotThrow();
    }

    // ---------------------------------------------------------------- keys that bound and did nothing

    /// <summary>
    /// P4-20 wired <c>Mode</c> through to the consumer host, so declaring it is no longer refused —
    /// the R-17 refusal existed only because the key bound and did nothing.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_LeaseMode_IsAcceptedNowThatTheHostImplementsIt()
    {
        var options = StreamConfigBinder.Bind(FromJson("""
        {
          "Streams": {
            "Consumers": [ { "Topic": "orders", "Instances": { "Mode": "Lease" } } ]
          }
        }
        """));

        var act = () => StreamConfigBinder.Validate(options, Production, 5000, logger: null);

        act.Should().NotThrow("Lease mode claims its partitions from the ownership registry, and the host now builds that registry");
    }

    /// <summary>
    /// The other half of R-17's rule, from the Lease side: <c>Count</c> and <c>Index</c> are read by
    /// nothing in Lease mode, so declaring a pool size that has no effect is refused rather than
    /// ignored.
    /// </summary>
    [Theory]
    [InlineData("\"Count\": 2")]
    [InlineData("\"Index\": 0")]
    [InlineData("\"Count\": 2, \"Index\": 1")]
    [Trait("TestType", "UnitTest")]
    public void Validate_LeaseMode_WithACountOrIndex_IsRefused(string keys)
    {
        var message = Throws($$"""
        {
          "Streams": {
            "Consumers": [ { "Topic": "orders", "Instances": { "Mode": "Lease", {{keys}} } } ]
          }
        }
        """).Message;

        message.Should().Contain("Streams:Consumers[0]:Instances:Mode is Lease");
        message.Should().Contain("Streams:Consumers[0]:Instances:Count");
        message.Should().Contain("Streams:Consumers[0]:Instances:Index");
    }

    [Theory]
    [InlineData("\"LeaseTtlSeconds\": 120, \"LeaseRenewSeconds\": 45")]
    [InlineData("\"LeaseTtlSeconds\": 120")]
    [InlineData("\"LeaseRenewSeconds\": 5")]
    [Trait("TestType", "UnitTest")]
    public void Validate_LeaseTimings_AreAcceptedBecauseTheHostPassesThemThrough(string keys)
    {
        var options = StreamConfigBinder.Bind(FromJson($$"""
        {
          "Streams": {
            "Consumers": [ { "Topic": "orders", "Instances": { {{keys}} } } ]
          }
        }
        """));

        var act = () => StreamConfigBinder.Validate(options, Production, 5000, logger: null);

        act.Should().NotThrow("the consumer host hands both timings to OwnershipRegistryOptions in either mode");
    }

    /// <summary>
    /// A renew interval at or past the TTL means every claim expires between renewals — the one
    /// timing combination that cannot work, in either mode.
    /// </summary>
    [Theory]
    [InlineData("\"LeaseRenewSeconds\": 45")]
    [InlineData("\"LeaseTtlSeconds\": 10, \"LeaseRenewSeconds\": 10")]
    [Trait("TestType", "UnitTest")]
    public void Validate_LeaseRenewAtOrPastTheTtl_IsRefused(string keys)
    {
        var message = Throws($$"""
        {
          "Streams": {
            "Consumers": [ { "Topic": "orders", "Instances": { {{keys}} } } ]
          }
        }
        """).Message;

        message.Should().Contain("Streams:Consumers[0]:Instances:LeaseRenewSeconds");
        message.Should().Contain("Streams:Consumers[0]:Instances:LeaseTtlSeconds");
    }

    [Theory]
    [InlineData("\"LeaseTtlSeconds\": 0", "LeaseTtlSeconds")]
    [InlineData("\"LeaseRenewSeconds\": 0", "LeaseRenewSeconds")]
    [Trait("TestType", "UnitTest")]
    public void Validate_LeaseTimingsBelowOneSecond_AreRefused(string key, string expected)
    {
        var message = Throws($$"""
        {
          "Streams": {
            "Consumers": [ { "Topic": "orders", "Instances": { {{key}} } } ]
          }
        }
        """).Message;

        message.Should().Contain($"Streams:Consumers[0]:Instances:{expected}");
        message.Should().Contain("at least 1 second");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_LeaseTimingsAtTheirDefaults_AreAccepted()
    {
        var options = StreamConfigBinder.Bind(FromJson("""
        {
          "Streams": {
            "Consumers": [
              { "Topic": "orders", "Instances": { "Count": 2, "Index": 0, "LeaseTtlSeconds": 30, "LeaseRenewSeconds": 10 } }
            ]
          }
        }
        """));

        var act = () => StreamConfigBinder.Validate(options, Production, 5000, logger: null);

        act.Should().NotThrow("30s/10s is what the ownership registry claims and renews on, in Static mode too");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_ResetOnStart_IsRefusedBecauseNothingReadsIt()
    {
        var message = Throws("""
        {
          "Streams": {
            "Consumers": [ { "Topic": "orders", "ResetOnStart": true, "StartFrom": "Date", "StartFromDate": "2026-01-01T00:00:00Z" } ]
          }
        }
        """).Message;

        message.Should().Contain("Streams:Consumers[0]:ResetOnStart");
        message.Should().Contain("ResetPositionAsync");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_ResetOnStartFalse_IsAccepted()
    {
        var options = StreamConfigBinder.Bind(FromJson("""
        { "Streams": { "Consumers": [ { "Topic": "orders", "ResetOnStart": false } ] } }
        """));

        var act = () => StreamConfigBinder.Validate(options, Production, 5000, logger: null);

        act.Should().NotThrow();
    }

    // ---------------------------------------------------------------- the advisory rules need a logger

    /// <summary>
    /// The "Block mode without co-located partitions opens one connection per partition" warning is
    /// implemented in the binder (contrary to the P2 row in 12-remediation.md, which reads it as
    /// missing) — it had simply never been printed, because the only production caller passed
    /// <c>logger: null</c>.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_BlockWithoutCoLocation_WarnsWhenGivenALogger()
    {
        var options = StreamConfigBinder.Bind(FromJson("""
        {
          "Streams": {
            "Topics": { "orders": { "Partitions": 8, "CoLocatePartitions": false } },
            "Consumers": [ { "Topic": "orders", "ReadMode": "Block" } ]
          }
        }
        """));

        var logger = new CapturingLogger();
        StreamConfigBinder.Validate(options, Production, 5000, logger);

        logger.Lines.Should().ContainSingle(line => line.Level == LogLevel.Warning)
            .Which.Message.Should().Contain("dedicated Redis connections");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_UnconfiguredTopic_LogsTheDefaultsItWillRunOn()
    {
        var options = StreamConfigBinder.Bind(FromJson("""
        { "Streams": { "Consumers": [ { "Topic": "orders" } ] } }
        """));

        var logger = new CapturingLogger();
        StreamConfigBinder.Validate(options, Production, 5000, logger);

        logger.Lines.Should().Contain(line =>
            line.Level == LogLevel.Information && line.Message.Contains("is not present in Streams:Topics"));
    }

    /// <summary>
    /// The advisor is what makes the two tests above true of a running service rather than only of a
    /// unit test: it re-runs validation once the host has a real logger.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Advisor_EmitsTheAdvisoryLinesAtStartup()
    {
        var options = StreamConfigBinder.Bind(FromJson("""
        {
          "Streams": {
            "Topics": { "orders": { "Partitions": 8, "CoLocatePartitions": false } },
            "Consumers": [ { "Topic": "orders" } ]
          }
        }
        """));

        var logger = new CapturingLogger();
        var advisor = new StreamsConfigurationAdvisor(options, Production, logger);

        await advisor.StartAsync(TestContext.Current.CancellationToken);

        logger.Lines.Should().Contain(line => line.Level == LogLevel.Warning);

        await advisor.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Advisor_WithoutALogger_DoesNothing()
    {
        var advisor = new StreamsConfigurationAdvisor(new StreamOptions(), Production, logger: null);

        var act = async () => await advisor.StartAsync(TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync();
    }

    /// <summary>Records what an operator would have seen in the log.</summary>
    private sealed class CapturingLogger : ILogger
    {
        internal List<(LogLevel Level, string Message)> Lines { get; } = [];

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
            ArgumentNullException.ThrowIfNull(formatter);
            this.Lines.Add((logLevel, formatter(state, exception)));
        }
    }
}
