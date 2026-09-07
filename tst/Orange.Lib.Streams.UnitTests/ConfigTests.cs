using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Diagnostics;
using Orange.Lib.Streams.Errors;

namespace Orange.Lib.Streams.UnitTests;

/// <summary>
/// Unit tests for <see cref="StreamConfigBinder"/> — every validation rule, the zero-config
/// defaults, and the name sanitisation that keeps <c>CLIENT SETNAME</c> from failing.
/// </summary>
public class ConfigTests
{
    private const string Production = "Production";
    private const string Development = "Development";

    private static IConfiguration FromJson(string json)
        => new ConfigurationBuilder()
            .AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(json)))
            .Build();

    private static StreamConfigurationException Throws(StreamOptions options, string? environment = Production, int syncTimeoutMs = 5000)
        => Assert.Throws<StreamConfigurationException>(
            () => StreamConfigBinder.Validate(options, environment, syncTimeoutMs, logger: null));

    private static StreamOptions WithTopic(string name, TopicOptions topic)
        => new() { Topics = new Dictionary<string, TopicOptions> { [name] = topic } };

    private static StreamOptions WithConsumer(ConsumerOptions consumer)
        => new() { Consumer = "svc", Consumers = [consumer] };

    private static StreamOptions WithProducer(ProducerOptions producer)
        => new() { Producers = [producer] };

    // ---------------------------------------------------------------- defaults

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Bind_EmptyStreamsSection_YieldsAllDefaults()
    {
        var options = StreamConfigBinder.Bind(FromJson("""{ "Streams": { } }"""));

        options.Should().NotBeNull();
        options.ConnectionString.Should().BeNull();
        options.Consumer.Should().BeNull();
        options.Topics.Should().BeEmpty();
        options.Consumers.Should().BeEmpty();
        options.Producers.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Bind_MissingStreamsSection_YieldsAllDefaults()
    {
        var options = StreamConfigBinder.Bind(FromJson("""{ "Logging": { "LogLevel": { "Default": "Information" } } }"""));

        options.Should().NotBeNull();
        options.ConnectionString.Should().BeNull();
        options.Topics.Should().BeEmpty();
        options.Consumers.Should().BeEmpty();
        options.Producers.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Bind_EmptyAndMissingSection_ValidateCleanly()
    {
        foreach (var json in new[] { """{ "Streams": { } }""", "{ }" })
        {
            var options = StreamConfigBinder.Bind(FromJson(json));
            var act = () => StreamConfigBinder.Validate(options, Production, 5000, logger: null);
            act.Should().NotThrow();
        }
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Defaults_MatchDocumentedValues()
    {
        var topic = new TopicOptions();
        topic.Partitions.Should().Be(1);
        topic.MaxLen.Should().Be(10_000);
        topic.Trim.Should().Be(TrimMode.Approx);
        topic.RetentionSeconds.Should().BeNull();
        topic.CoLocatePartitions.Should().BeTrue();
        topic.ClampReleaseThreshold.Should().Be(0.8);

        var consumer = new ConsumerOptions();
        consumer.BatchSize.Should().Be(100);
        consumer.StartFrom.Should().Be(StartFrom.Stored);
        consumer.Persist.Should().Be(PersistMode.AsyncBatch);
        consumer.PersistIntervalMs.Should().Be(1000);
        consumer.ReadMode.Should().Be(ReadMode.Block);
        consumer.BlockMs.Should().Be(1000);
        consumer.OnError.Should().Be(ErrorPolicy.BestEffort);
        consumer.Instances.Should().BeNull();
        consumer.StartFromWhenMissing.Should().Be(StartFrom.Beginning);
        consumer.Backpressure.Enabled.Should().BeTrue();
        consumer.Backpressure.Capacity.Should().Be(4);

        var producer = new ProducerOptions();
        producer.Buffered.Should().BeFalse();
        producer.MaxWaitMs.Should().Be(20);
        producer.MaxBatch.Should().Be(500);
        producer.MaxQueue.Should().Be(100_000);
        producer.DropOldest.Should().BeFalse();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResolveTopic_UnconfiguredTopic_ReturnsDefaultsNotAnError()
    {
        var resolved = StreamConfigBinder.ResolveTopic(new StreamOptions(), "feed_source_racing", logger: null);

        resolved.Should().BeEquivalentTo(new TopicOptions());
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResolveConnectionString_Unset_UsesRedisDbDefault()
    {
        StreamConfigBinder.ResolveConnectionString(new StreamOptions())
            .Should().Be(StreamConfigBinder.DefaultConnectionString);

        StreamConfigBinder.ResolveConnectionString(new StreamOptions { ConnectionString = "   " })
            .Should().Be(StreamConfigBinder.DefaultConnectionString);

        StreamConfigBinder.ResolveConnectionString(new StreamOptions { ConnectionString = "localhost:6379" })
            .Should().Be("localhost:6379");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResolveSyncTimeoutMs_DefaultsAndParses()
    {
        StreamConfigBinder.ResolveSyncTimeoutMs(null).Should().Be(StreamConfigBinder.DefaultSyncTimeoutMs);
        StreamConfigBinder.ResolveSyncTimeoutMs("localhost:6379,syncTimeout=9000").Should().Be(9000);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResolveConsumerName_PrefersEntryThenRoot()
    {
        var options = new StreamOptions { Consumer = "root-consumer" };

        StreamConfigBinder.ResolveConsumerName(options, new ConsumerOptions { Topic = "t", Consumer = "own" })
            .Should().Be("own");

        StreamConfigBinder.ResolveConsumerName(options, new ConsumerOptions { Topic = "t" })
            .Should().Be("root-consumer");

        StreamConfigBinder.ResolveConsumerName(new StreamOptions(), new ConsumerOptions { Topic = "t" })
            .Should().NotBeNullOrWhiteSpace();
    }

    // ---------------------------------------------------------------- topic rules

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_NullTopicEntry_ThrowsNamingKey()
        => Throws(WithTopic("orders", null!)).Message.Should().Contain("Streams:Topics:orders");

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [Trait("TestType", "UnitTest")]
    public void Validate_TopicPartitionsBelowOne_ThrowsNamingKey(int partitions)
        => Throws(WithTopic("orders", new TopicOptions { Partitions = partitions }))
            .Message.Should().Contain("Streams:Topics:orders:Partitions");

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_TopicRetentionBelowOne_ThrowsNamingKey()
        => Throws(WithTopic("orders", new TopicOptions { RetentionSeconds = 0 }))
            .Message.Should().Contain("Streams:Topics:orders:RetentionSeconds");

    /// <summary>
    /// An explicit <c>MaxLen: 0</c> — a ceiling of zero, which is not a ceiling.
    /// </summary>
    /// <remarks>
    /// This used to be the <em>only</em> way the retention pairing rule could fire, and it was
    /// written as though it covered "RetentionSeconds without MaxLen": with a non-nullable
    /// <c>long MaxLen = 10_000</c>, an omitted key was indistinguishable from a configured 10,000, so
    /// the rule the plan actually asked for was unreachable and this test proved a different one.
    /// The omitted-key case now lives in
    /// <see cref="ConfigBindingTests.Validate_RetentionWithoutMaxLen_ThrowsNamingBothKeys"/>, bound
    /// from JSON where the distinction is real (R-17).
    /// </remarks>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_TopicRetentionWithMaxLenZero_ThrowsNamingBothKeys()
    {
        var message = Throws(WithTopic("orders", new TopicOptions { RetentionSeconds = 60, MaxLen = 0 })).Message;

        message.Should().Contain("Streams:Topics:orders:RetentionSeconds");
        message.Should().Contain("Streams:Topics:orders:MaxLen");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_TopicRetentionWithNoMaxLenAtAll_ThrowsNamingBothKeys()
    {
        var message = Throws(WithTopic("orders", new TopicOptions { RetentionSeconds = 60 })).Message;

        message.Should().Contain("Streams:Topics:orders:RetentionSeconds");
        message.Should().Contain("Streams:Topics:orders:MaxLen");
        message.Should().Contain("not configured");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_TrimNoneOutsideDevelopment_ThrowsNamingKeyAndEnvironment()
    {
        var message = Throws(WithTopic("orders", new TopicOptions { Trim = TrimMode.None })).Message;

        message.Should().Contain("Streams:Topics:orders:Trim");
        message.Should().Contain(Production);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_TrimNoneInDevelopment_IsAllowed()
    {
        var options = WithTopic("orders", new TopicOptions { Trim = TrimMode.None });
        var act = () => StreamConfigBinder.Validate(options, Development, 5000, logger: null);

        act.Should().NotThrow();
    }

    // ---------------------------------------------------------------- consumer rules

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_NullConsumerEntry_ThrowsNamingKey()
        => Throws(WithConsumer(null!)).Message.Should().Contain("Streams:Consumers[0]");

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [Trait("TestType", "UnitTest")]
    public void Validate_ConsumerWithoutTopic_ThrowsNamingKey(string? topic)
        => Throws(WithConsumer(new ConsumerOptions { Topic = topic! }))
            .Message.Should().Contain("Streams:Consumers[0]:Topic");

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    [Trait("TestType", "UnitTest")]
    public void Validate_ConsumerBatchSizeBelowOne_ThrowsNamingKey(int batchSize)
        => Throws(WithConsumer(new ConsumerOptions { Topic = "orders", BatchSize = batchSize }))
            .Message.Should().Contain("Streams:Consumers[0]:BatchSize");

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_NullBackpressure_ThrowsNamingKey()
        => Throws(WithConsumer(new ConsumerOptions { Topic = "orders", Backpressure = null! }))
            .Message.Should().Contain("Streams:Consumers[0]:Backpressure");

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_BackpressureCapacityBelowOne_ThrowsNamingKey()
        => Throws(WithConsumer(new ConsumerOptions { Topic = "orders", Backpressure = new BackpressureOptions { Capacity = 0 } }))
            .Message.Should().Contain("Streams:Consumers[0]:Backpressure:Capacity");

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_StartFromDateWithoutDate_ThrowsNamingKey()
    {
        var message = Throws(WithConsumer(new ConsumerOptions { Topic = "orders", StartFrom = StartFrom.Date })).Message;

        message.Should().Contain("Streams:Consumers[0]:StartFrom");
        message.Should().Contain("Streams:Consumers[0]:StartFromDate");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_StartFromDateWithDate_IsAllowed()
    {
        var options = WithConsumer(new ConsumerOptions
        {
            Topic = "orders",
            StartFrom = StartFrom.Date,
            StartFromDate = DateTimeOffset.UnixEpoch,
        });

        var act = () => StreamConfigBinder.Validate(options, Production, 5000, logger: null);
        act.Should().NotThrow();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_PersistNoneWithStartFromStored_ThrowsNamingBothKeys()
    {
        var message = Throws(WithConsumer(new ConsumerOptions
        {
            Topic = "orders",
            Persist = PersistMode.None,
            StartFrom = StartFrom.Stored,
        })).Message;

        message.Should().Contain("Streams:Consumers[0]:Persist");
        message.Should().Contain("Streams:Consumers[0]:StartFrom");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_DuplicateTopicConsumerPair_ThrowsNamingBothEntries()
    {
        var options = new StreamOptions
        {
            Consumer = "svc",
            Consumers =
            [
                new ConsumerOptions { Topic = "orders" },
                new ConsumerOptions { Topic = "orders" },
            ],
        };

        var message = Throws(options).Message;

        message.Should().Contain("Streams:Consumers[1]");
        message.Should().Contain("Streams:Consumers[0]");
        message.Should().Contain("orders");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_SameTopicDistinctConsumerNames_IsAllowed()
    {
        var options = new StreamOptions
        {
            Consumer = "svc",
            Consumers =
            [
                new ConsumerOptions { Topic = "orders", Consumer = "a" },
                new ConsumerOptions { Topic = "orders", Consumer = "b" },
            ],
        };

        var act = () => StreamConfigBinder.Validate(options, Production, 5000, logger: null);
        act.Should().NotThrow();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_BlockMsAtOrAboveSyncTimeout_ThrowsNamingKey()
    {
        var message = Throws(
            WithConsumer(new ConsumerOptions { Topic = "orders", ReadMode = ReadMode.Block, BlockMs = 5000 }),
            syncTimeoutMs: 5000).Message;

        message.Should().Contain("Streams:Consumers[0]:BlockMs");
        message.Should().Contain("syncTimeout");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_BlockMsIgnoredInPollMode()
    {
        var options = WithConsumer(new ConsumerOptions { Topic = "orders", ReadMode = ReadMode.Poll, BlockMs = 60_000 });
        var act = () => StreamConfigBinder.Validate(options, Production, 5000, logger: null);

        act.Should().NotThrow();
    }

    // ---------------------------------------------------------------- instance rules

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_InstanceCountBelowOne_ThrowsNamingKey()
        => Throws(WithConsumer(new ConsumerOptions { Topic = "orders", Instances = new InstanceOptions { Count = 0 } }))
            .Message.Should().Contain("Streams:Consumers[0]:Instances:Count");

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_InstanceIndexNegative_ThrowsNamingKey()
        => Throws(WithConsumer(new ConsumerOptions { Topic = "orders", Instances = new InstanceOptions { Index = -1 } }))
            .Message.Should().Contain("Streams:Consumers[0]:Instances:Index");

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_InstanceIndexNotLessThanCount_ThrowsNamingBothKeys()
    {
        var message = Throws(WithConsumer(new ConsumerOptions
        {
            Topic = "orders",
            Instances = new InstanceOptions { Count = 2, Index = 2 },
        })).Message;

        message.Should().Contain("Streams:Consumers[0]:Instances:Index");
        message.Should().Contain("Streams:Consumers[0]:Instances:Count");
    }

    // ---------------------------------------------------------------- producer rules

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_NullProducerEntry_ThrowsNamingKey()
        => Throws(WithProducer(null!)).Message.Should().Contain("Streams:Producers[0]");

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    [Trait("TestType", "UnitTest")]
    public void Validate_ProducerWithoutTopic_ThrowsNamingKey(string? topic)
        => Throws(WithProducer(new ProducerOptions { Topic = topic! }))
            .Message.Should().Contain("Streams:Producers[0]:Topic");

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_ProducerMaxBatchBelowOne_ThrowsNamingKey()
        => Throws(WithProducer(new ProducerOptions { Topic = "orders", MaxBatch = 0 }))
            .Message.Should().Contain("Streams:Producers[0]:MaxBatch");

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_ProducerMaxQueueBelowOne_ThrowsNamingKey()
        => Throws(WithProducer(new ProducerOptions { Topic = "orders", MaxQueue = 0 }))
            .Message.Should().Contain("Streams:Producers[0]:MaxQueue");

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResolveSyncTimeoutMs_UnparseableConnectionString_ThrowsNamingKey()
    {
        var ex = Assert.Throws<StreamConfigurationException>(
            () => StreamConfigBinder.ResolveSyncTimeoutMs("localhost:6379,syncTimeout=not-a-number"));

        ex.Message.Should().Contain("Streams:ConnectionString");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Validate_NullOptions_Throws()
        => Assert.Throws<ArgumentNullException>(() => StreamConfigBinder.Validate(null!, Production, 5000, null));

    // ---------------------------------------------------------------- name sanitisation

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamNames_RenderTemplatesAsSpecified()
    {
        StreamNames.ReaderThreadName("bet-processing", 2).Should().Be("streams-rd:bet-processing:i2");
        StreamNames.ReaderClientName("svc", "cons", 1, "svc-0").Should().Be("streams-rd:svc:cons:i1:svc-0");
        StreamNames.SharedClientName("svc", "svc-0").Should().Be("streams:svc:svc-0");
        StreamNames.ConsumerHostTaskName("cons").Should().Be("streams-host:cons");
    }

    [Theory]
    [InlineData("bad name")]
    [InlineData("bad\nname")]
    [InlineData("bad\r\nname")]
    [InlineData("bad\tname")]
    [InlineData("bad namewith bell")]
    [InlineData("naïve/consumer name")]
    [Trait("TestType", "UnitTest")]
    public void StreamNames_SanitiseSpacesAndControlCharacters(string dirty)
    {
        var names = new[]
        {
            StreamNames.ReaderThreadName(dirty, 0),
            StreamNames.ReaderClientName(dirty, dirty, 0, dirty),
            StreamNames.SharedClientName(dirty, dirty),
            StreamNames.ConsumerHostTaskName(dirty),
        };

        foreach (var name in names)
        {
            AssertSetNameSafe(name);
        }
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamNames_TruncateOverLongComponents()
    {
        var huge = new string('x', 4096);

        var names = new[]
        {
            StreamNames.ReaderThreadName(huge, 3),
            StreamNames.ReaderClientName(huge, huge, 3, huge),
            StreamNames.SharedClientName(huge, huge),
            StreamNames.ConsumerHostTaskName(huge),
        };

        foreach (var name in names)
        {
            AssertSetNameSafe(name);
            Encoding.UTF8.GetByteCount(name).Should().BeLessThanOrEqualTo(256);
        }
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamNames_EmptyComponentsBecomeUnknown()
    {
        StreamNames.SharedClientName(string.Empty, string.Empty).Should().Be("streams:unknown:unknown");
        StreamNames.ConsumerHostTaskName(string.Empty).Should().Be("streams-host:unknown");
        AssertSetNameSafe(StreamNames.ReaderClientName(string.Empty, string.Empty, 0, string.Empty));
    }

    /// <summary>
    /// Redis rejects a CLIENT SETNAME argument containing a space or a newline, so the produced name
    /// must contain only the safe alphabet and must be non-empty.
    /// </summary>
    private static void AssertSetNameSafe(string name)
    {
        name.Should().NotBeNullOrEmpty();
        name.Should().MatchRegex("^[A-Za-z0-9._:-]+$");
        name.Should().NotContain(" ");
        name.Should().NotContain("\n");
        name.Should().NotContain("\r");
        Encoding.UTF8.GetByteCount(name).Should().BeLessThanOrEqualTo(256);
    }
}
