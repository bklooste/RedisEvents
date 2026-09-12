using System.Globalization;
using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using RedisEvents.Admin;
using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Diagnostics;
using RedisEvents.Errors;
using RedisEvents.Extensions;
using RedisEvents.Producer;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>
/// Topic topology against real Redis: auto-create (S2), the refusal to shrink a topic (S3), and a
/// partition increase over an undrained backlog (S3b).
/// </summary>
/// <remarks>
/// <para>
/// These three cases are the whole of <c>EnsureTopicAsync</c>'s contract, and none of them can be
/// proved without a server. Auto-create exists because <c>XLEN</c>, <c>XINFO</c> and <c>XREAD</c> on
/// a key that has never been written behave differently from the same commands on an empty stream —
/// that difference is a Redis behaviour, not a library one, so a fake would prove nothing.
/// </para>
/// <para>
/// The pair S3/S3b is the asymmetry that makes the topology safe: increasing keeps every old
/// partition in range so its backlog still drains (lossless, at the cost of a one-off ordering
/// break), while decreasing would push partitions out of range and orphan whatever is unprocessed on
/// them (silent loss). Hence "never below the recorded count".
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class TopicLifecycleTests(RedisStreamsFixture fixture)
{
    /// <summary>S2 — a topic nobody has ever published to is consumable, empty, and lags zero.</summary>
    /// <remarks>
    /// The failure this guards against is not an exception in library code: it is Redis answering
    /// <c>ERR no such key</c> to <c>XINFO STREAM</c> and returning an error rather than zero, which
    /// would turn a brand-new topic's first health check into a false alarm. That is why
    /// <c>EnsureTopicAsync</c> creates each partition stream empty via the
    /// <c>XGROUP CREATE … MKSTREAM</c> / <c>XGROUP DESTROY</c> pair.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S2_consuming_a_never_published_topic_is_empty_not_an_error()
    {
        await fixture.FlushAllAsync();

        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var topicOptions = new TopicOptions { Partitions = 4 };
        var handler = new TestHandler();
        var log = new CapturingLogger();

        await using var host = NewHost(topic, consumer, topicOptions, handler, log);

        // The assertion is that this does not throw: nothing has ever written to this topic.
        await host.StartAsync(CancellationToken.None);

        host.OwnedPartitions.Should().Equal(0, 1, 2, 3);

        for (var partition = 0; partition < topicOptions.Partitions; partition++)
        {
            var key = StreamKeys.Stream(topic, partition, topicOptions.CoLocatePartitions);

            (await fixture.Db.KeyExistsAsync(key))
                .Should().BeTrue($"EnsureTopicAsync creates partition {partition} empty so XLEN and XINFO answer instead of erroring");
            (await fixture.Db.StreamLengthAsync(key)).Should().Be(0);
        }

        // The scratch consumer group used to force the stream into existence must not be left behind.
        var groups = await fixture.Db.StreamGroupInfoAsync(StreamKeys.Stream(topic, 0, topicOptions.CoLocatePartitions));
        groups.Should().BeEmpty("the create/destroy trick destroys the scratch group it created");

        // Lag on an untouched topic is zero, not unknown and not an error. The sampler is what reads
        // it, and reading it is the part that would blow up without the auto-create above.
        var monitors = TrackPartitions(topic, consumer, topicOptions);
        try
        {
            var sampler = new StreamLagSampler(fixture.Redis);
            await sampler.SampleOnceAsync(CancellationToken.None);

            monitors.Should().AllSatisfy(m =>
            {
                m.LagEntries.Should().Be(0, "an empty stream has nothing behind the tail");
                m.LagMs.Should().Be(0);
            });
        }
        finally
        {
            foreach (var monitor in monitors)
            {
                StreamLag.Forget(monitor);
            }
        }

        // Let the read loops sit on the empty streams for a beat and confirm nothing is invented.
        await RedisStreamsFixture.WaitUntilAsync(
            () => host.IsRunning,
            TimeSpan.FromSeconds(5),
            "the host to be running");

        handler.Count.Should().Be(0, "there is nothing on the topic to deliver");
        log.Errors.Should().BeEmpty();

        await host.StopAsync(CancellationToken.None);
    }

    /// <summary>S3 — a host configured below the recorded partition count refuses to start.</summary>
    /// <remarks>
    /// The guard has to fire at startup rather than at the first read: by the time a read happened
    /// the pod would have written its own smaller count back, and the entries on the partitions that
    /// fell out of range would be unreachable with nothing logged. Failing the pod is the loud
    /// alternative.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S3_a_partition_decrease_is_refused_at_startup()
    {
        await fixture.FlushAllAsync();

        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();

        // The topic is recorded at 8 partitions by whoever created it.
        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, new TopicOptions { Partitions = 8 });

        // This process is configured for 4 — a shrink.
        var shrunk = new TopicOptions { Partitions = 4 };
        await using var host = NewHost(topic, consumer, shrunk, new TestHandler(), new CapturingLogger());

        var act = async () => await host.StartAsync(CancellationToken.None);

        var thrown = await act.Should().ThrowAsync<StreamConfigurationException>(
            "decreasing the partition count would orphan every unprocessed message on partitions 4-7");

        thrown.Which.Message.Should().Contain(topic);
        thrown.Which.Message.Should().Contain("8");
        thrown.Which.Message.Should().Contain("4");

        // Refusing must not have written the smaller count back.
        var recorded = await fixture.Db.HashGetAsync(StreamKeys.TopicMeta(topic), StreamAdmin.MetaPartitionsField);
        ((int)recorded).Should().Be(8, "a refused start leaves the recorded topology alone");

        host.IsRunning.Should().BeFalse();
    }

    /// <summary>S3b — 4 to 8 partitions over an undrained backlog loses nothing.</summary>
    /// <remarks>
    /// <para>
    /// The backlog is published while no consumer is running, so partitions 0-3 hold real unread
    /// entries at the moment the count changes. Because the old partitions stay in range, the new
    /// 8-partition consumer reads them alongside the four empty new ones and every message still
    /// arrives — exactly once, which is the assertion that would fail if the increase had renumbered
    /// or rehashed anything.
    /// </para>
    /// <para>
    /// What is genuinely lost is per-key ordering for the duration of the drain, and the Warning is
    /// the only notice an operator gets, so it is asserted here rather than taken on trust.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S3b_a_partition_increase_drains_the_old_backlog_exactly_once()
    {
        await fixture.FlushAllAsync();

        const int messages = 400;

        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var before = new TopicOptions { Partitions = 4 };
        var after = new TopicOptions { Partitions = 8 };

        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, before);

        // Backlog first, consumer second: partitions 0-3 are undrained when the count moves.
        var publisher = new StreamPublisher(fixture.Db, topic, before);
        var expected = new List<string>(messages);

        for (var i = 0; i < messages; i++)
        {
            var body = string.Create(CultureInfo.InvariantCulture, $"msg-{i:D4}");
            expected.Add(body);

            _ = await publisher.PublishAsync(
                string.Create(CultureInfo.InvariantCulture, $"key-{i % 50}"),
                Encoding.UTF8.GetBytes(body),
                "lifecycle");
        }

        var backlog = new long[before.Partitions];
        for (var partition = 0; partition < before.Partitions; partition++)
        {
            backlog[partition] = await fixture.Db.StreamLengthAsync(StreamKeys.Stream(topic, partition, before.CoLocatePartitions));
        }

        backlog.Sum().Should().Be(messages);
        backlog.Should().AllSatisfy(l => l.Should().BePositive("the backlog must really be spread over the old partitions"));

        var handler = new TestHandler();
        var log = new CapturingLogger();

        // BlockMs is turned down from its 1000 ms default because the consumer host starts one
        // blocking XREAD per partition on a single dedicated reader connection, and a blocking read
        // parks that connection server-side: with eight partitions the idle ones make each partition
        // wait up to seven block intervals for its turn. At the default that drains 400 messages in
        // ~25-30 s and occasionally stalls outright (see the note in the report); at 100 ms the
        // worst-case round trip is 800 ms and the drain is prompt. The assertions below are
        // unchanged — this only stops the test measuring a known wiring gap instead of the topology.
        await using var host = NewHost(topic, consumer, after, handler, log, o => o with { StartFrom = StartFrom.Beginning, BlockMs = 100 });

        await host.StartAsync(CancellationToken.None);

        host.OwnedPartitions.Should().Equal(0, 1, 2, 3, 4, 5, 6, 7);

        // Exactly once: everything published arrives, and nothing arrives twice.
        await handler.WaitForExactlyAsync(messages, settle: TimeSpan.FromSeconds(1), timeout: TimeSpan.FromSeconds(60));
        handler.Bodies.Should().BeEquivalentTo(expected, "the old partitions stay in range, so their backlog still drains");

        // The four new streams exist and are empty — created by the reconcile, not by a publish.
        for (var partition = before.Partitions; partition < after.Partitions; partition++)
        {
            var key = StreamKeys.Stream(topic, partition, after.CoLocatePartitions);

            (await fixture.Db.KeyExistsAsync(key)).Should().BeTrue($"partition {partition} is new and must have been created");
            (await fixture.Db.StreamLengthAsync(key)).Should().Be(0);
        }

        // The recorded topology moved up.
        var recorded = await fixture.Db.HashGetAsync(StreamKeys.TopicMeta(topic), StreamAdmin.MetaPartitionsField);
        ((int)recorded).Should().Be(after.Partitions);

        // And the one-off ordering break was announced.
        var warning = log.Entries
            .Where(e => e.Level == LogLevel.Warning)
            .Select(e => e.Message)
            .FirstOrDefault(m => m.Contains("partition count increased", StringComparison.Ordinal));

        warning.Should().NotBeNull("an increase breaks per-key ordering until the old backlog drains, and that must be logged");
        warning.Should().Contain(topic).And.Contain("ordering");

        await host.StopAsync(CancellationToken.None);
    }

    /// <summary>Builds a consumer host wired to the fixture's Redis, with everything else defaulted.</summary>
    private StreamConsumerHost NewHost(
        string topic,
        string consumer,
        TopicOptions topicOptions,
        TestHandler handler,
        ILogger log,
        Func<ConsumerOptions, ConsumerOptions>? tweak = null)
    {
        var options = new StreamOptions
        {
            ConnectionString = fixture.ConnectionString,
            Topics = { [topic] = topicOptions },
        };

        var consumerOptions = new ConsumerOptions
        {
            Topic = topic,
            Consumer = consumer,
            BatchSize = 32,
            PersistIntervalMs = 100,
        };

        if (tweak is not null)
        {
            consumerOptions = tweak(consumerOptions);
        }

        return new StreamConsumerHost(
            options,
            consumerOptions,
            consumer,
            ((IBatchHandler)handler).HandleAsync,
            new StreamsConnectionProvider(options, services: null, log),
            log);
    }

    /// <summary>
    /// Registers a lag monitor per partition so the sampler has something to read.
    /// </summary>
    /// <remarks>
    /// The consumer host does not yet register monitors itself (nothing in the library calls
    /// <c>StreamLag.Track</c> — the health-check wiring is P2/P3 work), so the S2 lag assertion drives
    /// the sampler directly. That still exercises the part S2 is about: <c>XINFO STREAM</c> against a
    /// stream that only exists because the reconcile created it.
    /// </remarks>
    private static StreamPartitionMonitor[] TrackPartitions(string topic, string consumer, TopicOptions options)
    {
        var monitors = new StreamPartitionMonitor[options.Partitions];

        for (var partition = 0; partition < options.Partitions; partition++)
        {
            monitors[partition] = StreamLag.Track(
                topic,
                consumer,
                partition,
                StreamKeys.Stream(topic, partition, options.CoLocatePartitions));
        }

        return monitors;
    }

    /// <summary>A logger that keeps what it was told, for the assertions on the increase Warning.</summary>
    private sealed class CapturingLogger : ILogger
    {
        private readonly Lock gate = new();
        private readonly List<(LogLevel Level, string Message)> entries = [];

        internal IReadOnlyList<(LogLevel Level, string Message)> Entries
        {
            get
            {
                lock (this.gate)
                {
                    return this.entries.ToArray();
                }
            }
        }

        internal IReadOnlyList<string> Errors
            => this.Entries.Where(e => e.Level >= LogLevel.Error).Select(e => e.Message).ToArray();

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);

            var message = formatter(state, exception);

            lock (this.gate)
            {
                this.entries.Add((logLevel, message));
            }
        }

        private sealed class NullScope : IDisposable
        {
            internal static readonly NullScope Instance = new();

            public void Dispose()
            {
                // Nothing to release.
            }
        }
    }
}
