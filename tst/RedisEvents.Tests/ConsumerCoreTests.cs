using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Diagnostics;
using RedisEvents.Extensions;
using RedisEvents.Producer;
using RedisEvents.Wire;

namespace RedisEvents.Tests;

/// <summary>
/// The R-gate tests for the consumer core: the three things a real pod does that no test reached
/// before — fault under <see cref="ErrorPolicy.Fail"/> on more than one partition, publish its lag,
/// and stand a partition down on the non-co-located path.
/// </summary>
/// <remarks>
/// <para>
/// Every one of these goes <b>through the host</b>. That is the whole point: each finding it covers
/// was a piece of wiring the host never did, and every existing test either constructed the missing
/// component by hand (the lag sampler) or used the one shape that hid the gap
/// (<c>Partitions = 1</c> for <see cref="ErrorPolicy.Fail"/>).
/// </para>
/// <para>
/// <see cref="StreamLag.Clear"/> runs before each test because the monitors — and therefore the lag
/// gauges — are process-wide, and a leftover monitor from an earlier class would answer for this one.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class ConsumerCoreTests(RedisStreamsFixture fixture)
{
    private const string MessageType = "test.event";

    // ------------------------------------------------------------------ R-03

    /// <summary>
    /// S14b — <see cref="ErrorPolicy.Fail"/> on a <b>multi-partition</b> consumer stops the
    /// application.
    /// </summary>
    /// <remarks>
    /// S14 asserts the policy with <c>Partitions = 1</c>, where "the worker faulted" and "the
    /// consumer is dead" happen to be the same statement. With four partitions they are not: before
    /// R-03 a <c>Fail</c> faulted one worker, logged, and left the pod Ready and consuming the other
    /// three partitions forever — the pod restart the policy exists to trigger never happened,
    /// because nothing ever called <see cref="IHostApplicationLifetime.StopApplication"/>.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S14b_ErrorPolicy_Fail_on_a_multi_partition_consumer_stops_the_application()
    {
        await this.ResetAsync();

        var lifetime = new RecordingLifetime();
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();

        // Co-located (the default) and more than one owned partition: the shape S14 never ran.
        var topicOptions = new TopicOptions { Partitions = 4 };

        var handler = new TestHandler();
        handler.OnBatch = (batch, _) => Bodies(batch).Contains("boom")
            ? throw new InvalidOperationException("the whole downstream is gone")
            : ValueTask.CompletedTask;

        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);

        // Partition 2, not 0: the fault has to reach the application from any partition, not just
        // the first one the host happens to start.
        await publisher.PublishAsync("k", Utf8("boom"), MessageType, new PublishOptions(Partition: 2));

        await using var rig = this.Build(topic, topicOptions, Options(topic, ErrorPolicy.Fail), consumer, handler, lifetime);

        await rig.Host.StartAsync(CancellationToken.None);

        await RedisStreamsFixture.WaitUntilAsync(
            () => lifetime.Stopped,
            TimeSpan.FromSeconds(30),
            "ErrorPolicy.Fail to stop the application");

        rig.Log.Entries(LogLevel.Critical, "Stopping the application").Should()
            .NotBeEmpty("the operator has to be able to see why the pod went away");
    }

    /// <summary>
    /// S14c — the same, with <c>CoLocatePartitions = false</c>, where the fault arrives from a
    /// per-partition worker rather than the shared group.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S14c_ErrorPolicy_Fail_stops_the_application_on_the_non_colocated_path()
    {
        await this.ResetAsync();

        var lifetime = new RecordingLifetime();
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var topicOptions = new TopicOptions { Partitions = 4, CoLocatePartitions = false };

        var handler = new TestHandler();
        handler.OnBatch = (batch, _) => Bodies(batch).Contains("boom")
            ? throw new InvalidOperationException("the whole downstream is gone")
            : ValueTask.CompletedTask;

        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);
        await publisher.PublishAsync("k", Utf8("boom"), MessageType, new PublishOptions(Partition: 3));

        await using var rig = this.Build(topic, topicOptions, Options(topic, ErrorPolicy.Fail), consumer, handler, lifetime);

        await rig.Host.StartAsync(CancellationToken.None);

        await RedisStreamsFixture.WaitUntilAsync(
            () => lifetime.Stopped,
            TimeSpan.FromSeconds(30),
            "ErrorPolicy.Fail to stop the application from a per-partition worker");
    }

    /// <summary>
    /// A policy that is not <see cref="ErrorPolicy.Fail"/> must never stop the application — the
    /// counterpart that stops R-03 from being "fix it by stopping on everything".
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task StopPartition_does_not_stop_the_application()
    {
        await this.ResetAsync();

        var lifetime = new RecordingLifetime();
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var topicOptions = new TopicOptions { Partitions = 2 };

        var handler = new TestHandler();
        handler.OnBatch = (_, _) => throw new InvalidOperationException("this partition is done");

        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);
        await publisher.PublishAsync("k", Utf8("one"), MessageType, new PublishOptions(Partition: 0));

        await using var rig = this.Build(
            topic, topicOptions, Options(topic, ErrorPolicy.StopPartition), consumer, handler, lifetime);

        await rig.Host.StartAsync(CancellationToken.None);

        await RedisStreamsFixture.WaitUntilAsync(
            () => rig.Log.Entries(LogLevel.Error, "ErrorPolicy.StopPartition").Count > 0,
            TimeSpan.FromSeconds(30),
            "the partition to stand down");

        // A moment for a stop that should not come.
        await Task.Delay(TimeSpan.FromMilliseconds(500));

        lifetime.Stopped.Should().BeFalse(
            "StopPartition stands one partition down and leaves the pod serving — only Fail restarts it");
    }

    // ------------------------------------------------------------------ R-05

    /// <summary>
    /// R-05 — the lag gauges are populated <b>through the host</b>: nothing in the service starts a
    /// <see cref="StreamLagSampler"/>, so if the host does not, <c>streams.lag.ms</c>,
    /// <c>streams.lag.entries</c> and <c>streams.stream.length</c> are never emitted at all and the
    /// health check's <c>LagEntries</c> stays at its "never sampled" -1 forever.
    /// </summary>
    /// <remarks>
    /// The measurements are read with a <see cref="MeterListener"/> — the same way an exporter reads
    /// them — rather than off the internal maps, so the test fails for the same reason a dashboard
    /// would be empty. <c>lag.ms</c> is pushed every second and <c>lag.entries</c> sampled every
    /// fifteen, which is why the second wait is the long one.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Lag_gauges_are_populated_through_the_host()
    {
        await this.ResetAsync();

        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var topicOptions = new TopicOptions { Partitions = 1 };

        var handler = new TestHandler();
        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);

        await publisher.PublishAsync("k", Utf8("one"), MessageType, new PublishOptions(Partition: 0));

        await using var rig = this.Build(topic, topicOptions, Options(topic, ErrorPolicy.BestEffort), consumer, handler);
        using var meter = new GaugeReader(topic);

        await rig.Host.StartAsync(CancellationToken.None);

        await handler.WaitForAsync(1, TimeSpan.FromSeconds(30));

        await RedisStreamsFixture.WaitUntilAsync(
            () => meter.Has("streams.lag.ms"),
            TimeSpan.FromSeconds(15),
            "the host to start the lag sampler's one-second push");

        await RedisStreamsFixture.WaitUntilAsync(
            () => meter.Has("streams.lag.entries") && meter.Has("streams.stream.length"),
            TimeSpan.FromSeconds(45),
            "the host's lag sampler to run its XINFO STREAM pass");

        StreamLag.All.Should().OnlyContain(
            m => m.LagEntries >= 0,
            "the health check treats LagEntries = -1 as 'never sampled' and suppresses nothing on it");

        await rig.Host.StopAsync(CancellationToken.None);
    }

    // ------------------------------------------------------------------ R-08

    /// <summary>
    /// R-08 — on the non-co-located path a stood-down partition completes its channel writer, so the
    /// reader stops instead of parking in <c>WriteAsync</c> forever.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The old continuation was <c>OnlyOnFaulted</c>, and <see cref="ErrorPolicy.StopPartition"/> ends
    /// the processing loop <em>normally</em>. So nothing cancelled the partition and nothing completed
    /// its writer: the reader kept fetching into a channel nobody drained, wedged on the first full
    /// one, and every batch it had queued behind the processor's final drain kept its pooled array
    /// until the host was disposed.
    /// </para>
    /// <para>
    /// <c>Capacity = 1</c> and a backlog are what make it deterministic: the reader has more to write
    /// than the channel can hold, so after the processor stops it must attempt a write into a
    /// completed channel — the line asserted below — rather than idling at the tail where the bug is
    /// invisible.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task StopPartition_on_the_non_colocated_path_stops_the_reader_and_drains_fast()
    {
        await this.ResetAsync();

        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var topicOptions = new TopicOptions { Partitions = 2, CoLocatePartitions = false };

        var handler = new TestHandler();
        handler.OnBatch = (_, _) => throw new InvalidOperationException("this partition is done");

        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);

        for (var i = 0; i < 40; i++)
        {
            await publisher.PublishAsync("k", Utf8($"m{i}"), MessageType, new PublishOptions(Partition: 0));
        }

        var options = Options(topic, ErrorPolicy.StopPartition) with
        {
            BatchSize = 1,
            Backpressure = new BackpressureOptions { Capacity = 1 },
            ShutdownTimeoutSeconds = 10,
        };

        await using var rig = this.Build(topic, topicOptions, options, consumer, handler);

        await rig.Host.StartAsync(CancellationToken.None);

        await RedisStreamsFixture.WaitUntilAsync(
            () => rig.Log.Entries(LogLevel.Information, "has stood down while the host keeps running").Count > 0,
            TimeSpan.FromSeconds(30),
            "the stood-down partition's reader to be cancelled and its channel completed");

        var stopping = Stopwatch.StartNew();
        await rig.Host.StopAsync(CancellationToken.None);
        stopping.Stop();

        stopping.Elapsed.Should().BeLessThan(
            TimeSpan.FromSeconds(8),
            "a stood-down partition must not make every shutdown burn the whole ShutdownTimeoutSeconds");

        rig.Log.Entries(LogLevel.Warning, "did not drain within").Should()
            .BeEmpty("both loops of the stopped partition had already ended");

        rig.Log.Entries(LogLevel.Warning, "had a worker fail during shutdown").Should()
            .BeEmpty("a completed channel is an ordinary stop, not a fault (R-09)");
    }

    private static ConsumerOptions Options(string topic, ErrorPolicy onError)
        => new()
        {
            Topic = topic,
            BatchSize = 1,
            Persist = PersistMode.SyncBatch,
            OnError = onError,
        };

    private static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

    private static IEnumerable<string> Bodies(ReadOnlyMemory<StreamMsg> batch)
    {
        var bodies = new List<string>(batch.Length);

        for (var i = 0; i < batch.Length; i++)
        {
            bodies.Add(Encoding.UTF8.GetString(batch.Span[i].Body.Span));
        }

        return bodies;
    }

    private async Task ResetAsync()
    {
        await fixture.FlushAllAsync();
        StreamLag.Clear();
    }

    private Rig Build(
        string topic,
        TopicOptions topicOptions,
        ConsumerOptions consumer,
        string consumerName,
        TestHandler handler,
        IHostApplicationLifetime? lifetime = null)
    {
        var options = new StreamOptions
        {
            ConnectionString = fixture.ConnectionString,
            Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
        };

        var log = new CapturingLog();
        var connection = new StreamsConnectionProvider(options, services: null, log);

        var host = new StreamConsumerHost(
            options,
            consumer with { Topic = topic },
            consumerName,
            ((IBatchHandler)handler).HandleAsync,
            connection,
            log,
            lifetime);

        return new Rig(host, connection, log);
    }

    /// <summary>One started consumer and everything these tests interrogate it with.</summary>
    private sealed class Rig(StreamConsumerHost host, StreamsConnectionProvider connection, CapturingLog log)
        : IAsyncDisposable
    {
        internal StreamConsumerHost Host { get; } = host;

        internal CapturingLog Log { get; } = log;

        public async ValueTask DisposeAsync()
        {
            try
            {
                await this.Host.DisposeAsync();
            }
            finally
            {
                await connection.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// An <see cref="IHostApplicationLifetime"/> that records the stop instead of performing it.
    /// </summary>
    /// <remarks>
    /// A real generic host would tear the process down, which is exactly the behaviour under test and
    /// exactly what a test process cannot allow. Recording it is the assertion.
    /// </remarks>
    private sealed class RecordingLifetime : IHostApplicationLifetime
    {
        private readonly CancellationTokenSource stopping = new();
        private int stopped;

        public CancellationToken ApplicationStarted => CancellationToken.None;

        public CancellationToken ApplicationStopping => this.stopping.Token;

        public CancellationToken ApplicationStopped => CancellationToken.None;

        internal bool Stopped => Volatile.Read(ref this.stopped) == 1;

        public void StopApplication() => Volatile.Write(ref this.stopped, 1);
    }

    /// <summary>
    /// Reads the library's observable gauges the way an exporter does, filtered to one topic.
    /// </summary>
    private sealed class GaugeReader : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly ConcurrentDictionary<string, byte> seen = new(StringComparer.Ordinal);
        private readonly string topic;

        internal GaugeReader(string topic)
        {
            this.topic = topic;

            this.listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == StreamsDiagnostics.SourceName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };

            this.listener.SetMeasurementEventCallback<long>(this.Record);
            this.listener.SetMeasurementEventCallback<double>(this.Record);
            this.listener.Start();
        }

        /// <summary>Whether the named gauge has reported a value for this test's topic.</summary>
        internal bool Has(string instrument)
        {
            this.listener.RecordObservableInstruments();

            return this.seen.ContainsKey(instrument);
        }

        public void Dispose() => this.listener.Dispose();

        private void Record<T>(
            Instrument instrument,
            T measurement,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state)
        {
            _ = measurement;
            _ = state;

            for (var i = 0; i < tags.Length; i++)
            {
                if (tags[i].Key == "topic" && string.Equals(tags[i].Value as string, this.topic, StringComparison.Ordinal))
                {
                    this.seen.TryAdd(instrument.Name, 0);
                    return;
                }
            }
        }
    }

    /// <summary>A logger that keeps every line so a test can assert on what the host said.</summary>
    private sealed class CapturingLog : ILogger
    {
        private readonly ConcurrentQueue<(LogLevel Level, string Message)> entries = new();

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
            this.entries.Enqueue((logLevel, formatter(state, exception)));
        }

        internal IReadOnlyList<string> Entries(LogLevel level, string contains)
            => this.entries
                .Where(e => e.Level == level && e.Message.Contains(contains, StringComparison.Ordinal))
                .Select(static e => e.Message)
                .ToArray();

        private sealed class NullScope : IDisposable
        {
            internal static readonly NullScope Instance = new();

            public void Dispose()
            {
            }
        }
    }
}
