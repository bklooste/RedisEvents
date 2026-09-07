using System.Diagnostics;
using System.Globalization;

using Microsoft.Extensions.Logging.Abstractions;

using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;

namespace Orange.Lib.Streams.Tests;

/// <summary>
/// Throughput benchmarks against real Redis. Not part of the correctness suite — run explicitly with
/// <c>dotnet test --filter "TestType=PerfTest"</c>. Results are printed to test output as
/// messages/second so numbers can be compared across runs and machines rather than asserted on
/// (a CI box's absolute throughput is not a contract).
/// </summary>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "PerfTest")]
public sealed class PerfBenchmarkTests(RedisStreamsFixture fixture, ITestOutputHelper output)
{
    private const string MessageType = "perf";

    [Theory]
    [InlineData(128)]
    [InlineData(2048)]
    public async Task Direct_single_publish_throughput(int bodyBytes)
    {
        const int total = 5_000;

        var topic = fixture.NewTopic();
        var topicOptions = NoTrim(partitions: 4);
        var root = Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        var body = new byte[bodyBytes];

        var sw = Stopwatch.StartNew();
        for (var i = 0; i < total; i++)
        {
            _ = await publisher.PublishAsync("key", body, MessageType);
        }

        sw.Stop();
        Report($"Direct single XADD (body={bodyBytes}B)", total, sw.Elapsed);
    }

    [Theory]
    [InlineData(100)]
    [InlineData(500)]
    [InlineData(2_000)]
    public async Task Batch_publish_throughput(int batchSize)
    {
        const int total = 100_000;

        var topic = fixture.NewTopic();
        var topicOptions = NoTrim(partitions: 4);
        var root = Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        var buffers = new byte[batchSize][];
        for (var i = 0; i < buffers.Length; i++)
        {
            buffers[i] = new byte[128];
        }

        var bodies = new ReadOnlyMemory<byte>[buffers.Length];
        for (var i = 0; i < buffers.Length; i++)
        {
            bodies[i] = buffers[i];
        }

        var sw = Stopwatch.StartNew();
        for (var sent = 0; sent < total; sent += batchSize)
        {
            var count = Math.Min(batchSize, total - sent);
            await publisher.PublishBatchAsync("key", bodies.AsMemory(0, count), MessageType);
        }

        sw.Stop();
        Report($"Pipelined PublishBatchAsync (batch={batchSize})", total, sw.Elapsed);
    }

    [Theory]
    [InlineData(500, 10)]
    [InlineData(2_000, 25)]
    public async Task Buffered_publisher_throughput(int maxBatch, int maxWaitMs)
    {
        const int total = 100_000;

        var topic = fixture.NewTopic();
        var topicOptions = NoTrim(partitions: 4);
        var root = Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var direct = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        var producerOptions = new ProducerOptions
        {
            Topic = topic,
            MaxBatch = maxBatch,
            MaxWaitMs = maxWaitMs,
            MaxQueue = 50_000,
        };

        await using var buffered = new BufferedStreamPublisher(direct, producerOptions, topicOptions);
        var body = new byte[128];

        var sw = Stopwatch.StartNew();
        for (var i = 0; i < total; i++)
        {
            await buffered.EnqueueAsync("key", body, MessageType);
        }

        await buffered.FlushAsync();
        sw.Stop();

        Report($"BufferedStreamPublisher (maxBatch={maxBatch} maxWaitMs={maxWaitMs})", total, sw.Elapsed);
        output.WriteLine($"  dropped={buffered.DroppedCount} published={buffered.PublishedCount}");
    }

    [Theory]
    [InlineData(1, true, ReadMode.Block, 1)]
    [InlineData(4, true, ReadMode.Block, 1)]
    [InlineData(4, false, ReadMode.Block, 1)]
    [InlineData(4, true, ReadMode.Block, 4)]
    [InlineData(4, true, ReadMode.Poll, 1)]
    public async Task End_to_end_consume_throughput(int partitions, bool backpressure, ReadMode readMode, int readerThreads)
    {
        const int total = 100_000;

        var topic = fixture.NewTopic();
        var topicOptions = NoTrim(partitions);
        var root = Root(topic, topicOptions);

        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, topicOptions);

        await PublishSpreadAsync(publisher, total, keys: partitions * 4);

        var consumed = 0;

        ValueTask Handle(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            Interlocked.Add(ref consumed, batch.Length);
            return default;
        }

        var consumerOptions = new ConsumerOptions
        {
            Topic = topic,
            BatchSize = 500,
            BlockMs = 100,
            ReadMode = readMode,
            ReaderThreads = readerThreads,
            Persist = PersistMode.AsyncBatch,
            PersistIntervalMs = 250,
            StartFrom = StartFrom.Beginning,
            Backpressure = new BackpressureOptions { Enabled = backpressure, Capacity = 8 },
        };

        var host = new StreamConsumerHost(root, consumerOptions, "perf-consumer", Handle, connection, NullLogger.Instance);

        var sw = Stopwatch.StartNew();
        await host.StartAsync(CancellationToken.None);

        try
        {
            await RedisStreamsFixture.WaitUntilAsync(
                () => Volatile.Read(ref consumed) >= total,
                TimeSpan.FromSeconds(60),
                $"all {total} messages to be consumed");
        }
        finally
        {
            sw.Stop();
            await host.StopAsync(CancellationToken.None);
        }

        Report($"End-to-end consume (partitions={partitions} backpressure={backpressure} readMode={readMode} readerThreads={readerThreads})", total, sw.Elapsed);
    }

    private void Report(string label, int count, TimeSpan elapsed)
    {
        var perSecond = count / Math.Max(elapsed.TotalSeconds, 0.0001);
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"{label}: {count} msgs in {elapsed.TotalMilliseconds:0} ms = {perSecond:0} msg/s"));
    }

    private static TopicOptions NoTrim(int partitions) => new()
    {
        Partitions = partitions,
        Trim = TrimMode.None,
        MaxLen = long.MaxValue,
        BackgroundTrimIntervalSeconds = 0,
    };

    private StreamOptions Root(string topic, TopicOptions topicOptions) => new()
    {
        ConnectionString = fixture.ConnectionString,
        Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
    };

    private static async Task PublishSpreadAsync(StreamPublisher publisher, int count, int keys)
    {
        var byKey = new List<int>[keys];
        for (var k = 0; k < keys; k++)
        {
            byKey[k] = new List<int>((count / keys) + 1);
        }

        for (var i = 0; i < count; i++)
        {
            byKey[i % keys].Add(i);
        }

        await Task.WhenAll(Enumerable
            .Range(0, keys)
            .Select(k => PublishAsync(publisher, $"key-{k.ToString(CultureInfo.InvariantCulture)}", byKey[k]))
            .ToArray());
    }

    private static async Task PublishAsync(StreamPublisher publisher, string key, IReadOnlyList<int> indexes, int chunk = 1_000)
    {
        var body = new byte[64];
        var bodies = new ReadOnlyMemory<byte>[Math.Min(chunk, indexes.Count)];
        for (var i = 0; i < bodies.Length; i++)
        {
            bodies[i] = body;
        }

        for (var offset = 0; offset < indexes.Count; offset += bodies.Length)
        {
            var count = Math.Min(bodies.Length, indexes.Count - offset);
            await publisher.PublishBatchAsync(key, bodies.AsMemory(0, count), MessageType);
        }
    }
}
