using System.Text;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

using FluentAssertions;

using Orange.Lib.Streams.Wire;

using StackExchange.Redis;

namespace Orange.Lib.Streams.UnitTests;

/// <summary>
/// P0-21 (via R-21). The per-call micro-benchmarks for the three things every message pays for: the
/// entry codec, the partition router and stream-id parsing/formatting.
/// </summary>
/// <remarks>
/// <para>
/// These are deliberately <em>micro</em>-benchmarks. <c>PerfBenchmarkTests</c> in the service-test
/// project measures end-to-end throughput against a real Redis, which is dominated by the network
/// and says nothing about whether a change made the codec twice as expensive. This file answers that
/// question, and it is where an allocation regression on the hot path shows up: every benchmark is
/// <see cref="MemoryDiagnoserAttribute"/>-annotated, and the documented contract is that decoding
/// allocates only the strings <see cref="StreamMsg"/>'s shape forces and routing allocates nothing.
/// </para>
/// <para>
/// Run them with <c>dotnet test --filter "TestType=PerfTest"</c> — matching the trait the service
/// project's throughput benchmarks use, so neither runs in an ordinary unit-test pass. For real
/// numbers rather than a smoke run, run the same benchmark classes from a Release build; the runner
/// below uses a short in-process job so it stays usable from the test host.
/// </para>
/// </remarks>
[MemoryDiagnoser]
public class EntryCodecBenchmarks
{
    private readonly byte[] body = Encoding.UTF8.GetBytes(new string('x', 256));
    private readonly KeyValuePair<string, string>[] headers =
    [
        new("tenant", "au"),
        new("source", "feed-kalshi"),
    ];

    private StreamEntry entry;

    /// <summary>Builds the entry the decode benchmark reads, once.</summary>
    [GlobalSetup]
    public void Setup()
        => this.entry = new StreamEntry(
            new StreamId(1_700_000_000_000, 42).Format(),
            EntryCodec.Encode(this.body, "BetPlaced", "customer-1", "corr-1", traceParent: null, this.headers));

    /// <summary>Encoding one entry: the publisher's per-message cost.</summary>
    [Benchmark]
    public NameValueEntry[] Encode()
        => EntryCodec.Encode(this.body, "BetPlaced", "customer-1", "corr-1", traceParent: null, this.headers);

    /// <summary>Encoding without correlation id or headers — the minimum an entry can cost.</summary>
    [Benchmark]
    public NameValueEntry[] EncodeMinimal()
        => EntryCodec.Encode(this.body, "BetPlaced");

    /// <summary>Decoding one entry: the consumer's per-message cost, headers left packed.</summary>
    [Benchmark]
    public StreamMsg Decode() => EntryCodec.Decode(in this.entry, partition: 3);
}

/// <inheritdoc cref="EntryCodecBenchmarks"/>
[MemoryDiagnoser]
public class PartitionRouterBenchmarks
{
    private readonly byte[] key = Encoding.UTF8.GetBytes("customer-01HZY3W7Q4K8N2");
    private uint counter;

    /// <summary>Hashing a key onto a power-of-two partition count — the mask path.</summary>
    [Benchmark]
    public int ForKeyPowerOfTwo() => PartitionRouter.ForKey(this.key, 8);

    /// <summary>Hashing onto a partition count that is not a power of two — the reduce path.</summary>
    [Benchmark]
    public int ForKeyOddCount() => PartitionRouter.ForKey(this.key, 6);

    /// <summary>The single-partition short circuit, which must not hash at all.</summary>
    [Benchmark]
    public int ForKeySinglePartition() => PartitionRouter.ForKey(this.key, 1);

    /// <summary>Round-robin for a keyless publish.</summary>
    [Benchmark]
    public int RoundRobin() => PartitionRouter.RoundRobin(ref this.counter, 8);
}

/// <inheritdoc cref="EntryCodecBenchmarks"/>
[MemoryDiagnoser]
public class StreamIdBenchmarks
{
    private const string Text = "1700000000000-42";

    private readonly StreamId id = new(1_700_000_000_000, 42);
    private readonly char[] buffer = new char[32];

    /// <summary>Parsing an id off the wire — once per entry on the read path.</summary>
    [Benchmark]
    public StreamId Parse() => StreamId.Parse(Text);

    /// <summary>The non-throwing form, which the cursor and the position store use.</summary>
    [Benchmark]
    public bool TryParse() => StreamId.TryParse(Text, out _);

    /// <summary>Formatting an id into a caller's buffer: the allocation-free path.</summary>
    [Benchmark]
    public bool TryFormat() => this.id.TryFormat(this.buffer, out _);

    /// <summary>Formatting to a string, which every position write pays for.</summary>
    [Benchmark]
    public string Format() => this.id.Format();
}

/// <summary>
/// Runs the micro-benchmarks, and — in an ordinary unit-test pass — checks that their bodies still
/// measure what they claim to.
/// </summary>
public class MicroBenchmarkTests
{
    /// <summary>
    /// The smoke test that keeps the benchmarks honest: every benchmark body is invoked once and its
    /// result asserted. A benchmark that has quietly stopped exercising the thing it names — a
    /// changed signature, a setup that no longer builds a valid entry — is otherwise invisible,
    /// because nothing runs these in CI.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_benchmark_bodies_measure_what_they_say_they_do()
    {
        var codec = new EntryCodecBenchmarks();
        codec.Setup();

        codec.Encode().Should().HaveCount(5, "body, type, key, correlation id and the packed headers");
        codec.EncodeMinimal().Should().HaveCount(3, "body, type and an empty partition key");

        var decoded = codec.Decode();
        decoded.Type.Should().Be("BetPlaced");
        decoded.PartitionKey.Should().Be("customer-1");
        decoded.Partition.Should().Be(3);
        decoded.Id.Should().Be(new StreamId(1_700_000_000_000, 42));
        decoded.Body.Length.Should().Be(256);

        var router = new PartitionRouterBenchmarks();
        router.ForKeyPowerOfTwo().Should().BeInRange(0, 7);
        router.ForKeyOddCount().Should().BeInRange(0, 5);
        router.ForKeySinglePartition().Should().Be(0);
        router.RoundRobin().Should().BeInRange(0, 7);

        var ids = new StreamIdBenchmarks();
        ids.Parse().Should().Be(new StreamId(1_700_000_000_000, 42));
        ids.TryParse().Should().BeTrue();
        ids.TryFormat().Should().BeTrue();
        ids.Format().Should().Be("1700000000000-42");
    }

    /// <summary>
    /// The benchmarks themselves. Excluded from a unit-test run by its trait, exactly as the
    /// service project's throughput benchmarks are: <c>dotnet test --filter "TestType=PerfTest"</c>.
    /// </summary>
    [Fact]
    [Trait("TestType", "PerfTest")]
    public void Run_the_micro_benchmarks()
    {
        // In-process, and with the optimisation validator off, so this is runnable from the test host
        // in a Debug build. Numbers worth quoting come from a Release run of the same classes.
        var config = ManualConfig.CreateEmpty()
            .AddLogger(ConsoleLogger.Default)
            .AddJob(Job.ShortRun.WithToolchain(InProcessEmitToolchain.Instance))
            .WithOptions(ConfigOptions.DisableOptimizationsValidator);

        var summaries = BenchmarkRunner.Run(
            [typeof(EntryCodecBenchmarks), typeof(PartitionRouterBenchmarks), typeof(StreamIdBenchmarks)],
            config);

        summaries.Should().HaveCount(3);
        summaries.Should().OnlyContain(summary => summary.Reports.Length > 0);
        summaries.SelectMany(s => s.Reports).Should().OnlyContain(report => report.Success);
    }
}
