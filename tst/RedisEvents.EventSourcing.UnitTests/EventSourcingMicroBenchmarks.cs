using System.Text.Json;
using System.Text.Json.Serialization;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

using FluentAssertions;

using RedisEvents.EventSourcing;
using RedisEvents.Producer;
using RedisEvents.Projections;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests.EventSourcing;

/// <summary>
/// Micro-benchmarks for <c>RedisEvents.EventSourcing</c>'s own per-call cost, with no Redis anywhere
/// in this file: a hand-rolled <see cref="FakeStreamStore"/> just appends to a list and fabricates
/// ids, so what gets measured is exactly the package's own work — <see cref="EventTypeRegistry"/>
/// encode, the <c>es-version</c>/<c>es-id</c> header-list building in
/// <see cref="RedisEventRepository.SaveAsync"/>, and <see cref="EventProjector"/>'s decode-and-dispatch
/// — never the network or a real Redis round trip. Follows the same two-tier convention as
/// <c>StreamsMicroBenchmarks.cs</c>: this file is the in-process, allocation-focused tier;
/// <c>EventSourcingPerfBenchmarkTests.cs</c> in the service-test project is the real-Redis throughput
/// tier.
/// </summary>
/// <remarks>
/// <para>
/// <b>The allocation contract this benchmark holds the code to</b> (PLAN.md, "Benchmarks"): no
/// reflection anywhere in the save or dispatch path, no boxing beyond the one <c>object</c> every
/// event already becomes when it lands in <see cref="AggregateRoot.GetUncommittedChanges"/> or comes
/// back out of <see cref="EventTypeRegistry.TryDecode"/>, and otherwise only what <c>StreamMsg</c>'s
/// shape and JSON (de)serialisation themselves force — the UTF-8 body bytes, the small per-event
/// header list <see cref="RedisEventRepository"/> builds, and the decoded event instance. Every
/// benchmark here is <see cref="MemoryDiagnoserAttribute"/>-annotated, so a regression against that
/// contract shows up as an allocation-count change, not just a slower number.
/// </para>
/// <para>
/// Run them with <c>dotnet test --filter "TestType=PerfTest"</c> — the same trait
/// <c>StreamsMicroBenchmarks.cs</c> and the service project's throughput benchmarks both use, so an
/// ordinary <c>TestType=UnitTest</c> run never pays for them. For real numbers rather than a smoke
/// run, run these benchmark classes from a Release build; the runner below uses a short in-process
/// job so it stays usable from the test host.
/// </para>
/// </remarks>
[MemoryDiagnoser]
public class EventRepositorySaveBenchmarks
{
    private RedisEventRepository repository = null!;

    /// <summary>Builds a repository over the fake store once; the aggregate is fresh on every invocation.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var registry = new EventTypeRegistry().RegisterJson("bench.ticked", BenchJson.Default.BenchTicked);
        this.repository = new RedisEventRepository(new FakeStreamStore(), registry);
    }

    /// <summary>One event, one <see cref="RedisEventRepository.SaveAsync"/> call — the minimum a save can cost.</summary>
    [Benchmark]
    public async Task<int> Save_one_event()
    {
        var widget = new BenchWidget();
        widget.Tick(1);
        return await this.repository.SaveAsync(widget);
    }

    /// <summary>Ten events in one save — the same encode/header-building work, ten times over in one transaction.</summary>
    [Benchmark]
    public async Task<int> Save_batch_of_ten()
    {
        var widget = new BenchWidget();
        for (var i = 0; i < 10; i++)
        {
            widget.Tick(i);
        }

        return await this.repository.SaveAsync(widget);
    }
}

/// <inheritdoc cref="EventRepositorySaveBenchmarks"/>
[MemoryDiagnoser]
public class EventProjectorDispatchBenchmarks
{
    private EventProjector projector = null!;
    private StreamMsg[] batch = null!;

    /// <summary>Builds a projector bound to one no-op projection, and a one-message batch to decode and dispatch.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var registry = new EventTypeRegistry().RegisterJson("bench.ticked", BenchJson.Default.BenchTicked);
        this.projector = new EventProjector(registry, [new NoopProjection()]);

        var body = JsonSerializer.SerializeToUtf8Bytes(new BenchTicked(1), BenchJson.Default.BenchTicked);
        this.batch =
        [
            new StreamMsg(
                Body: body,
                Type: "bench.ticked",
                Id: new StreamId(1, 0),
                Partition: 0,
                PartitionKey: "widget-1",
                CorrelationId: "corr-1",
                TraceParent: null,
                Headers: HeaderBlock.Pack([new("es-version", "1")])),
        ];
    }

    /// <summary>Decode via the registry plus dispatch to one bound projection — the consumer's per-event cost.</summary>
    [Benchmark]
    public ValueTask Decode_and_dispatch_one_event() => this.projector.HandleAsync(this.batch, CancellationToken.None);

    /// <summary>A projection that does nothing, so only dispatch itself is on the clock.</summary>
    private sealed class NoopProjection : IProjection<BenchTicked>
    {
        public ValueTask HandleAsync(BenchTicked @event, EventMeta meta, CancellationToken ct) => ValueTask.CompletedTask;
    }
}

/// <summary>A cheap event to raise in bulk, mirroring <c>WidgetTicked</c> in the service-test project's own fixtures.</summary>
internal sealed record BenchTicked(int Sequence);

/// <summary>Source-generated JSON metadata: no reflection, so the benchmark measures what production code pays.</summary>
[JsonSerializable(typeof(BenchTicked))]
internal sealed partial class BenchJson : JsonSerializerContext;

/// <summary>A minimal event-sourced aggregate, defined here so these benchmarks exercise nothing but <see cref="RedisEventRepository"/>.</summary>
internal sealed class BenchWidget : AggregateRoot
{
    private readonly List<int> ticks = [];

    /// <summary>Registers the one handler this benchmark's aggregate needs.</summary>
    public BenchWidget() => this.On<BenchTicked>(e => this.ticks.Add(e.Sequence));

    /// <inheritdoc />
    public override string AggregateName => "BenchWidget";

    /// <inheritdoc />
    public override string Id => "bench-1";

    /// <summary>Raises one tick.</summary>
    public void Tick(int sequence) => this.Raise(new BenchTicked(sequence));
}

/// <summary>
/// A hand-rolled <see cref="IStreamStore"/> that only appends to a list and hands back fabricated
/// ids — no Redis, no <c>WATCH</c>/<c>EXEC</c>, no network, and no length check ever fails. This is
/// what isolates <see cref="RedisEventRepository.SaveAsync"/>'s own per-call cost from everything a
/// real <c>StreamStore</c> does against the server.
/// </summary>
internal sealed class FakeStreamStore : IStreamStore
{
    private long length;

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<StreamMsg>> ReadAsync(string name, StreamId after, int max, CancellationToken ct = default)
        => ValueTask.FromResult<IReadOnlyList<StreamMsg>>([]);

    /// <inheritdoc />
    public ValueTask<StreamId[]?> AppendAndPublishAsync(
        string name,
        long expectedLength,
        string partitionKey,
        IReadOnlyList<StateEvent> events,
        CancellationToken ct = default)
    {
        var ids = new StreamId[events.Count];
        for (var i = 0; i < events.Count; i++)
        {
            this.length++;
            ids[i] = new StreamId(this.length, 0);
        }

        return ValueTask.FromResult<StreamId[]?>(ids);
    }

    /// <inheritdoc />
    public ValueTask<StreamId[]> AppendAndPublishAsync(
        string name,
        string partitionKey,
        IReadOnlyList<StateEvent> events,
        CancellationToken ct = default)
    {
        var ids = new StreamId[events.Count];
        for (var i = 0; i < events.Count; i++)
        {
            this.length++;
            ids[i] = new StreamId(this.length, 0);
        }

        return ValueTask.FromResult(ids);
    }
}

/// <summary>
/// Runs the micro-benchmarks, and — in an ordinary unit-test pass — checks that their bodies still
/// measure what they claim to, exactly as <c>StreamsMicroBenchmarks.cs</c>'s <c>MicroBenchmarkTests</c> does.
/// </summary>
public class EventSourcingMicroBenchmarkTests
{
    /// <summary>
    /// The smoke test that keeps the benchmarks honest: every benchmark body is invoked once and its
    /// result asserted. A benchmark that has quietly stopped exercising the thing it names — a
    /// changed signature, a setup that no longer builds a valid fixture — is otherwise invisible,
    /// because nothing runs these in an ordinary CI pass.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_benchmark_bodies_measure_what_they_say_they_do()
    {
        var save = new EventRepositorySaveBenchmarks();
        save.Setup();

        (await save.Save_one_event()).Should().Be(1, "one uncommitted event on a fresh aggregate");
        (await save.Save_batch_of_ten()).Should().Be(10, "a fresh aggregate each call, ten uncommitted events in one save");

        var dispatch = new EventProjectorDispatchBenchmarks();
        dispatch.Setup();

        await dispatch.Decode_and_dispatch_one_event();
    }

    /// <summary>
    /// The benchmarks themselves. Excluded from a unit-test run by its trait, exactly as
    /// <c>StreamsMicroBenchmarks.cs</c>'s own benchmarks are: <c>dotnet test --filter "TestType=PerfTest"</c>.
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
            [typeof(EventRepositorySaveBenchmarks), typeof(EventProjectorDispatchBenchmarks)],
            config);

        summaries.Should().HaveCount(2);
        summaries.Should().OnlyContain(summary => summary.Reports.Length > 0);
        summaries.SelectMany(s => s.Reports).Should().OnlyContain(report => report.Success);
    }
}
