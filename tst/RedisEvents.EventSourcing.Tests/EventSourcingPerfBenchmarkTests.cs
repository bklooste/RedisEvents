using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RedisEvents.Config;
using RedisEvents.EventSourcing;
using RedisEvents.EventSourcing.Sample.Inventory;
using RedisEvents.Producer;
using RedisEvents.Projections;

namespace RedisEvents.Tests;

/// <summary>
/// Throughput benchmarks for <c>RedisEvents.EventSourcing</c> against real Redis. Not part of the
/// correctness suite — run explicitly with <c>dotnet test --filter "TestType=PerfTest"</c>, exactly
/// as <c>PerfBenchmarkTests.cs</c>'s core benchmarks are, and — per PLAN.md's "Benchmarks" section —
/// wired into the same invocation so there is one place operators look for "is this fast" across
/// every package. Results are printed to test output as events/second, loads/second and latency
/// percentiles so numbers can be compared across runs and machines rather than asserted on: a CI
/// box's absolute throughput is not a contract, comparability is the point.
/// </summary>
/// <remarks>
/// Uses <see cref="EventRepositoryTests"/>'s own <c>TestWidget</c> fixture for the save/load
/// benchmarks — plain round trips through <see cref="RedisEventRepository"/> and nothing else — and
/// the Inventory sample for the one benchmark that needs a real projector and a real view
/// (<see cref="End_to_end_save_to_projected_view_latency"/>).
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "PerfTest")]
public sealed class EventSourcingPerfBenchmarkTests(RedisStreamsFixture fixture, ITestOutputHelper output)
{
    /// <summary>Trimming an aggregate stream would break the version, so no benchmark here relies on it either.</summary>
    private static readonly TopicOptions Options = new() { Partitions = 1, Trim = TrimMode.None };

    [Fact]
    public async Task Save_throughput_one_event_per_save()
    {
        const int total = 2_000;

        var (repository, _) = this.Repository();
        var widget = TestWidget.Create("save-1", "First");
        _ = await repository.SaveAsync(widget);

        var sw = Stopwatch.StartNew();
        for (var i = 0; i < total; i++)
        {
            widget.Tick(i);
            _ = await repository.SaveAsync(widget);
        }

        sw.Stop();
        this.Report("Save throughput (1 event/save)", total, sw.Elapsed);
    }

    [Fact]
    public async Task Save_throughput_ten_events_per_save()
    {
        const int totalSaves = 500;
        const int eventsPerSave = 10;

        var (repository, _) = this.Repository();
        var widget = TestWidget.Create("save-10", "First");
        _ = await repository.SaveAsync(widget);

        var sw = Stopwatch.StartNew();
        for (var s = 0; s < totalSaves; s++)
        {
            for (var i = 0; i < eventsPerSave; i++)
            {
                widget.Tick((s * eventsPerSave) + i);
            }

            _ = await repository.SaveAsync(widget);
        }

        sw.Stop();
        this.Report("Save throughput (10 events/save)", totalSaves * eventsPerSave, sw.Elapsed);
    }

    /// <summary>
    /// Repeated loads of a short and a long aggregate, so the per-event cost of paging (a 1000-event
    /// aggregate is exactly <c>RedisEventRepository.LoadPageSize</c>, so it costs one extra, short,
    /// round trip beyond the short aggregate's single page) shows up as a number rather than a guess.
    /// </summary>
    [Fact]
    public async Task Load_throughput_short_and_long_history()
    {
        var (repository, _) = this.Repository();

        var shortWidget = TestWidget.Create("load-short", "First");
        for (var i = 0; i < 9; i++)
        {
            shortWidget.Tick(i);
        }

        _ = await repository.SaveAsync(shortWidget);

        var longWidget = TestWidget.Create("load-long", "First");
        for (var i = 0; i < 999; i++)
        {
            longWidget.Tick(i);
        }

        _ = await repository.SaveAsync(longWidget);

        const int shortIterations = 200;
        var swShort = Stopwatch.StartNew();
        for (var i = 0; i < shortIterations; i++)
        {
            _ = await repository.LoadAsync<TestWidget>("load-short");
        }

        swShort.Stop();

        const int longIterations = 50;
        var swLong = Stopwatch.StartNew();
        for (var i = 0; i < longIterations; i++)
        {
            _ = await repository.LoadAsync<TestWidget>("load-long");
        }

        swLong.Stop();

        this.Report("Load throughput (10-event aggregate)", shortIterations, swShort.Elapsed);
        this.Report("Load throughput (1000-event aggregate)", longIterations, swLong.Elapsed);

        var perEventShort = swShort.Elapsed.TotalMilliseconds / shortIterations / 10;
        var perEventLong = swLong.Elapsed.TotalMilliseconds / longIterations / 1000;
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"  per-event cost: short-history load {perEventShort:0.000} ms/event, long-history load {perEventLong:0.000} ms/event"));
    }

    /// <summary>
    /// Wall-clock from <see cref="IEventRepository.SaveAsync"/> returning to a real
    /// <see cref="IViewStore{TView}"/> reflecting it, through a real hosted
    /// <see cref="EventProjector"/> consumer — the one number in this file that is dominated by the
    /// consumer's read loop rather than by <see cref="RedisEventRepository"/> itself. Percentiles
    /// rather than a single average, in the style of <c>ReadModeComparisonTests.cs</c>: the tail is
    /// what a caller polling for its own write actually waits on.
    /// </summary>
    [Fact]
    public async Task End_to_end_save_to_projected_view_latency()
    {
        const int samples = 20;

        var topic = fixture.NewTopic();
        var host = this.BuildHost(topic, fixture.NewConsumer());

        try
        {
            await host.StartAsync();

            var repository = host.Services.GetRequiredService<IEventRepository>();
            var views = host.Services.GetRequiredService<IViewStore<InventoryDetail>>();

            var latencies = new double[samples];

            for (var i = 0; i < samples; i++)
            {
                var id = $"latency-{i.ToString(CultureInfo.InvariantCulture)}";
                var item = InventoryItem.Create(id, "Widget");

                var sw = Stopwatch.StartNew();
                await repository.SaveAsync(item);

                await RedisStreamsFixture.WaitUntilAsync(
                    async () => await views.GetAsync(id) is not null,
                    TimeSpan.FromSeconds(10),
                    "the real projector to carry the save into the real view");

                sw.Stop();
                latencies[i] = sw.Elapsed.TotalMilliseconds;
            }

            Array.Sort(latencies);
            var p50 = Percentile(latencies, 0.50);
            var p90 = Percentile(latencies, 0.90);

            output.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"Save -> projected view latency ({samples} samples): p50 {p50:0.0} ms, p90 {p90:0.0} ms, max {latencies[^1]:0.0} ms"));
        }
        finally
        {
            await host.StopAsync();
            host.Dispose();
        }
    }

    private void Report(string label, int count, TimeSpan elapsed)
    {
        var perSecond = count / Math.Max(elapsed.TotalSeconds, 0.0001);
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"{label}: {count} in {elapsed.TotalMilliseconds:0} ms = {perSecond:0}/s"));
    }

    private static double Percentile(double[] sorted, double q)
    {
        if (sorted.Length == 0)
        {
            return 0;
        }

        var rank = (int)Math.Ceiling(q * sorted.Length) - 1;
        return sorted[Math.Clamp(rank, 0, sorted.Length - 1)];
    }

    /// <summary>A repository on the fixture's shared connection, over a topic of this benchmark's own.</summary>
    private (RedisEventRepository Repository, string Topic) Repository([CallerMemberName] string hint = "repository")
    {
        var topic = fixture.NewTopic(hint);
        var registry = new EventTypeRegistry()
            .RegisterJson("widget.created", WidgetJson.Default.WidgetCreated)
            .RegisterJson("widget.renamed", WidgetJson.Default.WidgetRenamed)
            .RegisterJson("widget.ticked", WidgetJson.Default.WidgetTicked);

        return (new RedisEventRepository(new StreamStore(fixture.Db, topic, Options), registry), topic);
    }

    /// <summary>Builds a host wired exactly as a service would wire the Inventory sample, over one topic.</summary>
    private IHost BuildHost(string topic, string consumer)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
        {
            EnvironmentName = "Production",
        });

        builder.Configuration["Streams:ConnectionString"] = fixture.ConnectionString;
        builder.Configuration["Streams:Consumer"] = consumer;

        builder.AddEventStore(topic, InventoryEventTypes.Register)
               .AddEventProjector(topic)
               .AddRedisViewStore<InventoryDetail>(topic, "detail", InventoryJsonContext.Default.InventoryDetail)
               .AddProjection<InventoryDetailProjection>();

        return builder.Build();
    }
}
