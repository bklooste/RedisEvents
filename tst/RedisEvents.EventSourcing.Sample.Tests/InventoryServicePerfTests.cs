using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Json;

namespace RedisEvents.Tests;

/// <summary>
/// Throughput and latency of the two Inventory sample services over real HTTP, against a real
/// Testcontainers Redis. Not part of the correctness suite — run explicitly with
/// <c>dotnet test --filter "TestType=PerfTest"</c>, exactly like every other perf test in this repo.
/// Results are printed to test output as requests/second or latency percentiles so numbers can be
/// compared across runs; there is no assertion on the absolute number, matching
/// <c>EventSourcingPerfBenchmarkTests</c>'s convention — a CI box's throughput is not a contract.
/// </summary>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "PerfTest")]
public sealed class InventoryServicePerfTests(RedisStreamsFixture fixture, ITestOutputHelper output)
{
    [Fact]
    public async Task Create_item_throughput()
    {
        await fixture.FlushAllAsync();

        await using var commandApi = ServiceTestHosts.CommandApi(fixture);
        using var client = commandApi.CreateClient();

        const int total = 200;
        var sw = Stopwatch.StartNew();

        for (var i = 0; i < total; i++)
        {
            var id = string.Create(CultureInfo.InvariantCulture, $"perf-{i}-{Guid.NewGuid():N}");
            var response = await client.PostAsJsonAsync("/items", new { Id = id, Name = "Widget" });
            response.EnsureSuccessStatusCode();
        }

        sw.Stop();

        var perSecond = total / Math.Max(sw.Elapsed.TotalSeconds, 0.0001);
        output.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"POST /items throughput (command-side service, real HTTP): {total} in {sw.Elapsed.TotalMilliseconds:0} ms = {perSecond:0}/s"));
    }

    /// <summary>
    /// Write throughput under real concurrency, checked vs. unchecked. <see cref="Create_item_throughput"/>
    /// and its numbers in the package README are a single <see cref="HttpClient"/> firing requests
    /// sequentially — concurrency 1, so they measure per-request latency inverted, not the service's
    /// actual ceiling. This fires the same request shape from <see cref="ClientCount"/> concurrent
    /// clients against both <c>POST /items</c> (the WATCH-conditioned, concurrency-checked path) and
    /// <c>POST /items/no-check</c> (<see cref="IEventRepository.SaveWithoutConcurrencyCheckAsync"/>,
    /// added purely to make this comparison possible), so the two numbers isolate the cost of the
    /// per-save version check under contention rather than differing in anything else about the
    /// request.
    /// </summary>
    [Theory]
    [InlineData("/items", "checked")]
    [InlineData("/items/no-check", "unchecked")]
    public async Task Create_item_throughput_with_concurrent_clients(string path, string label)
    {
        await fixture.FlushAllAsync();

        await using var commandApi = ServiceTestHosts.CommandApi(fixture);

        const int clientCount = 10;
        const int perClient = 50;

        var clients = new HttpClient[clientCount];
        try
        {
            for (var i = 0; i < clientCount; i++)
            {
                clients[i] = commandApi.CreateClient();
            }

            var sw = Stopwatch.StartNew();

            var tasks = new Task[clientCount];
            for (var c = 0; c < clientCount; c++)
            {
                var client = clients[c];
                var clientIndex = c;
                tasks[c] = Task.Run(async () =>
                {
                    for (var i = 0; i < perClient; i++)
                    {
                        var id = string.Create(CultureInfo.InvariantCulture, $"perf-conc-{label}-{clientIndex}-{i}-{Guid.NewGuid():N}");
                        var response = await client.PostAsJsonAsync(path, new { Id = id, Name = "Widget" });
                        response.EnsureSuccessStatusCode();
                    }
                });
            }

            await Task.WhenAll(tasks);
            sw.Stop();

            var total = clientCount * perClient;
            var perSecond = total / Math.Max(sw.Elapsed.TotalSeconds, 0.0001);
            output.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"POST {path} throughput ({label}, command-side service, {clientCount} concurrent clients): " +
                $"{total} in {sw.Elapsed.TotalMilliseconds:0} ms = {perSecond:0}/s"));
        }
        finally
        {
            foreach (var client in clients)
            {
                client?.Dispose();
            }
        }
    }

    /// <summary>
    /// Read throughput against the real view-side service, mirroring
    /// <see cref="Create_item_throughput"/> for the other half of the CQRS split: one item is seeded
    /// through the command API, then <c>GET /items/{id}</c> is hit repeatedly on the view API once
    /// the projector has caught up, so every request measured is a real hit rather than a 404.
    /// </summary>
    [Fact]
    public async Task Get_item_throughput()
    {
        await fixture.FlushAllAsync();

        var commandApi = ServiceTestHosts.CommandApi(fixture);
        var viewApi = ServiceTestHosts.ViewApi(fixture, fixture.NewConsumer());

        try
        {
            using var commandClient = commandApi.CreateClient();
            using var viewClient = viewApi.CreateClient();

            var id = $"perf-get-{Guid.NewGuid():N}";
            var createResponse = await commandClient.PostAsJsonAsync("/items", new { Id = id, Name = "Widget" });
            createResponse.EnsureSuccessStatusCode();

            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await viewClient.GetAsync($"/items/{id}")).StatusCode == HttpStatusCode.OK,
                TimeSpan.FromSeconds(15),
                "the real projector to carry the seeded item into the real view before measuring reads");

            const int total = 200;
            var sw = Stopwatch.StartNew();

            for (var i = 0; i < total; i++)
            {
                var response = await viewClient.GetAsync($"/items/{id}");
                response.EnsureSuccessStatusCode();
            }

            sw.Stop();

            var perSecond = total / Math.Max(sw.Elapsed.TotalSeconds, 0.0001);
            output.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"GET /items/{{id}} throughput (view-side service, real HTTP): {total} in {sw.Elapsed.TotalMilliseconds:0} ms = {perSecond:0}/s"));
        }
        finally
        {
            await viewApi.DisposeQuietlyAsync();
            await commandApi.DisposeQuietlyAsync();
        }
    }

    /// <summary>
    /// Read throughput under real concurrency, mirroring
    /// <see cref="Create_item_throughput_with_concurrent_clients"/> for symmetry: 10 concurrent
    /// clients hitting <c>GET /items/{id}</c> on the real view-side service, vs.
    /// <see cref="Get_item_throughput"/>'s single sequential client.
    /// </summary>
    [Fact]
    public async Task Get_item_throughput_with_concurrent_clients()
    {
        await fixture.FlushAllAsync();

        var commandApi = ServiceTestHosts.CommandApi(fixture);
        var viewApi = ServiceTestHosts.ViewApi(fixture, fixture.NewConsumer());

        const int clientCount = 10;
        const int perClient = 50;
        var clients = new HttpClient[clientCount];

        try
        {
            using var commandClient = commandApi.CreateClient();

            var id = $"perf-get-conc-{Guid.NewGuid():N}";
            var createResponse = await commandClient.PostAsJsonAsync("/items", new { Id = id, Name = "Widget" });
            createResponse.EnsureSuccessStatusCode();

            for (var i = 0; i < clientCount; i++)
            {
                clients[i] = viewApi.CreateClient();
            }

            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await clients[0].GetAsync($"/items/{id}")).StatusCode == HttpStatusCode.OK,
                TimeSpan.FromSeconds(15),
                "the real projector to carry the seeded item into the real view before measuring reads");

            var sw = Stopwatch.StartNew();

            var tasks = new Task[clientCount];
            for (var c = 0; c < clientCount; c++)
            {
                var client = clients[c];
                tasks[c] = Task.Run(async () =>
                {
                    for (var i = 0; i < perClient; i++)
                    {
                        var response = await client.GetAsync($"/items/{id}");
                        response.EnsureSuccessStatusCode();
                    }
                });
            }

            await Task.WhenAll(tasks);
            sw.Stop();

            var total = clientCount * perClient;
            var perSecond = total / Math.Max(sw.Elapsed.TotalSeconds, 0.0001);
            output.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"GET /items/{{id}} throughput (view-side service, {clientCount} concurrent clients): " +
                $"{total} in {sw.Elapsed.TotalMilliseconds:0} ms = {perSecond:0}/s"));
        }
        finally
        {
            foreach (var client in clients)
            {
                client?.Dispose();
            }

            await viewApi.DisposeQuietlyAsync();
            await commandApi.DisposeQuietlyAsync();
        }
    }

    /// <summary>
    /// The number nothing else in this repo measures: wall-clock from a <c>POST /items</c> completing
    /// on the real command-side service to the first successful <c>GET /items/{id}</c> on the real
    /// view-side service — two independently hosted processes, talking only through the real Redis
    /// topic between them. Percentiles rather than a single average, in the same style (and using the
    /// same rank-based percentile calculation) as
    /// <c>EventSourcingPerfBenchmarkTests.End_to_end_save_to_projected_view_latency</c>, the
    /// library-level equivalent of this same measurement without the two HTTP hops.
    /// </summary>
    [Fact]
    public async Task Create_to_view_latency()
    {
        await fixture.FlushAllAsync();

        var commandApi = ServiceTestHosts.CommandApi(fixture);
        var viewApi = ServiceTestHosts.ViewApi(fixture, fixture.NewConsumer());

        try
        {
            using var commandClient = commandApi.CreateClient();
            using var viewClient = viewApi.CreateClient();

            const int samples = 20;
            var latencies = new double[samples];

            for (var i = 0; i < samples; i++)
            {
                var id = $"perf-e2e-{i.ToString(CultureInfo.InvariantCulture)}-{Guid.NewGuid():N}";

                var sw = Stopwatch.StartNew();

                var createResponse = await commandClient.PostAsJsonAsync("/items", new { Id = id, Name = "Widget" });
                createResponse.EnsureSuccessStatusCode();

                await RedisStreamsFixture.WaitUntilAsync(
                    async () => (await viewClient.GetAsync($"/items/{id}")).StatusCode == HttpStatusCode.OK,
                    TimeSpan.FromSeconds(15),
                    $"the view-side service's HTTP endpoint to reflect item '{id}'");

                sw.Stop();
                latencies[i] = sw.Elapsed.TotalMilliseconds;
            }

            Array.Sort(latencies);
            var p50 = Percentile(latencies, 0.50);
            var p90 = Percentile(latencies, 0.90);

            output.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"POST /items -> GET /items/{{id}} latency, over real HTTP across both services ({samples} samples): " +
                $"p50 {p50:0.0} ms, p90 {p90:0.0} ms, max {latencies[^1]:0.0} ms"));
        }
        finally
        {
            await viewApi.DisposeQuietlyAsync();
            await commandApi.DisposeQuietlyAsync();
        }
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
}
