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
