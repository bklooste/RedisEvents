using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Json;

namespace RedisEvents.Tests;

/// <summary>
/// Throughput of the command-side sample service over real HTTP, against a real Testcontainers
/// Redis. Not part of the correctness suite — run explicitly with
/// <c>dotnet test --filter "TestType=PerfTest"</c>, exactly like every other perf test in this repo.
/// Results are printed to test output as requests/second so numbers can be compared across runs;
/// there is no assertion on the absolute number, matching <c>EventSourcingPerfBenchmarkTests</c>'s
/// convention — a CI box's throughput is not a contract.
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
}
