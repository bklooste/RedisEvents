using System.Net;
using System.Net.Http.Json;

using FluentAssertions;

using RedisEvents.EventSourcing.Sample.Inventory;

namespace RedisEvents.Tests;

/// <summary>
/// Hosts both Inventory sample services for real — <c>RedisEvents.EventSourcing.Sample.Inventory.CommandApi</c>
/// and <c>.ViewApi</c>, each a genuine <see cref="Microsoft.AspNetCore.Mvc.Testing.WebApplicationFactory{TEntryPoint}"/>
/// over the fixture's real Testcontainers Redis — and talks to them purely over HTTP, exactly as two
/// independently deployed microservices would be exercised. This is deliberately not more of the
/// library's own correctness coverage (that lives in <c>RedisEvents.EventSourcing.UnitTests</c> and
/// <c>RedisEvents.EventSourcing.Tests</c>); it proves the two-service split itself works end to end.
/// </summary>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class InventoryServiceEndToEndTests(RedisStreamsFixture fixture)
{
    /// <summary>
    /// Create, rename, check in, remove and deactivate — each command issued as a real HTTP call to
    /// the command-side service, each change polled for on the read-side service's own HTTP endpoint,
    /// with nothing shared between the two processes but the real Redis topic between them.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Full_http_round_trip_through_both_services()
    {
        await fixture.FlushAllAsync();

        var commandApi = ServiceTestHosts.CommandApi(fixture);
        var viewApi = ServiceTestHosts.ViewApi(fixture, fixture.NewConsumer());

        try
        {
            using var commandClient = commandApi.CreateClient();
            using var viewClient = viewApi.CreateClient();

            var id = $"item-{Guid.NewGuid():N}";

            var createResponse = await commandClient.PostAsJsonAsync("/items", new { Id = id, Name = "Widget" });
            createResponse.StatusCode.Should().Be(HttpStatusCode.Created, "creating a new item must succeed");

            var created = await PollViewAsync(viewClient, id, d => d is { Name: "Widget", Active: true, CurrentCount: 0 });
            created.Version.Should().Be(1);

            var renameResponse = await commandClient.PostAsJsonAsync($"/items/{id}/rename", new { NewName = "Widget v2" });
            renameResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            await PollViewAsync(viewClient, id, d => d.Name == "Widget v2");

            var checkInResponse = await commandClient.PostAsJsonAsync($"/items/{id}/check-in", new { Count = 5 });
            checkInResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            await PollViewAsync(viewClient, id, d => d.CurrentCount == 5);

            var removeResponse = await commandClient.PostAsJsonAsync($"/items/{id}/remove", new { Count = 2 });
            removeResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            await PollViewAsync(viewClient, id, d => d.CurrentCount == 3);

            var deactivateResponse = await commandClient.PostAsync($"/items/{id}/deactivate", content: null);
            deactivateResponse.StatusCode.Should().Be(HttpStatusCode.OK);
            var final = await PollViewAsync(viewClient, id, d => !d.Active);

            final.Should().BeEquivalentTo(new InventoryDetail(id, "Widget v2", false, 3, 5));
        }
        finally
        {
            await viewApi.DisposeQuietlyAsync();
            await commandApi.DisposeQuietlyAsync();
        }
    }

    /// <summary>
    /// Fires a batch of concurrent renames at the same item through the command-side service's real
    /// HTTP endpoint. <c>IEventRepository.SaveAsync</c>'s optimistic version check means at most one
    /// per contended version can win; every loser must come back as this sample's mapped HTTP 409
    /// (see <c>Program.cs</c>'s <c>catch (ConcurrencyException)</c>), never a silently-lost update or
    /// an unmapped 500. The read side must then settle on exactly the winners' outcome.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Concurrent_renames_surface_a_conflict_and_the_view_settles_on_one_outcome()
    {
        await fixture.FlushAllAsync();

        var commandApi = ServiceTestHosts.CommandApi(fixture);
        var viewApi = ServiceTestHosts.ViewApi(fixture, fixture.NewConsumer());

        try
        {
            using var commandClient = commandApi.CreateClient();
            using var viewClient = viewApi.CreateClient();

            var id = $"item-{Guid.NewGuid():N}";
            var createResponse = await commandClient.PostAsJsonAsync("/items", new { Id = id, Name = "Original" });
            createResponse.StatusCode.Should().Be(HttpStatusCode.Created);

            const int concurrency = 10;
            var renameTasks = Enumerable.Range(0, concurrency)
                .Select(i => commandClient.PostAsJsonAsync($"/items/{id}/rename", new { NewName = $"Renamed-{i}" }))
                .ToArray();

            var responses = await Task.WhenAll(renameTasks);

            var successes = responses.Count(r => r.IsSuccessStatusCode);
            var conflicts = responses.Count(r => r.StatusCode == HttpStatusCode.Conflict);

            (successes + conflicts).Should().Be(
                concurrency,
                "every response must be either a clean win or the mapped 409 — nothing silently swallowed and nothing unmapped");
            successes.Should().BeGreaterThan(0, "at least one of the racing renames must win");
            conflicts.Should().BeGreaterThan(
                0,
                "at least one of the racing renames must lose the optimistic version check and surface as HTTP 409, " +
                "rather than corrupting the aggregate's history");

            // Exactly one winner per contended version: the aggregate started at version 1 (the create),
            // so its final version is 1 plus however many renames actually won the race.
            var expectedFinalVersion = 1 + successes;

            var final = await PollViewAsync(viewClient, id, d => d.Version == expectedFinalVersion);
            final.Name.Should().StartWith("Renamed-", "the view must reflect one of the renames that actually won, not the pre-race name");
        }
        finally
        {
            await viewApi.DisposeQuietlyAsync();
            await commandApi.DisposeQuietlyAsync();
        }
    }

    /// <summary>Polls the read-side service's own HTTP endpoint until <paramref name="predicate"/> holds.</summary>
    private static async Task<InventoryDetail> PollViewAsync(
        HttpClient viewClient,
        string id,
        Func<InventoryDetail, bool> predicate)
    {
        InventoryDetail? last = null;

        await RedisStreamsFixture.WaitUntilAsync(
            async () =>
            {
                var response = await viewClient.GetAsync($"/items/{id}");
                if (!response.IsSuccessStatusCode)
                {
                    return false;
                }

                last = await response.Content.ReadFromJsonAsync<InventoryDetail>();
                return last is not null && predicate(last);
            },
            TimeSpan.FromSeconds(15),
            $"the read-side service's view for '{id}' to reflect the expected change");

        return last!;
    }
}
