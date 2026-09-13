using RedisEvents.EventSourcing;
using RedisEvents.EventSourcing.Sample.Inventory;

// The command side of the two-service Inventory sample: one line of RedisEvents.EventSourcing
// wiring (AddEventStore), then a handful of minimal-API endpoints that each do exactly the
// load -> decide -> save loop the package README's quickstart shows. No controllers, no Swagger —
// this is a demo host, not the library, so plain reflection-based minimal-API JSON binding is used
// rather than a source-generated JsonSerializerContext for the request DTOs below.
var builder = WebApplication.CreateBuilder(args);
builder.AddEventStore("inventory", InventoryEventTypes.Register);

var app = builder.Build();

app.MapPost("/items", async (CreateItemRequest req, IEventRepository repo, CancellationToken ct) =>
{
    var item = InventoryItem.Create(req.Id, req.Name);
    var version = await repo.SaveAsync(item, ct: ct);
    return Results.Created($"/items/{item.Id}", new { item.Id, version });
});

// Benchmark-only twin of POST /items: same load -> decide -> save shape, but through
// SaveWithoutConcurrencyCheckAsync, so a throughput comparison isolates the cost of the WATCH-based
// version check itself rather than anything else about the request.
app.MapPost("/items/no-check", async (CreateItemRequest req, IEventRepository repo, CancellationToken ct) =>
{
    var item = InventoryItem.Create(req.Id, req.Name);
    var version = await repo.SaveWithoutConcurrencyCheckAsync(item, ct: ct);
    return Results.Created($"/items/{item.Id}", new { item.Id, version });
});

app.MapPost("/items/{id}/rename", async (string id, RenameItemRequest req, IEventRepository repo, CancellationToken ct) =>
{
    var item = await repo.LoadAsync<InventoryItem>(id, ct);
    if (item is null)
    {
        return Results.NotFound();
    }

    try
    {
        item.Rename(req.NewName);
        var version = await repo.SaveAsync(item, ct: ct);
        return Results.Ok(new { id, version });
    }
    catch (ConcurrencyException ex)
    {
        return Results.Conflict(new { ex.Message });
    }
});

app.MapPost("/items/{id}/check-in", async (string id, CheckInRequest req, IEventRepository repo, CancellationToken ct) =>
{
    var item = await repo.LoadAsync<InventoryItem>(id, ct);
    if (item is null)
    {
        return Results.NotFound();
    }

    try
    {
        item.CheckIn(req.Count);
        var version = await repo.SaveAsync(item, ct: ct);
        return Results.Ok(new { id, version });
    }
    catch (ConcurrencyException ex)
    {
        return Results.Conflict(new { ex.Message });
    }
});

app.MapPost("/items/{id}/remove", async (string id, RemoveRequest req, IEventRepository repo, CancellationToken ct) =>
{
    var item = await repo.LoadAsync<InventoryItem>(id, ct);
    if (item is null)
    {
        return Results.NotFound();
    }

    try
    {
        item.Remove(req.Count);
        var version = await repo.SaveAsync(item, ct: ct);
        return Results.Ok(new { id, version });
    }
    catch (ConcurrencyException ex)
    {
        return Results.Conflict(new { ex.Message });
    }
});

app.MapPost("/items/{id}/deactivate", async (string id, IEventRepository repo, CancellationToken ct) =>
{
    var item = await repo.LoadAsync<InventoryItem>(id, ct);
    if (item is null)
    {
        return Results.NotFound();
    }

    try
    {
        item.Deactivate();
        var version = await repo.SaveAsync(item, ct: ct);
        return Results.Ok(new { id, version });
    }
    catch (ConcurrencyException ex)
    {
        return Results.Conflict(new { ex.Message });
    }
});

app.Run();

/// <summary>Creates a new inventory item.</summary>
internal sealed record CreateItemRequest(string Id, string Name);

/// <summary>Renames an existing inventory item.</summary>
internal sealed record RenameItemRequest(string NewName);

/// <summary>Checks stock into an existing inventory item.</summary>
internal sealed record CheckInRequest(int Count);

/// <summary>Removes stock from an existing inventory item.</summary>
internal sealed record RemoveRequest(int Count);

/// <summary>The entry point type, exposed so <c>WebApplicationFactory&lt;Program&gt;</c> can host this service in tests.</summary>
public partial class Program;
