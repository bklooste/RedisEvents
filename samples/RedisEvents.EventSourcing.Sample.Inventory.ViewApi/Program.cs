using RedisEvents.EventSourcing.Sample.Inventory;
using RedisEvents.Projections;

// The read side of the two-service Inventory sample: an event projector consuming the same topic
// the command side (RedisEvents.EventSourcing.Sample.Inventory.CommandApi) writes to, a Redis-backed
// view store, and one query endpoint — the whole read side of the package README's quickstart.
var builder = WebApplication.CreateBuilder(args);
builder.AddEventProjector("inventory", InventoryEventTypes.Register)
       .AddRedisViewStore<InventoryDetail>("inventory", "detail", InventoryJsonContext.Default.InventoryDetail)
       .AddProjection<InventoryDetailProjection>();

var app = builder.Build();

app.MapGet("/items/{id}", async (string id, IViewStore<InventoryDetail> views, CancellationToken ct) =>
    await views.GetAsync(id, ct) is { } detail ? Results.Ok(detail) : Results.NotFound());

app.Run();

/// <summary>The entry point type, exposed so <c>WebApplicationFactory&lt;Program&gt;</c> can host this service in tests.</summary>
public partial class Program;
