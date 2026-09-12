namespace RedisEvents.EventSourcing.Sample.Inventory;

/// <summary>The read-side view of one inventory item, kept up to date by <see cref="InventoryDetailProjection"/>.</summary>
public sealed record InventoryDetail(string Id, string Name, bool Active, int CurrentCount, int Version);
