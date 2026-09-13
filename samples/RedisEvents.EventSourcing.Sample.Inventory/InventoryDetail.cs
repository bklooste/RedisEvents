using RedisEvents.Wire;

namespace RedisEvents.EventSourcing.Sample.Inventory;

/// <summary>The read-side view of one inventory item, kept up to date by <see cref="InventoryDetailProjection"/>.</summary>
/// <param name="LastEventId">
/// The <see cref="EventMeta.Id"/> of the last event applied to this view — Redis's own per-partition
/// stream entry id, not an aggregate version — so a redelivered event can be recognised and skipped.
/// </param>
public sealed record InventoryDetail(string Id, string Name, bool Active, int CurrentCount, StreamId LastEventId);
