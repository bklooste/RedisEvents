using System.Text.Json.Serialization;

namespace RedisEvents.EventSourcing.Sample.Inventory;

/// <summary>Source-generated JSON metadata for every inventory event and view, so serialisation stays AOT-safe.</summary>
[JsonSerializable(typeof(ItemCreated))]
[JsonSerializable(typeof(ItemRenamed))]
[JsonSerializable(typeof(ItemsCheckedIn))]
[JsonSerializable(typeof(ItemsRemoved))]
[JsonSerializable(typeof(ItemDeactivated))]
[JsonSerializable(typeof(InventoryDetail))]
internal partial class InventoryJsonContext : JsonSerializerContext;
