using RedisEvents.Projections;

namespace RedisEvents.EventSourcing.Sample.Inventory;

/// <summary>Registers every inventory event's wire type, for both the command side and the read side to share.</summary>
internal static class InventoryEventTypes
{
    /// <summary>
    /// Registers the wire type for each inventory event. Passed as the <c>Action&lt;EventTypeRegistry&gt;</c>
    /// that <c>AddEventStore</c> and <c>AddEventProjector</c> both take, so the command side and the read
    /// side always agree on how events are (de)serialised.
    /// </summary>
    public static void Register(EventTypeRegistry events)
    {
        events.RegisterJson("inventory.created", InventoryJsonContext.Default.ItemCreated)
              .RegisterJson("inventory.renamed", InventoryJsonContext.Default.ItemRenamed)
              .RegisterJson("inventory.checked-in", InventoryJsonContext.Default.ItemsCheckedIn)
              .RegisterJson("inventory.removed", InventoryJsonContext.Default.ItemsRemoved)
              .RegisterJson("inventory.deactivated", InventoryJsonContext.Default.ItemDeactivated);
    }
}
