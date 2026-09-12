namespace RedisEvents.EventSourcing.Sample.Inventory;

/// <summary>
/// The command-side aggregate for one inventory item: no infrastructure in sight, just event handlers
/// and the invariants that decide when a new event may be raised.
/// </summary>
public sealed class InventoryItem : AggregateRoot
{
    private string id = "";
    private string name = "";
    private bool active;
    private int currentCount;

    public InventoryItem()
    {
        On<ItemCreated>(e =>
        {
            id = e.Id;
            name = e.Name;
            active = true;
        });

        On<ItemRenamed>(e => name = e.NewName);
        On<ItemsCheckedIn>(e => currentCount += e.Count);
        On<ItemsRemoved>(e => currentCount -= e.Count);
        On<ItemDeactivated>(e => active = false);
    }

    public override string AggregateName => "Inventory";

    public override string Id => id;

    /// <summary>Creates a new inventory item by raising its first event.</summary>
    public static InventoryItem Create(string id, string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("name is not valid", nameof(name));
        }

        var item = new InventoryItem();
        item.Raise(new ItemCreated(id, name));
        return item;
    }

    public void Rename(string newName)
    {
        if (string.IsNullOrEmpty(newName))
        {
            throw new ArgumentException("newName is not valid", nameof(newName));
        }

        Raise(new ItemRenamed(newName));
    }

    public void CheckIn(int count)
    {
        if (count <= 0)
        {
            throw new InvalidOperationException("count must be positive");
        }

        Raise(new ItemsCheckedIn(count));
    }

    public void Remove(int count)
    {
        if (count <= 0)
        {
            throw new InvalidOperationException("count must be positive");
        }

        if (count > currentCount)
        {
            throw new InvalidOperationException("cannot remove more than current count");
        }

        Raise(new ItemsRemoved(count));
    }

    public void Deactivate()
    {
        if (!active)
        {
            throw new InvalidOperationException("already deactivated");
        }

        Raise(new ItemDeactivated());
    }
}
