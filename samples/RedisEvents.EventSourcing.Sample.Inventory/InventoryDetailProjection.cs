namespace RedisEvents.EventSourcing.Sample.Inventory;

/// <summary>
/// Keeps <see cref="InventoryDetail"/> up to date from inventory events.
/// </summary>
/// <remarks>
/// RedisEvents delivers at least once, so this projection can be asked to handle the same event twice
/// — for example after a crash between the view being written and the consumer's position being
/// saved. <see cref="ItemCreated"/> is a plain <c>SetAsync</c>, which is idempotent on its own since
/// writing the same value again changes nothing observable. Every other handler reads the current
/// view first and skips the update when <c>current.LastEventId &gt;= meta.Id</c>: without that guard a
/// redelivered <see cref="ItemsCheckedIn"/> or <see cref="ItemsRemoved"/> would apply its count a
/// second time and silently corrupt the running total. <c>meta.Id</c> is Redis's own per-partition
/// stream entry id, not an aggregate version — nothing here depends on these events having come from
/// an <see cref="AggregateRoot"/>.
/// </remarks>
public sealed class InventoryDetailProjection(IViewStore<InventoryDetail> views) :
    IProjection<ItemCreated>,
    IProjection<ItemRenamed>,
    IProjection<ItemsCheckedIn>,
    IProjection<ItemsRemoved>,
    IProjection<ItemDeactivated>
{
    public ValueTask HandleAsync(ItemCreated @event, EventMeta meta, CancellationToken ct) =>
        views.SetAsync(@event.Id, new InventoryDetail(@event.Id, @event.Name, true, 0, meta.Id), ct);

    public async ValueTask HandleAsync(ItemRenamed @event, EventMeta meta, CancellationToken ct)
    {
        var current = await views.GetAsync(meta.PartitionKey, ct);
        if (current is null || current.LastEventId.CompareTo(meta.Id) >= 0)
        {
            return;
        }

        await views.SetAsync(meta.PartitionKey, current with { Name = @event.NewName, LastEventId = meta.Id }, ct);
    }

    public async ValueTask HandleAsync(ItemsCheckedIn @event, EventMeta meta, CancellationToken ct)
    {
        var current = await views.GetAsync(meta.PartitionKey, ct);
        if (current is null || current.LastEventId.CompareTo(meta.Id) >= 0)
        {
            return;
        }

        await views.SetAsync(
            meta.PartitionKey,
            current with { CurrentCount = current.CurrentCount + @event.Count, LastEventId = meta.Id },
            ct);
    }

    public async ValueTask HandleAsync(ItemsRemoved @event, EventMeta meta, CancellationToken ct)
    {
        var current = await views.GetAsync(meta.PartitionKey, ct);
        if (current is null || current.LastEventId.CompareTo(meta.Id) >= 0)
        {
            return;
        }

        await views.SetAsync(
            meta.PartitionKey,
            current with { CurrentCount = current.CurrentCount - @event.Count, LastEventId = meta.Id },
            ct);
    }

    public async ValueTask HandleAsync(ItemDeactivated @event, EventMeta meta, CancellationToken ct)
    {
        var current = await views.GetAsync(meta.PartitionKey, ct);
        if (current is null || current.LastEventId.CompareTo(meta.Id) >= 0)
        {
            return;
        }

        await views.SetAsync(meta.PartitionKey, current with { Active = false, LastEventId = meta.Id }, ct);
    }
}
