namespace RedisEvents.EventSourcing.Sample.Inventory;

/// <summary>An inventory item was created with an id and a starting name.</summary>
public sealed record ItemCreated(string Id, string Name);

/// <summary>An inventory item was given a new name.</summary>
public sealed record ItemRenamed(string NewName);

/// <summary>Stock for an inventory item was checked in.</summary>
public sealed record ItemsCheckedIn(int Count);

/// <summary>Stock for an inventory item was removed.</summary>
public sealed record ItemsRemoved(int Count);

/// <summary>An inventory item was deactivated. Carries no data beyond having happened.</summary>
public sealed record ItemDeactivated;
