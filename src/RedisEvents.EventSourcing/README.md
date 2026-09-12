# RedisEvents.EventSourcing

A light event-sourced aggregate root (command side) and a typed event projector (read side) on top
of [RedisEvents](../RedisEvents/README.md). It rides the same partitioned topics, positions and
error contract core already provides — this package adds only what an event store needs beyond
that: an aggregate's own stream as its source of truth, optimistic concurrency, and reflection-free
dispatch on both sides.

**This is a library, not a service.** It has no ASP.NET Core dependency and starts nothing on its
own beyond what `AddEventStore`/`AddEventProjector` wire into your host.

A full worked example — events, an aggregate, a projection, wired end to end — lives in
[`samples/RedisEvents.EventSourcing.Sample.Inventory`](../../samples/RedisEvents.EventSourcing.Sample.Inventory);
the snippets below are lifted directly from it.

## Quickstart

**1. Events** — plain records, with source-generated JSON for AOT:

```csharp
public sealed record ItemCreated(string Id, string Name);
public sealed record ItemRenamed(string NewName);
public sealed record ItemsCheckedIn(int Count);
public sealed record ItemsRemoved(int Count);
public sealed record ItemDeactivated;

[JsonSerializable(typeof(ItemCreated))]
[JsonSerializable(typeof(ItemRenamed))]
[JsonSerializable(typeof(ItemsCheckedIn))]
[JsonSerializable(typeof(ItemsRemoved))]
[JsonSerializable(typeof(ItemDeactivated))]
internal partial class InventoryJsonContext : JsonSerializerContext;

internal static class InventoryEventTypes
{
    public static void Register(EventTypeRegistry events) => events
        .RegisterJson("inventory.created", InventoryJsonContext.Default.ItemCreated)
        .RegisterJson("inventory.renamed", InventoryJsonContext.Default.ItemRenamed)
        .RegisterJson("inventory.checked-in", InventoryJsonContext.Default.ItemsCheckedIn)
        .RegisterJson("inventory.removed", InventoryJsonContext.Default.ItemsRemoved)
        .RegisterJson("inventory.deactivated", InventoryJsonContext.Default.ItemDeactivated);
}
```

The wire type string (`"inventory.created"`) is chosen once, by you, and never derived from the
class name — see [Serialisation](#serialisation-and-the-event-type-registry) for why that matters.

**2. The aggregate** — command side, no infrastructure in sight:

```csharp
public sealed class InventoryItem : AggregateRoot
{
    string id = "";
    string name = "";
    bool active;
    int currentCount;

    public InventoryItem()
    {
        On<ItemCreated>(e => { id = e.Id; name = e.Name; active = true; });
        On<ItemRenamed>(e => name = e.NewName);
        On<ItemsCheckedIn>(e => currentCount += e.Count);
        On<ItemsRemoved>(e => currentCount -= e.Count);
        On<ItemDeactivated>(e => active = false);
    }

    public override string AggregateName => "Inventory";
    public override string Id => id;

    public static InventoryItem Create(string id, string name)
    {
        if (string.IsNullOrEmpty(name))
            throw new ArgumentException("name is not valid", nameof(name));

        var item = new InventoryItem();
        item.Raise(new ItemCreated(id, name));
        return item;
    }

    public void Rename(string newName) => Raise(new ItemRenamed(newName));

    public void Deactivate()
    {
        if (!active) throw new InvalidOperationException("already deactivated");
        Raise(new ItemDeactivated());
    }

    // CheckIn / Remove: raise ItemsCheckedIn / ItemsRemoved after their own validation.
}
```

`On<TEvent>` replaces `dynamic`-based `Apply` dispatch: it is registered once, in the constructor, so
every event type the aggregate can ever see is known statically — no reflection, and raising or
replaying an event nobody registered a handler for throws immediately instead of silently doing
nothing.

**3. Wiring, command side** — three lines:

```csharp
builder.AddEventStore("inventory", InventoryEventTypes.Register);

// later, injected as IEventRepository
var item = await repository.LoadAsync<InventoryItem>(id)
    ?? throw new KeyNotFoundException(id);

item.Rename("New name");
await repository.SaveAsync(item);   // expectedVersion defaults to item.Version
```

`LoadAsync` returns `null` — not an exception — when the id has no history; "does this aggregate
exist" is an ordinary question, not an error path.

**4. A projection and the read side** — one class, four lines of wiring:

```csharp
public sealed record InventoryDetail(string Id, string Name, bool Active, int CurrentCount, int Version);

public sealed class InventoryDetailProjection(IViewStore<InventoryDetail> views) :
    IProjection<ItemCreated>, IProjection<ItemRenamed>
{
    public ValueTask HandleAsync(ItemCreated e, EventMeta meta, CancellationToken ct) =>
        views.SetAsync(e.Id, new InventoryDetail(e.Id, e.Name, true, 0, meta.Version), ct);

    public async ValueTask HandleAsync(ItemRenamed e, EventMeta meta, CancellationToken ct)
    {
        var current = await views.GetAsync(meta.AggregateId, ct);
        if (current is null || current.Version >= meta.Version) return;   // at-least-once: skip a redelivery
        await views.SetAsync(meta.AggregateId, current with { Name = e.NewName, Version = meta.Version }, ct);
    }
}

builder.AddEventProjector("inventory", InventoryEventTypes.Register)
       .AddRedisViewStore<InventoryDetail>("inventory", "detail", InventoryJsonContext.Default.InventoryDetail)
       .AddProjection<InventoryDetailProjection>();

// later, injected as IViewStore<InventoryDetail>
var detail = await views.GetAsync(id);
```

A class may implement `IProjection<TEvent>` for as many event types as it likes — each is bound
automatically, with no registration beyond implementing the interface.

That's the whole quickstart: events + a JSON context, one aggregate class, one projection class, and
seven lines of `builder.Add...` wiring across both sides. No `IServiceCollection` ceremony.

## How events are stored

| Purpose | Key |
|---|---|
| Aggregate stream (source of truth) | `{topic}:state:es:<AggregateName>:<id>` |
| Topic partition (projection feed) | `s:{topic}:<p>` |

`SaveAsync` appends every uncommitted event to the aggregate's own stream **and** publishes each to
the topic (partitioned by the aggregate's id, so its events land on one partition and stay ordered
for the projector) in a single `MULTI`/`EXEC`:

```
WATCH {topic}:state:es:Inventory:42        (the version check)
MULTI
  XADD {topic}:state:es:Inventory:42 * ...    ← the aggregate's own history, no MAXLEN, ever
  XADD s:{topic}:<p> MAXLEN ~ n * ...         ← the topic, trimmed like any publish
EXEC
```

**`Version` is the aggregate stream's length.** It is the only definition a single Redis round trip
can enforce as a concurrency check, and it needs no second key to stay in sync. The corollary is an
invariant this package relies on and nothing in it ever violates: **an aggregate's own stream is
never trimmed.** If something outside this package ever did, every later concurrency check would
silently compare against the wrong number.

Both writes are one transaction: a version-check failure applies neither, and a connection lost
around `EXEC` leaves an outcome that is unknown but never torn — the aggregate's history and the
topic can never disagree about what happened. Every published event carries an `es-version` header
(its 1-based version) so a projection can tell a redelivery from new information.

## Optimistic concurrency

```csharp
try
{
    await repository.SaveAsync(item);
}
catch (ConcurrencyException)
{
    item = await repository.LoadAsync<InventoryItem>(id) ?? throw new KeyNotFoundException(id);
    item.Rename("New name");   // re-decide against the current state
    await repository.SaveAsync(item);
}
```

`ConcurrencyException` is a plain `Exception`, not a `RedisEvents.Errors.DontIgnoreException`.
Retrying an identical save can never succeed after a lost race — only a reload and a fresh decision
fixes it — so deriving from `DontIgnoreException` would turn a caller-resolvable condition into a
partition blocked forever. A command handler that genuinely must not give up wraps this in its own
policy:

```csharp
sealed class InventoryCommandStuckException(string message, Exception inner) : DontIgnoreException(message, inner);

for (var attempt = 0; ; attempt++)
{
    try { await repository.SaveAsync(item); break; }
    catch (ConcurrencyException ex) when (attempt < 3) { item = await Reload(); }
    catch (ConcurrencyException ex) { throw new InventoryCommandStuckException("gave up after 3 retries", ex); }
}
```

## Serialisation and the event-type registry

`RegisterJson` covers the default, AOT-safe path via a source-generated `JsonTypeInfo<T>`. The wire
type string is chosen once and never derived from `.FullName` or `.Name` — a class rename is then a
pure refactor, because it is the string, not the CLR name, that a stream of history already written
is keyed on.

For anything else — MessagePack, a hand-rolled binary format — `Register` takes a raw serialise/
deserialise pair, adding no package dependency:

```csharp
events.Register<ItemCreated>(
    "inventory.created",
    e => MessagePackSerializer.Serialize(e, options),
    b => MessagePackSerializer.Deserialize<ItemCreated>(b, options));
```

This mirrors the same `Func<ReadOnlyMemory<byte>, TMessage?>` seam
[`RedisEvents.MessagePack`](../RedisEvents.MessagePack/README.md) plugs into core through, so its
size-gated LZ4 compression options can be passed straight through as `options`.

## View stores

`IViewStore<TView>` is an optional convenience, not a requirement — a projection may write anywhere
it likes. `AddRedisViewStore<TView>` stores one Redis **hash per view type** (`{topic}:view:<name>`,
field = view id, value = JSON), which is fine for up to tens of thousands of small views. Beyond
that, or for any query other than "by id" or "all", implement `IViewStore<TView>` yourself — for
example over Azure Table Storage:

```csharp
public sealed class TableViewStore<TView>(TableClient table, JsonTypeInfo<TView> json) : IViewStore<TView>
    where TView : class
{
    public async ValueTask<TView?> GetAsync(string id, CancellationToken ct = default)
    {
        try
        {
            var entity = await table.GetEntityAsync<TableEntity>("view", id, cancellationToken: ct);
            return JsonSerializer.Deserialize(entity.Value.GetString("Json"), json);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public Task SetAsync(string id, TView view, CancellationToken ct = default) =>
        table.UpsertEntityAsync(new TableEntity("view", id) { ["Json"] = JsonSerializer.Serialize(view, json) }, cancellationToken: ct).AsTask();

    // DeleteAsync, ListAsync: DeleteEntityAsync / QueryAsync<TableEntity>(e => e.PartitionKey == "view").
}
```

`InMemoryViewStore<TView>` is provided for tests and for trying a projection out before wiring up a
real store.

**Consumers are at-least-once**, so a projection can be asked to handle the same event twice — after
a crash between the view being written and the position being saved. `SetAsync` on its own is
idempotent (writing the same value again changes nothing observable); anything accumulative (a
running count) needs its own guard, comparing `EventMeta.Version` against a version stored on the
view, as `InventoryDetailProjection` does above.

## Error handling

The projector has no error policy of its own; it has core's. Per event, in batch order: an unknown
wire type or a known type with no projection bound to it is skipped at `Debug`; a decode failure
always throws; a projection that throws propagates unchanged and immediately, and the rest of the
batch is not processed. From there it is exactly
[core's error contract](../RedisEvents/README.md#the-error-contract): an ordinary exception logs the
batch and advances past it, and a `DontIgnoreException` subclass blocks the partition and retries
with backoff while every other partition keeps running.

## Limits

- **No snapshots.** `LoadAsync` replays an aggregate's full history every time. Add snapshotting only
  once a real aggregate's replay time says so — most never will.
- **No view-store abstraction beyond `IViewStore<TView>`.** View storage, indexing and querying stay
  the service's concern; this package supplies a minimal default and a seam, not a framework.
- **Never trim an aggregate's own stream.** Its length is its version — see
  [How events are stored](#how-events-are-stored).
- **A topic needs `CoLocatePartitions: true`** (the default) to host an event store: `AddEventStore`
  fails fast at startup otherwise, because there is no non-transactional fallback to offer.

## Benchmarks

`dotnet test --filter "TestType=PerfTest"` runs, alongside core's own: an in-process
`[MemoryDiagnoser]` micro-benchmark for `SaveAsync`/`EventProjector.HandleAsync`'s own per-call cost
(no Redis), and a real-Redis throughput benchmark reporting save/load throughput and save-to-projected-
view latency. Neither asserts on absolute numbers — a CI box's throughput is not a contract; they
exist so a change that regresses this package's hot path shows up before a reviewer has to notice it
by eye.
