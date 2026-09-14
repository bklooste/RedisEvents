# RedisEvents.Projections

Typed event projections and view stores on top of [RedisEvents](../RedisEvents/README.md): reflection-
free dispatch from a standard partitioned topic to `IProjection<TEvent>` handlers, and a minimal
`IViewStore<TView>` seam (with a Redis-backed default) for the views they maintain.

**Usable entirely on its own.** This package depends only on core RedisEvents — it has no idea what an
event-sourced aggregate is, and needs none to exist. The sibling
[`RedisEvents.EventSourcing`](../RedisEvents.EventSourcing/README.md) package (event-sourced
aggregates, per-aggregate Redis streams) depends on *this* one for `EventTypeRegistry`, so the two
share one registry per topic when a service uses both — but the dependency runs one way only. A
service that just wants typed projections over an ordinary topic — published by a plain
`IStreamPublisher.PublishAsync` call, or by anything else — needs nothing from that package at all.

**This is a library, not a service.** It has no ASP.NET Core dependency and starts nothing on its own
beyond what `AddEventProjector` wires into your host.

A full worked example lives in
[`samples/RedisEvents.EventSourcing.Sample.Inventory`](../../samples/RedisEvents.EventSourcing.Sample.Inventory) —
the events and projection class are the read side of the same Inventory example the sibling package's
own README walks through end to end, including an event store. The snippets below stand on their own
without one.

## Quickstart

**1. Events and a registry** — plain records, with source-generated JSON for AOT:

```csharp
public sealed record ItemCreated(string Id, string Name);
public sealed record ItemRenamed(string NewName);

[JsonSerializable(typeof(ItemCreated))]
[JsonSerializable(typeof(ItemRenamed))]
internal partial class InventoryJsonContext : JsonSerializerContext;

internal static class InventoryEventTypes
{
    public static void Register(EventTypeRegistry events) => events
        .RegisterJson("inventory.created", InventoryJsonContext.Default.ItemCreated)
        .RegisterJson("inventory.renamed", InventoryJsonContext.Default.ItemRenamed);
}
```

The wire type string (`"inventory.created"`) is chosen once, by you, and never derived from the class
name — see [Serialisation](#serialisation-and-the-event-type-registry) for why that matters.

**2. A projection** — one class, implementing `IProjection<TEvent>` for whichever events it cares about:

```csharp
public sealed record InventoryDetail(string Id, string Name, bool Active, StreamId LastEventId);

public sealed class InventoryDetailProjection(IViewStore<InventoryDetail> views) :
    IProjection<ItemCreated>, IProjection<ItemRenamed>
{
    public ValueTask HandleAsync(ItemCreated e, EventMeta meta, CancellationToken ct) =>
        views.SetAsync(e.Id, new InventoryDetail(e.Id, e.Name, true, meta.Id), ct);

    public async ValueTask HandleAsync(ItemRenamed e, EventMeta meta, CancellationToken ct)
    {
        var current = await views.GetAsync(meta.PartitionKey, ct);
        if (current is null || current.LastEventId >= meta.Id) return;   // at-least-once: skip a redelivery
        await views.SetAsync(meta.PartitionKey, current with { Name = e.NewName, LastEventId = meta.Id }, ct);
    }
}
```

`meta.PartitionKey` and `meta.Id` are the wire message's own partition key and Redis stream entry id —
nothing here assumes the event came from an event-sourced aggregate. This projector works identically
against a topic published by a plain `IStreamPublisher.PublishAsync` call, because it depends on
nothing beyond a standard RedisEvents topic — see [View stores](#view-stores) below for why `meta.Id`
is the right redelivery guard either way.

A class may implement `IProjection<TEvent>` for as many event types as it likes — each is bound
automatically, with no registration beyond implementing the interface.

**3. Wiring** — four lines:

```csharp
builder.AddEventProjector("inventory", InventoryEventTypes.Register)
       .AddRedisViewStore<InventoryDetail>("inventory", "detail", InventoryJsonContext.Default.InventoryDetail)
       .AddProjection<InventoryDetailProjection>();

// later, injected as IViewStore<InventoryDetail>
var detail = await views.GetAsync(id);
```

That's the whole quickstart: events + a JSON context, one projection class, and three lines of
`builder.Add...` wiring. No `IServiceCollection` ceremony, and no event store anywhere.

## Registering a projection without a class

Two shorthands, for when a whole class is more ceremony than the projection needs:

```csharp
// General: resolve whatever you need from IServiceProvider, called once per matching event.
builder.AddProjection<ItemCreated>((sp, e, meta, ct) =>
    sp.GetRequiredService<IViewStore<InventoryDetail>>().SetAsync(e.Id, new InventoryDetail(e.Id, e.Name, true, meta.Id), ct));

// Shorthand for the common set-semantics case: map the event to a view, SetAsync(meta.PartitionKey, ...) happens for you.
builder.AddProjection<ItemCreated, InventoryDetail>((e, meta) =>
    new InventoryDetail(e.Id, e.Name, true, meta.Id));
```

The shorthand needs no `EventMeta.Id` guard — `SetAsync` is idempotent under at-least-once redelivery
on its own — which is exactly why it only fits the "replace the whole view" case. A handler that must
read the current view first (an accumulating count, a conditional update, a different id than the
partition key) needs the general delegate overload, or a class, instead. All three ways of registering
a projection — class, general delegate, shorthand delegate — dispatch through the same `EventProjector`
and can be mixed freely on one topic.

## Projecting from more than one stream

A single `EventProjector`/consumer host is bound to exactly one topic — there is no multi-topic
subscription primitive. But `AddProjection<T>` registers a projection *class* (or delegate), not a
topic-scoped instance, so calling `AddEventProjector` more than once with the same projection type
gives you one shared instance fed by two independent consumer groups — a cross-stream projection built
from two single-stream subscriptions:

```csharp
builder.AddEventProjector("inventory", InventoryEventTypes.Register)
       .AddRedisViewStore<InventoryDetail>("inventory", "detail", InventoryJsonContext.Default.InventoryDetail)
       .AddProjection<CombinedProjection>();

builder.AddEventProjector("shipping", ShippingEventTypes.Register)
       .AddProjection<CombinedProjection>();   // same instance, second stream
```

`CombinedProjection` need only implement `IProjection<TEvent>` for the event types it cares about from
each stream; every `AddEventProjector` topic can dispatch to it as long as the wire type is registered
on that topic and the projection implements the matching interface. `StreamMsg` carries no
stream/topic name, so if provenance matters to the projection's logic, encode it in the wire type or
the event payload itself rather than trying to recover it from `EventMeta`.

## Serialisation and the event-type registry

`RegisterJson` covers the default, AOT-safe path via a source-generated `JsonTypeInfo<T>`. The wire
type string is chosen once and never derived from `.FullName` or `.Name` — a class rename is then a
pure refactor, because it is the string, not the CLR name, that a stream of history already written is
keyed on.

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

**One registry per topic, shared with the sibling package.** If a service also calls the sibling
`RedisEvents.EventSourcing` package's `AddEventStore` for the same topic, the two calls (in either
order) reuse one `EventTypeRegistry` rather than building two — so the aggregate's own stream and its
projections can never disagree about wire format. `AddEventProjector`'s `events` argument is only
required the first time a topic is seen by either package on the same builder.

## View stores

`IViewStore<TView>` is an optional convenience, not a requirement — a projection may write anywhere it
likes. `AddRedisViewStore<TView>` stores one Redis **hash per view type** (`{topic}:view:<name>`,
field = view id, value = JSON), which is fine for up to tens of thousands of small views. Beyond that,
or for any query other than "by id" or "all", implement `IViewStore<TView>` yourself — for example over
Azure Table Storage:

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

**Consumers are at-least-once**, so a projection can be asked to handle the same event twice — after a
crash between the view being written and the position being saved. `SetAsync` on its own is idempotent
(writing the same value again changes nothing observable); anything accumulative (a running count)
needs its own guard, comparing `EventMeta.Id` against the id stored on the view, as
`InventoryDetailProjection` does above. `Id` is Redis's own stream entry id — unique and strictly
increasing within its partition, and identical on every redelivery of that entry — not an aggregate
version, so this guard works the same whether or not the topic's producer is an event-sourced write
side at all. A topic is partitioned by key, so every event sharing one `Id`-ordered history already
arrives at one projector instance in publish order; nothing extra is needed to make that comparison
meaningful.

## Error handling

The projector has no error policy of its own; it has core's. Per event, in batch order: an unknown
wire type or a known type with no projection bound to it is skipped at `Debug`; a decode failure always
throws; a projection that throws propagates unchanged and immediately, and the rest of the batch is not
processed. From there it is exactly
[core's error contract](../RedisEvents/README.md#the-error-contract): an ordinary exception logs the
batch and advances past it, and a `DontIgnoreException` subclass blocks the partition and retries with
backoff while every other partition keeps running.

## Limits

- **No view-store abstraction beyond `IViewStore<TView>`.** View storage, indexing and querying stay
  the service's concern; this package supplies a minimal default and a seam, not a framework.
- **One topic per `EventProjector`.** See [Projecting from more than one stream](#projecting-from-more-than-one-stream)
  for the workaround.

## See also

- [`RedisEvents.EventSourcing`](../RedisEvents.EventSourcing/README.md) — event-sourced aggregates
  (the write side), for when a topic's own producer needs optimistic concurrency and a replayable
  source of truth rather than just publishing plain events.
- The README embedded in the `RedisEvents` NuGet package — core's error contract, config and runbooks.
