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
the snippets below are lifted directly from it. Two minimal, separately-runnable ASP.NET Core
services built on top of it — a command-side API
([`.CommandApi`](../../samples/RedisEvents.EventSourcing.Sample.Inventory.CommandApi)) and a
read-side API ([`.ViewApi`](../../samples/RedisEvents.EventSourcing.Sample.Inventory.ViewApi)) —
show the same example as two real, independently deployable microservices; both are exercised
together, over real HTTP and a real Redis, by
[`tst/RedisEvents.EventSourcing.Sample.Tests`](../../tst/RedisEvents.EventSourcing.Sample.Tests).

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
public sealed record InventoryDetail(string Id, string Name, bool Active, int CurrentCount, StreamId LastEventId);

public sealed class InventoryDetailProjection(IViewStore<InventoryDetail> views) :
    IProjection<ItemCreated>, IProjection<ItemRenamed>
{
    public ValueTask HandleAsync(ItemCreated e, EventMeta meta, CancellationToken ct) =>
        views.SetAsync(e.Id, new InventoryDetail(e.Id, e.Name, true, 0, meta.Id), ct);

    public async ValueTask HandleAsync(ItemRenamed e, EventMeta meta, CancellationToken ct)
    {
        var current = await views.GetAsync(meta.PartitionKey, ct);
        if (current is null || current.LastEventId >= meta.Id) return;   // at-least-once: skip a redelivery
        await views.SetAsync(meta.PartitionKey, current with { Name = e.NewName, LastEventId = meta.Id }, ct);
    }
}

builder.AddEventProjector("inventory", InventoryEventTypes.Register)
       .AddRedisViewStore<InventoryDetail>("inventory", "detail", InventoryJsonContext.Default.InventoryDetail)
       .AddProjection<InventoryDetailProjection>();

// later, injected as IViewStore<InventoryDetail>
var detail = await views.GetAsync(id);
```

`meta.PartitionKey` and `meta.Id` are the wire message's own partition key and Redis stream entry id —
nothing here assumes `ItemCreated`/`ItemRenamed` came from an `AggregateRoot`. This projector works
identically against a topic published by a plain `IStreamPublisher.PublishAsync` call, because it
depends on nothing beyond a standard RedisEvents topic — see [View stores](#view-stores) below for why
`meta.Id` is the right redelivery guard either way.

A class may implement `IProjection<TEvent>` for as many event types as it likes — each is bound
automatically, with no registration beyond implementing the interface.

That's the whole quickstart: events + a JSON context, one aggregate class, one projection class, and
seven lines of `builder.Add...` wiring across both sides. No `IServiceCollection` ceremony.

## Projecting from more than one stream

A single `EventProjector`/consumer host is bound to exactly one topic — there is no multi-topic
subscription primitive. But `AddProjection<T>` registers a projection *class*, not a
topic-scoped instance, so calling `AddEventProjector` more than once with the same projection
type gives you one shared instance fed by two independent consumer groups — a cross-stream
projection built from two single-stream subscriptions:

```csharp
builder.AddEventProjector("inventory", InventoryEventTypes.Register)
       .AddRedisViewStore<InventoryDetail>("inventory", "detail", InventoryJsonContext.Default.InventoryDetail)
       .AddProjection<CombinedProjection>();

builder.AddEventProjector("shipping", ShippingEventTypes.Register)
       .AddProjection<CombinedProjection>();   // same instance, second stream
```

`CombinedProjection` need only implement `IProjection<TEvent>` for the event types it cares about
from each stream; every `AddEventProjector` topic can dispatch to it as long as the wire type is
registered on that topic and the projection implements the matching interface. `StreamMsg` carries
no stream/topic name, so if provenance matters to the projection's logic, encode it in the wire
type or the event payload itself rather than trying to recover it from `EventMeta`.

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
topic can never disagree about what happened. Every published event also carries an `es-version`
header (its 1-based position in the aggregate's stream) purely as an informational aid for a human
reading the raw stream — `EventProjector` does not read it, and no projection needs to: `EventMeta.Id`
already lets a projection tell a redelivery from new information, on this topic or any other, whether
or not its producer is this package's own event store. See [View stores](#view-stores).

## Optimistic concurrency

The check matters whenever an event is emitted from a read-decide-write step over aggregate state
(two concurrent commands can both read the same stale state and both be allowed to commit, silently
violating an invariant), and is safe to skip for streams that only append independent facts with no
such decision behind them (telemetry, logs, per-key single-writer streams) — see
`SaveWithoutConcurrencyCheckAsync` below.

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
running count) needs its own guard, comparing `EventMeta.Id` against the id stored on the view, as
`InventoryDetailProjection` does above. `Id` is Redis's own stream entry id — unique and strictly
increasing within its partition, and identical on every redelivery of that entry — not an aggregate
version, so this guard works the same whether or not the topic's producer is this package's own
`AddEventStore`. A topic is partitioned by key, so every event sharing one `Id`-ordered history
already arrives at one projector instance in publish order; nothing extra is needed to make that
comparison meaningful.

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

This package is measured at three levels, each isolating a different thing: an in-process
`[MemoryDiagnoser]` micro-benchmark for `SaveAsync`/`EventProjector.HandleAsync`'s own per-call cost
with no Redis anywhere; a real-Redis, no-HTTP benchmark for `RedisEventRepository` throughput and the
save-to-projected-view latency a single process sees; and the real Inventory sample — two
independently hosted ASP.NET Core services, [`.CommandApi`](../../samples/RedisEvents.EventSourcing.Sample.Inventory.CommandApi)
and [`.ViewApi`](../../samples/RedisEvents.EventSourcing.Sample.Inventory.ViewApi) — talking only
over real HTTP and a real Redis topic between them. The first two show what the library costs in
isolation; the third is the number that actually matters for a CQRS split like this one, because it's
the only one that includes the HTTP hops and two separate processes a real deployment would have.
None of these tests assert on absolute numbers — a CI box's throughput is not a contract; they exist
so a change that regresses this package's hot path shows up before a reviewer has to notice it by eye.

The numbers below are from this repo's own suites, run on the same single dev machine as the root
README's [Performance](../../README.md#performance) numbers (13th Gen Intel Core i7-13700KF, WSL2,
one un-tuned `redis:8-alpine` container via Testcontainers) — a directional baseline, not an SLA.
Run-to-run variance on shared/virtualized hardware is normal; re-run the suites on your own target
infrastructure for numbers you'd actually size capacity against.

**Table 1 — micro-benchmark** (`EventSourcingMicroBenchmarks.cs`, BenchmarkDotNet, no Redis):

```
dotnet test tst/RedisEvents.EventSourcing.UnitTests --filter "TestType=PerfTest"
```

| Operation | Mean | Allocated |
| --- | --- | --- |
| `Save_one_event` (`RedisEventRepository.SaveAsync`, 1 event) | ~570 ns | ~1,088 B |
| `Save_batch_of_ten` (`RedisEventRepository.SaveAsync`, 10 events) | ~4.12 μs | ~4,280 B |
| `Decode_and_dispatch_one_event` (`EventProjector.HandleAsync`) | ~260 ns | ~352 B |

**Table 2 — library throughput** (`EventSourcingPerfBenchmarkTests.cs`, real Redis, no HTTP):

```
dotnet test tst/RedisEvents.EventSourcing.Tests --filter "TestType=PerfTest"
```

| Measurement | Configuration | Result |
| --- | --- | --- |
| Save throughput | 1 event/save | ~1,486 saves/s |
| Save throughput | 10 events/save | ~15,365 saves/s (~154,000 events/s) |
| Load throughput | 10-event aggregate | ~2,349 loads/s (~0.043 ms/event) |
| Load throughput | 1000-event aggregate | ~112 loads/s (~0.009 ms/event) |
| Save → projected view latency | 20 samples, p50 / p90 / max | 3.2 ms / 5.2 ms / 14.4 ms |

**Table 3 — real service, over HTTP** (`InventoryServicePerfTests.cs`; both microservices hosted for
real, real Redis, no in-process shortcuts):

```
dotnet test tst/RedisEvents.EventSourcing.Sample.Tests --filter "TestType=PerfTest"
```

| Measurement | Configuration | Result |
| --- | --- | --- |
| Write throughput | `POST /items`, command-side service | ~765 req/s |
| Read throughput | `GET /items/{id}`, view-side service | ~2,543 req/s |
| Create → view latency | `POST /items` → first `GET /items/{id}` 200, 20 samples, p50 / p90 / max | 1.4 ms / 2.6 ms / 15.3 ms |

**A note on what "throughput" means above.** Every number in Table 3 so far — and the ~1,486 saves/s
and ~2,349 loads/s in Table 2 — comes from a single `HttpClient`/connection firing requests one at a
time in a sequential loop. That is concurrency 1: it is really a latency measurement (round trips per
second = 1 / round-trip time), not the service's throughput ceiling under load. Table 4 below repeats
the write and read measurements with 10 concurrent clients, which is what actually exercises request
pipelining, connection-pool parallelism and Redis's own ability to serve overlapping commands.

**Table 4 — real service, 10 concurrent clients** (`InventoryServicePerfTests.cs`, same real
services and real Redis as Table 3):

```
dotnet test tst/RedisEvents.EventSourcing.Sample.Tests --filter "FullyQualifiedName~concurrent_clients"
```

| Measurement | Configuration | Result |
| --- | --- | --- |
| Write throughput, concurrency-checked | `POST /items` (optimistic-concurrency `SaveAsync`), 10 concurrent clients | ~1,400 req/s |
| Write throughput, no concurrency check | `POST /items/no-check` (`SaveWithoutConcurrencyCheckAsync`, no `WATCH`), 10 concurrent clients | ~7,900 req/s |
| Read throughput | `GET /items/{id}`, view-side service, 10 concurrent clients | ~13,500 req/s |

`POST /items/no-check` is a benchmark-only twin of `POST /items` added specifically for this
comparison: same load → decide → save shape, but through the new
`IEventRepository.SaveWithoutConcurrencyCheckAsync` / `IStreamStore.AppendAndPublishAsync(name,
partitionKey, events, ct)` overload, which appends and publishes in one `MULTI`/`EXEC` exactly like
the checked path but with no `WATCH` and no version comparison — it always applies, and never throws
`ConcurrencyException`. The ~5.5x gap between the two under 10 concurrent clients is the cost of the
per-save optimistic-concurrency check once requests are actually overlapping: at concurrency 1 there
is nothing to contend with `WATCH` against, so the two paths cost about the same; under real
concurrency, every checked write against the *same* Redis connection pool competes with the others'
`WATCH`/`MULTI`/`EXEC` round trips, where the unconditioned path only issues a plain transaction. This
API is additive and opt-in — `SaveAsync`'s behavior and guarantees are unchanged. See
[Optimistic concurrency](#optimistic-concurrency) above for when it's appropriate to reach for it.

The takeaway is where the cost actually sits. `Save_one_event`'s own per-call overhead is under a
microsecond and allocation-light (Table 1), so the ~1,486 saves/s ceiling in Table 2 is almost
entirely the real Redis round trip, not this package's own bookkeeping — the same shape core's own
[Performance](../../README.md#performance) numbers show for `XADD`. The write path barely changes
shape once it's behind real HTTP either (~765 req/s in Table 3 vs. ~1,486 saves/s in Table 2 is the
ASP.NET Core request pipeline and JSON binding on top of the same underlying save, not a new
bottleneck). The read path is *faster* over HTTP than the write path, because `GET /items/{id}` is
one Redis hash read behind a thin endpoint, with no aggregate replay and no `XADD` involved. The one
number that genuinely changes character is create-to-view latency: a p50 in the low single-digit
milliseconds with an occasional double-digit-millisecond tail (both in Table 2's in-process form and
Table 3's real-HTTP form) — that tail is the real `EventProjector` consumer's own read-loop
scheduling (it polls and batches rather than pushing synchronously the instant `SaveAsync` returns),
not the write or the extra HTTP hop; a caller that polls a view expecting it to reflect a write it
just made should expect that shape, not a flat constant.
