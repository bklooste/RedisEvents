# RedisEvents

Internal messaging over Redis Streams: partitioned topics, ordered per key, at-least-once delivery,
positions stored in Redis. It is what services use to talk to each other. `Orange.Lib.Kafka*` is for
external integrations only.

Read the error contract below **before** writing a handler. It is the part of this library that is
not discoverable from the API — the compiler will happily accept a handler that silently drops
money.

> Every code sample on this page is compiled as part of the unit test suite, in
> [`library/tst/RedisEvents.UnitTests/ReadmeSamples.cs`](../../tst/RedisEvents.UnitTests/ReadmeSamples.cs).
> If you change one here, change it there — a sample that stops compiling fails the build, which is
> the only reason to trust anything printed below.

---

## The error contract

Four sentences, and they are the whole thing:

1. **An ordinary exception means the message is logged and skipped.** Not retried. Not parked
   anywhere. There is no dead-letter queue. The position advances past the batch and the next batch
   is processed. This is `ErrorPolicy.BestEffort`, and it is the default.
2. **The library does no retrying.** None. If a failure is transient, *the handler* retries it —
   Polly, a `for` loop, whatever suits. The library cannot know whether your work is idempotent or
   whether a second attempt would help, so it does not guess.
3. **If a message must not be skipped, throw a `DontIgnoreException` subclass.** The partition then
   blocks and retries the same batch with backoff (1s → 2 → 4 → 8 → 16 → 30s cap) until it succeeds.
   The position never advances past it. Other partitions keep running.
4. **Blocking is not free.** Lag grows for as long as the block lasts, and a long enough block plus
   `MAXLEN` trimming means the *upstream* end of the stream is thrown away while you are stuck —
   data loss, arriving later and from the other direction. Blocking is the right tool for "Redis is
   down" and "the ledger is unreachable". It is the wrong tool for "this one message is malformed",
   which will still be malformed in six hours.

### The decision table

Every handler makes this choice, whether or not its author noticed.

| Situation | What to do |
|---|---|
| Malformed message, will never succeed | Let it throw — log and skip is correct |
| Transient downstream failure, retry will fix it | Retry inside the handler |
| Downstream is down and skipping loses money | Throw a `DontIgnoreException` subclass |
| Message is important but not urgent | Handler parks it somewhere durable, then returns normally |

**Row 1 — malformed.** Do nothing special. The batch is logged at `Error` with the topic, partition,
consumer, the id range and the message type, and the partition moves on.

```csharp
public sealed class MalformedRow : IBatchHandler
{
    public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            // JsonException on a body that will never parse. Nothing is caught: the batch is
            // logged at Error, the position advances, and the partition keeps moving.
            var order = JsonSerializer.Deserialize<Order>(batch.Span[i].Body.Span)
                ?? throw new JsonException("null order");
            _ = order;
        }

        return ValueTask.CompletedTask;
    }
}
```

**Row 2 — transient.** Retry in the handler. Bound the attempts: an unbounded in-handler retry is a
block with none of the observability of one (`streams.blocked` will read zero while the partition
goes nowhere).

```csharp
public sealed class TransientRow(IPricingClient pricing) : IBatchHandler
{
    public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            var msg = batch.Span[i];

            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    await pricing.PriceAsync(msg.Body, ct).ConfigureAwait(false);
                    break;
                }
                catch (HttpRequestException) when (attempt < 3)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100 * (1 << attempt)), ct).ConfigureAwait(false);
                }
            }
        }
    }
}
```

**Row 3 — must not skip.** Derive from `DontIgnoreException`. That is the entire opt-in; the type is
the signal, there is no configuration to set.

```csharp
public sealed class LedgerUnavailableException(string message, Exception inner)
    : DontIgnoreException(message, inner);

public sealed class MustNotSkipRow(ILedger ledger) : IBatchHandler
{
    public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            var msg = batch.Span[i];

            try
            {
                await ledger.PostAsync(msg.Body, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Skipping this loses money, so block instead. Lag will grow while the ledger is
                // down — that is the deliberate trade, and it is only correct because the
                // alternative is a settlement that never happens.
                throw new LedgerUnavailableException(
                    $"Ledger rejected entry {msg.Id.Format()}; blocking rather than skipping.", ex);
            }
        }
    }
}
```

**Row 4 — important but not urgent.** Park it and return normally. The partition keeps moving, lag
stays flat, and nothing is lost — the message is now someone else's problem, on purpose. `Copy()` is
mandatory here; see [the pooling edge](#the-batch-is-borrowed-memory).

```csharp
public sealed class ParkingRow(IParkingLot parked) : IBatchHandler
{
    public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            var msg = batch.Span[i];

            try
            {
                Handle(msg);
            }
            catch (InvalidOperationException ex)
            {
                // Copy: the batch array and the body both go back to their pools the moment this
                // handler returns, so anything kept has to own its storage.
                await parked.ParkAsync(msg.Copy(), ex, ct).ConfigureAwait(false);
            }
        }
    }
}
```

### `ErrorPolicy`, and why you probably shouldn't change it

`Streams:Consumers[n]:OnError` picks what an *ordinary* exception does. `DontIgnoreException` ignores
this setting entirely — it always blocks.

| `ErrorPolicy` | Behaviour | When |
|---|---|---|
| `BestEffort` (default) | Log at `Error`, advance past the batch, continue | Almost always. A poison message must not wedge a partition forever |
| `StopPartition` | Log at `Error`, do **not** advance, stop this partition; others keep running | Rare. Prefer `DontIgnoreException`, which retries instead of standing still |
| `Fail` | Log at `Critical`, stop the application, let Kubernetes restart the pod | A crash loop with extra steps; only for a consumer that cannot meaningfully run degraded |

**What `Fail` actually does.** It calls `IHostApplicationLifetime.StopApplication()`. That is a
*graceful* stop, not an `Environment.Exit`: `StopAsync` still runs, so positions are flushed and the
healthy partitions drain what they had before the process ends. The pod then exits and the
orchestrator restarts it, and the un-drained tail is redelivered on the next start.

Two things follow from that, and neither is discoverable from the enum:

- **The lifetime has to be there.** Register through `AddStream…`, which resolves
  `IHostApplicationLifetime` from the container and hands it to the host. (The host type itself is
  `internal` since R-23, so `AddStream…` is the only way to wire it.) With no lifetime in the
  container, `Fail` can only log at `Critical` and say so — the partition is dead and the process
  will *not* restart itself.
- **A `StreamConfigurationException` is fatal under every policy**, not just `Fail`. It comes from
  startup validation; no amount of running fixes it, and a pod that stays Ready while consuming
  nothing is the worst available outcome.

Faults raised while the host is already stopping are logged and go no further — a shutdown race is
not a policy failure.

**The blocking retry of rule 3 is implemented**, and is what a `DontIgnoreException` gets under
*every* `ErrorPolicy`. The partition retries the same batch on the 1s → 2 → 4 → 8 → 16 → 30s ladder
and then every 30s, with no attempt limit; the position is never advanced past it; the `Error` line
is rate-limited to one per 30s while the block lasts, and recovery is logged with the attempt
count and the elapsed time. `streams.blocked` and `streams.block.duration_ms` are what make it
visible — alert on them, because a blocked partition is silent otherwise. Shutdown cuts the backoff
short and the batch redelivers on the next start.

---

## Quickstart

Zero config. No `Streams` section in appsettings, no options object; every value is a record default
and the defaults are the ones most services want.

```csharp
builder.AddStream<OrderHandler>("orders");
```

That registers `OrderHandler` as a singleton, wires a consumer host for the topic, connects to
`redis-db.infra:6379`, and starts reading from the stored position (or the beginning of the stream if
there is none).

The handler is called once per **batch**, not once per message:

```csharp
public sealed class OrderHandler : IBatchHandler
{
    public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            var msg = batch.Span[i];
            var order = JsonSerializer.Deserialize<Order>(msg.Body.Span);
            _ = order;
        }

        return ValueTask.CompletedTask;
    }
}
```

For a consumer too small to justify a class, register a delegate:

```csharp
builder.AddStream("orders", (batch, ct) =>
{
    for (var i = 0; i < batch.Length; i++)
    {
        Handle(batch.Span[i]);
    }

    return ValueTask.CompletedTask;
});
```

Or take them one at a time — same batching, same positions, same error handling underneath:

```csharp
public sealed class OneOrderHandler : IMessageHandler
{
    public ValueTask HandleAsync(in StreamMsg msg, CancellationToken ct)
    {
        Handle(msg);
        return ValueTask.CompletedTask;
    }
}
```

There is no envelope. `StreamMsg.Body` is exactly the bytes the publisher passed in; deserialising is
yours, which is what keeps this library free of any dependency on your message model.

### Configuring a consumer

Config is the normal route — `Streams:Consumers` in appsettings, then `AddStream<T>()` (single
entry), `AddStream<T>(index)`, or `AddStream<T>(topic)`. When a value is derived at startup, or you
are in a test, override in code instead:

```csharp
builder.AddStream<OrderHandler>(consumer => consumer with
{
    Topic = "orders",
    BatchSize = 250,
    Persist = PersistMode.SyncBatch,
    OnError = ErrorPolicy.BestEffort,
});
```

```jsonc
{
  "Streams": {
    "ConnectionString": "redis-db.infra:6379,abortConnect=false", // default; must be redis-db
    "Consumer": "bet-processing",                                  // default: entry assembly name
    "Topics": {
      "orders": { "Partitions": 4, "MaxLen": 100000, "Trim": "Approx" }
    },
    "Consumers": [
      { "Topic": "orders", "BatchSize": 100, "Persist": "AsyncBatch", "OnError": "BestEffort" }
    ]
  }
}
```

Keys worth knowing, with their defaults:

| Key | Default | What goes wrong |
|---|---|---|
| `ConnectionString` | `redis-db.infra:6379,abortConnect=false` | Point it at `redis-cache` and you lose everything on restart — see below |
| `Consumer` | entry assembly name | Two services sharing a name share a position hash and each see half the messages |
| `Topics:<t>:Partitions` | `1` | Can be increased, never decreased. Caps parallelism |
| `Topics:<t>:MaxLen` | `10000` | Below worst-case consumer lag, trimming eats unprocessed messages |
| `Topics:<t>:Trim` | `Approx` | `None` is refused outside `Development` — it is an OOM footgun |
| `Topics:<t>:StateMetadata` | `false` | State-store entries hold only body and type; `true` also keeps correlation id, trace and headers, forever |
| `Topics:<t>:StreamNamespace` | unset (process namespace) | Reads and writes the topic's stream (`s:`) and meta (`m:`) under `<ns>:re:` instead of `<env>:re:`, so several environments can consume one shared feed. Positions, leases and work-queue groups stay per environment. Changing it on a running consumer carries its old positions over — reset it — see [Shared streams](#shared-streams-across-environments) |
| `Consumers[n]:BatchSize` | `100` | Bigger batches amortise round trips and widen the redelivery window |
| `Consumers[n]:Persist` | `AsyncBatch` | `SyncBatch`/`SyncMessage` cost a Redis round trip; `None` never stores a position |
| `Consumers[n]:PersistIntervalMs` | `1000` | This is your duplicate window on a crash |
| `Consumers[n]:OnError` | `BestEffort` | See the table above |
| `Consumers[n]:BlockMs` | `1000` | Must stay well under the connection's `syncTimeout` (5000ms default) — validated at startup |
| `Consumers[n]:StartFrom` | `Stored` | `Stored` with `Persist: None` is a contradiction and is refused at startup |
| `Consumers[n]:Backpressure:Capacity` | `4` | Batches in flight per partition |
| `Consumers[n]:Delivery` | `Ordered` | `WorkQueue` gives up per-key ordering — see below |
| `Consumers[n]:UnhealthyBehindSeconds` | `0` (off) | The only health threshold safe to restart on: behind the tail with a frozen position. Set it shorter than your slowest batch and a healthy consumer is restarted mid-batch — see [Health](#health-one-set-of-rules-two-hosts) |
| `Consumers[n]:UnhealthyStalledSeconds` | `300` | No progress at all — no read returning, no batch completing, position writes failing — for this long: Unhealthy, and under `Lease` the lease is handed to another instance. Measured locally, so it works when Redis cannot be reached. Set it longer than your slowest batch — see [A stalled partition](#a-stalled-partition-is-handed-on) |

Misconfiguration throws `StreamConfigurationException` at startup, with the offending key named in
the message. It never waits until 3am to tell you.

### Shared streams across environments

Every key carries the process namespace, `<env>:re:`, from `ASPNETCORE_ENVIRONMENT` /
`DOTNET_ENVIRONMENT` (or `STREAMS_KEY_PREFIX` verbatim), so `dev` and `tst` on one Redis never see each
other's keys. To have both consume one feed that a shared tier (`np`) publishes once, point the topic
at the tier's namespace:

```jsonc
{ "Streams": { "Topics": { "mult_feed_events": { "Partitions": 2, "StreamNamespace": "np" } } } }
```

| Key | Namespace | Example (process `dev`) |
|---|---|---|
| partition streams | the topic's | `np:re:s:{mult_feed_events}:0` |
| topic meta | the topic's | `np:re:m:{mult_feed_events}` |
| positions, reset markers | the process's | `dev:re:p:{mult_feed_events}:<consumer>` |
| ownership / leases | the process's | `dev:re:o:{mult_feed_events}:<consumer>` |
| `WorkQueue` consumer group (on the shared stream) | the process's, in the group name | `dev:re:<consumer>` |
| outbox state, dedupe markers, projection views | the process's | `dev:re:{mult_feed_events}:view:...` |

- Each environment keeps its own cursor; a `WorkQueue` group carries the environment in its name only
  when the stream is in another namespace, so existing groups keep their names.
- Only the namespace that owns a stream trims it. A process ignores `BackgroundTrimIntervalSeconds` for a
  topic in another namespace (it could only see its own namespace's positions to clamp to); the inline
  `MAXLEN` still bounds the stream, and other environments' positions never hold it back.
- The admin endpoints read the stream and `m:` hash in the configured namespace;
  `StreamAdmin.ReadTopicLayoutAsync(redis, topic, streamNamespace)` does the same for code.
- The value is lowercased and limited to letters, digits, `-` and `_`. Naming the process's own
  environment changes nothing.
- The stream must be on the same Redis as the process's own keys: there is no per-topic connection.

---

## Publishing

`AddStreamPublisher` registers a real publisher — keyed on the topic — and, for a buffered one, the
background pump that drains it.

```csharp
builder.AddStreamPublisher("orders");
```

Inject `IStreamPublisher`. With exactly one publisher registered the unkeyed resolution works; with
two or more it throws rather than guessing, so name the topic:

```csharp
public sealed class OrderGateway([FromKeyedServices("orders")] IStreamPublisher publisher)
{
    public ValueTask<StreamId> PlaceAsync(string orderId, ReadOnlyMemory<byte> body, CancellationToken ct)
        => publisher.PublishAsync(orderId, body, type: "OrderPlaced", ct: ct);
}
```

The first argument is the **partition key**: equal keys land on the same partition and are therefore
ordered against each other. An empty key round-robins, which is what you want for something with no
per-entity ordering requirement and the wrong thing for anything with a lifecycle.

`buffered: true` — or `Streams:Producers[n]:Buffered` — swaps the direct publisher for a buffered one
that also satisfies `IStreamBufferedPublisher` (`EnqueueAsync` / `FlushAsync`), and starts its pump.
Passing an argument that contradicts the configured value is refused at startup with both sources
named; omit it to take the configured value. See [buffered publishes are lost on a
crash](#buffered-publishes-are-lost-on-a-crash) before choosing one.

---

## Typed publish and consume

The calls above take raw bytes and a `type` string you write out by hand (`"OrderPlaced"`). When a
topic carries one CLR type, that is boilerplate: `PublishAsync<T>` / `PublishBatchAsync<T>` /
`EnqueueAsync<T>` serialise `T` as JSON for you, and `type` defaults to `typeof(T).FullName` — pass
it explicitly only when you need a type string that does not match the CLR name (a migration, or a
name shared across languages).

They take a source-generated `JsonTypeInfo<T>`, never a reflection-based `JsonSerializerOptions` —
the same AOT-safety rule the rest of the library follows. Define the context once:

```csharp
[JsonSerializable(typeof(Order))]
internal sealed partial class OrderJson : JsonSerializerContext;
```

Publishing:

```csharp
public sealed class TypedOrderGateway([FromKeyedServices("orders")] IStreamPublisher publisher)
{
    public ValueTask<StreamId> PlaceAsync(string orderId, Order order, CancellationToken ct)
        => publisher.PublishAsync(orderId, order, OrderJson.Default.Order, ct: ct);
}
```

The same overload exists on `IStreamBufferedPublisher.EnqueueAsync`. Consuming is the mirror image —
`Deserialize<T>` on a single `StreamMsg`, or on a whole `ReadOnlyMemory<StreamMsg>` batch at once:

```csharp
public sealed class OrderBatchHandler : IBatchHandler
{
    public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        var orders = batch.Deserialize(OrderJson.Default.Order);
        for (var i = 0; i < orders.Length; i++)
        {
            _ = orders[i];
        }

        return ValueTask.CompletedTask;
    }
}
```

Neither side checks `StreamMsg.Type` against `T` — a topic carrying more than one message type still
branches on `Type` itself before deserialising, same as with the raw bytes API.

### Typed handlers

`Deserialize<T>` above still leaves you calling it by hand inside an ordinary `IBatchHandler`. When a
consumer only ever wants `T`, skip that line entirely: implement `IBatchHandler<T>` or
`IMessageHandler<T>` instead, and register with the extra `typeInfo` argument —
`AddStream<THandler, TMessage>` deserialises for you before the handler is called:

```csharp
public sealed class TypedOrderHandler : IBatchHandler<Order>
{
    public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg<Order>> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            Order? order = batch.Span[i].Value;        // already deserialised
            StreamId id = batch.Span[i].Envelope.Id;    // the envelope is still there for Id, headers, etc.
        }

        return ValueTask.CompletedTask;
    }
}

builder.AddStream<TypedOrderHandler, Order>("orders", OrderJson.Default.Order);
```

`StreamMsg<T>` pairs the deserialised value with the original `StreamMsg` envelope, so nothing about
partition, correlation id, or headers is lost by moving to the typed handler. A `IMessageHandler<T>`
sibling exists too, taking `in StreamMsg<T>` one message at a time, the same relationship
`IMessageHandler` has to `IBatchHandler`.

A large batch is deserialised in parallel (chunks of 10 messages, split across cores) rather than one
message at a time — below that size the parallelism overhead costs more than it saves, so a small
batch stays a plain loop. Either way, a body that fails to deserialise is that entry's fault, not the
batch's: it is logged at `Error` with its id and type and dropped, and the handler is called with the
rest of the batch (not at all if every entry failed). A `DontIgnoreException` thrown by a deserialiser
still propagates and blocks the partition. A single-message `IMessageHandler<T>` is unchanged: the
failure is an ordinary exception for that one message, logged and skipped.

`AddStream<THandler, TMessage>` also has a `Func<ReadOnlyMemory<byte>, TMessage?>`-based overload
underneath the `JsonTypeInfo<TMessage>` one shown above — the serialisation-format-agnostic seam a
non-JSON format plugs into, with zero access to this library's internals needed.
[`RedisEvents.MessagePack`](../RedisEvents.MessagePack/README.md) is the one that already exists.

---

## The outbox

The problem the outbox solves is the two-write gap: a service writes its state, then publishes the
event announcing it, and dies in between. State says the bet is placed; the stream says nothing ever
happened.

Here the state store and the broker are the same Redis, so the classic outbox table and relay process
collapse into one `MULTI`/`EXEC`:

```csharp
var stateKey = Outbox.StateKey("orders", $"order:{orderId}");

var id = await Outbox.WriteAndPublishAsync(
    db,
    tran => tran.HashSetAsync(stateKey, [new HashEntry("status", "placed")]),   // queue only — no await in here
    topic: "orders",
    partitionKey: orderId,
    body: body,
    type: "OrderPlaced",
    stateKeys: [stateKey],
    topicOptions: topicOptions,
    ct: ct);
```

Four things about it are load-bearing, and each has its own way of going wrong:

1. **The delegate must not `await`.** A command issued on an `ITransaction` returns a task that does
   not complete until `ExecuteAsync` runs, and `ExecuteAsync` cannot run until the delegate returns —
   so awaiting inside it hangs forever. Worse, `async db => await …` binds as `async void`: the state
   write ends up *outside* the transaction and nothing tells you. Queue commands, discard the tasks
   they return, return synchronously.
2. **`MULTI`/`EXEC` is isolation, not rollback.** No other client sees a half-applied state, and
   neither write can be lost to a crash between them. It is *not* "both or neither": a command that
   fails at run time — `WRONGTYPE`, say — does not stop the others applying. Handlers still have to
   be idempotent.
3. **One hash slot.** Stream keys are tagged on the topic, so a state key must carry the same tag.
   `Outbox.StateKey(topic, name)` builds one, and passing `stateKeys` gets the tag checked before
   anything is sent — otherwise a cluster deployment fails later with `CROSSSLOT`. Where the state
   genuinely cannot share a slot, take the escape hatch: write the state, then publish, and make the
   consumer idempotent.
4. **The shared multiplexer only.** A transaction is bound to one connection, and a consumer's
   dedicated reader connection is read-only and may be parked in a blocking `XREAD`. Passing one is
   refused.

Optimistic concurrency needs no Lua: `conditions` map straight onto `WATCH`.

```csharp
var id = await Outbox.WriteAndPublishAsync(
    db,
    tran => tran.HashSetAsync(stateKey, [new HashEntry("version", expected + 1)]),
    topic: "orders", partitionKey: orderId, body: body, type: "OrderPlaced",
    conditions: [Condition.HashEqual(stateKey, "version", expected)],
    stateKeys: [stateKey],
    topicOptions: topicOptions,
    ct: ct);

if (id is null)
{
    // A condition failed: someone else moved the version on and NOTHING was applied.
    // null is not a general failure signal — every other failure throws.
}
```

`Outbox.WriteAndPublishManyAsync` takes several `OutboxPublish` entries in one transaction, on the
same terms.

---

### Forwarding one topic onto another

A service that consumes one topic and announces what it saw on another — a ledger's events mapped onto
a public `transactions` topic — cannot join the source's transaction, and its read is at-least-once: a
redelivery, a restart before a position flush, or a lost consumer position replaying the whole
retained source would all republish events already forwarded. `TopicForwarder` makes that exactly once
in effect with two guards, written in the same `MULTI`/`EXEC` as the publish:

- a durable **high-water mark** per source — the position of the last event forwarded from it. Anything
  at or below it is skipped, which is what makes a full replay harmless. This is the guarantee.
- an `Idempotency` marker per dedupe id, for redeliveries inside its TTL (a week by default). An
  optimisation, not a lock.

```csharp
var forwarder = new TopicForwarder(sharedDb, "transactions", streamOptions);

// in the projection handler for the source topic:
await forwarder.ForwardAsync(
    source: "customer_wallet",                       // one ordered source: a topic, or "{topic}:{partition}"
    dedupeId: $"{meta.PartitionKey}:{e.TransactionId}",
    position: meta.Id,
    [new ForwardedMessage(customerId, body, typeof(WalletTransaction).FullName!)]);
```

It returns `false` when the event was forwarded already (or a concurrent forward from the same source
moved the mark first) — nothing was written. A source must deliver in order, because the mark only
moves forward: one partition of a topic is one source.

## Idempotency

Delivery is **at-least-once**. Your handler will see the same message twice, and the sooner you
design for that the less it costs:

- A crash loses up to `PersistIntervalMs` (default 1000ms) of position writes, so everything
  processed in that window is redelivered.
- A blocking retry re-runs the **same batch** from the start. A handler that failed halfway through
  a batch of 100 runs its first 50 again.
- `PersistMode.SyncMessage` narrows the window to one message and still does not give exactly-once:
  the handler can succeed and the process die before the position write lands.

So: **write handlers that tolerate re-delivery.** The stream entry id is a natural dedupe key — Redis
assigns it, it is unique within a partition, and it is identical on every redelivery of that entry.

`Idempotency.TryBeginAsync` is that, in one Redis round trip (`SET key 1 NX PX ttl`), if you have no
better place to keep the claim:

```csharp
public sealed class DedupedHandler(IDatabase db, ILedger ledger) : IBatchHandler
{
    private static readonly TimeSpan Window = TimeSpan.FromHours(24);

    public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            var msg = batch.Span[i];

            // false = seen before, within the window. Size the TTL above the replay window: a claim
            // that expires before a replay reaches it dedupes nothing.
            if (!await Idempotency.TryBeginAsync(db, "orders", msg.Id.Format(), Window, ct).ConfigureAwait(false))
            {
                continue;
            }

            await ledger.PostAsync(msg.Body, ct).ConfigureAwait(false);
        }
    }
}
```

It is a *claim*, not a transaction: if the process dies between the claim and the work, the message
is neither processed nor eligible for redelivery until the TTL expires. Where that matters, claim
inside the same write that does the work — which is what [the outbox](#the-outbox) is for — or use a
store you control:

```csharp
public sealed class IdempotentHandler(ISeenSet seen, ILedger ledger) : IBatchHandler
{
    public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            var msg = batch.Span[i];
            var key = $"{msg.Partition}:{msg.Id.Format()}";

            if (!await seen.TryClaimAsync(key, ct).ConfigureAwait(false))
            {
                continue;
            }

            await ledger.PostAsync(msg.Body, ct).ConfigureAwait(false);
        }
    }
}
```

A natural business key (order id, bet id) is better still than the entry id where one exists, because
it also dedupes across a republish — the same order published twice gets two entry ids and one
business key. Handlers that are naturally idempotent (last-write-wins state) should do none of this;
it is a round trip bought for nothing.

---

## Sharp edges

Each of these has cost somebody a day. The reason is attached because the reason is what you will
remember.

### Streams must live on `redis-db`, never `redis-cache`

`redis-cache` runs with `--save "" --appendonly no`, so a pod restart loses **every stream and every
stored position** — messages that were never consumed, and the record of what was. Not "the cache is
cold": the data is gone and consumers restart from the beginning of an empty stream. The default
connection string points at `redis-db.infra:6379` for exactly this reason; overriding it is the only
way to get this wrong.

### The batch is borrowed memory

`ReadOnlyMemory<StreamMsg>` is a window over a pooled array, and each `StreamMsg.Body` aliases the
Redis read buffer. Both are recycled the instant your handler returns. Retain either past the await
and a later read fills that array with different messages, which your retained reference then reads
as if they were yours — silent, plausible-looking corruption in an unrelated part of the system.

```csharp
public static StreamMsg[] KeepForLater(ReadOnlyMemory<StreamMsg> batch) => batch.Copy();
```

`Copy()` allocates — one array plus one byte buffer for the whole batch — which is why it is opt-in.
In `DEBUG` builds the returned array is poison-filled, so this mistake shows up as a `StreamMsg.Type`
reading `!! RedisEvents: this batch array was returned to the pool !!` rather than as garbage.

### Partition count goes up and never comes down

Decreasing is refused at startup: partitions `newN..oldN-1` would fall out of the read range and
anything unprocessed on them would be orphaned with no way to notice. Increasing is supported and
lossless, but costs a one-off ordering break — a key that hashed to partition 2 may now hash to
partition 9, so its old backlog and its new messages are consumed concurrently until the backlog
drains. **Drain to near-zero lag before increasing.** The break is logged at `Warning` when it
happens.

### `MaxLen` must exceed worst-case consumer lag

Trimming does not know or care what has been consumed. If a consumer is 20 000 messages behind on a
topic with `MaxLen: 10000`, trimming deletes messages it has not read yet. The trim clamp holds
trimming back for a lagging consumer, but it is **released above 80 %** of `MaxLen`
(`ClampReleaseThreshold`) — at that point sacrificing the backlog beats an OOM kill that takes the
whole Redis instance, and every other service on it, down with it. That release fires the
`streams.clamp.released` metric; treat it as data loss that has already happened.

### `Instances:Mode = Static` needs a StatefulSet

`Lease` is the default: partitions are claimed from the ownership registry, so a plain `Deployment`
scales and no instance count is maintained. `Static` needs a StatefulSet ordinal plus
`STREAMS_INSTANCE_COUNT`, and must be asked for by name.

Under `Static`, a `Deployment` gives no stable ordinal, so every pod resolves to index 0, every pod
owns every partition, and every message is processed N times. The ownership arithmetic is driven by
`STREAMS_INSTANCE_INDEX` (or the trailing `-<n>` of a StatefulSet `POD_NAME`) and
`STREAMS_INSTANCE_COUNT`.

`STREAMS_INSTANCE_COUNT` must track `spec.replicas`. Scale up without it and the new pods own
nothing; scale down without it and the partitions the departed pods owned are consumed by nobody, in
silence. Change both together and confirm `streams.partitions.unowned` returns to 0.

### `Delivery = WorkQueue` gives up per-key ordering

**The default, `Ordered`, is the Kafka / EventHub model.** A Kafka or EventHub consumer group is a
named cursor per partition, and that is exactly what the default path already is: `Consumer` is the
group id, `p:{topic}:{consumer}` is the per-partition committed cursor, and the ownership registry
assigns partitions to members. If you came here looking for "consumer groups", you already have them
and need no key.

`Delivery = WorkQueue` is a different thing: a work queue over `XREADGROUP`. Entries of one
partition go to whichever member asks first, so two pods interleave them and **per-key order is
lost**, with nothing reassembling it afterwards.

| | `Ordered` (default) | `WorkQueue` |
|---|---|---|
| Reads with | `XREAD` | `XREADGROUP` |
| Readers per partition | one owner | every member competes |
| Per-key order | preserved | **lost** |
| Position cursor | `p:{topic}:{consumer}`, ours | the group's last-delivered id, Redis's |
| Position reset | `StreamAdmin` rewrites the hash | routes to `XGROUP SETID` |
| Cost per batch | one coalesced `HSET` per flush interval | an extra `XACK` round trip, plus PEL bookkeeping per entry |
| Redelivery after a pod dies mid-batch | the partition's new owner re-reads from the last flushed position | a sibling claims the entries out of the PEL (`XAUTOCLAIM`) |
| Use it for | projections, read models, anything keyed | order-independent commands that may be rejected and retried |

So `WorkQueue` trades ordering for competing-consumer redelivery. Take it only when that trade is
the one you want — never for a projection, where applying two events to one key out of order
silently corrupts the read model.

The obsolete `UseConsumerGroup` key still binds for one version: `true` is read as
`Delivery: WorkQueue` and logs a warning naming the new key. Setting both keys to values that
disagree is refused at startup.

### Buffered publishes are lost on a crash

A buffered producer acknowledges into an in-memory queue and flushes on a timer (`MaxWaitMs`,
default 20ms). Anything still queued when the process dies is gone, with no record that it existed.
That is the correct trade for telemetry and the wrong one for money; use [the outbox](#the-outbox)
where the publish must survive the process.

`FlushAsync` is the only way to turn a buffered publish back into a durable one, and it reports **per
flush cycle**: it faults only if an entry it covered failed, so a caller can use it to decide whether
its own message is safe. Under `DropOldest` it can also fault with an `InvalidOperationException`
saying the marker itself was shed — a shed marker covered nothing and cannot promise anything, and
completing it as success would let a caller treat shed data as durable.

---

## Runbooks

The full set lives in `docs/guides/streams.md`. Two that belong next to the API:

### Replay from a date

Preview first, always. The preview reports how much would be reprocessed per partition and whether
the date has already been trimmed away — a reset older than retention cannot resurrect entries that
no longer exist, and it will not tell you that afterwards.

```csharp
var preview = await StreamAdmin.PreviewResetAsync(redis, topic, consumer, from, ct: ct)
    .ConfigureAwait(false);

return preview.Describe();
```

Then **scale the consumer to zero**, reset, and scale back up. Resetting under a running consumer
races the position it is writing.

```csharp
var written = await StreamAdmin.ResetPositionAsync(redis, topic, consumer, from, ct: ct)
    .ConfigureAwait(false);
```

### Who owns which partition

An unowned partition is one nobody is reading, and it looks exactly like a quiet topic from the
outside. This, the startup ownership log line, and `CLIENT LIST` on `redis-db` are the three ways to
tell the difference.

```csharp
var map = await StreamAdmin.GetOwnershipAsync(redis, topic, consumer, ct: ct).ConfigureAwait(false);
return map.Unowned;
```

---

## Health: one set of rules, two hosts

`StreamHealth` grades every partition this process reads into Healthy, Degraded or Unhealthy. It
lives in core, so both kinds of host run the same rules. `RedisEvents.HealthChecks`' `StreamsHealthCheck`
is an adapter that turns its report into a `HealthCheckResult`; that package has no ASP.NET Core
dependency, so a web service maps the check to `/health` and a Generic Host worker registers the same
check and acts on it through an `IHealthCheckPublisher`. A host that wants no health-check
abstractions at all calls the rules directly:

```csharp
var report = StreamHealth.Evaluate(services);   // partitions + consumer hosts + the shared connection

if (report.Status == StreamHealthStatus.Unhealthy)
{
    logger.LogCritical("Streams unhealthy ({Rule}): {Description}", report.Rule, report.Description);
    lifetime.StopApplication();   // graceful: the claim is released, so the replacement starts clean
}
```

`report.Rule` names the rule that fired, so a host acts on an enum, not on the wording of
`Description`. Core does not reference `Microsoft.Extensions.Diagnostics.HealthChecks`; if your
worker wraps this in an `IHealthCheck`, map `StreamHealthStatus` to `HealthStatus` by name with a
`switch` — the two enums number in opposite directions.

| Verdict | When |
|---|---|
| **Unhealthy** | The Redis connection is down · a partition **stalled** past `UnhealthyStalledSeconds`, or stood down for stalling · a partition blocked past `UnhealthyBlockSeconds` · a partition whose stop `Escalates`, stopped past `UnhealthyStoppedSeconds` with entries waiting · a partition **behind the tail with a frozen position** past `UnhealthyBehindSeconds` (off by default) · no registered consumer started · every partition stopped |
| **Degraded** | Nothing has started reading yet · a partition blocked or stopped inside its threshold, or stopped by `ErrorPolicy` · lag past `UnhealthyLagMs` · an ownership gap or overlap · a consumer caught up and receiving nothing past `DegradedSilenceSeconds` (off by default) |

### A frozen position: one clock, two meanings

A position that has stopped moving is one of two things, and the tail of the stream says which.

| The tail is… | So… | Setting | Verdict |
|---|---|---|---|
| ahead of the position | an entry is waiting and the consumer is not reading it | `UnhealthyBehindSeconds` | **Unhealthy**: a restart hands the partition to something that will |
| not ahead | there is nothing to read | `DegradedSilenceSeconds` | **Degraded**: the consumer is working |

```json
"Streams": { "Consumers": [ { "Topic": "feed_odds", "UnhealthyBehindSeconds": 300, "DegradedSilenceSeconds": 600 } ] }
```

**Silence has no Unhealthy setting, on purpose.** A caught-up consumer with nothing to read is not
at fault: either the topic is quiet, or its producer is broken, or the consumer was deployed reading
a key nothing writes to. A restart changes none of the three, so failing the pod on silence is a
restart loop for exactly as long as the topic stays quiet, and every restart hands partitions on for
nothing. Alert on the Degraded report (`report.Rule == StreamHealthRule.Silent`) and fix the producer.

- **Per consumer, all partitions.** Silence is judged over every partition of the consumer that this
  instance is reading. One idle partition beside busy siblings is traffic shape, not silence.
- **Off by default**, because only the service knows whether a quiet topic is normal. Set it on the
  consumer of a topic that must be busy, and leave it off the others.
- A tail that was never sampled, or whose sample is over 60 seconds old, is unknown, and unknown is
  not silent. A partition that is starting, blocked or stopped answers to its own rule.
- `StreamHealth.FindSilent(partitions)` returns the silent consumer for a host that wants the fact
  without the grading.

### Why lag never restarts a consumer, and what does

`UnhealthyLagMs` is Degraded and stays Degraded, for two reasons. Restarting a consumer whose only
problem is a backlog drops its in-flight batches and makes the backlog worse, on a loop. And `LagMs`
is the *age of the last processed entry* — it says how old the work is, not whether any is waiting.
On a bursty topic it is largest exactly when the consumer is idle and caught up, so as a restart
trigger it fires when nothing is wrong.

`UnhealthyBehindSeconds` is the rule that is safe to restart on:

> the id of the stream's last entry is **strictly greater** than the partition's position — there is
> provably an entry it has not read — **and** that position has not changed for N seconds.

```json
"Streams": { "Consumers": [ { "Topic": "orders", "UnhealthyBehindSeconds": 300 } ] }
```

- **Any change of position restarts the clock.** A consumer that is behind but advancing is never
  failed, however large the backlog.
- **A caught-up consumer on an idle topic is never behind** (tail = position), for a day or a year.
  When the next entry lands the clock starts *then*; the consumer gets the whole window to read it.
- **It does not trust the consumer.** The tail is read from Redis by the lag sampler — the
  `XINFO STREAM` it already issues per partition every 15 seconds for `streams.lag.entries`, so it
  costs no extra round trip. `LagEntries` and `IsCaughtUp` are the consumer's own account of itself,
  which is exactly what a wedged consumer gets wrong, and the rule reads neither.
- **Unknown is not Unhealthy.** Before the first sample, or once a sample is more than 60 seconds
  old (four missed samples), the partition is not graded behind. So is an empty stream, and a
  position left ahead of the tail by a trim.
- **Only partitions this instance reads are graded.** Under `Instances:Mode = Lease` a partition
  another instance holds has no monitor here at all, and a newly acquired one starts its clock at
  acquisition.
- **Declared states keep their own thresholds.** A partition blocked on a `DontIgnoreException`
  answers to `UnhealthyBlockSeconds` and a stopped one to `UnhealthyStoppedSeconds`, not to this
  window; an `ErrorPolicy.StopPartition` stop stays Degraded, because a restart would only replay the
  entry it stopped on. When several rules fire at once the report names the first of: connection,
  stalled, blocked too long, stopped too long, behind the tail.

Choose N longer than the slowest batch the handler can legitimately take — the position moves when a
batch completes — and not below about 30, which leaves fewer than two tail samples. Detection takes
between N and N + 15 seconds. It is ignored under `Delivery = WorkQueue`, where members share a
partition's entries and one of them sitting behind a tail another read is normal. Leave it at 0 for
a consumer whose backlog is meant to sit (a restart helps nothing there).

What it cannot see: a consumer reading the *wrong* stream — a mismatched key prefix — finds an empty
stream whose tail legitimately equals its position. Only a host that knows data should be arriving
can call that a fault; the snapshot below carries what such a host needs, and the library takes no
view.

### A stalled partition is handed on

Behind-the-tail compares the position against a tail the lag sampler reads from Redis, over this
process's connection. When that connection is what has gone wrong — reads timing out, replies the
client cannot parse after a desync — the sample stops arriving, the rule stands down ("unknown is not
Unhealthy"), and the best the check can say is Degraded. Meanwhile, under `Lease`, the ownership
registry kept renewing the partition's lease from its own timer, so no other instance could take it.
That is how one pod held both partitions of a topic for four hours after an out-of-memory crash.

`UnhealthyStalledSeconds` (default 300) closes that gap on the process's own clock:

> a partition this instance owns has made **no progress** for N seconds — no read has returned,
> empty or not; no batch has completed; or one batch has been with the handler that long; or its
> position writes have been failing that long.

- **An idle topic never stalls.** In `ReadMode.Block` an empty read returns every `BlockMs`, and an
  empty read is progress. A slow handler that completes batches never stalls either.
- **Declared states keep their own rules.** A partition blocked retrying a `DontIgnoreException` is
  graded by `UnhealthyBlockSeconds`; a policy stop is a stop. A partition whose *reads* keep failing
  is stalled — the block-and-retry on a Redis failure is what stops a Redis blip killing a partition,
  and this is what stops it holding the lease when the fault is in this process instead.
- **Under `Lease` the lease goes.** The host stops the partition's worker, gives the lease up at once
  and will not claim it back for `max(2 × LeaseTtlSeconds, UnhealthyStalledSeconds)`; while it holds a
  quarantine it also stops counting towards the fair share, so the instance taking over can take every
  partition rather than its old share. The next read side gets a fresh reader connection. A worker that
  faults under a non-fatal policy is stood down the same way, at once.
- **And it stays Unhealthy** (rule `Stalled`, `StreamPartitionStatus.StoodDownStalled`) until it reads
  the partition again or restarts — the fault is in the process, so a restart is still what fixes it.
  Under `Static` there is no lease to hand on, and the Unhealthy report is the whole remedy.

Choose N longer than the slowest batch the handler can legitimately take. Detection takes between N
and N + 1 seconds.

### Which replies are trusted, and how

RESP has no request/response correlation. If a connection's replies ever slip against its requests —
StackExchange.Redis says it can happen and "may not be noticed immediately" — every later reply lands on
the wrong command. The library guards each kind of traffic in proportion:

| Traffic | Connection | Guard |
|---|---|---|
| Partition ownership: lease script, presence, quarantine, release, the flusher's reads of the ownership hash | its own, cloned from the shared one, `highIntegrity` **on** | every reply checksum-verified; a mismatch reconnects. A few commands a second, so the ECHO per command costs nothing |
| `XREAD` (`ReadMode.Block`) | the dedicated reader, `highIntegrity` **off** | every reply validated: requested keys only, ids strictly after the cursor sent and ascending, scalars where scalars belong. A failure rebuilds the reader and resumes from the recorded position |
| Publishes, position writes, reset markers, admin, lag sampling | the shared connection, as you configured it | none added — an ECHO per publish is not worth it, and a misreported position write costs at most a redelivery |

The `XREAD` validation is enough because the reply describes itself: it names the stream key and every
entry id, so a reply meant for another read (another partition's, or this one's from an older cursor)
fails it, and the position the loop resumes from comes from what the handler recorded, never from the
bad reply. What it cannot catch: a slipped reply that is indistinguishable from the right one — a repeat
of the same key and cursor, which carries the same entries and is harmless — and a desync on the
shared connection, where publishes and position writes are not verified (a misreported write shows up
as a redelivery after a restart, and the stall rule fires on positions that keep failing).

### Reading the signals yourself

`StreamStatus.Partitions()` is the snapshot `StreamHealth` grades, for a host that wants its own
policy, its own metric, or a diagnostic endpoint that says more than one word:

```csharp
foreach (var p in StreamStatus.Partitions())
{
    // "Nothing written and nothing read for ten minutes" on a topic this service knows is busy.
    if (!p.IsBehindTail && p.TailId is not null
        && p.PositionUnchangedMs > 600_000 && p.TailUnchangedMs > 600_000)
    {
        logger.LogWarning("{Topic}[{Partition}]/{Consumer} has been silent for {Seconds:F0}s (position {Position}, tail {Tail})",
            p.Topic, p.Partition, p.Consumer, p.PositionUnchangedMs / 1000, p.Position, p.TailId);
    }
}
```

Each `StreamPartitionStatus` is a snapshot, not a handle: the monitors stay internal and a reading
cannot change under you. It carries:

| | |
|---|---|
| Identity and state | `Topic`, `Consumer`, `Partition`, `State`, `StopReason`, and `Escalates` — whether a stop turns Unhealthy; read this, not the wording of `StopReason` |
| Progress | `Position` (where it has read up to; unlike `LastProcessed` it is the resume position, not `0-0`, between a restart and the first batch) and `PositionUnchangedMs` |
| The stream | `TailId` (last entry's id; `0-0` when empty; `null` before the first sample), `TailSampleAgeMs`, `TailUnchangedMs` (since a sample last found a new entry; a trim does not count) |
| The verdict's inputs | `IsBehindTail`, `BehindMs` (continuously behind with a frozen position), `BlockedMs`, `StoppedMs`, `LagMs`, `LagEntries` (`-1` when the sampler has not run), `IsCaughtUp` |
| Thresholds | `UnhealthyLagMs`, `UnhealthyBlockSeconds`, `UnhealthyStoppedSeconds`, `UnhealthyBehindSeconds` — that consumer's own, so a caller applies the configured value instead of inventing one |

`StreamHealth.Grade(in p)` grades one partition on those; `StreamHealth.Evaluate(partitions, context)`
is the whole verdict as a pure function, for a filtered snapshot or a test.

Poll it on a timer from a health check or a watchdog, never on a message path: it allocates per
partition.

## Metrics worth alerting on

| Metric | Means |
|---|---|
| `streams.errors` | Handler failures, tagged by `policy` and `exception.type`. A `policy=best_effort` count is messages **skipped** |
| `streams.blocked` | A partition is blocked on a `DontIgnoreException` and going nowhere |
| `streams.clamp.released` | Trimming overran a lagging consumer — data has been lost |
| `streams.partitions.unowned` | Partitions nobody is consuming; usually `STREAMS_INSTANCE_COUNT` drift under `Instances:Mode = Static` |
| `streams.batch.duration` | Handler latency; the first thing to check when lag climbs |
