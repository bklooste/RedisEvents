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
| `Topics:<t>:Partitions` | `2` | Can be increased, never decreased. Caps parallelism |
| `Topics:<t>:MaxLen` | `10000` | Below worst-case consumer lag, trimming eats unprocessed messages |
| `Topics:<t>:Trim` | `Approx` | `None` is refused outside `Development` — it is an OOM footgun |
| `Consumers[n]:BatchSize` | `100` | Bigger batches amortise round trips and widen the redelivery window |
| `Consumers[n]:Persist` | `AsyncBatch` | `SyncBatch`/`SyncMessage` cost a Redis round trip; `None` never stores a position |
| `Consumers[n]:PersistIntervalMs` | `1000` | This is your duplicate window on a crash |
| `Consumers[n]:OnError` | `BestEffort` | See the table above |
| `Consumers[n]:BlockMs` | `1000` | Must stay well under the connection's `syncTimeout` (5000ms default) — validated at startup |
| `Consumers[n]:StartFrom` | `Stored` | `Stored` with `Persist: None` is a contradiction and is refused at startup |
| `Consumers[n]:Backpressure:Capacity` | `4` | Batches in flight per partition |

Misconfiguration throws `StreamConfigurationException` at startup, with the offending key named in
the message. It never waits until 3am to tell you.

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
batch stays a plain loop. Either way, a deserialisation failure surfaces as an ordinary exception from
the handler call, subject to the same `ErrorPolicy` as any other failure — there is nothing special
about a malformed body versus a bug in your own handler logic.

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

### Scaling past one replica needs a StatefulSet

A `Deployment` gives no stable ordinal, so every pod resolves to index 0, every pod owns every
partition, and every message is processed N times. The ownership arithmetic is driven by
`STREAMS_INSTANCE_INDEX` (or the trailing `-<n>` of a StatefulSet `POD_NAME`) and
`STREAMS_INSTANCE_COUNT`.

`STREAMS_INSTANCE_COUNT` must track `spec.replicas`. Scale up without it and the new pods own
nothing; scale down without it and the partitions the departed pods owned are consumed by nobody, in
silence. Change both together and confirm `streams.partitions.unowned` returns to 0.

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

## Metrics worth alerting on

| Metric | Means |
|---|---|
| `streams.errors` | Handler failures, tagged by `policy` and `exception.type`. A `policy=best_effort` count is messages **skipped** |
| `streams.blocked` | A partition is blocked on a `DontIgnoreException` and going nowhere |
| `streams.clamp.released` | Trimming overran a lagging consumer — data has been lost |
| `streams.partitions.unowned` | Partitions nobody is consuming; usually `STREAMS_INSTANCE_COUNT` drift |
| `streams.batch.duration` | Handler latency; the first thing to check when lag climbs |
