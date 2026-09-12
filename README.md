# RedisEvents

[![CI](https://github.com/bklooste/RedisEvents/actions/workflows/ci.yml/badge.svg)](https://github.com/bklooste/RedisEvents/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/RedisEvents.svg)](https://www.nuget.org/packages/RedisEvents)

A production-grade .NET messaging library built on **Redis Streams** — partitioned topics, ordered
delivery per key, at-least-once semantics, and consumer positions stored durably in Redis. No broker
to run beyond Redis itself, no envelope format imposed on your messages, and an error-handling model
that is explicit rather than implicit.

It grew out of internal service-to-service messaging for a live betting platform (order placement,
settlement, feed ingestion) and is published here as a standalone library. The code ships under the
original `RedisEvents` / `RedisEvents.Web` namespaces.

## Why Redis Streams instead of Kafka/EventHub/RabbitMQ

- **One less system to run.** If you already run Redis for caching or state, streams live in the same
  cluster — no separate broker, no Zookeeper/KRaft, no extra ops surface.
- **Ordering and state share a connection.** The [outbox](#outbox--exactly-once-state-transition)
  writes application state and publishes the event in a single `MULTI`/`EXEC`, because the state store
  and the broker are the same Redis instance.
- **You get Kafka-shaped guarantees where they matter** — partitioned topics, per-key ordering,
  consumer groups with durable positions, replay from a point in time — without Kafka's operational
  weight.
- **Positions stored in the DB by default** - so will return to current  state automatically on failure.
- **It's fast, and the numbers below are measured, not marketed.** Publishing pipelines over
  `StackExchange.Redis`, batches and buffers rather than round-tripping per message, and the wire
  codec is a flat array with no serializer indirection — see [Performance](#performance) for real
  throughput and per-call latency numbers from this repo's own benchmark suite.

## Features

- **Partitioned topics with per-key ordering.** Messages with the same partition key land on the same
  partition and are processed in order; different keys can process concurrently. Partition count can
  grow (lossless, logged) but never shrink (refused at startup — it would orphan unread entries).
- **At-least-once delivery with an explicit error contract.** An ordinary exception logs and skips the
  message (no silent retry, no hidden dead-letter queue). A `DontIgnoreException` subclass blocks the
  partition and retries the same batch with exponential backoff (1s → 2 → 4 → 8 → 16 → 30s cap) until
  it succeeds, while every other partition keeps running. The choice is made by the exception type, not
  by configuration — see [the error contract](src/RedisEvents/README.md#the-error-contract).
- **Outbox / exactly-once state transition.** `Outbox.WriteAndPublishAsync` writes your application
  state and publishes the resulting event in one Redis transaction, closing the classic "state
  committed, event never published" gap — no separate outbox table or relay process needed.
- **Idempotency helpers.** A one-round-trip `SET NX PX` claim (`Idempotency.TryBeginAsync`) for
  deduplicating redeliveries, keyed on the stream entry id or your own business key.
- **Batch or single-message handlers.** Implement `IBatchHandler` for throughput, `IMessageHandler` for
  simplicity, or register a delegate directly — same partitioning, positions, and error handling
  underneath either way.
- **Buffered or direct publishing.** A direct publisher for durability-sensitive writes, or a buffered
  publisher (`IStreamBufferedPublisher`) that batches into background flushes for high-throughput,
  telemetry-shaped workloads — with an explicit, documented trade-off about what buffering can lose on
  a crash.
- **Multi-instance ownership without a coordinator.** Partition ownership is computed from
  `STREAMS_INSTANCE_INDEX` / `STREAMS_INSTANCE_COUNT` (or a StatefulSet pod ordinal) — no external
  leader election, no split-brain window.
- **Admin & operability built in.** `StreamAdmin` exposes position preview/reset and ownership
  inspection for runbooks; `RedisEvents.Web` adds ASP.NET Core health checks and optional
  minimal-API admin endpoints for the same operations, kept in a separate package so a headless worker
  never needs to reference ASP.NET Core.
- **AOT-friendly and dependency-light.** The core library (`RedisEvents`) targets `net10.0`,
  is `IsAotCompatible`, and depends on nothing beyond `StackExchange.Redis`, `OpenTelemetry.Api`, and
  the `Microsoft.Extensions.*` abstractions — no ASP.NET Core reference required to run a consumer.
- **OpenTelemetry metrics out of the box** — `streams.errors`, `streams.blocked`,
  `streams.clamp.released`, `streams.partitions.unowned`, `streams.batch.duration` — so a blocked
  partition or a lagging trim is observable rather than silent.
- **Zero required configuration.** `builder.AddStream<OrderHandler>("orders")` is a complete,
  working consumer; every setting has a sane default and is only overridden when you need to.
- **MessagePack, as an opt-in package.** [`RedisEvents.MessagePack`](src/RedisEvents.MessagePack/README.md)
  adds the same typed publish/consume convenience over MessagePack instead of JSON, with size-gated
  LZ4 compression — a message over 1024 bytes compresses automatically, decoding needs no
  special-casing either way, and the threshold (or auto-compression itself) is configurable.

## Quickstart

A typed handler receives an already-deserialised value — no manual `JsonSerializer.Deserialize` call,
no hand-written `type` string:

```csharp
[JsonSerializable(typeof(Order))]
internal partial class OrderJson : JsonSerializerContext;

public sealed class OrderHandler : IMessageHandler<Order>
{
    public ValueTask HandleAsync(in StreamMsg<Order> msg, CancellationToken ct)
    {
        Order? order = msg.Value;
        // ... handle it
        return ValueTask.CompletedTask;
    }
}

builder.AddStream<OrderHandler, Order>("orders", OrderJson.Default.Order);
```

Publishing is the same shape — a `JsonTypeInfo<T>` in, no serialising or type string by hand:

```csharp
builder.AddStreamPublisher("orders");

// later, injected as IStreamPublisher
await publisher.PublishAsync(orderId, order, OrderJson.Default.Order, ct: ct);
```

Prefer raw bytes — your own (de)serialisation, no `JsonSerializerContext` to write? The untyped
`IMessageHandler`/`IStreamPublisher` members are still there underneath and need no typed
registration at all:

```csharp
public sealed class OrderHandler : IMessageHandler
{
    public ValueTask HandleAsync(in StreamMsg msg, CancellationToken ct)
    {
        var order = JsonSerializer.Deserialize<Order>(msg.Body.Span);
        // ... handle it
        return ValueTask.CompletedTask;
    }
}
```

```csharp
await publisher.PublishAsync(orderId, body, type: "OrderPlaced", ct: ct);
```

### Batch handlers

The same registration works with a handler that receives a whole batch at once instead of one
message at a time — same partitioning, positions, and error handling underneath, just fewer round
trips through the handler when per-message overhead adds up. Typed:

```csharp
public sealed class OrderBatchHandler : IBatchHandler<Order>
{
    public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg<Order>> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            Order? order = batch.Span[i].Value;
            // ... handle it
        }

        return ValueTask.CompletedTask;
    }
}

builder.AddStream<OrderBatchHandler, Order>("orders", OrderJson.Default.Order);
```

Or raw bytes, the same way as the single-message handler above:

```csharp
public sealed class OrderBatchHandler : IBatchHandler
{
    public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            var order = JsonSerializer.Deserialize<Order>(batch.Span[i].Body.Span);
            // ... handle it
        }

        return ValueTask.CompletedTask;
    }
}
```

Full configuration reference, the outbox, idempotency helpers, and every sharp edge worth knowing
about before running this in production are documented in
**[src/RedisEvents/README.md](src/RedisEvents/README.md)** — read the error contract
section first; it is the part that isn't discoverable from the API surface.

The ASP.NET Core health check and admin endpoints (position reset, ownership map) live in
**[src/RedisEvents.Web/README.md](src/RedisEvents.Web/README.md)** and are an optional,
separate reference so a headless consumer never pulls in ASP.NET Core.

## Performance

Speed comes from staying close to the metal: publishing pipelines multiple `XADD`s over one
`StackExchange.Redis` connection instead of round-tripping per message, the buffered publisher
coalesces enqueues into background flushes, and the wire codec (`EntryCodec`) writes a flat
`NameValueEntry[]` with no reflection-based serializer in the hot path. Partition routing is a
non-cryptographic hash with a power-of-two fast path, and the single-partition case is a
constant — no hashing at all.

The numbers below are from this repo's own benchmark suite (`PerfBenchmarkTests` and
`StreamsMicroBenchmarks`), run on a single dev machine (13th Gen Intel Core i7-13700KF, WSL2,
one un-tuned `redis:8-alpine` container) — a directional baseline, not an SLA. Run-to-run
variance on shared/virtualized hardware is normal; re-run the suite on your own target
infrastructure for numbers you'd actually size capacity against.

**Throughput** — `dotnet test tst/RedisEvents.Tests --filter "TestType=PerfTest"`, Release build:

| Path | Configuration | Throughput |
| --- | --- | --- |
| Direct publish (`XADD`, unpipelined) | 128 B body | ~6,200 msg/s |
| Direct publish (`XADD`, unpipelined) | 2048 B body | ~6,300 msg/s |
| Pipelined `PublishBatchAsync` | batch of 100 | ~225,000 msg/s |
| Pipelined `PublishBatchAsync` | batch of 500 | ~137,000 msg/s |
| Pipelined `PublishBatchAsync` | batch of 2,000 | ~159,000 msg/s |
| `BufferedStreamPublisher` | batch ≤500, flush ≤10ms | ~389,000 msg/s |
| `BufferedStreamPublisher` | batch ≤2,000, flush ≤25ms | ~422,000 msg/s |
| End-to-end consume | 1 partition, blocking reads | ~278,000 msg/s |
| End-to-end consume | 4 partitions, blocking reads | ~255,000 msg/s |
| End-to-end consume | 4 partitions, 4 reader threads | ~343,000 msg/s |
| End-to-end consume | 4 partitions, polling reads | ~310,000 msg/s |
| End-to-end consume | 4 partitions, backpressure disabled | ~502,000 msg/s |

The takeaway isn't any single number — it's the shape: a single unpipelined `XADD` is bound by
Redis round-trip latency (~150μs on this box), so it sits around 6K msg/s regardless of body
size; pipelining or buffering removes that round trip from the hot path and throughput jumps by
one to two orders of magnitude. Pick direct publishing when a message must be durable the
instant the call returns, and the buffered publisher when you can tolerate a small window of
loss on crash in exchange for throughput — the trade-off is spelled out in
[the buffered publisher docs](src/RedisEvents/README.md).

**Low-level latency** — `dotnet test tst/RedisEvents.UnitTests --filter "TestType=PerfTest"`
(BenchmarkDotNet, Release build), the per-call cost every message pays on the hot path:

| Operation | Mean | Allocations |
| --- | --- | --- |
| `EntryCodec.Encode` (body + type + key + correlation id + headers) | ~125 ns | ~408 B |
| `EntryCodec.Encode` minimal (body + type only) | ~41 ns | ~216 B |
| `EntryCodec.Decode` | ~786 ns | small — only the strings `StreamMsg` exposes |
| `PartitionRouter.ForKey`, power-of-two partition count | ~2.0 ns | 0 |
| `PartitionRouter.ForKey`, non-power-of-two partition count | ~2.3 ns | 0 |
| `PartitionRouter.ForKey`, single partition | effectively 0 ns | 0 |
| `PartitionRouter.RoundRobin` | ~3.7 ns | 0 |
| `StreamId.Parse` / `TryParse` | ~12 ns | 0 |
| `StreamId.TryFormat` (into a caller buffer) | ~7 ns | 0 |
| `StreamId.Format` (allocates a string) | ~16 ns | small |

In other words: everything on the publish and routing path that *can* be allocation-free *is*
allocation-free — routing a message costs a couple of nanoseconds and no garbage, so at
production throughput the codec, not the library's own bookkeeping, is what shows up in a
profiler.

## Security

This library doesn't invent its own auth, encryption, or access-control layer — it hands your
`ConnectionString` straight to `StackExchange.Redis` and inherits whatever Redis itself is
configured to enforce:

- **Authentication** via Redis `AUTH` (a password on the connection string) or Redis 6+ **ACL
  users** (`user=...,password=...`), scoped down to only the commands and key patterns a
  producer or consumer actually needs.
- **Encryption in transit** via `ssl=true` on the connection string (or `stunnel`/a sidecar) if
  your Redis deployment terminates TLS — the library has no opinion here beyond what
  `StackExchange.Redis` supports.
- **Network isolation** (VPC/security group, private subnet, firewall) is what actually keeps an
  unauthenticated client from reaching Redis at all, and is worth more than any
  application-level control.

Two things worth calling out explicitly: [idempotency claims](src/RedisEvents/README.md)
and the outbox are correctness mechanisms, not authorization boundaries — they stop double
processing, not a party with Redis access from reading or forging messages. Treat Redis
credentials with the same care as a database password, since with `XADD`/`XRANGE` access to a
topic's streams they effectively are one.



## Repository layout

```
src/
  RedisEvents/             core library — producers, consumers, positions, outbox, admin, wire format
  RedisEvents.Web/         ASP.NET Core health check + admin minimal-API endpoints
  RedisEvents.MessagePack/ MessagePack-typed publish/consume, with size-gated LZ4 compression
tst/
  RedisEvents.UnitTests/  fast, no-infrastructure unit tests (run in CI on every push)
  RedisEvents.Tests/      integration tests against a real Redis via Testcontainers
```

## Building & testing

Requires the .NET 10 SDK.

```bash
dotnet build RedisEvents.slnx

# fast unit tests, no external dependencies
dotnet test tst/RedisEvents.UnitTests

# integration tests — spins up Redis via Testcontainers, needs Docker
dotnet test tst/RedisEvents.Tests
```

Every code sample in `src/RedisEvents/README.md` is compiled as part of the unit test suite
(`tst/RedisEvents.UnitTests/ReadmeSamples.cs`), so the documentation cannot silently drift from
the API.

## License

[MIT](LICENSE)
