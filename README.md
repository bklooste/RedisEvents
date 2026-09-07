# AdvancedRedisStreams

A production-grade .NET messaging library built on **Redis Streams** — partitioned topics, ordered
delivery per key, at-least-once semantics, and consumer positions stored durably in Redis. No broker
to run beyond Redis itself, no envelope format imposed on your messages, and an error-handling model
that is explicit rather than implicit.

It grew out of internal service-to-service messaging for a live betting platform (order placement,
settlement, feed ingestion) and is published here as a standalone library. The code ships under the
original `Orange.Lib.Streams` / `Orange.Lib.Streams.Web` namespaces.

## Why Redis Streams instead of Kafka/RabbitMQ

- **One less system to run.** If you already run Redis for caching or state, streams live in the same
  cluster — no separate broker, no Zookeeper/KRaft, no extra ops surface.
- **Ordering and state share a connection.** The [outbox](#outbox--exactly-once-state-transition)
  writes application state and publishes the event in a single `MULTI`/`EXEC`, because the state store
  and the broker are the same Redis instance.
- **You get Kafka-shaped guarantees where they matter** — partitioned topics, per-key ordering,
  consumer groups with durable positions, replay from a point in time — without Kafka's operational
  weight.

## Features

- **Partitioned topics with per-key ordering.** Messages with the same partition key land on the same
  partition and are processed in order; different keys can process concurrently. Partition count can
  grow (lossless, logged) but never shrink (refused at startup — it would orphan unread entries).
- **At-least-once delivery with an explicit error contract.** An ordinary exception logs and skips the
  message (no silent retry, no hidden dead-letter queue). A `DontIgnoreException` subclass blocks the
  partition and retries the same batch with exponential backoff (1s → 2 → 4 → 8 → 16 → 30s cap) until
  it succeeds, while every other partition keeps running. The choice is made by the exception type, not
  by configuration — see [the error contract](src/Orange.Lib.Streams/README.md#the-error-contract).
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
  inspection for runbooks; `Orange.Lib.Streams.Web` adds ASP.NET Core health checks and optional
  minimal-API admin endpoints for the same operations, kept in a separate package so a headless worker
  never needs to reference ASP.NET Core.
- **AOT-friendly and dependency-light.** The core library (`Orange.Lib.Streams`) targets `net10.0`,
  is `IsAotCompatible`, and depends on nothing beyond `StackExchange.Redis`, `OpenTelemetry.Api`, and
  the `Microsoft.Extensions.*` abstractions — no ASP.NET Core reference required to run a consumer.
- **OpenTelemetry metrics out of the box** — `streams.errors`, `streams.blocked`,
  `streams.clamp.released`, `streams.partitions.unowned`, `streams.batch.duration` — so a blocked
  partition or a lagging trim is observable rather than silent.
- **Zero required configuration.** `builder.AddStream<OrderHandler>("orders")` is a complete,
  working consumer; every setting has a sane default and is only overridden when you need to.

## Quickstart

```csharp
builder.AddStream<OrderHandler>("orders");
```

```csharp
public sealed class OrderHandler : IBatchHandler
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

Publishing:

```csharp
builder.AddStreamPublisher("orders");

// later, injected as IStreamPublisher
await publisher.PublishAsync(orderId, body, type: "OrderPlaced", ct: ct);
```

Full configuration reference, the outbox, idempotency helpers, and every sharp edge worth knowing
about before running this in production are documented in
**[src/Orange.Lib.Streams/README.md](src/Orange.Lib.Streams/README.md)** — read the error contract
section first; it is the part that isn't discoverable from the API surface.

The ASP.NET Core health check and admin endpoints (position reset, ownership map) live in
**[src/Orange.Lib.Streams.Web/README.md](src/Orange.Lib.Streams.Web/README.md)** and are an optional,
separate reference so a headless consumer never pulls in ASP.NET Core.

## Repository layout

```
src/
  Orange.Lib.Streams/       core library — producers, consumers, positions, outbox, admin, wire format
  Orange.Lib.Streams.Web/   ASP.NET Core health check + admin minimal-API endpoints
tst/
  Orange.Lib.Streams.UnitTests/  fast, no-infrastructure unit tests (run in CI on every push)
  Orange.Lib.Streams.Tests/      integration tests against a real Redis via Testcontainers
```

## Building & testing

Requires the .NET 10 SDK.

```bash
dotnet build AdvancedRedisStreams.slnx

# fast unit tests, no external dependencies
dotnet test tst/Orange.Lib.Streams.UnitTests

# integration tests — spins up Redis via Testcontainers, needs Docker
dotnet test tst/Orange.Lib.Streams.Tests
```

Every code sample in `src/Orange.Lib.Streams/README.md` is compiled as part of the unit test suite
(`tst/Orange.Lib.Streams.UnitTests/ReadmeSamples.cs`), so the documentation cannot silently drift from
the API.

## License

[MIT](LICENSE)
