# Changelog

Every push to `main` publishes a new patch version automatically (see `version.json` — height-based,
not manually tagged), so not every version number gets its own entry here. This file tracks what
actually changed.

## 2026-09-25

### Fixed

- **A contested stand-down no longer strands a partition for the life of the process, and no longer
  fires on an ordinary rolling deploy.** In `InstanceMode.Static` — the default — a `Deployment`
  rolling over surges to two pods of one ordinal, both of which hold a live presence claim and flush
  the same position. The id tiebreak stood one of them down on the first sight of that, landing on
  the *incoming* pod about half the time; the latch was never cleared, so the surviving pod stayed
  `Ready` and read nothing until an operator restarted it. The Lease-mode claim guard added in
  0.2.15 does not cover this shape, because under Static both instances rewrite the claim field and
  it proves nothing.
  - `ConsumerOptions.ContestedGraceSeconds` (default 60) — under Static, an overlap must persist
    this long before anybody stands down. A handover ends when the predecessor exits; a wrong
    instance count does not. An overlap that outlives the window is judged exactly as before, by the
    same tiebreak, so a genuine second writer still ends with one side stopped. Set it to 0 for the
    previous behaviour. Lease mode ignores it — there the claim settles the question outright.
  - `ConsumerOptions.ContestedRecheckSeconds` (default 30) — a stood-down partition re-probes the
    presence field of the instance it stood down for, and when that instance is gone from the
    ownership hash the consumer restarts its read side and reads the partition again. The evidence
    is the same evidence the stand-down used, so this admits no writer a first flush arriving at
    that moment would not have admitted. Bounded to five recoveries per host. Set it to 0 to keep a
    stand-down permanent.
  - A partition that stays stood down now logs at Warning on every re-check instead of once, so an
    overlap that really is a misconfiguration keeps saying so.
- **A pod shut down on an already-cancelled token now releases its ownership claims.**
  `OwnershipRegistry`'s release early-returned when the caller's token was already cancelled — which
  is the token a host stopping under SIGTERM hands it — so the departing instance's claim and
  presence fields survived until their TTL lapsed and its successor read it as a live second writer
  for that whole window. The release is one compare-and-delete round trip and no longer takes a
  token.

## 2026-09-24 (2)

### Added

- **`TopicForwarder`** (`RedisEvents.Producer`) — republishes events from one topic onto another
  exactly once in effect, with a per-source high-water mark and a dedupe marker written in the same
  `MULTI`/`EXEC` as the publish. Upstreamed from a consumer's hand-rolled transactions outbox. See
  the core README's *Forwarding one topic onto another*.

## 2026-09-24

### Added

- **`repository.ExecuteAsync<TAggregate, T>(id, decide, ...)`** — the uncached load-decide-save-retry
  loop for one command, next to `CachedAggregate`'s cached one. Replaces the loop services were
  hand-rolling per aggregate. Returns `CommandResult<T>(Value, Version)`; an exhausted retry lets the
  last `ConcurrencyException` propagate, as `CachedAggregate` does. See the EventSourcing README's
  *Running commands*.
- **`AggregateDecision<T>.Refused(result)`** — a final decision that must have raised nothing. Both
  runners throw `InvalidOperationException` if the aggregate raised events before refusing.
- **`repository.LoadOrCreateAsync<T>(id)`** — the loaded aggregate, or a new one bound to `id`.
- **`AggregateRoot.BindId(id)`**, and `AggregateRoot.Id` is now `virtual` (was `abstract`), defaulting
  to the bound id. `LoadAsync` binds the id it loaded by. Aggregates that override `Id` are unaffected.

### Changed

- **Saving an aggregate whose `Id` is empty now throws `InvalidOperationException`** instead of
  writing to the stream `es:{AggregateName}:`.

## 2026-09-21

### Fixed

- **`RedisEvents.Tracing` did nothing.** `OpenTelemetryRedisTracingApplier.ApplyTracing` had an empty
  body and nothing ever called `ConfigureTracing`, so referencing the package traced no Redis
  command. Connections the library creates itself — a `Streams:ConnectionString` that differs from
  the container's multiplexer, and every consumer reader connection — are now registered with the
  tracer provider's `StackExchangeRedisInstrumentation`. Opt in with `AddRedisTracing()` on the host
  builder and `AddRedisEventsConnectionTracing()` on the tracer provider; a connection made before
  the provider is built is held until it exists.
- **The tracing applier is no longer applied to a reused multiplexer.** That connection belongs to
  the host, which instruments it itself; applying here too would register it twice and emit every
  command span twice.

## 2026-09-18

### Fixed

- **A Lease-mode handoff could leave a partition read by nobody until the new owner restarted.**
  The two-writer guard asked only whether the foreign writer in a position field was still *alive*.
  On a rolling deploy the outgoing owner stays alive (it keeps its other partitions and so its
  presence field) while its last async flush for the partition it gave up lands after the new owner
  has claimed it. The new owner read that as a live rival, lost the id tiebreak, stood the partition
  down — and kept the lease, so the peer never re-claimed it. Seen on `offer-odds` for eight hours;
  the pod passed liveness throughout. The guard now also reads the partition's claim field in the
  same `HMGET`: a live writer that does not hold the claim is a late flush, logged at Information
  and ignored. Issue #14 proved the *graceful* release was safe; this is the ungraceful sibling.
- **A contested stand-down now marks its partition monitor**, so the health check reports it
  (`partitionsStopped`, Degraded) instead of Healthy.

### Added

- **`Streams:Consumers:<n>:UnhealthyStoppedSeconds`** (default `300`). A partition that stood down
  outside any `ErrorPolicy` decision — a contested position, a retired co-located slot — and is
  behind a stream that is still being written turns the health check **Unhealthy** once it has
  been stopped this long, so a liveness restart hands the partition on. `ErrorPolicy.StopPartition`
  stops are an operator's decision and stay Degraded.

### Changed — breaking (wire)

- **`IEventRepository.SaveAsync` no longer stamps `es-version` or `es-id` headers.** Nothing in the
  library read either one, yet together they cost 58 packed bytes on every published event. They also
  forced an `h` field onto events that would otherwise have none, and a `Guid` plus a header list on
  every save. Events now carry only the caller's correlation id and headers; a save with no options
  writes no header field at all.
  - Migration: a consumer that read `es-version` or `es-id` should use `EventMeta.Id` (the Redis
    stream id: unique, increasing, identical on every redelivery) as an idempotency guard or an
    event identity. A producer that needs a per-event version can pass it in `PublishOptions.Headers`.

### Changed

- **State-stream entries are now body and type only.** `IStreamStore.AppendAndPublishAsync` (and so
  `RedisEventRepository.SaveAsync`) used to write the same fields on the aggregate's own stream as on
  the topic publish: partition key, correlation id, `traceparent` and headers (`es-version`,
  `es-id`). Loading an aggregate reads none of them, the partition key just repeats the stream name,
  and state streams are never trimmed, so those bytes were kept forever. For small events they
  outweighed the body several times over. State entries now carry only `b` and `t`. The topic copy is
  unchanged, and it is what projections and consumers read.
  - New `Streams:Topics:<t>:StateMetadata` (default `false`). Set it to `true` to keep correlation
    id, trace and headers on state entries as well. The partition key is never written on a state
    entry.
  - No codec version change. Readers already treated those fields as optional, so existing streams
    read unchanged and one stream can mix old and new entries. `IStreamStore.ReadAsync` now reports
    an empty `PartitionKey` (and, by default, no correlation id, trace or headers) for entries
    written from this version on.
  - Added a regression test proving that neither inline `MAXLEN` nor the background retention sweep
    ever trims a state stream, even on an aggressively trimmed topic.

## 2026-09-17

### Fixed

- **`AddEventProjector` for a second topic on the same builder silently broke the first.** It
  registered `EventProjector` as a plain `AddSingleton`, so a service calling it more than once (one
  `AddEventProjector` per topic it projects — the documented, intended usage) collided on .NET DI's
  last-registration-wins rule for an unkeyed service type: every topic's stream consumer resolved
  `EventProjector` unkeyed and got the *same* instance — whichever topic's registration ran last, with
  its `EventTypeRegistry` and projection list. Every earlier topic's messages then decoded against the
  wrong registry, matched nothing, and were silently skipped: position advanced, nothing logged, no
  error — because "an unrecognised wire type" is legitimately not a fault (see `EventProjector`'s own
  remarks). Found via a real instance: a service projecting two topics had zero writes reach its read
  view for one of them, with a fully caught-up consumer position and not a single error line.
  `AddEventProjector` now registers `EventProjector` **keyed by topic** (`AddKeyedSingleton`, the same
  pattern `EventTypeRegistry` already used), via a new keyed `AddStream<THandler>(topic, serviceKey)`
  overload in core that `StreamConsumerHost` resolves through `GetRequiredKeyedService` when a
  registration carries a key. A service projecting exactly one topic is unaffected either way; a
  service projecting more than one now gets each its own correctly-wired projector.

## 2026-09-15

### Added

- `RedisEvents.EventSourcing.CachedAggregate<TAggregate>`: the load-decide-save-retry loop every
  command-side write path around `IEventRepository` was hand-rolling slightly differently. Holds one
  cached aggregate instance, never trusts it across a save whose outcome is unknown, and on a lost
  optimistic-concurrency check reloads and re-decides — up to a caller-chosen `maxAttempts`, after
  which the `ConcurrencyException` is left to propagate, same as calling `SaveAsync` with no retry at
  all. `AggregateDecision<T>.Redo()` lets a decision discard the instance it was handed and try again,
  uncounted against the retry budget, for a reason that isn't contention (a batched command that raised
  events and then failed and must be dropped before the rest re-apply; a no-op decision made against a
  cached copy that turned out to be stale). Deliberately not a lock, an actor, or a cache with its own
  eviction policy — how access is serialized and how long an idle instance lives stay the caller's call.
- `CachedAggregate<TAggregate>.RunAsync` gained an optional `expectedVersion` selector, found needed by
  the first real migration onto it (exchange-orderbook's `Market`, snapshot-restorable so
  `AggregateRoot.Version` alone is not its true stream position — the save was losing its concurrency
  check against a version nothing had actually written). Defaults to `aggregate.Version`, unchanged from
  before this existed.

## 2026-09-14

### Changed — breaking

- **The read side of `RedisEvents.EventSourcing` is now its own package, `RedisEvents.Projections`.**
  `EventProjector`, `IProjection<TEvent>`, `EventMeta`, `EventTypeRegistry`, `IViewStore<TView>`,
  `InMemoryViewStore<TView>`, `RedisViewStore<TView>`, and the `AddEventProjector`/`AddProjection`/
  `AddRedisViewStore` registration API all moved to `RedisEvents.Projections`. This is the natural
  conclusion of yesterday's change decoupling the projector from any aggregate concept: it already
  depended on nothing but a standard RedisEvents topic, so it no longer needs to ship inside the
  event-sourcing package at all. `RedisEvents.EventSourcing` now depends on `RedisEvents.Projections`
  (for the shared `EventTypeRegistry`, so `AddEventStore` and `AddEventProjector` on the same topic
  still agree on wire format) — the dependency runs one way only, so a service that just wants typed
  projections over an ordinary topic now pulls in nothing aggregate-related at all.
  - Migration: add a `PackageReference` to `RedisEvents.Projections`, and change
    `using RedisEvents.EventSourcing;` to `using RedisEvents.Projections;` in any file that uses the
    types above. `AggregateRoot`, `IEventRepository`, `RedisEventRepository`, `ConcurrencyException`
    and `AddEventStore` stay in `RedisEvents.EventSourcing`, unchanged.
  - `RedisEvents.Projections` has its own test project (`RedisEvents.Projections.UnitTests`,
    `RedisEvents.Projections.Tests`) and README, and is independently packable/publishable.

## 2026-09-13 (2)

### Added

- **`RedisEvents.EventSourcing`: register a projection as a delegate, not just a class.**
  `AddProjection<TEvent>(Func<IServiceProvider, TEvent, EventMeta, CancellationToken, ValueTask>)` lets
  a handler be an inline closure — resolve whatever it needs (an `IViewStore<TView>`, a repository)
  from the `IServiceProvider` handed to it, with no `IProjection<TEvent>` class to define just to
  satisfy the interface. `AddProjection<TEvent, TView>(Func<TEvent, EventMeta, TView>)` goes one step
  further for the common set-semantics case: map the event to a view and it is written through
  `IViewStore<TView>.SetAsync(meta.PartitionKey, ...)` for you. Both compose with everything else
  unchanged — `AddEventProjector`, `AddRedisViewStore`, and class-based `AddProjection<TProjection>`
  projections all dispatch through the same `EventProjector`, in any mix.

### Changed — breaking

- **`RedisEvents.EventSourcing`'s read side no longer depends on an aggregate at all.**
  `EventProjector` previously required every event to carry an `es-version` header — stamped only by
  this package's own `AddEventStore`/`IEventRepository.SaveAsync` — and threw `InvalidOperationException`
  on any message missing one. That made the projector unusable against a topic published by an ordinary
  `IStreamPublisher.PublishAsync` call, which is most topics: the read side should depend only on
  standard RedisEvents streams and the partitioning they already guarantee, not on how the write side
  happened to publish.
  - `EventMeta.Version` (`int`) is removed. `EventMeta.AggregateId` is renamed to `PartitionKey` — it
    was always just the wire message's own partition key, with no assumption an aggregate exists.
  - Use `EventMeta.Id` (`RedisEvents.Wire.StreamId`, already present, `IComparable<StreamId>`) for the
    same redelivery/idempotency guard `Version` was used for: Redis's own per-partition stream entry id
    is unique, strictly increasing, and identical on every redelivery — exactly what an accumulative
    projection needs, and it requires nothing from the producer.
  - `EventProjector` no longer reads or requires the `es-version` header. `IEventRepository.SaveAsync`
    still stamps it on every published event (informational only, for a human reading the raw stream) —
    the write side is unchanged.
  - Migration for an existing projection: replace `meta.AggregateId` with `meta.PartitionKey`, and
    replace a `current.Version >= meta.Version` guard with `current.LastEventId >= meta.Id` (or whatever
    field name a view chooses for the last-applied `StreamId`).

## 2026-09-13

### Added

- **`RedisEvents.MessagePack`**: MessagePack-typed publish/consume — `PublishAsync<T>`,
  `PublishBatchAsync<T>`, `EnqueueAsync<T>`, `Deserialize<T>`, and `IMessageHandler<T>`/
  `IBatchHandler<T>` registration, mirroring the JSON typed convenience in core. Bodies over 1024
  bytes (configurable) are automatically LZ4-compressed; decoding needs no special handling either
  way. Kept as a separate package, like `RedisEvents.Web`, so core carries no MessagePack dependency.
- **`RedisEvents.EventSourcing`**: a light event-sourced aggregate root (`AggregateRoot`, `On<TEvent>`/
  `Raise<TEvent>`, no reflection or `dynamic` dispatch) and a typed event projector
  (`IProjection<TEvent>`, `EventProjector`) on top of streams. An aggregate's own Redis stream is the
  source of truth; `IEventRepository.SaveAsync` appends to it and publishes to the topic in one
  transaction, guarded by an optimistic-concurrency check (`ConcurrencyException` on a lost race). The
  event-type registry (`EventTypeRegistry`) maps a stable wire-type string to a CLR type explicitly, so
  a class rename never breaks replay, and plugs into any serialiser (JSON by default, MessagePack via
  a lambda pair, no new dependency). The projector rides core's ordinary consumer, so positions, replay
  and the error contract are exactly core's. `IViewStore<TView>` + a Redis-backed implementation give a
  minimal read-model store; a service may swap in its own (Azure Tables, SQL, …). Core gained a small
  supporting primitive, `IStreamStore` (`Producer.IStreamStore`/`AddStreamStore`), and a
  `StreamsConnection.GetSharedDatabase` facade, both usable directly. See
  [`src/RedisEvents.EventSourcing/README.md`](src/RedisEvents.EventSourcing/README.md) and the worked
  example under `samples/RedisEvents.EventSourcing.Sample.Inventory/`.
- `IEventRepository.SaveWithoutConcurrencyCheckAsync` / a matching unconditioned
  `IStreamStore.AppendAndPublishAsync` overload: append and publish in one transaction with no `WATCH`
  and no `ConcurrencyException`, for streams with no read-decide-write invariant to protect (see
  "Optimistic concurrency" in the EventSourcing README for when that is and isn't the case). Wired into
  the sample as `POST /items/no-check`, benchmarked against the checked `POST /items` path under 10
  concurrent clients (~7,900 req/s vs ~1,400 req/s) — see the EventSourcing README's benchmark section.

## 2026-09-12

### Added

- Typed handlers: `IMessageHandler<T>` / `IBatchHandler<T>`, registered via
  `AddStream<THandler, TMessage>(topic, typeInfo)` — a handler receives an already-deserialised value
  instead of calling `Deserialize<T>` by hand. A large batch deserialises in parallel chunks of 10.
- `IPositionStore` is now genuinely pluggable: register your own via DI and it's used instead of the
  default `RedisPositionStore`. New `RedisEvents.Testing.MemoryPositionStore` for tests that want real
  load/save/reset semantics without a Redis position hash.

### Changed

- Renamed `Orange.Lib.Streams` / `Orange.Lib.Streams.Web` to `RedisEvents` / `RedisEvents.Web`
  (namespaces, NuGet package ids, and the GitHub repository).
- Removed `<RuntimeIdentifiers>linux-x64</RuntimeIdentifiers>` from both library projects — it was
  inert for `dotnet pack` output and read as a false platform restriction.

## 2026-09-07

### Added

- Initial public release: `RedisEvents` core library (partitioned Redis Streams messaging, outbox,
  idempotency, admin) and `RedisEvents.Web` (ASP.NET Core health checks and admin endpoints).
- GitHub Actions CI (build + test on every push/PR) and NuGet Trusted Publishing (OIDC) on push to
  `main`.
- Typed publish/consume convenience over JSON: `PublishAsync<T>`, `PublishBatchAsync<T>`,
  `EnqueueAsync<T>`, `Deserialize<T>`, all `JsonTypeInfo<T>`-only for AOT safety.
