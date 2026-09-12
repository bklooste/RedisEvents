# Changelog

Every push to `main` publishes a new patch version automatically (see `version.json` — height-based,
not manually tagged), so not every version number gets its own entry here. This file tracks what
actually changed.

## Unreleased

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
