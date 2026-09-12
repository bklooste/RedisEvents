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
