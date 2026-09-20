# RedisEvents.Tracing

OpenTelemetry tracing for the Redis connections RedisEvents creates itself — a `Streams:ConnectionString`
that differs from the container's `IConnectionMultiplexer`, and every consumer reader connection.
(A multiplexer registered in the container is the host's to instrument with `AddRedisInstrumentation`.)

```csharp
builder.AddRedisTracing();                       // host builder: hands connections to the applier

services.AddOpenTelemetry().WithTracing(t => t
    .AddRedisEventsConnectionTracing());         // tracer provider: receives them
```

A connection made before the tracer provider is built is held until it exists. Stream-level spans
(`streams.publish` / `streams.process`) come from `AddRedisEvents()` in the core package and need no
extra package.
