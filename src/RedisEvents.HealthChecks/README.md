# RedisEvents.HealthChecks

The health check for [RedisEvents](../RedisEvents/README.md) stream consumers, for any host. It depends on
`Microsoft.Extensions.Diagnostics.HealthChecks` and nothing from ASP.NET Core, so a Generic Host worker and a web
service register the same check and get the same answer.

| Type | What it is |
|---|---|
| `StreamsHealthCheck` | Healthy / Degraded / Unhealthy for every stream consumer in the process. An adapter: the rules are `StreamHealth` in core. |
| `StreamsHealthCheckExtensions.AddStreamsHealthCheck()` | Registers the check under the name and tag `streams`. Explicit, because core cannot register it. |

```csharp
builder.AddStream<MyHandler>("feed_source_racing");
builder.AddStreamsHealthCheck();          // name and tag: "streams"
```

- **A web service** maps it: `app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = c => c.Tags.Contains("streams") })`.
  `RedisEvents.Web` references this package, so a service that already uses the admin endpoints needs no extra reference.
- **A worker** has no endpoint to probe. Register an `IHealthCheckPublisher`; the framework runs the checks on a timer
  and the publisher decides what an Unhealthy report does (normally: stop the host so the orchestrator restarts it).

The rules are documented once, under
[Health: one set of rules, two hosts](../RedisEvents/README.md#health-one-set-of-rules-two-hosts). In short: only a
state a restart can fix is Unhealthy. Lag is Degraded, and so is silence (`DegradedSilenceSeconds`): a consumer that
is caught up and receiving nothing is working, and restarting it changes nothing upstream.
