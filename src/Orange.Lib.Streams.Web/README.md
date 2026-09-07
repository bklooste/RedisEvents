# Orange.Lib.Streams.Web

The ASP.NET Core half of [Orange.Lib.Streams](../Orange.Lib.Streams/README.md). Reference this only if
your service has a web host.

Why it exists (R-25 in `docs/plans/2026-09-06-orange-lib-streams/12-remediation.md`): the core library
used to carry `<FrameworkReference Include="Microsoft.AspNetCore.App" />`, so every consumer — including
a worker with no web host — dragged in the whole ASP.NET Core framework. Core now references hosting,
logging, configuration and StackExchange.Redis and nothing else; everything that needs ASP.NET or the
health-check abstractions lives here.

| Type | What it is |
|---|---|
| `StreamsHealthCheck` | The readiness answer for every stream consumer in the process. Healthy / Degraded / Unhealthy — lag and ownership problems are Degraded on purpose. |
| `StreamsHealthCheckExtensions.AddStreamsHealthCheck()` | Registers the check under the name and tag `streams`. Explicit, because core cannot register it. |
| `StreamAdminEndpoints.MapRoutes()` | Minimal-API routes for position reset preview/apply and the ownership map. Opt-in; map them behind an admin policy. |

```csharp
builder.AddStream<MyHandler>("feed_source_racing");
builder.AddStreamsHealthCheck();          // tag: "streams"

var app = builder.Build();
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = c => c.Tags.Contains("streams") });

var admin = app.MapGroup("/admin/streams").RequireAuthorization("admin");
StreamAdminEndpoints.MapRoutes(admin, app.Services.GetRequiredService<IConnectionMultiplexer>());
```

`library/tst/Orange.Lib.Streams.HeadlessSmoke` is the regression guard for the split: a worker that
references core alone, and fails at startup if any ASP.NET assembly reaches its dependency closure.

R-11 has since closed the admin findings that moved with the file: the ownership SCAN glob is built
from `StreamKeys` so it keeps the literal hash-tag braces, resets read the topic's real key layout
back from Redis instead of assuming co-location, the routes carry authorization metadata themselves,
route parameters are validated, an apply against a consumer that still holds claims answers 409, and
the full reset family (`from=`, `id=`, `to=start`, `to=end`, `partition=`, `maxCountScan=`) is
exposed. The health-check finding (connection coverage, R-16) is still open.
