# RedisEvents.Restate

Starts [Restate](https://restate.dev) workflows from a RedisEvents stream, in batches, without ever
acking a batch with a start that was lost.

You subclass `RestateStartHandler<T>` with a filter and a map. Each kept message becomes
`POST {ingress}/{workflow}/{key}/run/send`, keyed by your business id. The partition only advances
once every kept message in the batch has been accepted.

```csharp
public sealed class BonusPayoutStarter(HttpClient http)
    : RestateStartHandler<BetSettled>(http, PayoutJson.Default.Options, new RestateStartOptions
    {
        Ingress = new Uri("http://plat-restate:8080"),
    })
{
    // In memory, no I/O: most settled bets carry no claim and cost nothing.
    protected override bool Keep(in StreamMsg<BetSettled> msg) => msg.Value?.ClaimId is not null;

    protected override (string Workflow, string Key, object? Input) Map(StreamMsg<BetSettled> msg)
        => ("BonusPayout", msg.Value!.ClaimId!, new PayoutInput(msg.Value.BetId, msg.Value.Amount));
}

[JsonSerializable(typeof(PayoutInput))]
internal sealed partial class PayoutJson : JsonSerializerContext;

builder.AddStream<BonusPayoutStarter, BetSettled>("bets_settled", BetJson.Default.BetSettled);
```

## How a batch is handled

1. **Filter.** `Keep` runs for every message in memory. A batch with nothing kept makes no HTTP
   call and returns, so the position moves past it.
2. **Submit.** Kept messages are posted bounded-parallel (`MaxParallelism`, default 8). The input
   is the JSON body, serialized through the `JsonSerializerOptions` you pass in (use a
   source-generated context, which keeps it AOT-safe). Each post carries a W3C `traceparent`: the
   handler's own `restate.start {workflow}` span when tracing is on, otherwise the message's
   producer context.
3. **No `Idempotency-Key`.** The workflow key is the dedupe. Restate 1.8 answers `400` to an
   `Idempotency-Key` on a workflow handler and never starts the run. A second submit of the same
   key answers `202` with `"status":"PreviouslyAccepted"` and the first run's invocation id. This
   holds within the workflow service's `workflow_completion_retention`, which is 1 day by default,
   so set it to cover your redelivery window.
4. **Retry per item.** Up to `MaxAttempts` (default 5) attempts per item, 200 ms doubling to 5 s.
   - Any 2xx is accepted. `PreviouslyAccepted` is also counted as already started.
   - These are retried: a timeout (`AttemptTimeout`, default 10 s, or `HttpClient.Timeout`), an
     `HttpRequestException`, `408`, `425`, `429` and any 5xx.
   - Any other status is not retried.
5. **Ack or block.** If every kept item is accepted, `HandleAsync` returns and the batch is acked.
   If any item is still not accepted, it throws `RestateStartBlockedException`, a
   `DontIgnoreException` that names the failed `workflow/key` items. The partition then blocks and
   retries the whole batch every 1 s to 30 s until it succeeds. Items that were already accepted
   answer `PreviouslyAccepted` on the retry.

## Only one exception type leaves `HandleAsync`

`RestateStartBlockedException` is the only exception `HandleAsync` throws. Under the default
`ErrorPolicy.BestEffort`, core logs any other exception and **skips** the batch, which here would drop
starts without a trace. So all of these become a blocked batch:

- a throwing `Keep` or `Map`;
- an empty workflow or key;
- an input type the serializer does not know;
- transport failures and timeouts.

Cancellation is included:

- **Timeouts.** A timeout is an `OperationCanceledException` on a token that is not the worker's.
  Core treats an `OperationCanceledException` as shutdown only when the worker's own token is
  cancelled, so letting a timeout out would be a skip.
- **Cancelled handler token.** If the handler's token is cancelled before every start is accepted,
  the batch is reported blocked with `ShuttingDown = true`. The worker's block loop sees its own
  cancelled token and stops without advancing, so the batch redelivers on the next start. These
  shutdown blocks are not counted in `restate.start.blocked`.
- **Already accepted.** If every start was accepted before the token was cancelled, the call returns
  normally and the batch is acked.

The handler is a singleton shared by every partition. It keeps no per-call state and takes no lock,
because Restate gives exclusivity per workflow key.

**Before the handler is called.** Core deserialises the batch before `HandleAsync` runs, and a body
that fails to deserialise throws there, where `BestEffort` skips it. If a malformed message must not
be dropped, register a deserialiser that cannot throw, or use `ErrorPolicy.StopPartition` on that
consumer.

**Trimming.** A blocked partition holds its position while producers keep appending, and the inline
`MAXLEN` trim can delete entries the partition has not read yet. Size the source topic's `MaxLen` for
the longest Restate outage you will tolerate (per-partition publish rate times outage length), and
alert on `streams.blocked` and `streams.lag.entries`. This applies to every `DontIgnoreException`
consumer, not just this one.

## Metrics and traces

Register both with `AddRedisEventsRestate()` on the tracer and meter provider builders. The source
and meter are both named `RedisEvents.Restate`.

| Counter | Tags | Meaning |
|---|---|---|
| `restate.start.kept` | handler | messages `Keep` returned true for |
| `restate.start.filtered` | handler | messages dropped in memory |
| `restate.start.accepted` | handler, workflow | starts answered 2xx |
| `restate.start.already_started` | handler, workflow | of those, `PreviouslyAccepted` |
| `restate.start.retried` | handler, workflow | per-item attempts after the first |
| `restate.start.blocked` | handler | batches that threw `RestateStartBlockedException` (not counting shutdown) |

Each start gets a `restate.start {workflow}` client span. Its parent is the message's producer span
and it links to the batch's `streams.process` span.
