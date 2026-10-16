using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

using RedisEvents.Restate;
using RedisEvents.Wire;

namespace RedisEvents.Restate.UnitTests;

public sealed record Bet(string Id, string? Claim);

public sealed record StartInput(string BetId);

public sealed record UnknownInput(int Value);

[JsonSerializable(typeof(StartInput))]
internal sealed partial class TestJson : JsonSerializerContext;

/// <summary>The handler under test: keeps bets with a claim, starts <c>BonusPayout/{claim}</c>.</summary>
internal sealed class PayoutStarter : RestateStartHandler<Bet>
{
    private readonly Func<Bet, bool>? keep;
    private readonly Func<Bet, (string, string, object?)>? map;

    public PayoutStarter(
        HttpClient http,
        RestateStartOptions? options = null,
        Func<Bet, bool>? keep = null,
        Func<Bet, (string, string, object?)>? map = null)
        : base(http, TestJson.Default.Options, options)
    {
        this.keep = keep;
        this.map = map;
    }

    protected override bool Keep(in StreamMsg<Bet> msg)
        => this.keep is null ? msg.Value?.Claim is not null : this.keep(msg.Value!);

    protected override (string Workflow, string Key, object? Input) Map(StreamMsg<Bet> msg)
        => this.map is null
            ? ("BonusPayout", msg.Value!.Claim!, new StartInput(msg.Value.Id))
            : this.map(msg.Value!);
}

/// <summary>One request the fake ingress saw.</summary>
internal sealed record Seen(string Path, string WorkflowKey, bool HasIdempotencyKey, string? TraceParent, string? ContentType, string Body);

/// <summary>
/// A fake Restate ingress. By default it behaves like Restate 1.8 for workflow <c>/send</c>: the first
/// submit of a <c>{workflow}/{key}</c> path answers <c>202 Accepted</c>, every later one
/// <c>202 PreviouslyAccepted</c> with the same invocation id, and any request carrying
/// <c>Idempotency-Key</c> is answered <c>400</c> (and flagged, so a test can fail on it).
/// <see cref="Respond"/> can override the answer per request; returning <see langword="null"/> falls back
/// to the deduping behaviour.
/// </summary>
internal sealed class FakeIngress : HttpMessageHandler
{
    private readonly ConcurrentDictionary<string, string> started = new(StringComparer.Ordinal);
    private int inFlight;
    private int maxInFlight;
    private int idempotencyKeySeen;

    public ConcurrentQueue<Seen> Requests { get; } = new();

    public ConcurrentQueue<(string WorkflowKey, string Status)> Answers { get; } = new();

    public Func<Seen, CancellationToken, Task<HttpResponseMessage?>>? Respond { get; set; }

    public TimeSpan Latency { get; set; }

    public int MaxInFlight => Volatile.Read(ref this.maxInFlight);

    public bool IdempotencyKeySeen => Volatile.Read(ref this.idempotencyKeySeen) != 0;

    public HttpClient Client(string ingress = "http://restate.test:8080") => new(this, disposeHandler: false)
    {
        BaseAddress = new Uri(ingress),
    };

    public IReadOnlyList<(string WorkflowKey, string Status)> AnswersSnapshot() => [.. this.Answers];

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var now = Interlocked.Increment(ref this.inFlight);
        try
        {
            int seenMax;
            while (now > (seenMax = Volatile.Read(ref this.maxInFlight))
                && Interlocked.CompareExchange(ref this.maxInFlight, now, seenMax) != seenMax)
            {
            }

            var path = request.RequestUri!.AbsolutePath;
            var body = request.Content is null ? string.Empty : await request.Content.ReadAsStringAsync(cancellationToken);
            var seen = new Seen(
                path,
                WorkflowKeyOf(path),
                request.Headers.Contains("Idempotency-Key"),
                request.Headers.TryGetValues("traceparent", out var tp) ? tp.Single() : null,
                request.Content?.Headers.ContentType?.MediaType,
                body);
            this.Requests.Enqueue(seen);

            if (this.Latency > TimeSpan.Zero)
            {
                await Task.Delay(this.Latency, cancellationToken);
            }

            if (seen.HasIdempotencyKey)
            {
                Interlocked.Exchange(ref this.idempotencyKeySeen, 1);
                return Json(
                    HttpStatusCode.BadRequest,
                    "{\"code\":400,\"message\":\"cannot use the idempotency key with workflow handlers. The handler invocation will already be idempotent by the workflow key itself.\",\"source\":\"ingress\"}");
            }

            if (this.Respond is not null)
            {
                var custom = await this.Respond(seen, cancellationToken);
                if (custom is not null)
                {
                    return custom;
                }
            }

            return this.Dedupe(seen);
        }
        finally
        {
            Interlocked.Decrement(ref this.inFlight);
        }
    }

    private HttpResponseMessage Dedupe(Seen seen)
    {
        var candidate = "inv_" + Guid.NewGuid().ToString("N");
        var first = this.started.GetOrAdd(seen.WorkflowKey, candidate);
        var status = ReferenceEquals(first, candidate) ? "Accepted" : "PreviouslyAccepted";
        this.Answers.Enqueue((seen.WorkflowKey, status));

        return Json(HttpStatusCode.Accepted, $"{{\"invocationId\":\"{first}\",\"status\":\"{status}\"}}");
    }

    internal static HttpResponseMessage Json(HttpStatusCode status, string body)
        => new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    /// <summary><c>/{workflow}/{key}/run/send</c> → <c>{workflow}/{key}</c>, still escaped.</summary>
    private static string WorkflowKeyOf(string path)
    {
        var parts = path.Trim('/').Split('/');
        return parts.Length >= 2 ? $"{parts[0]}/{parts[1]}" : path;
    }
}

/// <summary>Collects this package's counters for one handler name, so parallel tests do not see each other.</summary>
internal sealed class RestateMetrics : IDisposable
{
    private readonly MeterListener listener = new();
    private readonly ConcurrentDictionary<string, long> totals = new();
    private readonly string handler;

    public RestateMetrics(string handler)
    {
        this.handler = handler;
        this.listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RestateOpenTelemetryExtensions.TelemetrySourceName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        this.listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "handler" && (string?)tag.Value == this.handler)
                {
                    this.totals.AddOrUpdate(instrument.Name, value, (_, current) => current + value);
                    return;
                }
            }
        });
        this.listener.Start();
    }

    public long this[string name] => this.totals.TryGetValue(name, out var v) ? v : 0;

    public void Dispose() => this.listener.Dispose();
}

internal static class Batches
{
    /// <summary><paramref name="size"/> bets, the ones <paramref name="claimed"/> picks carrying claim <c>c{i}</c>.</summary>
    public static StreamMsg<Bet>[] Of(int size, Func<int, bool> claimed, Func<int, string?>? traceParent = null)
    {
        var batch = new StreamMsg<Bet>[size];
        for (var i = 0; i < size; i++)
        {
            var bet = new Bet($"bet{i}", claimed(i) ? $"c{i}" : null);
            var envelope = new StreamMsg(
                Body: JsonSerializer.SerializeToUtf8Bytes(bet.Id),
                Type: "BetSettled",
                Id: new StreamId(1_000 + i, 0),
                Partition: 0,
                PartitionKey: bet.Id,
                CorrelationId: string.Empty,
                TraceParent: traceParent?.Invoke(i),
                Headers: default);
            batch[i] = new StreamMsg<Bet>(bet, envelope);
        }

        return batch;
    }

    /// <summary>A batch of <paramref name="size"/> in which every <paramref name="every"/>-th message is kept.</summary>
    public static StreamMsg<Bet>[] EveryNth(int size, int every) => Of(size, i => i % every == 0);
}

internal static class Fast
{
    /// <summary>Default options with millisecond retry delays, named for metric isolation.</summary>
    public static RestateStartOptions Options(string name, int maxAttempts = 5, TimeSpan? attemptTimeout = null) => new()
    {
        Name = name,
        MaxAttempts = maxAttempts,
        InitialRetryDelay = TimeSpan.FromMilliseconds(1),
        MaxRetryDelay = TimeSpan.FromMilliseconds(4),
        AttemptTimeout = attemptTimeout ?? TimeSpan.FromSeconds(10),
    };
}
