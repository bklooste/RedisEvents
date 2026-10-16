using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using RedisEvents.Consumer;
using RedisEvents.Restate.Diagnostics;
using RedisEvents.Wire;

namespace RedisEvents.Restate;

/// <summary>
/// An <see cref="IBatchHandler{T}"/> that starts one Restate workflow per kept message, keyed by a
/// business id, and acks the batch only once every start has been accepted.
/// </summary>
/// <typeparam name="T">The deserialised message type of the topic.</typeparam>
/// <remarks>
/// <para>
/// <b>What a subclass supplies.</b> <see cref="Keep"/>, an in-memory filter with no I/O, and
/// <see cref="Map"/>, which turns a kept message into <c>(workflow, key, input)</c>. A batch with nothing
/// kept makes no HTTP call and returns, so the position advances past it.
/// </para>
/// <para>
/// <b>The submit.</b> <c>POST {ingress}/{workflow}/{key}/{handler}/send</c>, the input as the JSON body,
/// a W3C <c>traceparent</c>, and deliberately <b>no</b> <c>Idempotency-Key</c>: Restate 1.8 answers
/// <c>400</c> to that header on a workflow handler and never starts the run. The workflow key is the
/// dedupe; a second submit of one key answers <c>202</c> <c>"status":"PreviouslyAccepted"</c>. Kept
/// items go out bounded-parallel (<see cref="RestateStartOptions.MaxParallelism"/>).
/// </para>
/// <para>
/// <b>The ack rule.</b> RedisEvents acks a batch all-or-nothing: one handler call, then the position
/// moves to the batch's last id; a throw moves nothing. So per-item work lives here. Each item is
/// retried in place (<see cref="RestateStartOptions.MaxAttempts"/>, doubling delay); any 2xx is
/// accepted; a timeout, a transport failure, <c>408</c>, <c>425</c>, <c>429</c> and any 5xx are retried;
/// any other status is not retried. If any item is still not accepted, <see cref="HandleAsync"/> throws
/// <see cref="RestateStartBlockedException"/>, a <c>DontIgnoreException</c>: the partition blocks and
/// retries the whole batch until it goes through, and the items already accepted dedupe.
/// </para>
/// <para>
/// <b>The exception boundary.</b> <see cref="RestateStartBlockedException"/> is the <em>only</em>
/// exception that leaves <see cref="HandleAsync"/>. Under the default <c>ErrorPolicy.BestEffort</c> any
/// other exception is logged and the batch is skipped, which here would silently lose starts. That
/// includes cancellation: a timeout is an <see cref="OperationCanceledException"/> on a token that is
/// not the worker's, and core only treats an <see cref="OperationCanceledException"/> as shutdown when
/// the worker's own token is cancelled, so letting one out is a skip. On a cancelled handler token the
/// batch is therefore also reported as blocked (<see cref="RestateStartBlockedException.ShuttingDown"/>);
/// the worker's block loop sees its cancelled token at once and stops without advancing. If every start
/// was accepted before the token was cancelled, the call returns normally and the batch is acked.
/// </para>
/// <para>
/// <b>Concurrency.</b> One instance is a singleton shared by every partition of its consumer, so it
/// keeps no per-call state, and it takes no lock: exclusivity per key is Restate's, by workflow key.
/// </para>
/// <para>
/// <b>Outside this handler.</b> Core deserialises the batch before calling <see cref="HandleAsync"/>; a
/// body that fails to deserialise throws an ordinary exception there and is skipped under
/// <c>BestEffort</c>. Register a deserialiser that cannot throw, or set the consumer's error policy to
/// <c>StopPartition</c>, if a malformed message must not be dropped.
/// </para>
/// </remarks>
public abstract class RestateStartHandler<T> : IBatchHandler<T>
{
    private const string TraceParentHeader = "traceparent";

    /// <summary>At most this much of a rejected response's body goes into the failure reason.</summary>
    private const int MaxReasonBodyChars = 300;

    private static readonly MediaTypeHeaderValue Json = new("application/json");

    private readonly HttpClient http;
    private readonly JsonSerializerOptions inputJson;
    private readonly RestateStartOptions options;
    private readonly ILogger? log;
    private readonly string ingressBase;
    private readonly string handlerSegment;
    private readonly KeyValuePair<string, object?> handlerTag;

    /// <summary>Creates the handler.</summary>
    /// <param name="http">
    /// The client used for every submit. Long-lived (the handler is a singleton); give it a
    /// <c>SocketsHttpHandler</c> with a <c>PooledConnectionLifetime</c>, or take it from
    /// <c>IHttpClientFactory</c>. Its <see cref="HttpClient.BaseAddress"/> is the ingress when
    /// <see cref="RestateStartOptions.Ingress"/> is not set.
    /// </param>
    /// <param name="inputJson">
    /// Options whose resolver knows every input type <see cref="Map"/> returns, normally a
    /// source-generated context's <c>Default.Options</c>, so serialization stays AOT-safe. An input of
    /// a type it does not know fails that item, which blocks the batch.
    /// </param>
    /// <param name="options">Settings; <see langword="null"/> for the defaults.</param>
    /// <param name="logger">Optional logger; retries are logged at Debug.</param>
    protected RestateStartHandler(
        HttpClient http,
        JsonSerializerOptions inputJson,
        RestateStartOptions? options = null,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(http);
        ArgumentNullException.ThrowIfNull(inputJson);

        this.options = options ?? new RestateStartOptions();
        this.options.Validate();

        var ingress = this.options.Ingress ?? http.BaseAddress
            ?? throw new ArgumentException(
                "Restate: no ingress address. Set RestateStartOptions.Ingress or the HttpClient's BaseAddress.",
                nameof(options));

        if (!ingress.IsAbsoluteUri || (ingress.Scheme != Uri.UriSchemeHttp && ingress.Scheme != Uri.UriSchemeHttps))
        {
            throw new ArgumentException($"Restate: the ingress address '{ingress}' must be an absolute http or https URI.", nameof(options));
        }

        this.http = http;
        this.inputJson = inputJson;
        this.log = logger;
        this.ingressBase = ingress.AbsoluteUri.TrimEnd('/');
        this.handlerSegment = Uri.EscapeDataString(this.options.Handler);
        this.handlerTag = new KeyValuePair<string, object?>("handler", this.options.Name ?? this.GetType().Name);
    }

    /// <summary>
    /// Whether this message starts a workflow. Runs in memory for every message of the batch, so it
    /// must not do I/O. A throw fails that message and blocks the batch; it does not skip it.
    /// </summary>
    /// <param name="msg">The message.</param>
    /// <returns><see langword="true"/> to start a workflow for it.</returns>
    protected abstract bool Keep(in StreamMsg<T> msg);

    /// <summary>
    /// The workflow to start for a kept message: the Restate workflow service name, the workflow key
    /// (the business id; the only dedupe) and the input, serialized with the <c>inputJson</c> options
    /// given to the constructor. A <see langword="null"/> input sends no body. A throw, or an empty
    /// workflow or key, fails that message and blocks the batch.
    /// </summary>
    /// <param name="msg">A message <see cref="Keep"/> returned <see langword="true"/> for.</param>
    /// <returns>The workflow service, the workflow key and the input.</returns>
    protected abstract (string Workflow, string Key, object? Input) Map(StreamMsg<T> msg);

    /// <inheritdoc />
    /// <exception cref="RestateStartBlockedException">
    /// One or more kept messages were not started; the only exception this method throws.
    /// </exception>
    public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg<T>> batch, CancellationToken ct)
    {
        StartItem[]? items = null;

        try
        {
            items = this.Prepare(batch.Span);

            if (items.Length == 0)
            {
                return;
            }

            await this.SubmitAllAsync(items, ct).ConfigureAwait(false);

            var blocked = Blocked(items, ct, inner: null);
            if (blocked is null)
            {
                return;
            }

            if (!blocked.ShuttingDown)
            {
                RestateDiagnostics.Blocked.Add(1, this.handlerTag);
            }

            throw blocked;
        }
        catch (RestateStartBlockedException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Defence in depth: everything below already catches per item, so reaching here is a
            // bug or an exhausted runtime. Whatever it is, it must not reach core as an ordinary
            // exception, which BestEffort would skip.
            var blocked = Blocked(items, ct, ex) ?? new RestateStartBlockedException(
                ["(batch)"], items?.Length ?? 0, [$"{ex.GetType().Name}: {ex.Message}"], ct.IsCancellationRequested, ex);

            if (!blocked.ShuttingDown)
            {
                RestateDiagnostics.Blocked.Add(1, this.handlerTag);
            }

            throw blocked;
        }
    }

    /// <summary>
    /// Filters and maps the batch synchronously, before any await: the batch memory is only valid for
    /// the duration of the call, and nothing of it is held past here except copied strings and bytes.
    /// </summary>
    private StartItem[] Prepare(ReadOnlySpan<StreamMsg<T>> batch)
    {
        List<StartItem>? items = null;
        var filtered = 0;
        var kept = 0;

        for (var i = 0; i < batch.Length; i++)
        {
            ref readonly var msg = ref batch[i];
            var id = msg.Envelope.Id;

            bool keep;
            try
            {
                keep = this.Keep(in msg);
            }
            catch (Exception ex)
            {
                (items ??= []).Add(StartItem.FailedBeforeKey(id, $"Keep threw {ex.GetType().Name}: {ex.Message}", ex));
                continue;
            }

            if (!keep)
            {
                filtered++;
                continue;
            }

            kept++;
            (items ??= []).Add(this.MapOne(in msg));
        }

        if (filtered > 0)
        {
            RestateDiagnostics.Filtered.Add(filtered, this.handlerTag);
        }

        if (kept > 0)
        {
            RestateDiagnostics.Kept.Add(kept, this.handlerTag);
        }

        return items is null ? [] : [.. items];
    }

    private StartItem MapOne(in StreamMsg<T> msg)
    {
        var id = msg.Envelope.Id;

        string workflow;
        string key;
        object? input;
        try
        {
            (workflow, key, input) = this.Map(msg);
        }
        catch (Exception ex)
        {
            return StartItem.FailedBeforeKey(id, $"Map threw {ex.GetType().Name}: {ex.Message}", ex);
        }

        if (string.IsNullOrEmpty(workflow) || string.IsNullOrEmpty(key))
        {
            return StartItem.FailedBeforeKey(id, "Map returned an empty workflow or key", error: null);
        }

        byte[]? body = null;
        if (input is not null)
        {
            try
            {
                body = JsonSerializer.SerializeToUtf8Bytes(input, this.inputJson.GetTypeInfo(input.GetType()));
            }
            catch (Exception ex)
            {
                return StartItem.Failed(workflow, key, id, $"serializing the {input.GetType().Name} input threw {ex.GetType().Name}: {ex.Message}", ex);
            }
        }

        var uri = new Uri($"{this.ingressBase}/{Uri.EscapeDataString(workflow)}/{Uri.EscapeDataString(key)}/{this.handlerSegment}/send");

        return new StartItem(workflow, key, id, uri, body, msg.Envelope.TraceParent);
    }

    private async ValueTask SubmitAllAsync(StartItem[] items, CancellationToken ct)
    {
        var pending = 0;
        for (var i = 0; i < items.Length; i++)
        {
            if (items[i].Outcome == StartOutcome.Pending)
            {
                pending++;
            }
        }

        if (pending == 0)
        {
            return;
        }

        if (pending == 1 || this.options.MaxParallelism == 1)
        {
            for (var i = 0; i < items.Length; i++)
            {
                if (items[i].Outcome == StartOutcome.Pending)
                {
                    await this.SubmitAsync(items[i], ct).ConfigureAwait(false);
                }
            }

            return;
        }

        // No token on the loop itself: a cancelled loop throws and abandons items mid-flight with no
        // outcome. Each body checks the handler's token instead and records a failed outcome, so the
        // loop always runs to completion and every item ends Accepted or Failed.
        await Parallel.ForEachAsync(
            items,
            new ParallelOptions { MaxDegreeOfParallelism = this.options.MaxParallelism },
            (item, _) => item.Outcome == StartOutcome.Pending ? this.SubmitAsync(item, ct) : default)
            .ConfigureAwait(false);
    }

    /// <summary>Submits one item with in-place retry. Never throws; the outcome is recorded on the item.</summary>
    private async ValueTask SubmitAsync(StartItem item, CancellationToken ct)
    {
        using var activity = this.StartActivity(item);

        try
        {
            var delay = this.options.InitialRetryDelay;

            for (var attempt = 1; ; attempt++)
            {
                if (ct.IsCancellationRequested)
                {
                    item.Fail("cancelled before it was accepted", error: null);
                    return;
                }

                var result = await this.AttemptAsync(item, activity, ct).ConfigureAwait(false);

                if (result is AttemptResult.Accepted or AttemptResult.AlreadyStarted)
                {
                    var workflowTag = new KeyValuePair<string, object?>("workflow", item.Workflow);
                    RestateDiagnostics.Accepted.Add(1, this.handlerTag, workflowTag);
                    if (result == AttemptResult.AlreadyStarted)
                    {
                        RestateDiagnostics.AlreadyStarted.Add(1, this.handlerTag, workflowTag);
                    }

                    item.Accept(result == AttemptResult.AlreadyStarted);
                    activity?.SetTag("restate.attempts", attempt);
                    activity?.SetTag("restate.already_started", result == AttemptResult.AlreadyStarted);
                    return;
                }

                if (result == AttemptResult.Permanent || attempt >= this.options.MaxAttempts)
                {
                    item.Fail(item.LastReason ?? "not accepted", item.LastError);
                    activity?.SetTag("restate.attempts", attempt);
                    activity?.SetStatus(ActivityStatusCode.Error, item.LastReason);
                    return;
                }

                RestateDiagnostics.Retried.Add(1, this.handlerTag, new KeyValuePair<string, object?>("workflow", item.Workflow));

                if (this.log?.IsEnabled(LogLevel.Debug) == true)
                {
                    this.log.LogDebug(
                        "Restate: start {Workflow}/{Key} (stream id {StreamId}) attempt {Attempt} of {MaxAttempts} failed: {Reason}. Retrying in {DelayMs} ms.",
                        item.Workflow,
                        item.Key,
                        item.StreamId.Format(),
                        attempt,
                        this.options.MaxAttempts,
                        item.LastReason,
                        (long)delay.TotalMilliseconds);
                }

                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, ct).ConfigureAwait(false);
                }

                var doubled = delay * 2;
                delay = doubled > this.options.MaxRetryDelay ? this.options.MaxRetryDelay : doubled;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            item.Fail("cancelled before it was accepted", error: null);
            activity?.SetStatus(ActivityStatusCode.Error, "cancelled");
        }
        catch (Exception ex)
        {
            item.Fail($"{ex.GetType().Name}: {ex.Message}", ex);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
        }
    }

    /// <summary>
    /// One submit. Classifies every way it can end; only a cancellation of the handler's own token
    /// leaves as an exception, for <see cref="SubmitAsync"/> to record.
    /// </summary>
    private async ValueTask<AttemptResult> AttemptAsync(StartItem item, Activity? activity, CancellationToken ct)
    {
        using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        attemptCts.CancelAfter(this.options.AttemptTimeout);

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, item.Uri);

            if (item.Body is not null)
            {
                request.Content = new ByteArrayContent(item.Body);
                request.Content.Headers.ContentType = Json;
            }

            var traceParent = TraceParentFor(activity, item.TraceParent);
            if (traceParent is not null)
            {
                request.Headers.TryAddWithoutValidation(TraceParentHeader, traceParent);
            }

            using var response = await this.http
                .SendAsync(request, HttpCompletionOption.ResponseContentRead, attemptCts.Token)
                .ConfigureAwait(false);

            var status = (int)response.StatusCode;

            if (status is >= 200 and < 300)
            {
                // Any 2xx is accepted. The body only tells the already-started metric apart; a body
                // that cannot be read or parsed does not un-accept the start.
                return await IsPreviouslyAcceptedAsync(response, attemptCts.Token).ConfigureAwait(false)
                    ? AttemptResult.AlreadyStarted
                    : AttemptResult.Accepted;
            }

            var snippet = await ReadSnippetAsync(response, attemptCts.Token).ConfigureAwait(false);
            item.SetLast(snippet.Length == 0 ? $"HTTP {status}" : $"HTTP {status}: {snippet}", error: null);

            return IsTransient(status) ? AttemptResult.Transient : AttemptResult.Permanent;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException ex)
        {
            // Not the handler's token: the attempt timeout or HttpClient.Timeout. Retried.
            item.SetLast($"timed out ({ex.GetType().Name})", ex);
            return AttemptResult.Transient;
        }
        catch (HttpRequestException ex)
        {
            item.SetLast($"{ex.GetType().Name}: {ex.Message}", ex);
            return AttemptResult.Transient;
        }
        catch (Exception ex)
        {
            // Unknown: retried like a transport failure. If it is permanent it fails after the
            // attempts and blocks, which is visible; guessing "skip" would not be.
            item.SetLast($"{ex.GetType().Name}: {ex.Message}", ex);
            return AttemptResult.Transient;
        }
    }

    private Activity? StartActivity(StartItem item)
    {
        if (!RestateDiagnostics.Source.HasListeners())
        {
            return null;
        }

        // Parent on the message's own producer context, so the workflow joins the trace of the event
        // that caused it; link the batch's streams.process span. Without a usable traceparent on the
        // message the batch span (Activity.Current) is the parent.
        var current = Activity.Current;
        ActivityContext parent = default;
        ActivityLink[]? links = null;

        if (item.TraceParent is not null && ActivityContext.TryParse(item.TraceParent, traceState: null, out var producer))
        {
            parent = producer;
            if (current is not null)
            {
                links = [new ActivityLink(current.Context)];
            }
        }

        var activity = RestateDiagnostics.Source.StartActivity(
            $"{RestateDiagnostics.StartSpanName} {item.Workflow}",
            ActivityKind.Client,
            parent,
            tags: null,
            links: links);

        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag("restate.workflow", item.Workflow);
            activity.SetTag("restate.workflow_key", item.Key);
            activity.SetTag("restate.handler", this.options.Handler);
            activity.SetTag("messaging.message.id", item.StreamId.Format());
        }

        return activity;
    }

    private static string? TraceParentFor(Activity? activity, string? fromMessage)
    {
        if (activity is { IdFormat: ActivityIdFormat.W3C })
        {
            return activity.Id;
        }

        if (!string.IsNullOrEmpty(fromMessage))
        {
            return fromMessage;
        }

        return Activity.Current is { IdFormat: ActivityIdFormat.W3C } current ? current.Id : null;
    }

    private static bool IsTransient(int status)
        => status is 408 or 425 or 429 or >= 500;

    private static async ValueTask<bool> IsPreviouslyAcceptedAsync(HttpResponseMessage response, CancellationToken ct)
    {
        try
        {
            var bytes = await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);
            if (bytes.Length == 0)
            {
                return false;
            }

            var body = JsonSerializer.Deserialize(bytes, RestateJsonContext.Default.RestateSendResponse);

            return string.Equals(body?.Status, RestateSendResponse.PreviouslyAccepted, StringComparison.Ordinal);
        }
        catch (Exception)
        {
            return false;
        }
    }

    private static async ValueTask<string> ReadSnippetAsync(HttpResponseMessage response, CancellationToken ct)
    {
        try
        {
            var text = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            text = text.ReplaceLineEndings(" ").Trim();

            return text.Length <= MaxReasonBodyChars ? text : string.Concat(text.AsSpan(0, MaxReasonBodyChars), "…");
        }
        catch (Exception)
        {
            return string.Empty;
        }
    }

    /// <summary>
    /// The exception for a batch with any item not accepted, or <see langword="null"/> when every item
    /// was. An item still <see cref="StartOutcome.Pending"/> (never reached) counts as failed.
    /// </summary>
    private static RestateStartBlockedException? Blocked(StartItem[]? items, CancellationToken ct, Exception? inner)
    {
        if (items is null)
        {
            return null;
        }

        List<string>? keys = null;
        List<string>? reasons = null;
        Exception? first = inner;

        foreach (var item in items)
        {
            if (item.Outcome == StartOutcome.Accepted)
            {
                continue;
            }

            (keys ??= []).Add(item.Label);
            (reasons ??= []).Add(item.Outcome == StartOutcome.Pending ? "not attempted" : item.LastReason ?? "not accepted");
            first ??= item.LastError;
        }

        return keys is null
            ? null
            : new RestateStartBlockedException(keys, items.Length, reasons!, ct.IsCancellationRequested, first);
    }

    private enum AttemptResult
    {
        Accepted = 0,
        AlreadyStarted = 1,
        Transient = 2,
        Permanent = 3,
    }

    private enum StartOutcome
    {
        Pending = 0,
        Accepted = 1,
        Failed = 2,
    }

    /// <summary>
    /// One kept message's start, copied out of the batch. Each is touched by exactly one submit body,
    /// so it needs no synchronisation; the parallel loop's completion publishes the outcomes.
    /// </summary>
    private sealed class StartItem
    {
        internal StartItem(string workflow, string key, StreamId streamId, Uri? uri, byte[]? body, string? traceParent)
        {
            this.Workflow = workflow;
            this.Key = key;
            this.StreamId = streamId;
            this.Uri = uri;
            this.Body = body;
            this.TraceParent = traceParent;
            this.Label = $"{workflow}/{key}";
        }

        internal string Workflow { get; }

        internal string Key { get; }

        internal StreamId StreamId { get; }

        internal Uri? Uri { get; }

        internal byte[]? Body { get; }

        internal string? TraceParent { get; }

        internal string Label { get; private init; }

        internal StartOutcome Outcome { get; private set; }

        internal bool AlreadyStarted { get; private set; }

        internal string? LastReason { get; private set; }

        internal Exception? LastError { get; private set; }

        internal static StartItem FailedBeforeKey(StreamId id, string reason, Exception? error)
        {
            var item = new StartItem(string.Empty, string.Empty, id, uri: null, body: null, traceParent: null)
            {
                Label = $"stream id {id.Format()}",
            };
            item.Fail(reason, error);
            return item;
        }

        internal static StartItem Failed(string workflow, string key, StreamId id, string reason, Exception? error)
        {
            var item = new StartItem(workflow, key, id, uri: null, body: null, traceParent: null);
            item.Fail(reason, error);
            return item;
        }

        internal void SetLast(string reason, Exception? error)
        {
            this.LastReason = reason;
            this.LastError = error;
        }

        internal void Fail(string reason, Exception? error)
        {
            this.Outcome = StartOutcome.Failed;
            this.LastReason = reason;
            this.LastError = error ?? this.LastError;
        }

        internal void Accept(bool alreadyStarted)
        {
            this.Outcome = StartOutcome.Accepted;
            this.AlreadyStarted = alreadyStarted;
        }
    }
}
