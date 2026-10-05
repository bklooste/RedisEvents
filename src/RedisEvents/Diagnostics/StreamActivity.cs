using System.Diagnostics;
using Microsoft.Extensions.Logging;
using RedisEvents.Wire;

namespace RedisEvents.Diagnostics;

/// <summary>
/// The three fields every streams span and metric is tagged with, passed as a struct so the
/// diagnostics helpers stay static and allocation-free (design principle 1 in the overview).
/// </summary>
/// <param name="Topic">The topic the batch was read from.</param>
/// <param name="Partition">The partition index the worker owns.</param>
/// <param name="Consumer">The consumer name.</param>
internal readonly record struct StreamSpanContext(string Topic, int Partition, string Consumer);

/// <summary>
/// Rebuilds trace and correlation context on the consumer side of the hop, from the entry rather
/// than from the thread.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists at all.</b> <see cref="Activity.Current"/> flows across <c>await</c> through
/// <c>ExecutionContext</c>, and that does not help here — relying on it would be a bug. The reader
/// and the processor are two <em>long-lived</em> loops: the processor captured its
/// <c>ExecutionContext</c> exactly once, when its task started, not once per message. Whatever
/// <see cref="Activity"/> was current on the reader thread for message #4000 is simply not ambiently
/// available when the processor gets to it.
/// </para>
/// <para>
/// So per-message context travels <b>in the entry</b>: the <c>p</c> field carries the producer's W3C
/// <c>traceparent</c> and the <c>c</c> field the correlation id. This class parses the first back
/// into an <see cref="ActivityContext"/> and pushes the second as a log scope, which is the only
/// mechanism that survives a decoupled reader and processor — and, usefully, produces identical
/// traces in inline (no-backpressure) mode where the hop never happened.
/// </para>
/// <para>
/// <b>Link versus parent.</b> A batch <em>parents</em> off the producer whenever it can do so without
/// lying, and <em>links</em> only when it cannot. A single-message batch — <c>IMessageHandler</c> with
/// batch size 1 — parents off its one producer. A multi-message batch whose entries all carry the
/// <em>same</em> trace id also parents off it: that is the shape a request-driven flow has, and
/// forking a new root there would split one logical operation into a trace per hop while recording
/// the relationship only as a backward link. Only a batch that genuinely mixes traces — the shape a
/// high-volume feed has — links to its producers and adopts none of them, because electing one
/// parent out of many would misattribute every other message in the batch.
/// <see cref="SharesOneTrace"/> is the predicate that separates the two, and it scans the whole
/// batch so a late disagreement cannot be missed.
/// </para>
/// <para>
/// <b>Cost when tracing is off.</b> Every entry point starts from
/// <see cref="ActivitySource.HasListeners"/> and <see cref="ActivitySource.StartActivity(string, ActivityKind, ActivityContext, IEnumerable{KeyValuePair{string, object?}}?, IEnumerable{ActivityLink}?, DateTimeOffset)"/>,
/// which returns <see langword="null"/> when nobody is listening — no parsing, no links, no tags,
/// no allocation. Attribute sets are guarded with <see cref="Activity.IsAllDataRequested"/> so a
/// sampled-out span costs nothing either. A bare <c>new Activity(...)</c> is never constructed: it
/// has no <see cref="ActivitySource"/> behind it and therefore exports no span at all, which is the
/// defect in the Kafka implementation this library replaces.
/// </para>
/// </remarks>
internal static class StreamActivity
{
    /// <summary>Span name prefix for the consumer span; the topic is appended.</summary>
    internal const string ProcessSpanName = "streams.process";

    /// <summary>
    /// The cap on trace links built for one batch. A 1,000-entry batch does not need 1,000 links —
    /// a handful is enough to reach the producers from the consumer span, and the cap keeps a large
    /// batch from turning into a large export.
    /// </summary>
    internal const int MaxTraceLinks = 16;

    /// <summary>
    /// Starts the consumer span for a batch, rebuilding the trace context from the entries'
    /// <c>traceparent</c>.
    /// </summary>
    /// <param name="ctx">Topic, partition and consumer, used for the span name and tags.</param>
    /// <param name="batch">The batch, already sliced to its real length.</param>
    /// <param name="last">The last stream id in the batch, tagged as the message id.</param>
    /// <returns>
    /// The started <see cref="Activity"/>, or <see langword="null"/> when nothing is listening — in
    /// which case not a single byte was parsed or allocated getting here.
    /// </returns>
    internal static Activity? StartProcess(in StreamSpanContext ctx, ReadOnlySpan<StreamMsg> batch, StreamId last)
    {
        if (!StreamsDiagnostics.Source.HasListeners())
        {
            return null;
        }

        var parent = default(ActivityContext);
        List<ActivityLink>? links = null;

        if (batch.Length == 1)
        {
            // Batch size 1: parent, so the trace runs straight through the hop.
            if (TryParseTraceParent(batch[0].TraceParent, out var single))
            {
                parent = single;
            }
        }
        else if (SharesOneTrace(batch, out var firstIndex)
            && TryParseTraceParent(batch[firstIndex].TraceParent, out var shared))
        {
            // One trace, many entries: parent off it, so a request-driven flow stays one trace
            // across the hop instead of forking a root per batch. Links still name the other
            // producer spans within that trace, so no causality is lost by picking a parent.
            parent = shared;

            var limit = batch.Length < MaxTraceLinks ? batch.Length : MaxTraceLinks;

            for (var i = 0; i < limit; i++)
            {
                if (i != firstIndex
                    && TryParseTraceParent(batch[i].TraceParent, out var sibling)
                    && sibling.SpanId != shared.SpanId)
                {
                    links ??= new List<ActivityLink>(limit);
                    links.Add(new ActivityLink(sibling));
                }
            }
        }
        else
        {
            // Many contexts in one batch: link them, never pick one as the parent.
            var limit = batch.Length < MaxTraceLinks ? batch.Length : MaxTraceLinks;

            for (var i = 0; i < limit; i++)
            {
                if (TryParseTraceParent(batch[i].TraceParent, out var linked))
                {
                    links ??= new List<ActivityLink>(limit);
                    links.Add(new ActivityLink(linked));
                }
            }
        }

        var activity = StreamsDiagnostics.Source.StartActivity(
            $"{ProcessSpanName} {ctx.Topic}",
            ActivityKind.Consumer,
            parent,
            tags: null,
            links: links);

        if (activity is { IsAllDataRequested: true })
        {
            // Attribute names come from StreamSpans so the producer and consumer ends of the hop
            // are tagged identically and a dashboard can group across both.
            activity.SetTag(StreamSpans.MessagingSystemKey, StreamSpans.MessagingSystem);
            activity.SetTag(StreamSpans.DestinationKey, ctx.Topic);
            activity.SetTag(StreamSpans.PartitionKey, ctx.Partition);
            activity.SetTag(StreamSpans.BatchCountKey, batch.Length);
            activity.SetTag(StreamSpans.ConsumerKey, ctx.Consumer);

            if (batch.Length > 0)
            {
                activity.SetTag(StreamSpans.MessageTypeKey, batch[0].Type);
                activity.SetTag(StreamSpans.MessageIdKey, last.Format());
                activity.SetTag(StreamSpans.LagMsKey, LagMs(batch[batch.Length - 1].EnqueuedTime));
            }
        }

        return activity;
    }

    /// <summary>
    /// Pushes the batch's correlation id as a log scope, so every handler log line carries it
    /// without the handler doing anything.
    /// </summary>
    /// <param name="log">The worker's logger.</param>
    /// <param name="batch">The batch; the first entry's <c>c</c> field is the batch's correlation id.</param>
    /// <returns>The scope to dispose after the handler returns, or <see langword="null"/> when there is nothing to push.</returns>
    /// <remarks>
    /// One scope per batch rather than per message: a batch is one unit of handler work, and a
    /// per-message scope would mean a push and a pop on the hot path for a value most messages
    /// share. Where per-message correlation matters, run with batch size 1.
    /// </remarks>
    internal static IDisposable? BeginCorrelationScope(ILogger log, ReadOnlySpan<StreamMsg> batch)
    {
        if (batch.Length == 0)
        {
            return null;
        }

        var correlationId = batch[0].CorrelationId;

        return string.IsNullOrEmpty(correlationId)
            ? null
            : log.BeginScope(new CorrelationScope(correlationId));
    }

    /// <summary>
    /// Offset of the 32-hex trace id within a W3C <c>traceparent</c> — <c>00-&lt;trace&gt;-&lt;span&gt;-&lt;flags&gt;</c>.
    /// </summary>
    private const int TraceIdOffset = 3;

    /// <summary>Length of the hex trace id within a <c>traceparent</c>.</summary>
    private const int TraceIdLength = 32;

    /// <summary>
    /// Whether every entry in the batch that carries a <c>traceparent</c> carries the <em>same</em>
    /// trace id, which is what makes it safe to parent the consumer span off one of them.
    /// </summary>
    /// <param name="batch">The batch, already sliced to its real length.</param>
    /// <param name="firstIndex">Index of the first entry carrying a usable <c>traceparent</c>.</param>
    /// <returns>
    /// <see langword="true"/> when at least one entry carries a trace id and no entry carries a
    /// different one. <see langword="false"/> when the batch mixes traces, or carries none at all —
    /// both of which leave the caller on the links path.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This compares the trace id as a <em>character span</em> rather than parsing each entry into an
    /// <see cref="ActivityContext"/>. A batch is read with <c>BatchSize</c> up to the hundreds, and
    /// the comparison is the hot path's only per-entry cost here; the single successful parse happens
    /// once, in the caller, on <paramref name="firstIndex"/>.
    /// </para>
    /// <para>
    /// The whole batch is scanned, not just the first <see cref="MaxTraceLinks"/> entries. Stopping at
    /// the cap would let a batch whose first entries agree but whose later ones do not be parented off
    /// a trace that does not own it — the one case this predicate exists to exclude.
    /// </para>
    /// </remarks>
    private static bool SharesOneTrace(ReadOnlySpan<StreamMsg> batch, out int firstIndex)
    {
        firstIndex = -1;

        for (var i = 0; i < batch.Length; i++)
        {
            var traceParent = batch[i].TraceParent;

            if (string.IsNullOrEmpty(traceParent))
            {
                // No opinion: an entry the producer stamped nothing on cannot contradict the others.
                continue;
            }

            if (traceParent.Length < TraceIdOffset + TraceIdLength)
            {
                // Malformed. Treat it as a disagreement rather than ignoring it: the entry may well
                // belong to another trace, and guessing in favour of a parent is the unsafe guess.
                return false;
            }

            if (firstIndex < 0)
            {
                firstIndex = i;
                continue;
            }

            var candidate = traceParent.AsSpan(TraceIdOffset, TraceIdLength);
            var incumbent = batch[firstIndex].TraceParent!.AsSpan(TraceIdOffset, TraceIdLength);

            if (!candidate.Equals(incumbent, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return firstIndex >= 0;
    }

    /// <summary>
    /// Parses a W3C <c>traceparent</c> out of an entry's <c>p</c> field.
    /// </summary>
    /// <param name="traceParent">The header value, or <see langword="null"/> when the producer stamped none.</param>
    /// <param name="context">The rebuilt context, or <see langword="default"/>.</param>
    /// <returns><see langword="true"/> when a usable context was recovered.</returns>
    /// <remarks>
    /// A malformed or absent value is never an error: the message is still processed, it simply
    /// starts a new trace. Trace context is diagnostics, and diagnostics must not drop data.
    /// </remarks>
    private static bool TryParseTraceParent(string? traceParent, out ActivityContext context)
    {
        if (string.IsNullOrEmpty(traceParent))
        {
            context = default;
            return false;
        }

        return ActivityContext.TryParse(traceParent, traceState: null, out context);
    }

    /// <summary>
    /// The current W3C <c>traceparent</c> to stamp on an outgoing entry's <c>p</c> field, or
    /// <see langword="null"/> when there is no ambient W3C activity to propagate.
    /// </summary>
    /// <remarks>
    /// The producer side of the same hop. On the publish path <see cref="Activity.Current"/> <em>is</em>
    /// the right source — the publish happens on the caller's own execution context, unlike the
    /// consumer's long-lived loops.
    /// </remarks>
    internal static string? CurrentTraceParent()
    {
        var current = Activity.Current;

        return current is { IdFormat: ActivityIdFormat.W3C } ? current.Id : null;
    }

    /// <summary>
    /// Milliseconds between an entry being accepted by Redis and now, floored at zero.
    /// </summary>
    /// <remarks>
    /// <see cref="StreamMsg.EnqueuedTime"/> comes from the stream id, so this is measured against the
    /// broker's clock at one end and ours at the other; a small negative reading is clock skew, not
    /// a negative lag, hence the floor.
    /// </remarks>
    internal static double LagMs(DateTimeOffset enqueued)
    {
        var lag = (DateTimeOffset.UtcNow - enqueued).TotalMilliseconds;

        return lag < 0 ? 0 : lag;
    }

    /// <summary>
    /// A one-entry structured log scope. A struct implementing the read-only list shape the logging
    /// providers look for, so the scope carries a named <c>CorrelationId</c> field rather than a
    /// formatted string, and costs one small boxing allocation per batch instead of a dictionary.
    /// </summary>
    private readonly struct CorrelationScope : IReadOnlyList<KeyValuePair<string, object?>>
    {
        private readonly string correlationId;

        internal CorrelationScope(string correlationId) => this.correlationId = correlationId;

        public int Count => 1;

        public KeyValuePair<string, object?> this[int index] => index == 0
            ? new KeyValuePair<string, object?>("CorrelationId", this.correlationId)
            : throw new ArgumentOutOfRangeException(nameof(index));

        public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
        {
            yield return new KeyValuePair<string, object?>("CorrelationId", this.correlationId);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => this.GetEnumerator();

        public override string ToString() => $"CorrelationId:{this.correlationId}";
    }
}
