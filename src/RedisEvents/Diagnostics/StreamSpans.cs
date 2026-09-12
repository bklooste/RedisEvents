using System.Diagnostics;
using RedisEvents.Wire;

namespace RedisEvents.Diagnostics;

/// <summary>
/// The library's two spans — <c>streams.publish {topic}</c> on the way in and
/// <c>streams.process {topic}</c> on the way out — and the attribute names they share.
/// </summary>
/// <remarks>
/// <para>
/// <b>Cost when tracing is off.</b> Everything here starts at
/// <see cref="ActivitySource.StartActivity(string, ActivityKind)"/>, which returns
/// <see langword="null"/> when no listener is registered: no span object, no tags, no allocation, one
/// predictable branch. Every attribute set is then guarded by
/// <see cref="Activity.IsAllDataRequested"/>, so a span that is created but sampled out costs nothing
/// beyond its own construction. A bare <c>new Activity(...)</c> is never constructed anywhere in this
/// library — it has no <see cref="ActivitySource"/> behind it and therefore exports no span at all,
/// which is exactly why the Kafka implementation being replaced produces no publish traces despite
/// appearing to instrument the path.
/// </para>
/// <para>
/// <b>Producer versus consumer.</b> The publish side lives here; the consumer side lives in
/// <see cref="StreamActivity"/>, which additionally has to rebuild the trace context out of the entry
/// (a long-lived reader loop has no useful ambient <see cref="Activity.Current"/>) and decide between
/// a parent and links for a batch. <see cref="StartProcess"/> forwards to it rather than repeating
/// any of that.
/// </para>
/// <para>
/// <b>Message id.</b> Redis assigns the entry id inside <c>XADD</c>, so
/// <c>messaging.message_id</c> cannot be known when the span starts. Call
/// <see cref="Published(Activity?, StreamId)"/> once the write returns.
/// </para>
/// </remarks>
internal static class StreamSpans
{
    /// <summary>Span name prefix for the producer span; the topic is appended.</summary>
    internal const string PublishSpanName = "streams.publish";

    /// <summary>Span name prefix for the consumer span; the topic is appended.</summary>
    internal const string ProcessSpanName = StreamActivity.ProcessSpanName;

    /// <summary>Value of <see cref="MessagingSystemKey"/> — the OpenTelemetry messaging system id.</summary>
    internal const string MessagingSystem = "redis_streams";

    /// <summary>Attribute: the messaging system, always <see cref="MessagingSystem"/>.</summary>
    internal const string MessagingSystemKey = "messaging.system";

    /// <summary>Attribute: the topic.</summary>
    internal const string DestinationKey = "messaging.destination";

    /// <summary>Attribute: the partition index within the topic.</summary>
    internal const string PartitionKey = "messaging.partition";

    /// <summary>Attribute: the message type (the entry's <c>t</c> field).</summary>
    internal const string MessageTypeKey = "messaging.message.type";

    /// <summary>Attribute: the Redis stream id, known only after the write or read.</summary>
    internal const string MessageIdKey = "messaging.message_id";

    /// <summary>Attribute: how many messages the span covers, on batch spans.</summary>
    internal const string BatchCountKey = "messaging.batch.message_count";

    /// <summary>Attribute: the consumer name.</summary>
    internal const string ConsumerKey = "streams.consumer";

    /// <summary>Attribute: how far behind the entry was when it was processed, in milliseconds.</summary>
    internal const string LagMsKey = "streams.lag_ms";

    /// <summary>
    /// Starts the producer span for one <c>XADD</c>, or one pipelined batch of them.
    /// </summary>
    /// <param name="topic">Topic being written to; also the second half of the span name.</param>
    /// <param name="partition">Partition the router chose.</param>
    /// <param name="messageType">The message type, or <see langword="null"/> for a mixed batch.</param>
    /// <param name="count">
    /// Messages covered by this span. Anything above 1 also emits
    /// <see cref="BatchCountKey"/>; a single publish leaves the attribute off entirely rather than
    /// tagging every span with <c>1</c>.
    /// </param>
    /// <returns>
    /// The started span, or <see langword="null"/> when nothing is listening — in which case the
    /// caller has spent one branch and allocated nothing.
    /// </returns>
    /// <remarks>
    /// The parent is whatever is ambient: unlike the consumer loops, a publish runs on the caller's
    /// own execution context, so <see cref="Activity.Current"/> genuinely is the originating work.
    /// That same context is what <see cref="StreamActivity.CurrentTraceParent"/> stamps into the
    /// entry's <c>p</c> field, which is how the consumer end of the hop finds its way back here.
    /// </remarks>
    internal static Activity? StartPublish(string topic, int partition, string? messageType, int count = 1)
    {
        var activity = StreamsDiagnostics.Source.StartActivity(
            $"{PublishSpanName} {topic}",
            ActivityKind.Producer);

        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag(MessagingSystemKey, MessagingSystem);
            activity.SetTag(DestinationKey, topic);
            activity.SetTag(PartitionKey, partition);

            if (!string.IsNullOrEmpty(messageType))
            {
                activity.SetTag(MessageTypeKey, messageType);
            }

            if (count > 1)
            {
                activity.SetTag(BatchCountKey, count);
            }
        }

        return activity;
    }

    /// <summary>
    /// Stamps the id Redis assigned onto a publish span. A no-op when the span is
    /// <see langword="null"/> or sampled out.
    /// </summary>
    /// <param name="activity">The span from <see cref="StartPublish"/>.</param>
    /// <param name="id">The id <c>XADD</c> returned.</param>
    internal static void Published(Activity? activity, StreamId id)
    {
        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag(MessageIdKey, id.Format());
        }
    }

    /// <summary>
    /// Stamps the id Redis assigned onto a publish span, from the raw value the client returned.
    /// </summary>
    /// <param name="activity">The span from <see cref="StartPublish"/>.</param>
    /// <param name="id">The id text, or <see langword="null"/> when the write returned none.</param>
    internal static void Published(Activity? activity, string? id)
    {
        if (activity is { IsAllDataRequested: true } && !string.IsNullOrEmpty(id))
        {
            activity.SetTag(MessageIdKey, id);
        }
    }

    /// <summary>
    /// Marks a span as failed. Safe on a <see langword="null"/> span, so a caller can wrap a publish
    /// in one <c>catch</c> without testing anything first.
    /// </summary>
    /// <param name="activity">The span, if there is one.</param>
    /// <param name="ex">The failure.</param>
    /// <remarks>
    /// The status is set even when the span is sampled out — status is not an attribute and is what a
    /// tail sampler keys off, so it must survive <see cref="Activity.IsAllDataRequested"/> being
    /// <see langword="false"/>.
    /// </remarks>
    internal static void Failed(Activity? activity, Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);

        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
    }

    /// <summary>
    /// Starts the consumer span for a batch. Forwards to <see cref="StreamActivity.StartProcess"/>,
    /// which owns the traceparent parsing and the parent-versus-links decision.
    /// </summary>
    /// <param name="ctx">Topic, partition and consumer.</param>
    /// <param name="batch">The batch, already sliced to its real length.</param>
    /// <param name="last">The last stream id in the batch.</param>
    /// <returns>The started span, or <see langword="null"/> when nothing is listening.</returns>
    internal static Activity? StartProcess(in StreamSpanContext ctx, ReadOnlySpan<StreamMsg> batch, StreamId last)
        => StreamActivity.StartProcess(in ctx, batch, last);
}
