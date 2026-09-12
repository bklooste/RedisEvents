using System.Diagnostics;
using System.Text;

using FluentAssertions;

using RedisEvents.Config;
using RedisEvents.Diagnostics;
using RedisEvents.Producer;

using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// R-16 (the producer half). <see cref="StreamSpans.StartPublish"/>,
/// <c>StreamSpans.Published</c> and <c>StreamSpans.Failed</c> were fully written and had
/// <b>zero call sites</b>: the producer only copied <see cref="Activity.Current"/> into the entry's
/// <c>p</c> field, so no <c>streams.publish</c> span ever reached a collector. A trace showed the
/// consumer's <c>streams.process</c> span and nothing at all on the way in, which is exactly the
/// half an operator needs when the question is "did the service publish it, or not?".
/// </summary>
/// <remarks>
/// <para>
/// <b>The listener is load-bearing.</b> <see cref="ActivitySource.StartActivity(string, ActivityKind)"/>
/// returns <see langword="null"/> when nothing is listening — that is what makes tracing free when it
/// is off — so without an <see cref="ActivityListener"/> sampling
/// <see cref="ActivitySamplingResult.AllDataAndRecorded"/> every assertion in this file would pass
/// vacuously against a publisher that creates no spans whatsoever. That is the exact failure being
/// closed here, so <see cref="SpanRig.StartRoot"/> throws rather than returning null.
/// </para>
/// <para>
/// No Redis: the publisher runs over the same <see cref="DispatchProxy"/>-backed
/// <c>RecordingDatabase</c> the rest of the producer unit tests use.
/// </para>
/// </remarks>
public class ProducerSpanTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Publish_emits_a_streams_publish_span_in_the_callers_trace()
    {
        var topic = NewTopic();
        using var rig = new SpanRig(topic);

        var db = new ProducerTests.RecordingDatabase();
        var publisher = new StreamPublisher(db.Database, topic, new TopicOptions { Partitions = 4 });

        string traceId;
        string rootSpanId;

        using (var root = rig.StartRoot("caller"))
        {
            traceId = root.TraceId.ToHexString();
            rootSpanId = root.SpanId.ToHexString();

            _ = await publisher.PublishAsync("customer-42", Body("hello"), "OrderPlaced");
        }

        var span = rig.Spans().Should().ContainSingle("one publish is one span").Subject;

        span.OperationName.Should().Be($"streams.publish {topic}");
        span.Kind.Should().Be(ActivityKind.Producer);
        span.TraceId.ToHexString().Should().Be(traceId, "the publish span belongs to the caller's trace");
        span.ParentSpanId.ToHexString().Should().Be(rootSpanId, "it hangs off the work that asked for the publish");

        Tag(span, "messaging.system").Should().Be("redis_streams");
        Tag(span, "messaging.destination").Should().Be(topic);
        Tag(span, "messaging.partition").Should().NotBeNull();
        Tag(span, "messaging.message.type").Should().Be("OrderPlaced");
        Tag(span, "messaging.message_id").Should().Be("1700000000000-7", "the id XADD returned is stamped after the write");
        Tag(span, "messaging.batch.message_count").Should().BeNull("a single publish is not a batch");
    }

    /// <summary>
    /// Adding the publish span must not move the context on the wire: the entry's
    /// <c>traceparent</c> still names the <em>caller</em>, exactly as it did before.
    /// </summary>
    /// <remarks>
    /// The consumer's link-versus-parent rule (<c>06-errors-and-observability.md</c>, "Spans") is
    /// written against the producer's own context, and the service-level <c>TraceContextTests</c>
    /// assert message by message which trace each entry was published under. Stamping the publish
    /// span instead would have been a nicer topology and a silent wire-contract change, so the
    /// traceparent is captured before the span starts. The span is a child of the same caller, so
    /// the hop is still one trace — the consumer span is a sibling of the publish, not its child.
    /// </remarks>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_entrys_traceparent_still_names_the_caller_not_the_publish_span()
    {
        var topic = NewTopic();
        using var rig = new SpanRig(topic);

        var db = new ProducerTests.RecordingDatabase();
        var publisher = new StreamPublisher(db.Database, topic);

        string rootSpanId;

        using (var root = rig.StartRoot("caller"))
        {
            rootSpanId = root.SpanId.ToHexString();
            _ = await publisher.PublishAsync("k", Body("hello"), "T");
        }

        var span = rig.Spans().Should().ContainSingle().Subject;
        var traceparent = db.LastFields[Wire.EntryCodec.TraceParentField];

        traceparent.Should().Contain(span.TraceId.ToHexString(), "the whole hop stays in one trace");
        traceparent.Should().Contain(rootSpanId, "the entry carries the caller's context, unchanged by the new span");
        traceparent.Should().NotContain(
            span.SpanId.ToHexString(),
            "adding a span must not silently change what the producer puts on the wire");
    }

    /// <summary>One span for a pipelined batch, carrying the message count rather than N spans.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_batch_publish_emits_one_span_carrying_its_message_count()
    {
        var topic = NewTopic();
        using var rig = new SpanRig(topic);

        var db = new ProducerTests.RecordingDatabase();
        var publisher = new StreamPublisher(db.Database, topic);

        ReadOnlyMemory<byte>[] bodies = [Body("a"), Body("b"), Body("c")];

        using (var root = rig.StartRoot("caller"))
        {
            await publisher.PublishBatchAsync("k", bodies, "T");
        }

        var span = rig.Spans().Should().ContainSingle("a pipelined batch is one round trip and one span").Subject;

        Tag(span, "messaging.batch.message_count").Should().Be("3");
        Tag(span, "messaging.message.type").Should().Be("T");
    }

    /// <summary>
    /// A failed publish leaves an Error span. The status is what a tail sampler keys off, so it must
    /// be set whether or not the span was sampled for attributes.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_failed_publish_marks_its_span_as_an_error()
    {
        var topic = NewTopic();
        using var rig = new SpanRig(topic);

        var db = new ProducerTests.RecordingDatabase
        {
            FailWith = new RedisConnectionException(ConnectionFailureType.SocketFailure, "gone"),
        };

        var publisher = new StreamPublisher(db.Database, topic);

        using (var root = rig.StartRoot("caller"))
        {
            var act = async () => await publisher.PublishAsync("k", Body("x"), "T");
            _ = await act.Should().ThrowAsync<Errors.StreamTransportException>();
        }

        var span = rig.Spans().Should().ContainSingle().Subject;
        span.Status.Should().Be(ActivityStatusCode.Error);
    }

    /// <summary>
    /// The publish span must not escape into the caller's ambient context.
    /// </summary>
    /// <remarks>
    /// <see cref="Activity.Current"/> is an <c>AsyncLocal</c>, and a span started in a
    /// non-<c>async</c> method mutates the <em>caller's</em> execution context with no way to restore
    /// it after the await — every subsequent operation on that thread would then be a child of a
    /// long-dead publish. That is why the traced path is its own <c>async</c> method; this asserts
    /// the property that requirement exists to protect.
    /// </remarks>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Publishing_does_not_leave_its_span_current()
    {
        var topic = NewTopic();
        using var rig = new SpanRig(topic);

        var db = new ProducerTests.RecordingDatabase();
        var publisher = new StreamPublisher(db.Database, topic);

        using var root = rig.StartRoot("caller");

        _ = await publisher.PublishAsync("k", Body("x"), "T");

        Activity.Current.Should().BeSameAs(root, "the publish span must have been restored on the way out");
    }

    /// <summary>With nothing listening no span exists at all — the "tracing is free when off" property.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task With_no_listener_no_span_is_created()
    {
        var topic = NewTopic();

        var db = new ProducerTests.RecordingDatabase();
        var publisher = new StreamPublisher(db.Database, topic);

        var before = Activity.Current;
        _ = await publisher.PublishAsync("k", Body("x"), "T");

        Activity.Current.Should().BeSameAs(before);
        db.LastFields.ContainsKey(Wire.EntryCodec.TraceParentField).Should().BeFalse(
            "with no ambient activity there is no traceparent to stamp");
    }

    private static string NewTopic() => $"span-{Guid.NewGuid():N}";

    private static ReadOnlyMemory<byte> Body(string text) => Encoding.UTF8.GetBytes(text);

    private static string? Tag(Activity span, string name) => span.GetTagItem(name)?.ToString();

    /// <summary>
    /// Collects the <c>streams.publish</c> spans for one topic. Scoped by span name so a
    /// concurrently running test class cannot leak spans into these assertions.
    /// </summary>
    private sealed class SpanRig : IDisposable
    {
        private readonly Lock gate = new();
        private readonly List<Activity> collected = [];
        private readonly ActivityListener listener;
        private readonly ActivitySource source;
        private readonly string spanName;

        internal SpanRig(string topic)
        {
            this.spanName = $"streams.publish {topic}";
            this.source = new ActivitySource($"RedisEvents.UnitTests.Publish.{topic}", "1.0.0");

            var mine = this.source;

            this.listener = new ActivityListener
            {
                ShouldListenTo = s => ReferenceEquals(s, mine) || s.Name == StreamsDiagnostics.SourceName,

                // AllData, not PropagationData: StartActivity returns null for anything less, and
                // IsAllDataRequested — which every attribute set is guarded by — would be false.
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                SampleUsingParentId = static (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = this.Collect,
            };

            ActivitySource.AddActivityListener(this.listener);
        }

        internal Activity StartRoot(string name)
        {
            Activity.Current = null;

            var activity = this.source.StartActivity(name, ActivityKind.Producer)
                ?? throw new InvalidOperationException(
                    "No activity was created, so the listener is not sampling and every assertion here would be vacuous.");

            return activity.IdFormat == ActivityIdFormat.W3C
                ? activity
                : throw new InvalidOperationException($"Activity '{name}' is {activity.IdFormat}, not W3C.");
        }

        internal Activity[] Spans()
        {
            lock (this.gate)
            {
                return [.. this.collected];
            }
        }

        public void Dispose()
        {
            this.listener.Dispose();
            this.source.Dispose();
        }

        private void Collect(Activity activity)
        {
            if (!string.Equals(activity.OperationName, this.spanName, StringComparison.Ordinal))
            {
                return;
            }

            lock (this.gate)
            {
                this.collected.Add(activity);
            }
        }
    }
}
