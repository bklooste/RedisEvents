using System.Diagnostics;
using System.Globalization;
using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Diagnostics;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;

namespace Orange.Lib.Streams.Tests;

/// <summary>
/// S21e — trace and correlation context survives the reader-to-processor hop, against real Redis.
/// </summary>
/// <remarks>
/// <para>
/// <b>What makes this subtle.</b> <see cref="Activity.Current"/> flows across <c>await</c> through
/// <c>ExecutionContext</c>, and that does not help here: the reader and the processor are two
/// <em>long-lived</em> loops, so the processor captured its execution context exactly once — when its
/// task started — not once per message. Per-message context therefore has to travel in the entry:
/// the <c>p</c> field carries the producer's W3C <c>traceparent</c> and <c>c</c> the correlation id
/// (<c>03-consumer-pipeline.md</c>, "Carrying trace and correlation context across the hop").
/// </para>
/// <para>
/// <b>What stops these tests being vacuous.</b> Every publish here happens under its own
/// <em>root</em> <see cref="Activity"/>, and the consumer host is started under a <em>different</em>
/// root activity of its own. So there are two ways the consumer could get its context, and they give
/// different answers: from the ambient execution context (one trace, the host's) or from the entry
/// (several traces, the producers'). The assertions name the producers' trace ids explicitly and
/// name the host's as forbidden — an implementation that leaned on ambient flow, or that stopped
/// stamping <c>p</c>, fails every one of them rather than passing by coincidence.
/// </para>
/// <para>
/// <b>Link versus parent.</b> A multi-message batch <em>links</em> to its producers' contexts, because
/// one batch legitimately carries many of them and electing one as the parent would misattribute the
/// rest; a single-message batch <em>parents</em> off the producer so the trace stays continuous
/// (<c>06-errors-and-observability.md</c>, "Spans"). Both halves are asserted, including the negative
/// half of each: a batch span must <em>not</em> adopt a producer's trace as its own, and a
/// single-message span must carry no links.
/// </para>
/// <para>
/// <b>Both backpressure modes.</b> Every test is a theory over <c>Backpressure.Enabled</c>. The hop
/// exists only when it is on; the plan requires identical traces either way, and the shared
/// <c>ProcessBatchAsync</c> is what makes that true, so the tests would catch either mode drifting.
/// </para>
/// <para>
/// Nothing here creates activities without a listener attached: the library uses
/// <see cref="ActivitySource.StartActivity(string, ActivityKind)"/> throughout, which returns
/// <see langword="null"/> when nobody listens, so a test without an <see cref="ActivityListener"/>
/// sampling <see cref="ActivitySamplingResult.AllDataAndRecorded"/> would assert on nothing at all.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class TraceContextTests(RedisStreamsFixture fixture)
{
    /// <summary>The type string every message in this file carries. Nothing here filters on it.</summary>
    private const string MessageType = "trace";

    /// <summary>One key, one partition: delivery order is publish order, so span N pairs with publish N.</summary>
    private const string Key = "trace-key";

    // ---------------------------------------------------------------------------------------
    // S21e — batch spans link to the producers
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// S21e. A multi-message batch span carries a link to every producer trace in the batch, carries
    /// none to the trace that was ambient when the consumer host started, and does not adopt any
    /// producer's trace as its own.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Three root traces publish 120 messages round-robin before the host starts, so the first read
    /// (<c>BatchSize</c> 100) is a single batch holding all three contexts — the exact case the plan
    /// says must be linked rather than parented.
    /// </para>
    /// <para>
    /// The assertions that can fail: dropping the <c>p</c> field on publish leaves the batch with no
    /// links; reading the context from <see cref="Activity.Current"/> instead of from the entry
    /// leaves it with no links too, and would put the host's trace where the producers' should be;
    /// parenting a batch off <c>batch[0]</c> makes the span's own trace id a producer trace id, which
    /// is asserted against; and losing the <see cref="StreamActivity.MaxTraceLinks"/> cap turns a
    /// 100-entry batch into 100 links, which is also asserted against.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("TestType", "ServiceTest")]
    public async Task S21e_a_batch_span_links_to_every_producer_trace_and_adopts_none_of_them(bool backpressure)
    {
        const int producers = 3;
        const int total = 120;
        const int batchSize = 100;

        var topic = fixture.NewTopic($"trace-links-{(backpressure ? "bp" : "inline")}");
        var consumerName = fixture.NewConsumer("trace-links");

        using var rig = new TraceRig(topic);

        var root = this.Root(topic);
        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, Single());

        // Three independent root traces. Each publish runs under one of them, so the entries carry
        // three different traceparents and the batch below has three contexts to reconcile.
        var roots = new Activity[producers];
        for (var i = 0; i < producers; i++)
        {
            roots[i] = rig.StartRoot($"producer-{i.ToString(CultureInfo.InvariantCulture)}");
        }

        var producerTraces = roots.Select(static a => a.TraceId).ToArray();
        producerTraces.Should().OnlyHaveUniqueItems("each publisher must be its own root trace for this test to mean anything");

        var traceParents = new string[total];

        for (var i = 0; i < total; i++)
        {
            var owner = roots[i % producers];
            Activity.Current = owner;
            traceParents[i] = owner.Id!;

            _ = await publisher.PublishAsync(Key, Body(i), MessageType);
        }

        Activity.Current = null;
        foreach (var producer in roots)
        {
            producer.Dispose();
        }

        var handler = new TestHandler();
        var host = new StreamConsumerHost(
            root, Options(topic, batchSize, backpressure), consumerName, handler.HandleAsync, connection);

        ActivityTraceId ambientTrace;

        try
        {
            // The host starts under a fourth, unrelated root trace. Its execution context is what the
            // reader and processor loops capture — and it must not be what the spans are attributed to.
            var ambient = rig.StartRoot("consumer-ambient");
            ambientTrace = ambient.TraceId;

            await host.StartAsync(CancellationToken.None);

            ambient.Dispose();
            Activity.Current = null;

            await handler.WaitForExactlyAsync(total, TimeSpan.FromMilliseconds(400));

            await RedisStreamsFixture.WaitUntilAsync(
                () => rig.MessagesSpanned() >= total,
                TimeSpan.FromSeconds(30),
                $"consumer spans covering all {total} messages to be stopped");
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        ambientTrace.Should().NotBe(default(ActivityTraceId));
        producerTraces.Should().NotContain(ambientTrace, "the host's trace must be unrelated to the publishers'");

        var spans = rig.ProcessSpans();
        spans.Should().NotBeEmpty("the library must start a consumer span per batch while a listener is attached");
        rig.MessagesSpanned().Should().Be(total, "every delivered message is covered by exactly one consumer span");

        foreach (var span in spans)
        {
            span.Kind.Should().Be(ActivityKind.Consumer);
            span.GetTagItem(StreamSpans.DestinationKey).Should().Be(topic);
            span.GetTagItem(StreamSpans.ConsumerKey).Should().Be(consumerName);
            span.GetTagItem(StreamSpans.PartitionKey).Should().Be(0);
        }

        var batchSpans = spans.Where(static s => Count(s) > 1).ToArray();
        batchSpans.Should().NotBeEmpty(
            "120 messages published before the consumer started must be read as batches of up to {0}",
            batchSize);

        var linked = new List<ActivityTraceId>();

        foreach (var span in batchSpans)
        {
            var links = span.Links.ToArray();

            links.Should().NotBeEmpty(
                "a batch span must link to the producers' contexts, which are recoverable only from the entries' 'p' field");
            links.Length.Should().BeLessThanOrEqualTo(
                StreamActivity.MaxTraceLinks,
                "a large batch must not turn into a large export");

            foreach (var link in links)
            {
                producerTraces.Should().Contain(
                    link.Context.TraceId,
                    "every link must name a trace the producer stamped on an entry");
                link.Context.TraceId.Should().NotBe(
                    ambientTrace,
                    "the consumer's ambient execution context is not per-message context and must never be linked");
                link.Context.SpanId.Should().NotBe(default(ActivitySpanId));

                linked.Add(link.Context.TraceId);
            }

            producerTraces.Should().NotContain(
                span.TraceId,
                "a batch carries many trace contexts, so it links to them and must not adopt one of them as its parent");
        }

        linked.Distinct().Should().BeEquivalentTo(
            producerTraces,
            "a batch spanning three producer traces must link to all three, not just to the first entry's");

        // And the wire itself: every entry carried the traceparent of the activity that published it.
        var delivered = handler.Messages;
        delivered.Should().HaveCount(total);

        for (var i = 0; i < total; i++)
        {
            delivered[i].TraceParent.Should().Be(
                traceParents[i],
                "the 'p' field is where per-message trace context lives; message {0} was published under {1}",
                i,
                traceParents[i]);
        }
    }

    // ---------------------------------------------------------------------------------------
    // S21e — single-message spans parent off the producer
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// S21e. With <c>BatchSize</c> 1 — the <c>IMessageHandler</c> shape — each consumer span is a
    /// child of the activity that published the message: same trace id, parent span id equal to the
    /// producer's span id, and no links.
    /// </summary>
    /// <remarks>
    /// Four messages, four distinct root traces, so the four consumer spans must land in four
    /// distinct traces. That is the assertion ambient flow cannot satisfy: a consumer that took its
    /// context from its own execution context would put all four spans in the single trace that was
    /// current when the host started, and the distinctness check fails immediately.
    /// </remarks>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("TestType", "ServiceTest")]
    public async Task S21e_a_single_message_span_parents_off_the_producer_that_published_it(bool backpressure)
    {
        const int total = 4;

        var topic = fixture.NewTopic($"trace-parent-{(backpressure ? "bp" : "inline")}");
        var consumerName = fixture.NewConsumer("trace-parent");

        using var rig = new TraceRig(topic);

        var root = this.Root(topic);
        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, Single());

        var producerTraces = new ActivityTraceId[total];
        var producerSpans = new ActivitySpanId[total];

        for (var i = 0; i < total; i++)
        {
            using var producer = rig.StartRoot($"producer-{i.ToString(CultureInfo.InvariantCulture)}");
            producerTraces[i] = producer.TraceId;
            producerSpans[i] = producer.SpanId;

            _ = await publisher.PublishAsync(Key, Body(i), MessageType);
        }

        Activity.Current = null;
        producerTraces.Should().OnlyHaveUniqueItems("four root publishes are four separate traces");

        var handler = new TestHandler();
        var host = new StreamConsumerHost(
            root, Options(topic, batchSize: 1, backpressure), consumerName, handler.HandleAsync, connection);

        ActivityTraceId ambientTrace;

        try
        {
            var ambient = rig.StartRoot("consumer-ambient");
            ambientTrace = ambient.TraceId;

            await host.StartAsync(CancellationToken.None);

            ambient.Dispose();
            Activity.Current = null;

            await handler.WaitForExactlyAsync(total, TimeSpan.FromMilliseconds(400));

            await RedisStreamsFixture.WaitUntilAsync(
                () => rig.ProcessSpans().Length >= total,
                TimeSpan.FromSeconds(30),
                $"all {total} consumer spans to be stopped");
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        var spans = rig.ProcessSpans();
        spans.Should().HaveCount(total, "BatchSize 1 gives one span per message");

        spans.Select(static s => s.TraceId).Should().OnlyHaveUniqueItems(
            "each message came from its own trace; a consumer reading context from its own ExecutionContext " +
            "instead of from the entry would put all of them in one trace");

        for (var i = 0; i < total; i++)
        {
            var span = spans[i];

            Count(span).Should().Be(1);
            span.Links.Should().BeEmpty("a single-message batch parents off the producer rather than linking to it");
            span.TraceId.Should().Be(
                producerTraces[i],
                "one partition key means publish order is delivery order, so span {0} belongs to publish {0}",
                i);
            span.ParentSpanId.Should().Be(
                producerSpans[i],
                "the span the producer had open is the parent, recovered from the entry's 'p' field");
            span.TraceId.Should().NotBe(ambientTrace, "the host's ambient trace must not appear anywhere in the consumer spans");
        }
    }

    // ---------------------------------------------------------------------------------------
    // S21e — the correlation id reaches the handler and the log scope
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// S21e. The correlation id published on the <c>c</c> field reaches the handler on the message and
    /// is pushed as a structured log scope around the handler call — and a message published without
    /// one pushes no scope at all.
    /// </summary>
    /// <remarks>
    /// <para>
    /// No <see cref="ActivityListener"/> is attached here on purpose: correlation must survive the hop
    /// whether or not anything is tracing, and with no listener the library's spans are never created
    /// at all. So this test exercises the correlation path alone.
    /// </para>
    /// <para>
    /// The negative half matters as much as the positive one. Two of the eight messages carry no
    /// correlation id, and the assertion is that exactly six scopes are pushed — a regression that
    /// pushed an empty scope for every batch would show up as eight.
    /// </para>
    /// </remarks>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("TestType", "ServiceTest")]
    public async Task S21e_the_correlation_id_reaches_the_handler_and_the_log_scope(bool backpressure)
    {
        const int correlated = 6;
        const int bare = 2;
        const int total = correlated + bare;

        var topic = fixture.NewTopic($"trace-corr-{(backpressure ? "bp" : "inline")}");
        var consumerName = fixture.NewConsumer("trace-corr");

        var root = this.Root(topic);
        await using var connection = new StreamsConnectionProvider(root, services: null, logger: null);
        var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, Single());

        var expected = Enumerable
            .Range(0, correlated)
            .Select(static i => $"corr-{i.ToString(CultureInfo.InvariantCulture)}")
            .ToArray();

        for (var i = 0; i < correlated; i++)
        {
            _ = await publisher.PublishAsync(Key, Body(i), MessageType, new PublishOptions(CorrelationId: expected[i]));
        }

        for (var i = 0; i < bare; i++)
        {
            _ = await publisher.PublishAsync(Key, Body(correlated + i), MessageType);
        }

        var handler = new TestHandler();
        var log = new ScopeRecordingLogger();

        // BatchSize 1: the correlation scope is per batch, so one message per batch is what makes a
        // per-message assertion legitimate rather than a coincidence of batching.
        var host = new StreamConsumerHost(
            root, Options(topic, batchSize: 1, backpressure), consumerName, handler.HandleAsync, connection, log);

        try
        {
            await host.StartAsync(CancellationToken.None);
            await handler.WaitForExactlyAsync(total, TimeSpan.FromMilliseconds(400));
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }

        var delivered = handler.Messages;
        delivered.Should().HaveCount(total);

        for (var i = 0; i < correlated; i++)
        {
            delivered[i].CorrelationId.Should().Be(
                expected[i],
                "the 'c' field carries the correlation id across the hop, per message");
        }

        for (var i = correlated; i < total; i++)
        {
            delivered[i].CorrelationId.Should().BeEmpty("a publish with no correlation id must decode to an empty one");
        }

        log.Scopes.Should().Equal(
            expected,
            "the processor pushes each batch's correlation id as a structured scope, in delivery order, " +
            "and pushes nothing for a batch that has none");
    }

    // ---------------------------------------------------------------------------------------
    // Rig
    // ---------------------------------------------------------------------------------------

    /// <summary>One partition and no trimming: publish order is delivery order and nothing is dropped.</summary>
    private static TopicOptions Single() => new()
    {
        Partitions = 1,
        Trim = TrimMode.None,
        MaxLen = long.MaxValue,
        BackgroundTrimIntervalSeconds = 0,
    };

    private static ConsumerOptions Options(string topic, int batchSize, bool backpressure) => new()
    {
        Topic = topic,
        BatchSize = batchSize,
        BlockMs = 200,
        Persist = PersistMode.AsyncBatch,
        PersistIntervalMs = 100,
        StartFrom = StartFrom.Beginning,
        Backpressure = new BackpressureOptions { Enabled = backpressure, Capacity = 4 },
    };

    /// <summary>The message count a consumer span covers, from its own batch-size attribute.</summary>
    private static int Count(Activity span) => (int)(span.GetTagItem(StreamSpans.BatchCountKey) ?? 0);

    private static ReadOnlyMemory<byte> Body(int index)
        => Encoding.UTF8.GetBytes(index.ToString(CultureInfo.InvariantCulture));

    private StreamOptions Root(string topic) => new()
    {
        ConnectionString = fixture.ConnectionString,
        Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = Single() },
    };

    /// <summary>
    /// An <see cref="ActivitySource"/> for the test's own producer and ambient activities, plus a
    /// listener that samples everything and collects this topic's consumer spans.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The listener is not optional. The library only ever creates activities through
    /// <see cref="ActivitySource.StartActivity(string, ActivityKind)"/>, which returns
    /// <see langword="null"/> when nobody is listening — that is the "tracing costs nothing when
    /// disabled" property — so without one, both the producer activities and the consumer spans would
    /// silently be <see langword="null"/> and every assertion below would be vacuous.
    /// <see cref="StartRoot"/> therefore throws rather than returning null.
    /// </para>
    /// <para>
    /// Only spans named for <em>this test's</em> topic are collected, so a concurrently running class
    /// cannot leak spans into these assertions.
    /// </para>
    /// </remarks>
    private sealed class TraceRig : IDisposable
    {
        private readonly Lock gate = new();
        private readonly List<Activity> spans = [];
        private readonly ActivityListener listener;
        private readonly string spanName;

        internal TraceRig(string topic)
        {
            this.spanName = $"{StreamActivity.ProcessSpanName} {topic}";
            this.Source = new ActivitySource($"Orange.Lib.Streams.Tests.Trace.{topic}", "1.0.0");

            var source = this.Source;

            this.listener = new ActivityListener
            {
                ShouldListenTo = s => ReferenceEquals(s, source) || s.Name == StreamsDiagnostics.SourceName,
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                SampleUsingParentId = static (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = this.Collect,
            };

            ActivitySource.AddActivityListener(this.listener);
        }

        /// <summary>The source the test's own producer and ambient activities come from.</summary>
        internal ActivitySource Source { get; }

        /// <summary>
        /// Starts a fresh <em>root</em> activity — <see cref="Activity.Current"/> is cleared first, so
        /// it cannot end up a child of the previous one and share its trace id.
        /// </summary>
        internal Activity StartRoot(string name)
        {
            Activity.Current = null;

            var activity = this.Source.StartActivity(name, ActivityKind.Producer)
                ?? throw new InvalidOperationException(
                    "No activity was created; the listener is not sampling, and every assertion in this test would be vacuous.");

            return activity.IdFormat == ActivityIdFormat.W3C
                ? activity
                : throw new InvalidOperationException(
                    $"Activity '{name}' is {activity.IdFormat}, not W3C, so the publisher would stamp no traceparent.");
        }

        /// <summary>This topic's stopped consumer spans, in the order they stopped.</summary>
        internal Activity[] ProcessSpans()
        {
            lock (this.gate)
            {
                return this.spans.ToArray();
            }
        }

        /// <summary>How many messages the collected consumer spans cover in total.</summary>
        internal int MessagesSpanned()
        {
            lock (this.gate)
            {
                var total = 0;
                foreach (var span in this.spans)
                {
                    total += Count(span);
                }

                return total;
            }
        }

        public void Dispose()
        {
            this.listener.Dispose();
            this.Source.Dispose();
        }

        private void Collect(Activity activity)
        {
            if (!string.Equals(activity.OperationName, this.spanName, StringComparison.Ordinal))
            {
                return;
            }

            lock (this.gate)
            {
                this.spans.Add(activity);
            }
        }
    }

    /// <summary>
    /// An <see cref="ILogger"/> that records the correlation id of every scope pushed around a batch.
    /// </summary>
    /// <remarks>
    /// <see cref="IsEnabled"/> is <see langword="false"/> so the host's own logging costs nothing, but
    /// scopes are pushed unconditionally, which is exactly the behaviour under test. A scope whose
    /// state is not the structured shape the logging providers expect is recorded as
    /// <see cref="Unstructured"/> rather than ignored, so a regression to a formatted string would
    /// fail the assertion instead of quietly passing it.
    /// </remarks>
    private sealed class ScopeRecordingLogger : ILogger
    {
        internal const string Unstructured = "<unstructured>";

        private readonly Lock gate = new();
        private readonly List<string> scopes = [];

        /// <summary>The correlation ids pushed as scopes, in order.</summary>
        internal IReadOnlyList<string> Scopes
        {
            get
            {
                lock (this.gate)
                {
                    return this.scopes.ToArray();
                }
            }
        }

        public IDisposable BeginScope<TState>(TState state)
            where TState : notnull
        {
            var recorded = Unstructured;

            if (state is IReadOnlyList<KeyValuePair<string, object?>> fields)
            {
                foreach (var field in fields)
                {
                    if (string.Equals(field.Key, "CorrelationId", StringComparison.Ordinal))
                    {
                        recorded = field.Value?.ToString() ?? string.Empty;
                    }
                }
            }

            lock (this.gate)
            {
                this.scopes.Add(recorded);
            }

            return NoopScope.Instance;
        }

        public bool IsEnabled(LogLevel logLevel) => false;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            // Nothing: this logger exists to observe scopes, not messages.
        }

        private sealed class NoopScope : IDisposable
        {
            internal static readonly NoopScope Instance = new();

            public void Dispose()
            {
                // Nothing to release.
            }
        }
    }
}
