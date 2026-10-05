using System.Diagnostics;

using FluentAssertions;

using RedisEvents.Diagnostics;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// The parent-versus-links decision in <c>StreamActivity.StartProcess</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>What was wrong.</b> Any batch of more than one entry took the links path unconditionally, which
/// leaves <c>parent</c> as <see langword="default"/> — and a consumer span with no parent is a
/// <em>root</em>. A request-driven flow whose events all belong to one trace therefore came out as a
/// separate trace per hop, joined only by a one-way link, with no end-to-end duration and nothing in
/// the upstream trace pointing forward. Because the branch tests the <em>actual</em> read length and
/// not the configured <c>BatchSize</c>, it also split intermittently: the same service parented
/// correctly when idle and forked a root the moment two entries arrived together.
/// </para>
/// <para>
/// A batch that genuinely mixes traces must still link and adopt no parent — electing one of many
/// parents would misattribute every other message in the batch. That case is covered end-to-end by
/// <c>TraceContextTests.S21e_a_batch_span_links_to_every_producer_trace_and_adopts_none_of_them</c>;
/// the cases here are the ones that need no Redis.
/// </para>
/// <para>
/// <b>The listener is load-bearing.</b> <c>StartProcess</c> returns <see langword="null"/> when
/// nothing is listening, so without a sampling <see cref="ActivityListener"/> every assertion below
/// would pass vacuously.
/// </para>
/// </remarks>
public class ConsumerSpanParentingTests
{
    private const string Topic = "bet";
    private const string Consumer = "bet_outcome";

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_batch_sharing_one_trace_parents_off_it_so_the_trace_survives_the_hop()
    {
        using var rig = new ProcessSpanRig();
        var producer = rig.NewTrace();

        var batch = new[]
        {
            Entry(0, producer.TraceParent()),
            Entry(1, producer.TraceParent()),
            Entry(2, producer.TraceParent()),
        };

        using var span = rig.StartProcess(batch);

        span.Should().NotBeNull("the listener is sampling, so a consumer span must be created");
        span!.TraceId.Should().Be(
            producer.TraceId,
            "every entry came from one trace, so the hop must continue it rather than starting a root");
        span.ParentSpanId.Should().Be(
            producer.SpanId,
            "the producer's span is the parent, which is what makes the waterfall continuous");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_batch_mixing_traces_still_adopts_no_parent()
    {
        using var rig = new ProcessSpanRig();
        var first = rig.NewTrace();
        var second = rig.NewTrace();

        first.TraceId.Should().NotBe(second.TraceId, "this test is vacuous unless the traces really differ");

        var batch = new[]
        {
            Entry(0, first.TraceParent()),
            Entry(1, second.TraceParent()),
        };

        using var span = rig.StartProcess(batch);

        span.Should().NotBeNull();
        span!.ParentSpanId.Should().Be(
            default(ActivitySpanId),
            "one batch carrying two traces has no single parent, so it must be a root that links to both");
        span.TraceId.Should().NotBe(first.TraceId);
        span.TraceId.Should().NotBe(second.TraceId);
        span.Links.Select(static l => l.Context.TraceId)
            .Should().BeEquivalentTo([first.TraceId, second.TraceId], "both producers must stay reachable");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_shared_trace_still_links_the_other_producer_spans_within_it()
    {
        using var rig = new ProcessSpanRig();
        var parent = rig.NewTrace();

        // Two spans, one trace: the shape of a flow that fans out inside a single request.
        using var sibling = rig.StartChild(parent);

        sibling.TraceId.Should().Be(parent.TraceId, "a child shares its parent's trace");
        sibling.SpanId.Should().NotBe(parent.SpanId);

        var batch = new[]
        {
            Entry(0, parent.TraceParent()),
            Entry(1, sibling.Id!),
        };

        using var span = rig.StartProcess(batch);

        span.Should().NotBeNull();
        span!.ParentSpanId.Should().Be(parent.SpanId, "the first usable context is the parent");
        span.Links.Select(static l => l.Context.SpanId)
            .Should().BeEquivalentTo(
                [sibling.SpanId],
                "picking a parent must not lose the other producer spans that contributed to the batch");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_late_disagreement_is_not_missed_by_scanning_only_the_link_cap()
    {
        using var rig = new ProcessSpanRig();
        var bulk = rig.NewTrace();
        var odd = rig.NewTrace();

        // The first MaxTraceLinks entries agree; the disagreement sits past the cap. Scanning only as
        // far as the cap would parent this batch off a trace that does not own all of it.
        var batch = new StreamMsg[StreamActivity.MaxTraceLinks + 4];

        for (var i = 0; i < batch.Length; i++)
        {
            batch[i] = Entry(i, bulk.TraceParent());
        }

        batch[^1] = Entry(batch.Length - 1, odd.TraceParent());

        using var span = rig.StartProcess(batch);

        span.Should().NotBeNull();
        span!.ParentSpanId.Should().Be(
            default(ActivitySpanId),
            "the batch spans two traces, so it must stay a root however far into the batch that shows up");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Entries_without_a_traceparent_do_not_block_parenting()
    {
        using var rig = new ProcessSpanRig();
        var producer = rig.NewTrace();

        var batch = new[]
        {
            Entry(0, traceParent: null),
            Entry(1, producer.TraceParent()),
            Entry(2, string.Empty),
        };

        using var span = rig.StartProcess(batch);

        span.Should().NotBeNull();
        span!.TraceId.Should().Be(
            producer.TraceId,
            "an entry the producer stamped nothing on cannot contradict the ones it did");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_batch_with_no_trace_context_at_all_is_a_root_with_no_links()
    {
        using var rig = new ProcessSpanRig();

        var batch = new[] { Entry(0, traceParent: null), Entry(1, traceParent: null) };

        using var span = rig.StartProcess(batch);

        span.Should().NotBeNull("missing trace context is never an error — the message is still processed");
        span!.ParentSpanId.Should().Be(default(ActivitySpanId));
        span.Links.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_malformed_traceparent_is_treated_as_a_disagreement_not_ignored()
    {
        using var rig = new ProcessSpanRig();
        var producer = rig.NewTrace();

        var batch = new[]
        {
            Entry(0, producer.TraceParent()),
            Entry(1, "00-deadbeef"),
        };

        using var span = rig.StartProcess(batch);

        span.Should().NotBeNull();
        span!.ParentSpanId.Should().Be(
            default(ActivitySpanId),
            "a truncated value may belong to another trace, and guessing in favour of a parent is the unsafe guess");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_single_message_batch_parents_and_carries_no_links()
    {
        using var rig = new ProcessSpanRig();
        var producer = rig.NewTrace();

        using var span = rig.StartProcess([Entry(0, producer.TraceParent())]);

        span.Should().NotBeNull();
        span!.ParentSpanId.Should().Be(producer.SpanId);
        span.Links.Should().BeEmpty("there is nothing else in the batch to link to");
    }

    private static StreamMsg Entry(int index, string? traceParent) => new(
        Body: ReadOnlyMemory<byte>.Empty,
        Type: "MBet",
        Id: new StreamId(1, index),
        Partition: 0,
        PartitionKey: "k",
        CorrelationId: "c",
        TraceParent: traceParent,
        Headers: HeaderBlock.Empty);

    /// <summary>
    /// A sampling listener over the library's own source, plus a source of its own for standing up
    /// producer traces to parent off.
    /// </summary>
    private sealed class ProcessSpanRig : IDisposable
    {
        private readonly ActivityListener listener;
        private readonly ActivitySource source;

        internal ProcessSpanRig()
        {
            this.source = new ActivitySource($"RedisEvents.UnitTests.Process.{Guid.NewGuid()}", "1.0.0");

            var mine = this.source;

            this.listener = new ActivityListener
            {
                ShouldListenTo = s => ReferenceEquals(s, mine) || s.Name == StreamsDiagnostics.SourceName,

                // AllData, not PropagationData: StartActivity returns null for anything less.
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                SampleUsingParentId = static (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
            };

            ActivitySource.AddActivityListener(this.listener);
        }

        internal Activity NewTrace()
        {
            Activity.Current = null;

            return this.Started("producer");
        }

        internal Activity StartChild(Activity parent)
        {
            Activity.Current = parent;

            try
            {
                return this.Started("producer-child");
            }
            finally
            {
                Activity.Current = null;
            }
        }

        /// <summary>
        /// Calls the method under test with no ambient activity, so a parent can only have come from
        /// the entries themselves rather than from the caller's execution context.
        /// </summary>
        internal Activity? StartProcess(ReadOnlySpan<StreamMsg> batch)
        {
            Activity.Current = null;

            return StreamActivity.StartProcess(
                new StreamSpanContext(Topic, 0, Consumer),
                batch,
                batch.Length == 0 ? StreamId.Min : batch[^1].Id);
        }

        public void Dispose()
        {
            Activity.Current = null;
            this.listener.Dispose();
            this.source.Dispose();
        }

        private Activity Started(string name)
        {
            var activity = this.source.StartActivity(name, ActivityKind.Producer)
                ?? throw new InvalidOperationException(
                    "No activity was created, so the listener is not sampling and every assertion here would be vacuous.");

            return activity.IdFormat == ActivityIdFormat.W3C
                ? activity
                : throw new InvalidOperationException($"Activity '{name}' is {activity.IdFormat}, not W3C.");
        }
    }
}

/// <summary>Reads the W3C <c>traceparent</c> an activity would stamp on an outgoing entry.</summary>
internal static class ConsumerSpanParentingExtensions
{
    internal static string TraceParent(this Activity activity) => activity.Id!;
}
