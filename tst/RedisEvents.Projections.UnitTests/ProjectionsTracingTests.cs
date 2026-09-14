using System.Diagnostics;
using System.Text.Json.Serialization;

using FluentAssertions;

using RedisEvents.Projections;
using RedisEvents.Projections.Diagnostics;
using RedisEvents.Wire;

namespace RedisEvents.Projections.UnitTests;

#pragma warning disable CA1852 // The JSON context has to be a partial class for the generator.

/// <summary>
/// Covers the package's own OpenTelemetry span — <c>projections.project</c> — added alongside
/// <see cref="EventProjector"/>. The equivalent spans for a save/load, <c>eventsourcing.save</c> and
/// <c>eventsourcing.load</c>, are covered by the sibling <c>RedisEvents.EventSourcing</c> package's
/// own <c>EventSourcingTracingTests</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>The listener is load-bearing</b>, exactly as in core's own <c>ProducerSpanTests</c>:
/// <see cref="ActivitySource.StartActivity(string, ActivityKind)"/> returns <see langword="null"/>
/// when nobody is listening, so without an <see cref="ActivityListener"/> sampling
/// <see cref="ActivitySamplingResult.AllDataAndRecorded"/> every assertion below would pass
/// vacuously against code that creates no spans at all.
/// </para>
/// <para>
/// The package's <see cref="ActivitySource"/> is a single static instance shared by the whole test
/// assembly, so every test gives its aggregate a fresh <see cref="Guid"/> id and filters the
/// collected spans down to that id — the same defence a parallel test run needs regardless of
/// whether these particular tests currently run concurrently.
/// </para>
/// </remarks>
public class ProjectionsTracingTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task HandleAsync_emits_one_project_span_per_dispatched_event()
    {
        using var rig = new SpanRig();
        var registry = NewRegistry();
        var projection = new SpyProjection();
        var projector = new EventProjector(registry, [projection]);

        var partitionKey = NewId();
        var first = ProjectMsg(partitionKey, ms: 1);
        var second = ProjectMsg(partitionKey, ms: 2);

        await projector.HandleAsync(new[] { first, second }, CancellationToken.None);

        projection.Received.Should().HaveCount(2);

        var spans = rig.For(partitionKey).ToArray();
        spans.Should().HaveCount(2, "one span per dispatched event, not per bound projection");
        spans.Should().OnlyContain(s => s.OperationName == "projections.project touched");
        spans.Should().OnlyContain(s => s.Kind == ActivityKind.Internal);

        Tag(spans[0], ProjectionsSpans.StreamIdKey).Should().Be(first.Id.Format());
        Tag(spans[1], ProjectionsSpans.StreamIdKey).Should().Be(second.Id.Format());
        spans.Should().OnlyContain(s => Tag(s, ProjectionsSpans.WireTypeKey) == "touched");
    }

    /// <summary>
    /// The critical regression check: adding tracing must not change
    /// <see cref="EventProjector"/>'s error contract in any way. The exact same exception instance
    /// must still come out of <see cref="EventProjector.HandleAsync"/>, and the rest of the batch —
    /// including the second binder for the very event that threw — must still never run.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_throwing_projection_marks_its_span_as_an_error_and_still_propagates_the_exception_unchanged()
    {
        using var rig = new SpanRig();
        var registry = NewRegistry();

        var boom = new InvalidOperationException("boom");
        var thrower = new ThrowingProjection(boom);
        var spy = new SpyProjection();
        var projector = new EventProjector(registry, [thrower, spy]);

        var partitionKey = NewId();
        var first = ProjectMsg(partitionKey, ms: 1);
        var second = ProjectMsg(partitionKey, ms: 2);

        var act = async () => await projector.HandleAsync(new[] { first, second }, CancellationToken.None);

        (await act.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(boom);

        spy.Received.Should().BeEmpty(
            "the second binder for the first event, and the second event entirely, must never run — unchanged from before tracing existed");

        var spans = rig.For(partitionKey).ToArray();
        spans.Should().ContainSingle("only the first event's span was ever started; the throw stopped the batch before the second");
        spans[0].Status.Should().Be(ActivityStatusCode.Error);
        spans[0].StatusDescription.Should().Be("boom");
    }

    /// <summary>
    /// With no listener registered — the default for every other test in the suite — tracing must
    /// cost nothing observable: a projector dispatch behaves exactly as it did before this package
    /// had any tracing of its own.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task With_no_listener_project_still_works()
    {
        var registry = NewRegistry();
        var projection = new SpyProjection();
        var projector = new EventProjector(registry, [projection]);
        var msg = ProjectMsg(NewId(), ms: 1);

        var act = async () => await projector.HandleAsync(new[] { msg }, CancellationToken.None);
        await act.Should().NotThrowAsync();
        projection.Received.Should().ContainSingle();
    }

    private static string NewId() => Guid.NewGuid().ToString("N");

    private static EventTypeRegistry NewRegistry() =>
        new EventTypeRegistry().RegisterJson("touched", TouchedJson.Default.Touched);

    private static string? Tag(Activity span, string name) => span.GetTagItem(name)?.ToString();

    /// <summary>
    /// A batch entry for <see cref="EventProjector.HandleAsync"/> — no headers at all. The projector
    /// needs nothing beyond a <see cref="StreamMsg"/>'s ordinary fields, so a message that never went
    /// near an event store projects exactly the same way as one that did.
    /// </summary>
    private static StreamMsg ProjectMsg(string partitionKey, long ms) => new(
        Body: System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new Touched(partitionKey), TouchedJson.Default.Touched),
        Type: "touched",
        Id: new StreamId(ms, 0),
        Partition: 0,
        PartitionKey: partitionKey,
        CorrelationId: string.Empty,
        TraceParent: null,
        Headers: HeaderBlock.Empty);

    private sealed class SpyProjection : IProjection<Touched>
    {
        public List<Touched> Received { get; } = [];

        public ValueTask HandleAsync(Touched @event, EventMeta meta, CancellationToken ct)
        {
            this.Received.Add(@event);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingProjection(Exception toThrow) : IProjection<Touched>
    {
        public ValueTask HandleAsync(Touched @event, EventMeta meta, CancellationToken ct) => throw toThrow;
    }

    /// <summary>
    /// Collects the package's spans, scoped per test by the partition key every test gives itself —
    /// the package's <see cref="ActivitySource"/> is one static instance shared by every test in the
    /// assembly, so nothing here may assume it is the only listener or the only span in flight.
    /// </summary>
    private sealed class SpanRig : IDisposable
    {
        private readonly Lock gate = new();
        private readonly List<Activity> collected = [];
        private readonly ActivityListener listener;

        internal SpanRig()
        {
            this.listener = new ActivityListener
            {
                ShouldListenTo = s => s.Name == ProjectionsDiagnostics.SourceName,

                // AllData, not PropagationData: StartActivity returns null for anything less, and
                // IsAllDataRequested — which every attribute set is guarded by — would be false.
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                SampleUsingParentId = static (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = this.Collect,
            };

            ActivitySource.AddActivityListener(this.listener);
        }

        internal IEnumerable<Activity> For(string partitionKey)
        {
            lock (this.gate)
            {
                return this.collected
                    .Where(a => a.GetTagItem(ProjectionsSpans.PartitionKeyKey)?.ToString() == partitionKey)
                    .ToArray();
            }
        }

        public void Dispose() => this.listener.Dispose();

        private void Collect(Activity activity)
        {
            lock (this.gate)
            {
                this.collected.Add(activity);
            }
        }
    }
}

internal sealed record Touched(string Id);

[JsonSerializable(typeof(Touched))]
internal partial class TouchedJson : JsonSerializerContext;
