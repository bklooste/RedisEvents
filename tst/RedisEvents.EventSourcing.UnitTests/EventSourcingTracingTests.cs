using System.Diagnostics;
using System.Text.Json.Serialization;

using FluentAssertions;

using RedisEvents.EventSourcing;
using RedisEvents.EventSourcing.Diagnostics;
using RedisEvents.Producer;
using RedisEvents.Projections;
using RedisEvents.Wire;

namespace RedisEvents.EventSourcing.UnitTests;

#pragma warning disable CA1852 // The JSON context has to be a partial class for the generator.

/// <summary>
/// Covers the package's own OpenTelemetry spans — <c>eventsourcing.save</c> and
/// <c>eventsourcing.load</c> — added alongside <see cref="RedisEventRepository"/>. The equivalent
/// span for a projection dispatch, <c>projections.project</c>, is covered by the sibling
/// <c>RedisEvents.Projections</c> package's own <c>ProjectionsTracingTests</c>.
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
/// <see cref="RedisEventRepository"/> is exercised here against a hand-rolled fake
/// <see cref="IStreamStore"/> rather than real Redis: none of these tests are about the server
/// arbitrating a version check (that is <c>EventRepositoryTests</c>'s job, against
/// <c>RedisStreamsFixture</c>) — they are about whether a call opens the right span with the right
/// tags, which a fake answers just as well and far faster.
/// </para>
/// <para>
/// The package's <see cref="ActivitySource"/> is a single static instance shared by the whole test
/// assembly, so every test gives its aggregate a fresh <see cref="Guid"/> id and filters the
/// collected spans down to that id — the same defence a parallel test run needs regardless of
/// whether these particular tests currently run concurrently.
/// </para>
/// </remarks>
public class EventSourcingTracingTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task SaveAsync_emits_a_save_span_tagged_with_the_aggregate_and_the_outcome()
    {
        using var rig = new SpanRig();
        var registry = NewRegistry();
        var store = new FakeStreamStore { AppendResult = [new StreamId(1, 0)] };
        var repo = new RedisEventRepository(store, registry);

        var id = NewId();
        var aggregate = TracedThing.Create(id);

        var newVersion = await repo.SaveAsync(aggregate);

        newVersion.Should().Be(1);

        var span = rig.For(id).Should().ContainSingle().Subject;
        span.OperationName.Should().Be($"eventsourcing.save {TracedThing.Family}");
        span.Kind.Should().Be(ActivityKind.Client);
        span.Status.Should().Be(ActivityStatusCode.Unset, "a successful save is not an error");

        Tag(span, EventSourcingSpans.AggregateNameKey).Should().Be(TracedThing.Family);
        Tag(span, EventSourcingSpans.AggregateIdKey).Should().Be(id);
        Tag(span, EventSourcingSpans.ExpectedVersionKey).Should().Be("0");
        Tag(span, EventSourcingSpans.VersionKey).Should().Be("1");
        Tag(span, EventSourcingSpans.EventCountKey).Should().Be("1");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task SaveAsync_that_loses_its_version_check_marks_the_span_as_a_conflict()
    {
        using var rig = new SpanRig();
        var registry = NewRegistry();
        var store = new FakeStreamStore { AppendResult = null };
        var repo = new RedisEventRepository(store, registry);

        var id = NewId();
        var aggregate = TracedThing.Create(id);

        var act = async () => await repo.SaveAsync(aggregate);
        await act.Should().ThrowAsync<ConcurrencyException>();

        var span = rig.For(id).Should().ContainSingle().Subject;
        span.Status.Should().Be(ActivityStatusCode.Error);
        span.StatusDescription.Should().ContainEquivalentOf("concurrency");
    }

    /// <summary>
    /// The counterpart to the conflict test above: a genuine, unexpected failure must still read as
    /// a failure, with a different status description, so an operator can tell the two apart.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task SaveAsync_that_throws_marks_the_span_as_a_failure_distinct_from_a_conflict()
    {
        using var rig = new SpanRig();
        var registry = NewRegistry();
        var store = new FakeStreamStore { ThrowOnAppend = new InvalidOperationException("transport is gone") };
        var repo = new RedisEventRepository(store, registry);

        var id = NewId();
        var aggregate = TracedThing.Create(id);

        var act = async () => await repo.SaveAsync(aggregate);
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("transport is gone");

        var span = rig.For(id).Should().ContainSingle().Subject;
        span.Status.Should().Be(ActivityStatusCode.Error);
        span.StatusDescription.Should().Be("transport is gone");
        span.StatusDescription.Should().NotContainEquivalentOf(
            "concurrency",
            "a transport failure must read differently from a lost version check");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task LoadAsync_emits_a_load_span_tagged_with_the_version_found()
    {
        using var rig = new SpanRig();
        var registry = NewRegistry();

        var id = NewId();
        var store = new FakeStreamStore { ReadResult = [HistoryMsg(id, version: 1)] };
        var repo = new RedisEventRepository(store, registry);

        var loaded = await repo.LoadAsync<TracedThing>(id);

        loaded.Should().NotBeNull();

        var span = rig.For(id).Should().ContainSingle().Subject;
        span.OperationName.Should().Be($"eventsourcing.load {TracedThing.Family}");
        span.Kind.Should().Be(ActivityKind.Client);
        Tag(span, EventSourcingSpans.FoundKey).Should().Be("True");
        Tag(span, EventSourcingSpans.VersionKey).Should().Be("1");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task LoadAsync_with_no_history_tags_the_span_as_not_found()
    {
        using var rig = new SpanRig();
        var registry = NewRegistry();

        var id = NewId();
        var store = new FakeStreamStore { ReadResult = [] };
        var repo = new RedisEventRepository(store, registry);

        var loaded = await repo.LoadAsync<TracedThing>(id);

        loaded.Should().BeNull();

        var span = rig.For(id).Should().ContainSingle().Subject;
        Tag(span, EventSourcingSpans.FoundKey).Should().Be("False");
        Tag(span, EventSourcingSpans.VersionKey).Should().BeNull("nothing was found, so there is no version to report");
    }

    /// <summary>
    /// With no listener registered — the default for every other test in the suite — tracing must
    /// cost nothing observable: save and load both behave exactly as they did before this package
    /// had any tracing of its own.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task With_no_listener_save_and_load_still_work()
    {
        var registry = NewRegistry();
        var id = NewId();

        var store = new FakeStreamStore { AppendResult = [new StreamId(1, 0)] };
        var repo = new RedisEventRepository(store, registry);
        var aggregate = TracedThing.Create(id);

        (await repo.SaveAsync(aggregate)).Should().Be(1);

        store.ReadResult = [HistoryMsg(id, version: 1)];
        var loaded = await repo.LoadAsync<TracedThing>(id);
        loaded.Should().NotBeNull();
    }

    private static string NewId() => Guid.NewGuid().ToString("N");

    private static EventTypeRegistry NewRegistry() =>
        new EventTypeRegistry().RegisterJson("touched", TracedThingJson.Default.Touched);

    private static string? Tag(Activity span, string name) => span.GetTagItem(name)?.ToString();

    /// <summary>A history entry for <see cref="RedisEventRepository.LoadAsync{TAggregate}"/>: body and type are all loading reads.</summary>
    private static StreamMsg HistoryMsg(string aggregateId, int version) => new(
        Body: System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new Touched(aggregateId), TracedThingJson.Default.Touched),
        Type: "touched",
        Id: new StreamId(version, 0),
        Partition: 0,
        PartitionKey: aggregateId,
        CorrelationId: string.Empty,
        TraceParent: null,
        Headers: HeaderBlock.Empty);

    /// <summary>A minimal event-sourced aggregate, defined here so these tests exercise the repository and nothing else.</summary>
    private sealed class TracedThing : AggregateRoot
    {
        internal const string Family = "TracedThing";

        private string id = string.Empty;

        public TracedThing() => this.On<Touched>(e => this.id = e.Id);

        public override string AggregateName => Family;

        public override string Id => this.id;

        public static TracedThing Create(string id)
        {
            var thing = new TracedThing();
            thing.Raise(new Touched(id));
            return thing;
        }
    }

    /// <summary>
    /// A fake <see cref="IStreamStore"/>: no Redis, just the two return shapes
    /// <see cref="RedisEventRepository"/> reacts to — a version-check win or loss on append, and a
    /// page (possibly empty) on read.
    /// </summary>
    private sealed class FakeStreamStore : IStreamStore
    {
        internal IReadOnlyList<StreamMsg> ReadResult { get; set; } = [];

        internal StreamId[]? AppendResult { get; set; }

        internal Exception? ThrowOnAppend { get; set; }

        public ValueTask<IReadOnlyList<StreamMsg>> ReadAsync(string name, StreamId after, int max, CancellationToken ct = default)
            => ValueTask.FromResult(this.ReadResult);

        public ValueTask<StreamId[]?> AppendAndPublishAsync(
            string name,
            long expectedLength,
            string partitionKey,
            IReadOnlyList<StateEvent> events,
            CancellationToken ct = default)
        {
            if (this.ThrowOnAppend is not null)
            {
                throw this.ThrowOnAppend;
            }

            return ValueTask.FromResult(this.AppendResult);
        }

        public ValueTask<StreamId[]> AppendAndPublishAsync(
            string name,
            string partitionKey,
            IReadOnlyList<StateEvent> events,
            CancellationToken ct = default)
        {
            if (this.ThrowOnAppend is not null)
            {
                throw this.ThrowOnAppend;
            }

            return ValueTask.FromResult(this.AppendResult ?? []);
        }
    }

    /// <summary>
    /// Collects the package's spans, scoped per test by the aggregate id every test gives itself —
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
                ShouldListenTo = s => s.Name == EventSourcingDiagnostics.SourceName,

                // AllData, not PropagationData: StartActivity returns null for anything less, and
                // IsAllDataRequested — which every attribute set is guarded by — would be false.
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                SampleUsingParentId = static (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = this.Collect,
            };

            ActivitySource.AddActivityListener(this.listener);
        }

        internal IEnumerable<Activity> For(string id)
        {
            lock (this.gate)
            {
                return this.collected
                    .Where(a => a.GetTagItem(EventSourcingSpans.AggregateIdKey)?.ToString() == id)
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
internal partial class TracedThingJson : JsonSerializerContext;
