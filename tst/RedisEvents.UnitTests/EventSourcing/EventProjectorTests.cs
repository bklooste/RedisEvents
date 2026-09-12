using System.Text.Json.Serialization;

using FluentAssertions;

using RedisEvents.Errors;
using RedisEvents.EventSourcing;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests.EventSourcing;

/// <summary>
/// Covers <see cref="EventProjector"/>'s dispatch rules: decoding via <see cref="EventTypeRegistry"/>,
/// fan-out to every bound <see cref="IProjection{TEvent}"/> (including a class implementing it more
/// than once, and several projection instances binding to the same event), the two silent-skip paths,
/// the missing/unparsable <c>es-version</c> header, and — the point of the type — that it never
/// catches an exception a projection throws.
/// </summary>
public class EventProjectorTests
{
    private static readonly EventA SampleA = new("a-1");
    private static readonly EventB SampleB = new("b-1");

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_single_bound_projection_receives_the_event_with_correct_meta()
    {
        var registry = new EventTypeRegistry().RegisterJson("event.a", EventProjectorTestsJson.Default.EventA);
        var projection = new SpyProjection();
        var projector = new EventProjector(registry, [projection]);

        var msg = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "corr-1", version: 3);

        await projector.HandleAsync(new[] { msg }, CancellationToken.None);

        projection.ReceivedA.Should().ContainSingle().Which.Should().Be(SampleA);
        projection.LastMeta.AggregateId.Should().Be("agg-1");
        projection.LastMeta.Version.Should().Be(3);
        projection.LastMeta.CorrelationId.Should().Be("corr-1");
        projection.LastMeta.Id.Should().Be(msg.Id);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_projection_implementing_two_event_types_is_dispatched_for_both_with_no_extra_registration()
    {
        var registry = new EventTypeRegistry()
            .RegisterJson("event.a", EventProjectorTestsJson.Default.EventA)
            .RegisterJson("event.b", EventProjectorTestsJson.Default.EventB);
        var projection = new SpyProjection();
        var projector = new EventProjector(registry, [projection]);

        var msgA = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", version: 1);
        var msgB = Build(SampleB, "event.b", partitionKey: "agg-1", correlationId: "c", version: 2);

        await projector.HandleAsync(new[] { msgA, msgB }, CancellationToken.None);

        projection.ReceivedA.Should().ContainSingle().Which.Should().Be(SampleA);
        projection.ReceivedB.Should().ContainSingle().Which.Should().Be(SampleB);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Two_different_projection_instances_both_bound_to_the_same_event_both_receive_it()
    {
        var registry = new EventTypeRegistry().RegisterJson("event.a", EventProjectorTestsJson.Default.EventA);
        var first = new SpyProjection();
        var second = new SpyProjection();
        var projector = new EventProjector(registry, [first, second]);

        var msg = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", version: 1);

        await projector.HandleAsync(new[] { msg }, CancellationToken.None);

        first.ReceivedA.Should().ContainSingle().Which.Should().Be(SampleA);
        second.ReceivedA.Should().ContainSingle().Which.Should().Be(SampleA);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_event_with_an_unregistered_wire_type_is_skipped_with_no_exception_and_no_dispatch()
    {
        var registry = new EventTypeRegistry().RegisterJson("event.a", EventProjectorTestsJson.Default.EventA);
        var projection = new SpyProjection();
        var projector = new EventProjector(registry, [projection]);

        var msg = Build(SampleA, "event.unknown", partitionKey: "agg-1", correlationId: "c", version: 1);

        var act = async () => await projector.HandleAsync(new[] { msg }, CancellationToken.None);

        await act.Should().NotThrowAsync();
        projection.ReceivedA.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_registered_event_with_no_bound_projection_is_skipped_with_no_exception()
    {
        var registry = new EventTypeRegistry()
            .RegisterJson("event.a", EventProjectorTestsJson.Default.EventA)
            .RegisterJson("event.b", EventProjectorTestsJson.Default.EventB);
        // A projection that only implements IProjection<EventB>, so an EventA message binds to nothing.
        var projection = new BOnlyProjection();
        var projector = new EventProjector(registry, [projection]);

        var msg = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", version: 1);

        var act = async () => await projector.HandleAsync(new[] { msg }, CancellationToken.None);

        await act.Should().NotThrowAsync();
        projection.ReceivedB.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_ordinary_exception_from_a_projection_propagates_unchanged_and_stops_the_rest_of_the_batch()
    {
        var registry = new EventTypeRegistry().RegisterJson("event.a", EventProjectorTestsJson.Default.EventA);
        var thrower = new ThrowingProjection(new InvalidOperationException("boom"));
        var spy = new SpyProjection();
        var projector = new EventProjector(registry, [thrower, spy]);

        var first = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", version: 1);
        var second = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", version: 2);

        var act = async () => await projector.HandleAsync(new[] { first, second }, CancellationToken.None);

        (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("boom");

        // The thrower's own binding for the first event ran before it threw, but the second binder
        // for that same first event (spy) never got a chance, and the second event was never reached.
        spy.ReceivedA.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_DontIgnoreException_subclass_from_a_projection_propagates_unchanged()
    {
        var registry = new EventTypeRegistry().RegisterJson("event.a", EventProjectorTestsJson.Default.EventA);
        var thrower = new ThrowingProjection(new TestDontIgnoreException("blocked"));
        var projector = new EventProjector(registry, [thrower]);

        var msg = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", version: 1);

        var act = async () => await projector.HandleAsync(new[] { msg }, CancellationToken.None);

        await act.Should().ThrowAsync<TestDontIgnoreException>().WithMessage("blocked");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Missing_es_version_header_throws_InvalidOperationException()
    {
        var registry = new EventTypeRegistry().RegisterJson("event.a", EventProjectorTestsJson.Default.EventA);
        var projection = new SpyProjection();
        var projector = new EventProjector(registry, [projection]);

        var msg = BuildWithoutVersionHeader(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c");

        var act = async () => await projector.HandleAsync(new[] { msg }, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Unparsable_es_version_header_throws_InvalidOperationException()
    {
        var registry = new EventTypeRegistry().RegisterJson("event.a", EventProjectorTestsJson.Default.EventA);
        var projection = new SpyProjection();
        var projector = new EventProjector(registry, [projection]);

        var msg = BuildWithRawVersionHeader(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", rawVersion: "not-a-number");

        var act = async () => await projector.HandleAsync(new[] { msg }, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    private static StreamMsg Build<T>(T message, string wireType, string partitionKey, string correlationId, int version)
        where T : notnull
        => BuildWithRawVersionHeader(message, wireType, partitionKey, correlationId, version.ToString());

    private static StreamMsg BuildWithRawVersionHeader<T>(T message, string wireType, string partitionKey, string correlationId, string rawVersion)
        where T : notnull
        => new(
            Body: System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(message, ResolveTypeInfo<T>()),
            Type: wireType,
            Id: new StreamId(1, 0),
            Partition: 0,
            PartitionKey: partitionKey,
            CorrelationId: correlationId,
            TraceParent: null,
            Headers: HeaderBlock.Pack([new("es-version", rawVersion)]));

    private static StreamMsg BuildWithoutVersionHeader<T>(T message, string wireType, string partitionKey, string correlationId)
        where T : notnull
        => new(
            Body: System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(message, ResolveTypeInfo<T>()),
            Type: wireType,
            Id: new StreamId(1, 0),
            Partition: 0,
            PartitionKey: partitionKey,
            CorrelationId: correlationId,
            TraceParent: null,
            Headers: HeaderBlock.Empty);

    private static System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> ResolveTypeInfo<T>()
    {
        object typeInfo = typeof(T) == typeof(EventA)
            ? EventProjectorTestsJson.Default.EventA
            : EventProjectorTestsJson.Default.EventB;

        return (System.Text.Json.Serialization.Metadata.JsonTypeInfo<T>)typeInfo;
    }

    private sealed class SpyProjection : IProjection<EventA>, IProjection<EventB>
    {
        public List<EventA> ReceivedA { get; } = [];

        public List<EventB> ReceivedB { get; } = [];

        public EventMeta LastMeta { get; private set; }

        public ValueTask HandleAsync(EventA @event, EventMeta meta, CancellationToken ct)
        {
            ReceivedA.Add(@event);
            LastMeta = meta;
            return ValueTask.CompletedTask;
        }

        public ValueTask HandleAsync(EventB @event, EventMeta meta, CancellationToken ct)
        {
            ReceivedB.Add(@event);
            LastMeta = meta;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class BOnlyProjection : IProjection<EventB>
    {
        public List<EventB> ReceivedB { get; } = [];

        public ValueTask HandleAsync(EventB @event, EventMeta meta, CancellationToken ct)
        {
            ReceivedB.Add(@event);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingProjection(Exception toThrow) : IProjection<EventA>
    {
        public ValueTask HandleAsync(EventA @event, EventMeta meta, CancellationToken ct) => throw toThrow;
    }

    private sealed class TestDontIgnoreException(string message) : DontIgnoreException(message);
}

internal sealed record EventA(string Id);

internal sealed record EventB(string Id);

[JsonSerializable(typeof(EventA))]
[JsonSerializable(typeof(EventB))]
internal sealed partial class EventProjectorTestsJson : JsonSerializerContext;
