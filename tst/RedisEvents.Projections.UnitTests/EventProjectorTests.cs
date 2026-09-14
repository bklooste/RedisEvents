using System.Text.Json.Serialization;

using FluentAssertions;

using RedisEvents.Errors;
using RedisEvents.Projections;
using RedisEvents.Wire;

namespace RedisEvents.Projections.UnitTests;

/// <summary>
/// Covers <see cref="EventProjector"/>'s dispatch rules: decoding via <see cref="EventTypeRegistry"/>,
/// fan-out to every bound <see cref="IProjection{TEvent}"/> (including a class implementing it more
/// than once, and several projection instances binding to the same event), the two silent-skip paths,
/// that it depends on nothing beyond a standard <see cref="StreamMsg"/> — no header is required, so a
/// message published by something other than this package's own event store projects the same as one
/// that was — and, the point of the type, that it never catches an exception a projection throws.
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

        var msg = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "corr-1", id: new StreamId(1, 0));

        await projector.HandleAsync(new[] { msg }, CancellationToken.None);

        projection.ReceivedA.Should().ContainSingle().Which.Should().Be(SampleA);
        projection.LastMeta.PartitionKey.Should().Be("agg-1");
        projection.LastMeta.CorrelationId.Should().Be("corr-1");
        projection.LastMeta.Id.Should().Be(msg.Id);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_message_with_no_headers_at_all_projects_without_error()
    {
        // No es-version, no anything: the projector depends only on a StreamMsg's ordinary fields
        // (Type, Body, PartitionKey, Id, CorrelationId) and nothing an event store specifically stamps
        // — a plain publisher that never heard of AddEventStore projects exactly the same way.
        var registry = new EventTypeRegistry().RegisterJson("event.a", EventProjectorTestsJson.Default.EventA);
        var projection = new SpyProjection();
        var projector = new EventProjector(registry, [projection]);

        var msg = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", id: new StreamId(1, 0), headers: HeaderBlock.Empty);

        var act = async () => await projector.HandleAsync(new[] { msg }, CancellationToken.None);

        await act.Should().NotThrowAsync();
        projection.ReceivedA.Should().ContainSingle().Which.Should().Be(SampleA);
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

        var msgA = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", id: new StreamId(1, 0));
        var msgB = Build(SampleB, "event.b", partitionKey: "agg-1", correlationId: "c", id: new StreamId(1, 1));

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

        var msg = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", id: new StreamId(1, 0));

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

        var msg = Build(SampleA, "event.unknown", partitionKey: "agg-1", correlationId: "c", id: new StreamId(1, 0));

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

        var msg = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", id: new StreamId(1, 0));

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

        var first = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", id: new StreamId(1, 0));
        var second = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", id: new StreamId(1, 1));

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

        var msg = Build(SampleA, "event.a", partitionKey: "agg-1", correlationId: "c", id: new StreamId(1, 0));

        var act = async () => await projector.HandleAsync(new[] { msg }, CancellationToken.None);

        await act.Should().ThrowAsync<TestDontIgnoreException>().WithMessage("blocked");
    }

    private static StreamMsg Build<T>(T message, string wireType, string partitionKey, string correlationId, StreamId id, HeaderBlock headers = default)
        where T : notnull
        => new(
            Body: System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(message, ResolveTypeInfo<T>()),
            Type: wireType,
            Id: id,
            Partition: 0,
            PartitionKey: partitionKey,
            CorrelationId: correlationId,
            TraceParent: null,
            Headers: headers);

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
