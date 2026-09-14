using System.Text.Json;
using System.Text.Json.Serialization;

using FluentAssertions;

using RedisEvents.Errors;
using RedisEvents.Projections;

namespace RedisEvents.Projections.UnitTests;

/// <summary>
/// Covers <see cref="EventTypeRegistry"/>: the JSON and hand rolled serialiser seams, duplicate
/// registration failing fast, unknown-type lookups, and the class-rename scenario the wire type
/// string exists to survive.
/// </summary>
public class EventTypeRegistryTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void RegisterJson_round_trips_an_event_through_encode_and_try_decode()
    {
        var registry = new EventTypeRegistry()
            .RegisterJson("widget.created", EventTypeRegistryTestsJson.Default.WidgetCreated);

        var (wireType, body) = registry.Encode(new WidgetCreated("w-1", "Widget"));

        wireType.Should().Be("widget.created");

        registry.TryDecode(wireType, body, out var decoded).Should().BeTrue();
        decoded.Should().BeOfType<WidgetCreated>().Which.Should().Be(new WidgetCreated("w-1", "Widget"));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Register_round_trips_an_event_through_a_hand_rolled_serialiser_pair()
    {
        // Proves the seam works with any lambda pair, no new dependency required — the same shape
        // RedisEvents.MessagePack plugs into core with.
        var registry = new EventTypeRegistry().Register<WidgetCreated>(
            "widget.created",
            e => JsonSerializer.SerializeToUtf8Bytes(e, EventTypeRegistryTestsJson.Default.WidgetCreated),
            b => JsonSerializer.Deserialize(b.Span, EventTypeRegistryTestsJson.Default.WidgetCreated)!);

        var (wireType, body) = registry.Encode(new WidgetCreated("w-2", "Gadget"));

        registry.TryDecode(wireType, body, out var decoded).Should().BeTrue();
        decoded.Should().Be(new WidgetCreated("w-2", "Gadget"));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Registering_the_same_wire_type_twice_throws()
    {
        var registry = new EventTypeRegistry()
            .RegisterJson("widget.created", EventTypeRegistryTestsJson.Default.WidgetCreated);

        var act = () => registry.RegisterJson("widget.created", EventTypeRegistryTestsJson.Default.WidgetRenamed);

        act.Should().Throw<StreamConfigurationException>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Registering_the_same_clr_type_twice_under_different_wire_types_throws()
    {
        var registry = new EventTypeRegistry()
            .RegisterJson("widget.created", EventTypeRegistryTestsJson.Default.WidgetCreated);

        var act = () => registry.RegisterJson("widget.created.v2", EventTypeRegistryTestsJson.Default.WidgetCreated);

        act.Should().Throw<StreamConfigurationException>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void TryDecode_with_an_unregistered_wire_type_returns_false()
    {
        var registry = new EventTypeRegistry()
            .RegisterJson("widget.created", EventTypeRegistryTestsJson.Default.WidgetCreated);

        registry.TryDecode("unknown.type", ReadOnlyMemory<byte>.Empty, out var decoded).Should().BeFalse();
        decoded.Should().BeNull();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Encode_on_an_unregistered_clr_type_throws()
    {
        var registry = new EventTypeRegistry()
            .RegisterJson("widget.created", EventTypeRegistryTestsJson.Default.WidgetCreated);

        var act = () => registry.Encode(new WidgetRenamed("New name"));

        act.Should().Throw<InvalidOperationException>().WithMessage("*WidgetRenamed*");
    }

    /// <summary>
    /// The rename test: the wire type string is independent of the CLR type name. Two registries,
    /// each registering a differently named-but-shaped CLR type under the SAME wire type string, both
    /// round-trip successfully through their own registry — proving a class rename (the wire type
    /// stays put; only the CLR type changes) never breaks replay.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_wire_type_is_independent_of_the_clr_type_name_across_a_rename()
    {
        var v1Registry = new EventTypeRegistry()
            .RegisterJson("widget.created", EventTypeRegistryTestsJson.Default.WidgetCreatedV1);

        var v2Registry = new EventTypeRegistry()
            .RegisterJson("widget.created", EventTypeRegistryTestsJson.Default.WidgetCreatedV2);

        var (v1WireType, v1Body) = v1Registry.Encode(new WidgetCreatedV1("w-1", "Widget"));
        var (v2WireType, v2Body) = v2Registry.Encode(new WidgetCreatedV2("w-1", "Widget"));

        v1WireType.Should().Be("widget.created");
        v2WireType.Should().Be("widget.created");

        v1Registry.TryDecode(v1WireType, v1Body, out var v1Decoded).Should().BeTrue();
        v1Decoded.Should().Be(new WidgetCreatedV1("w-1", "Widget"));

        v2Registry.TryDecode(v2WireType, v2Body, out var v2Decoded).Should().BeTrue();
        v2Decoded.Should().Be(new WidgetCreatedV2("w-1", "Widget"));
    }
}

internal sealed record WidgetCreated(string Id, string Name);

internal sealed record WidgetRenamed(string NewName);

/// <summary>Stand-in for the "old" class name in the rename scenario.</summary>
internal sealed record WidgetCreatedV1(string Id, string Name);

/// <summary>Same shape as <see cref="WidgetCreatedV1"/>, standing in for the class after a rename.</summary>
internal sealed record WidgetCreatedV2(string Id, string Name);

[JsonSerializable(typeof(WidgetCreated))]
[JsonSerializable(typeof(WidgetRenamed))]
[JsonSerializable(typeof(WidgetCreatedV1))]
[JsonSerializable(typeof(WidgetCreatedV2))]
internal sealed partial class EventTypeRegistryTestsJson : JsonSerializerContext;
