using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using FluentAssertions;
using global::MessagePack;
using RedisEvents.Testing;

namespace RedisEvents.MessagePack.UnitTests;

/// <summary>
/// Builds the MessagePack golden vectors under <c>tst/vectors/messagepack.json</c> from the real
/// serialiser and the real publish path, and checks the committed file still matches. Run with
/// <c>REGENERATE_VECTORS=1</c> to rewrite it.
/// </summary>
/// <remarks>
/// The large payload's body is taken from <see cref="MessagePackPublishExtensions"/> itself, so the
/// file holds the <c>Lz4BlockArray</c> bytes a publisher really writes. The <c>Lz4Block</c> case is
/// what the spec says readers must accept; the library's publish path does not produce it.
/// </remarks>
public sealed class GoldenVectorTests
{
    private const int ThresholdBytes = 1024;

    private static readonly MessagePackSerializerOptions LzReadOptions =
        MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4BlockArray);

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Messagepack_bodies_match_golden_file()
    {
        var cases = new List<MessagePackCase>();

        // 1. Keyed (array) object, below the threshold: the plain uncompressed encoding.
        var keyed = new MpKeyedOrder(7, "seven");
        var keyedBody = MessagePackSerializer.Serialize(keyed, MessagePackSerializerOptions.Standard);
        MessagePackSerializer.Deserialize<MpKeyedOrder>(keyedBody, MessagePackSerializerOptions.Standard).Should().Be(keyed);
        cases.Add(new MessagePackCase(
            "small-keyed-int",
            typeof(MpKeyedOrder).FullName!,
            "uncompressed",
            keyedBody.Length,
            Describe(keyed),
            Convert.ToBase64String(keyedBody)));

        // 2. String-keyed (map) object, below the threshold.
        var named = new MpNamedOrder(8, "eight");
        var namedBody = MessagePackSerializer.Serialize(named, MessagePackSerializerOptions.Standard);
        MessagePackSerializer.Deserialize<MpNamedOrder>(namedBody, MessagePackSerializerOptions.Standard).Should().Be(named);
        cases.Add(new MessagePackCase(
            "small-string-keyed-map",
            typeof(MpNamedOrder).FullName!,
            "uncompressed",
            namedBody.Length,
            Describe(named),
            Convert.ToBase64String(namedBody)));

        // 3. Large payload through the library's publish path: size-gated, Lz4BlockArray.
        var large = new MpLargeOrder
        {
            Id = 1,
            Lines = Enumerable.Range(0, 200).Select(i => $"line-{i:000}-the quick brown fox jumps").ToArray(),
        };
        var uncompressedLarge = MessagePackSerializer.Serialize(large, MessagePackSerializerOptions.Standard);
        uncompressedLarge.Length.Should().BeGreaterThan(ThresholdBytes, "the payload must exceed the publish threshold");

        var publisher = new InMemoryStreamPublisher();
        await publisher.PublishAsync("k", large, MessagePackSerializerOptions.Standard);
        var published = publisher.Published.Should().ContainSingle().Subject;

        var publishedBody = published.Body.ToArray();
        publishedBody.Should().Equal(
            MessagePackSerializer.Serialize(large, MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4BlockArray)),
            "the publish path must write the Lz4BlockArray form");
        publishedBody.Length.Should().BeLessThan(uncompressedLarge.Length);
        // Read the way MessagePackConsumeExtensions does: Standard alone does not decode LZ4 bodies.
        MessagePackSerializer.Deserialize<MpLargeOrder>(publishedBody, LzReadOptions)
            .Lines.Should().Equal(large.Lines);

        cases.Add(new MessagePackCase(
            "large-keyed-int-publish-lz4blockarray",
            published.Type,
            "Lz4BlockArray",
            uncompressedLarge.Length,
            Describe(large),
            Convert.ToBase64String(publishedBody)));

        // 4. The same payload in the Lz4Block form, which readers must also accept.
        var lz4Block = MessagePackSerializer.Serialize(
            large,
            MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4Block));
        MessagePackSerializer.Deserialize<MpLargeOrder>(lz4Block, LzReadOptions)
            .Lines.Should().Equal(large.Lines);
        cases.Add(new MessagePackCase(
            "large-keyed-int-lz4block",
            typeof(MpLargeOrder).FullName!,
            "Lz4Block",
            uncompressedLarge.Length,
            Describe(large),
            Convert.ToBase64String(lz4Block)));

        GoldenVectorFiles.Check("messagepack.json", new MessagePackFile(ThresholdBytes, cases));
    }

    /// <summary>
    /// Describes an object from its <c>[Key]</c> members, so the description always matches the
    /// type the bytes were produced from.
    /// </summary>
    private static JsonObject Describe(object value)
    {
        var members = value.GetType()
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => (Property: p, Key: p.GetCustomAttribute<KeyAttribute>()))
            .Where(m => m.Key is not null)
            .OrderBy(m => m.Key!.IntKey.HasValue ? 0 : 1)
            .ThenBy(m => m.Key!.IntKey ?? 0)
            .ThenBy(m => m.Key!.StringKey, StringComparer.Ordinal)
            .ToList();

        var layout = members.Any(m => m.Key!.StringKey is not null) ? "map" : "array";
        var array = new JsonArray();
        foreach (var (property, key) in members)
        {
            object keyValue = key!.IntKey is int i ? i : key.StringKey!;
            array.Add(new JsonObject
            {
                ["key"] = JsonSerializer.SerializeToNode(keyValue),
                ["name"] = property.Name,
                ["value"] = JsonSerializer.SerializeToNode(property.GetValue(value)),
            });
        }

        return new JsonObject
        {
            ["layout"] = layout,
            ["members"] = array,
        };
    }

    private sealed record MessagePackCase(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("type")] string Type,
        [property: JsonPropertyName("compression")] string Compression,
        [property: JsonPropertyName("uncompressed_length")] int UncompressedLength,
        [property: JsonPropertyName("description")] JsonObject Description,
        [property: JsonPropertyName("body_b64")] string BodyB64);

    private sealed record MessagePackFile(
        [property: JsonPropertyName("threshold_bytes")] int ThresholdBytes,
        [property: JsonPropertyName("cases")] IReadOnlyList<MessagePackCase> Cases);
}

[MessagePackObject]
public sealed record MpKeyedOrder([property: Key(0)] int Id, [property: Key(1)] string Name);

[MessagePackObject]
public sealed record MpNamedOrder([property: Key("id")] int Id, [property: Key("name")] string Name);

[MessagePackObject]
public sealed class MpLargeOrder
{
    [Key(0)]
    public int Id { get; init; }

    [Key(1)]
    public string[] Lines { get; set; } = [];
}
