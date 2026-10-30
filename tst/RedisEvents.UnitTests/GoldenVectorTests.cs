using System.Globalization;
using System.IO.Hashing;
using System.Text;
using System.Text.Json.Serialization;
using FluentAssertions;
using RedisEvents.Admin;
using RedisEvents.Config;
using RedisEvents.Errors;
using RedisEvents.Ownership;
using RedisEvents.Positions;
using RedisEvents.Producer;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// Builds the golden wire vectors under <c>tst/vectors/</c> from the real C# code and checks the
/// committed files still match. Run with <c>REGENERATE_VECTORS=1</c> to rewrite them.
/// </summary>
/// <remarks>
/// Only the invalid packed-header bytes are literal input (they must not be producible by Pack). Every
/// expected value is computed by the library. Each vector is also exercised by the decoding or
/// encoding code it describes, so a file that drifted from the code fails here, and a file that
/// the code no longer accepts fails here too.
/// </remarks>
[Collection(GoldenVectorCollection.Name)]
public sealed class GoldenVectorTests
{
    // ------------------------------------------------------------------ entries.json

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Entries_match_golden_file()
    {
        var cases = new List<EntryCase>();
        void Add(string name, EntryInput input) => cases.Add(new EntryCase(name, input, ToFields(EncodeEntry(input))));

        Add("minimal", new EntryInput(B64("hello"), "order.created", PartitionKey: null, CorrelationId: null, TraceParent: null, Headers: null, State: false));
        Add("empty-key", new EntryInput(B64("hello"), "order.created", PartitionKey: "", CorrelationId: null, TraceParent: null, Headers: null, State: false));
        Add("all-fields", new EntryInput(
            B64("{\"orderId\":42,\"total\":19.95}"),
            "order.placed.v2",
            PartitionKey: "customer-42",
            CorrelationId: "c-123",
            TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            Headers: new[] { new[] { "tenant", "acme" }, new[] { "schema", "2" } },
            State: false));
        Add("unicode-key-and-type", new EntryInput(
            B64("café ✓"),
            "commande.créé.🎉",
            PartitionKey: "é✓🎉",
            CorrelationId: null,
            TraceParent: null,
            Headers: null,
            State: false));
        Add("binary-body-zero-bytes", new EntryInput(
            Convert.ToBase64String(new byte[] { 0x00, 0x01, 0x00, 0xff, 0x00, 0x00, 0x80 }),
            "blob",
            PartitionKey: "bin",
            CorrelationId: null,
            TraceParent: null,
            Headers: null,
            State: false));
        Add("state-minimal", new EntryInput(B64("event-body"), "es.order.placed", PartitionKey: null, CorrelationId: null, TraceParent: null, Headers: null, State: true));
        Add("state-with-metadata", new EntryInput(
            B64("event-body"),
            "es.order.placed",
            PartitionKey: null,
            CorrelationId: "c-9",
            TraceParent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            Headers: new[] { new[] { "source", "orders" } },
            State: true));
        Add("headers-present", new EntryInput(
            B64("payload"),
            "order.placed",
            PartitionKey: "k",
            CorrelationId: null,
            TraceParent: null,
            Headers: new[] { new[] { "a:1", "2:ab" }, new[] { "k", "" } },
            State: false));

        GoldenVectorFiles.Check("entries.json", cases);
    }

    // ------------------------------------------------------------------ headers.json

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Headers_match_golden_file()
    {
        var sixtyFour = Enumerable.Range(0, HeaderBlock.MaxCount)
            .Select(i => new[] { $"h{i:00}", $"v{i}" })
            .ToArray();

        var valid = new List<HeaderCase>
        {
            HeaderCaseFor("one", new[] { new[] { "k", "v" } }),
            HeaderCaseFor("many", Enumerable.Range(0, 10).Select(i => new[] { $"key-{i}", $"value-{i}" }).ToArray()),
            HeaderCaseFor("colon-and-digits", new[]
            {
                new[] { "a:1", "2:ab" },
                new[] { "12:34", "5:6:7" },
                new[] { "::", "9" },
            }),
            HeaderCaseFor("empty-value", new[] { new[] { "flag", string.Empty } }),
            HeaderCaseFor("unicode", new[]
            {
                new[] { "ключ", "значение" },
                new[] { "emoji🎉", "✓" },
            }),
            HeaderCaseFor("duplicate-keys", new[] { new[] { "x", "1" }, new[] { "x", "2" }, new[] { "x", "1" } }),
            HeaderCaseFor("max-64", sixtyFour),
        };

        var invalid = new[]
        {
            new InvalidHeaderCase("non-digit-length", B64Ascii("1x:a")),
            new InvalidHeaderCase("empty-length", B64Ascii(":a")),
            new InvalidHeaderCase("unterminated-length", B64Ascii("12")),
            new InvalidHeaderCase("length-past-end", B64Ascii("3:ab")),
            new InvalidHeaderCase("key-without-value", B64Ascii("1:a")),
        };

        var oversized = new string('x', HeaderBlock.MaxBytes + 8);
        var packRejects = new[]
        {
            new PackRejectCase("65-headers", Enumerable.Range(0, HeaderBlock.MaxCount + 1).Select(i => new[] { $"h{i:00}", "v" }).ToArray()),
            new PackRejectCase("over-8192-bytes", new[] { new[] { "big", oversized } }),
        };

        GoldenVectorFiles.Check("headers.json", new HeadersFile(valid, invalid, packRejects));

        // Behaviour the file encodes, checked against the real code.
        foreach (var case_ in valid)
        {
            var expected = case_.Headers.Select(h => (h[0], h[1])).ToList();
            var packed = Convert.FromBase64String(case_.PackedB64);
            Enumerate(packed).Should().Equal(expected, $"valid case {case_.Name} must decode to its headers in order");
        }

        foreach (var case_ in invalid)
        {
            var packed = Convert.FromBase64String(case_.PackedB64);
            Action decode = () => Enumerate(packed);
            decode.Should().Throw<StreamTransportException>($"invalid case {case_.Name} must be rejected by the decoder");
        }

        foreach (var case_ in packRejects)
        {
            var headers = case_.Headers.Select(h => new KeyValuePair<string, string>(h[0], h[1])).ToList();
            Action pack = () => HeaderBlock.Pack(headers);
            pack.Should().Throw<ArgumentException>($"pack-reject case {case_.Name} must be refused by Pack");
        }
    }

    // ------------------------------------------------------------------ routing.json

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Routing_match_golden_file()
    {
        string[] keys =
        {
            "a",
            "order-1",
            "customer:42",
            "é",
            "✓ unicode 🎉",
            string.Empty,
            string.Concat(Enumerable.Repeat("abcdefghij", 30)),
            "3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c",
        };
        int[] partitionCounts = { 1, 2, 3, 4, 7, 8, 16, 64, 100 };

        var routes = new List<RouteCase>();
        foreach (var key in keys)
        {
            foreach (var partitions in partitionCounts)
            {
                // An empty key round-robins when there is more than one partition, so it has no
                // deterministic partition to record. Only the single-partition case is pinned.
                if (key.Length == 0 && partitions != 1)
                {
                    continue;
                }

                var partition = PartitionRouter.ForKey(Encoding.UTF8.GetBytes(key), partitions);
                partition.Should().BeInRange(0, partitions - 1);
                routes.Add(new RouteCase(key, partitions, partition));
            }
        }

        var hashes = keys
            .Where(key => key.Length > 0)
            .Select(key => new XxhCase(key, XxHash3.HashToUInt64(Encoding.UTF8.GetBytes(key)).ToString(CultureInfo.InvariantCulture)))
            .ToList();

        GoldenVectorFiles.Check("routing.json", new RoutingFile(routes, hashes));
    }

    // ------------------------------------------------------------------ keys.json

    private static readonly string[] Topics = { "orders", "a.b-c" };
    private const string Consumer = "svc";
    private const string TestPrefix = "test:re:";

    private static readonly string[] PrefixVariables =
    {
        "STREAMS_KEY_PREFIX", "ASPNETCORE_ENVIRONMENT", "DOTNET_ENVIRONMENT",
    };

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Keys_and_prefix_resolution_match_golden_file()
    {
        var saved = PrefixVariables.ToDictionary(v => v, Environment.GetEnvironmentVariable);
        try
        {
            Environment.SetEnvironmentVariable("STREAMS_KEY_PREFIX", TestPrefix);
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", null);
            Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", null);

            var keys = new List<KeyCase>();
            foreach (var topic in Topics)
            {
                keys.Add(new KeyCase($"{topic}:stream:p0:colocated", StreamKeys.Stream(topic, 0, coLocate: true)));
                keys.Add(new KeyCase($"{topic}:stream:p3:spread", StreamKeys.Stream(topic, 3, coLocate: false)));
                keys.Add(new KeyCase($"{topic}:positions", StreamKeys.Positions(topic, Consumer)));
                keys.Add(new KeyCase($"{topic}:positions-meta", StreamKeys.PositionsMeta(topic, Consumer)));
                keys.Add(new KeyCase($"{topic}:ownership", StreamKeys.Ownership(topic, Consumer)));
                keys.Add(new KeyCase($"{topic}:topic-meta", StreamKeys.TopicMeta(topic)));
                keys.Add(new KeyCase($"{topic}:stream:p0:colocated:ns-np", StreamKeys.Stream(topic, 0, coLocate: true, streamNamespace: "np")));
                keys.Add(new KeyCase($"{topic}:stream:p3:spread:ns-np", StreamKeys.Stream(topic, 3, coLocate: false, streamNamespace: "np")));
                keys.Add(new KeyCase($"{topic}:topic-meta:ns-np", StreamKeys.TopicMeta(topic, streamNamespace: "np")));
                keys.Add(new KeyCase($"{topic}:state", Outbox.StateKey(topic, "es:Order:42")));
                keys.Add(new KeyCase($"{topic}:dedupe", Idempotency.Key(topic, "evt-1")));
            }

            var prefixCases = new List<PrefixCase>
            {
                PrefixFor("aspnetcore-development", aspnet: "Development", dotnet: null, streams: null),
                PrefixFor("dotnet-qa-only", aspnet: null, dotnet: "QA", streams: null),
                PrefixFor("both-set-aspnetcore-wins", aspnet: "Staging", dotnet: "QA", streams: null),
                PrefixFor("none-is-production", aspnet: null, dotnet: null, streams: null),
                PrefixFor("override-verbatim", aspnet: "Development", dotnet: null, streams: "custom:"),
                PrefixFor("blank-override-ignored", aspnet: "Development", dotnet: null, streams: "   "),
            };

            GoldenVectorFiles.Check("keys.json", new KeysFile(TestPrefix, keys, prefixCases));
        }
        finally
        {
            foreach (var pair in saved)
            {
                Environment.SetEnvironmentVariable(pair.Key, pair.Value);
            }
        }
    }

    private static PrefixCase PrefixFor(string name, string? aspnet, string? dotnet, string? streams)
    {
        var saved = PrefixVariables.ToDictionary(v => v, Environment.GetEnvironmentVariable);
        try
        {
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", aspnet);
            Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", dotnet);
            Environment.SetEnvironmentVariable("STREAMS_KEY_PREFIX", streams);

            return new PrefixCase(
                name,
                new PrefixEnv(aspnet, dotnet, streams),
                KeyNamespace.Prefix());
        }
        finally
        {
            foreach (var pair in saved)
            {
                Environment.SetEnvironmentVariable(pair.Key, pair.Value);
            }
        }
    }

    // ------------------------------------------------------------------ formats.json

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Text_formats_match_golden_file()
    {
        var streamIdFormat = new[]
        {
            new StreamIdFormatCase(new StreamId(0, 0).Format(), "0", "0"),
            new StreamIdFormatCase(new StreamId(1, 0).Format(), "1", "0"),
            new StreamIdFormatCase(new StreamId(1_726_000_000_000L, 42).Format(), "1726000000000", "42"),
            new StreamIdFormatCase(new StreamId(long.MaxValue, long.MaxValue).Format(), long.MaxValue.ToString(CultureInfo.InvariantCulture), long.MaxValue.ToString(CultureInfo.InvariantCulture)),
        };

        var streamIdParse = new[]
        {
            ParseCase("0-0"),
            ParseCase("1-0"),
            ParseCase("1726000000000-42"),
            ParseCase("9223372036854775807-9223372036854775807"),
            ParseCase("9223372036854775808-0"),
            ParseCase("18446744073709551615-0"),
            ParseCase(string.Empty),
            ParseCase("-"),
            ParseCase("1-"),
            ParseCase("-1"),
            ParseCase("-1-2"),
            ParseCase("1"),
            ParseCase("1-2-3"),
            ParseCase("a-1"),
            ParseCase("1-b"),
            ParseCase(" 1-2"),
            ParseCase("+1-2"),
            ParseCase("1--2"),
            ParseCase("1- 2"),
        };

        var admin = StreamAdminIdentity();
        var guid = new Guid("3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c");
        var positionValues = new[]
        {
            new PositionValueCase(new StreamId(1_726_000_000_000L, 42).Format(), guid.ToString("D", CultureInfo.InvariantCulture), PositionRecord.Format(new StreamId(1_726_000_000_000L, 42), guid)),
            new PositionValueCase(StreamId.Min.Format(), admin, PositionRecord.Format(StreamId.Min, RedisPositionStore.AdminInstanceId)),
        };

        var issued = new DateTimeOffset(2026, 10, 25, 12, 0, 0, TimeSpan.Zero).AddTicks(1_234_567);
        var target = new StreamId(1_726_000_000_000L, 42);
        var resetMarker = new ResetMarkerCase(
            target.Format(),
            O(issued),
            StreamAdmin.ResetMarkerField(3),
            StreamAdmin.FormatResetMarker(target, issued));

        var resetParse = new[]
        {
            ResetParse(resetMarker.Value),
            ResetParse("1726000000000-42|2026-10-25T14:00:00.5000000+02:00"),
            ResetParse("5-0"),
        };

        var owners = new[]
        {
            new OwnerCase(guid.ToString("D", CultureInfo.InvariantCulture), "orders-consumer-0", new PartitionOwner(0, guid, "orders-consumer-0").Format()),
            new OwnerCase(guid.ToString("D", CultureInfo.InvariantCulture), string.Empty, new PartitionOwner(0, guid, string.Empty).Format()),
        };

        var dates = new[]
        {
            DateOf(DateTimeOffset.UnixEpoch),
            DateOf(new DateTimeOffset(2026, 10, 25, 12, 0, 0, TimeSpan.Zero)),
            DateOf(new DateTimeOffset(2026, 10, 25, 12, 0, 0, TimeSpan.Zero).AddTicks(1_234_567)),
            DateOf(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.FromHours(5.5))),
            DateOf(new DateTimeOffset(2026, 3, 29, 1, 30, 0, TimeSpan.FromHours(-8))),
        };

        GoldenVectorFiles.Check(
            "formats.json",
            new FormatsFile(streamIdFormat, streamIdParse, positionValues, resetMarker, resetParse, owners, admin, dates));
    }

    // ------------------------------------------------------------------ helpers

    private static string StreamAdminIdentity()
        => RedisPositionStore.AdminInstanceId.ToString("D", CultureInfo.InvariantCulture);

    private static StreamIdParseCase ParseCase(string input)
    {
        var ok = StreamId.TryParse(input.AsSpan(), out var id);
        return new StreamIdParseCase(input, ok, ok ? id.Ms.ToString(CultureInfo.InvariantCulture) : null, ok ? id.Seq.ToString(CultureInfo.InvariantCulture) : null);
    }

    private static ResetParseCase ResetParse(string value)
    {
        var ok = StreamAdmin.TryParseResetMarker(value.AsSpan(), out var target, out var issued);
        return new ResetParseCase(value, ok, ok ? target.Format() : null, ok ? O(issued) : null);
    }

    private static DateCase DateOf(DateTimeOffset value)
        => new(value.Ticks.ToString(CultureInfo.InvariantCulture), (int)value.Offset.TotalMinutes, O(value));

    private static string O(DateTimeOffset value) => value.ToString("O", CultureInfo.InvariantCulture);

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string B64(string text) => Convert.ToBase64String(Bytes(text));

    private static string B64Ascii(string text) => Convert.ToBase64String(Encoding.ASCII.GetBytes(text));

    private static HeaderCase HeaderCaseFor(string name, string[][] headers)
    {
        var pairs = headers.Select(h => new KeyValuePair<string, string>(h[0], h[1])).ToList();
        var packed = HeaderBlock.Pack(pairs).Packed.ToArray();
        return new HeaderCase(name, headers, Convert.ToBase64String(packed));
    }

    private static List<(string Key, string Value)> Enumerate(byte[] packed)
    {
        var result = new List<(string Key, string Value)>();
        foreach (var header in HeaderBlock.FromPacked(packed))
        {
            result.Add((header.Key, header.Value));
        }

        return result;
    }

    private static NameValueEntry[] EncodeEntry(EntryInput input)
    {
        var body = Convert.FromBase64String(input.BodyB64);
        var headers = input.Headers?
            .Select(h => new KeyValuePair<string, string>(h[0], h[1]))
            .ToList();

        return input.State
            ? EntryCodec.EncodeState(body, input.Type, input.CorrelationId, input.TraceParent, headers)
            : EntryCodec.Encode(body, input.Type, input.PartitionKey, input.CorrelationId, input.TraceParent, headers);
    }

    private static string[][] ToFields(NameValueEntry[] entries)
        => entries
            .Select(e => new[]
            {
                (string?)e.Name ?? throw new InvalidOperationException("field name must be a string"),
                Convert.ToBase64String((byte[]?)e.Value ?? Array.Empty<byte>()),
            })
            .ToArray();

    // ------------------------------------------------------------------ file shapes

    private sealed record EntryInput(
        [property: JsonPropertyName("body_b64")] string BodyB64,
        [property: JsonPropertyName("type")] string Type,
        [property: JsonPropertyName("partitionKey")] string? PartitionKey,
        [property: JsonPropertyName("correlationId")] string? CorrelationId,
        [property: JsonPropertyName("traceParent")] string? TraceParent,
        [property: JsonPropertyName("headers")] string[][]? Headers,
        [property: JsonPropertyName("state")] bool State);

    private sealed record EntryCase(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("input")] EntryInput Input,
        [property: JsonPropertyName("fields")] string[][] Fields);

    private sealed record HeaderCase(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("headers")] string[][] Headers,
        [property: JsonPropertyName("packed_b64")] string PackedB64);

    private sealed record InvalidHeaderCase(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("packed_b64")] string PackedB64);

    private sealed record PackRejectCase(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("headers")] string[][] Headers);

    private sealed record HeadersFile(
        [property: JsonPropertyName("valid")] IReadOnlyList<HeaderCase> Valid,
        [property: JsonPropertyName("invalid")] IReadOnlyList<InvalidHeaderCase> Invalid,
        [property: JsonPropertyName("pack_rejects")] IReadOnlyList<PackRejectCase> PackRejects);

    private sealed record RouteCase(
        [property: JsonPropertyName("key")] string Key,
        [property: JsonPropertyName("partitions")] int Partitions,
        [property: JsonPropertyName("partition")] int Partition);

    private sealed record XxhCase(
        [property: JsonPropertyName("key")] string Key,
        [property: JsonPropertyName("hash_u64_decimal_string")] string Hash);

    private sealed record RoutingFile(
        [property: JsonPropertyName("routes")] IReadOnlyList<RouteCase> Routes,
        [property: JsonPropertyName("xxh3")] IReadOnlyList<XxhCase> Xxh3);

    private sealed record KeyCase(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("key")] string Key)
    {
        public KeyCase(string name, RedisKey key)
            : this(name, (string)key!)
        {
        }
    }

    private sealed record PrefixEnv(
        [property: JsonPropertyName("ASPNETCORE_ENVIRONMENT")] string? AspNetCore,
        [property: JsonPropertyName("DOTNET_ENVIRONMENT")] string? DotNet,
        [property: JsonPropertyName("STREAMS_KEY_PREFIX")] string? StreamsKeyPrefix);

    private sealed record PrefixCase(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("env")] PrefixEnv Env,
        [property: JsonPropertyName("prefix")] string Prefix);

    private sealed record KeysFile(
        [property: JsonPropertyName("prefix")] string Prefix,
        [property: JsonPropertyName("keys")] IReadOnlyList<KeyCase> Keys,
        [property: JsonPropertyName("prefix_resolution")] IReadOnlyList<PrefixCase> PrefixResolution);

    private sealed record StreamIdFormatCase(
        [property: JsonPropertyName("formatted")] string Formatted,
        [property: JsonPropertyName("ms")] string Ms,
        [property: JsonPropertyName("seq")] string Seq);

    private sealed record StreamIdParseCase(
        [property: JsonPropertyName("input")] string Input,
        [property: JsonPropertyName("valid")] bool Valid,
        [property: JsonPropertyName("ms")] string? Ms,
        [property: JsonPropertyName("seq")] string? Seq);

    private sealed record PositionValueCase(
        [property: JsonPropertyName("id")] string Id,
        [property: JsonPropertyName("instanceId")] string InstanceId,
        [property: JsonPropertyName("value")] string Value);

    private sealed record ResetMarkerCase(
        [property: JsonPropertyName("target")] string Target,
        [property: JsonPropertyName("issuedUtc")] string IssuedUtc,
        [property: JsonPropertyName("field")] string Field,
        [property: JsonPropertyName("value")] string Value);

    private sealed record ResetParseCase(
        [property: JsonPropertyName("value")] string Value,
        [property: JsonPropertyName("valid")] bool Valid,
        [property: JsonPropertyName("target")] string? Target,
        [property: JsonPropertyName("issuedUtc")] string? IssuedUtc);

    private sealed record OwnerCase(
        [property: JsonPropertyName("instanceId")] string InstanceId,
        [property: JsonPropertyName("pod")] string Pod,
        [property: JsonPropertyName("value")] string Value);

    private sealed record DateCase(
        [property: JsonPropertyName("ticks")] string Ticks,
        [property: JsonPropertyName("offsetMinutes")] int OffsetMinutes,
        [property: JsonPropertyName("o")] string O);

    private sealed record FormatsFile(
        [property: JsonPropertyName("stream_id_format")] IReadOnlyList<StreamIdFormatCase> StreamIdFormat,
        [property: JsonPropertyName("stream_id_parse")] IReadOnlyList<StreamIdParseCase> StreamIdParse,
        [property: JsonPropertyName("position_value")] IReadOnlyList<PositionValueCase> PositionValue,
        [property: JsonPropertyName("reset_marker")] ResetMarkerCase ResetMarker,
        [property: JsonPropertyName("reset_marker_parse")] IReadOnlyList<ResetParseCase> ResetMarkerParse,
        [property: JsonPropertyName("owner_value")] IReadOnlyList<OwnerCase> Owner,
        [property: JsonPropertyName("admin_instance_id")] string AdminInstanceId,
        [property: JsonPropertyName("datetime_offset_o")] IReadOnlyList<DateCase> DateTimeOffsets);
}
