using System.Text;
using FluentAssertions;
using RedisEvents.Errors;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// Wire-format tests: stream ids, entry/header codec, partition routing and key layout.
/// These lock down every byte the library puts in Redis (plan 01-wire-format-and-topology.md).
/// </summary>
public class WireTests
{
    // ---------------------------------------------------------------- StreamId

    [Theory]
    [InlineData(0L, 0L)]
    [InlineData(1L, 0L)]
    [InlineData(1_726_000_000_000L, 42L)]
    [InlineData(long.MaxValue, 0L)]
    [InlineData(0L, long.MaxValue)]
    [InlineData(long.MaxValue, long.MaxValue)]
    [Trait("TestType", "UnitTest")]
    public void StreamId_FormatParse_RoundTrips(long ms, long seq)
    {
        var id = new StreamId(ms, seq);

        var text = id.Format();
        var parsed = StreamId.Parse(text);

        parsed.Should().Be(id);
        text.Should().Be($"{ms}-{seq}");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamId_Min_IsZeroDashZero()
    {
        StreamId.Min.Format().Should().Be("0-0");
        StreamId.Parse("0-0").Should().Be(StreamId.Min);
        StreamId.Min.Ms.Should().Be(0);
        StreamId.Min.Seq.Should().Be(0);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamId_Max_RoundTripsAtLongMaxValue()
    {
        var text = StreamId.Max.Format();

        text.Should().Be($"{long.MaxValue}-{long.MaxValue}");
        StreamId.Parse(text).Should().Be(StreamId.Max);
        StreamId.Max.Should().BeGreaterThan(new StreamId(long.MaxValue, long.MaxValue - 1));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamId_ToString_MatchesFormat()
    {
        var id = new StreamId(1234, 5);
        id.ToString().Should().Be(id.Format()).And.Be("1234-5");
    }

    [Theory]
    [InlineData("")]
    [InlineData("123")]           // missing '-'
    [InlineData("-5")]            // no ms component
    [InlineData("5-")]            // no seq component
    [InlineData("abc-1")]         // non-numeric ms
    [InlineData("1-abc")]         // non-numeric seq
    [InlineData("1-2-3")]         // trailing junk after seq
    [InlineData(" 1-2")]          // leading whitespace is not allowed (NumberStyles.None)
    [InlineData("+1-2")]          // sign is not allowed
    [InlineData("-1--2")]
    [InlineData("99999999999999999999-0")] // overflows long
    [Trait("TestType", "UnitTest")]
    public void StreamId_Parse_Rejects_MalformedText(string text)
    {
        StreamId.TryParse(text, out _).Should().BeFalse();
        var act = () => StreamId.Parse(text);
        act.Should().Throw<FormatException>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamId_FromDate_MatchesUnixMilliseconds()
    {
        var when = new DateTimeOffset(2026, 9, 6, 11, 22, 33, 444, TimeSpan.Zero);

        var id = StreamId.FromDate(when);

        id.Ms.Should().Be(when.ToUnixTimeMilliseconds());
        id.Seq.Should().Be(0);
        id.Timestamp.Should().Be(when);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamId_FromDate_IgnoresOffset_AndUsesInstant()
    {
        var utc = new DateTimeOffset(2026, 9, 6, 0, 0, 0, TimeSpan.Zero);
        var sameInstantElsewhere = utc.ToOffset(TimeSpan.FromHours(10));

        StreamId.FromDate(sameInstantElsewhere).Should().Be(StreamId.FromDate(utc));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamId_Ordering_SortsByMsThenSeq()
    {
        var ids = new[]
        {
            new StreamId(2, 0),
            new StreamId(1, 9),
            new StreamId(1, 0),
            new StreamId(1, 10),   // same ms, higher seq
            StreamId.Max,
            StreamId.Min,
        };

        var sorted = ids.OrderBy(x => x).ToArray();

        sorted.Should().ContainInOrder(
            StreamId.Min,
            new StreamId(1, 0),
            new StreamId(1, 9),
            new StreamId(1, 10),
            new StreamId(2, 0),
            StreamId.Max);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamId_SameMillisecond_ComparesBySequence()
    {
        var earlier = new StreamId(1_726_000_000_000L, 1);
        var later = new StreamId(1_726_000_000_000L, 2);

        earlier.CompareTo(later).Should().BeNegative();
        (earlier < later).Should().BeTrue();
        (earlier <= later).Should().BeTrue();
        (later > earlier).Should().BeTrue();
        (later >= earlier).Should().BeTrue();
        var same = new StreamId(1_726_000_000_000L, 1);
        earlier.CompareTo(same).Should().Be(0);
        (earlier <= same).Should().BeTrue();
        (earlier >= same).Should().BeTrue();
    }

    // -------------------------------------------------------------- EntryCodec

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void EntryCodec_EncodeDecode_RoundTripsEveryField()
    {
        var body = Encoding.UTF8.GetBytes("{\"betId\":\"abc\"}");
        var headers = new List<KeyValuePair<string, string>>
        {
            new("brand", "orange"),
            new("attempt", "3"),
        };

        var entries = EntryCodec.Encode(
            body,
            type: "Orange.Contracts.BetPlaced",
            partitionKey: "cust-42",
            correlationId: "corr-99",
            traceParent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            headers: headers);

        var id = new StreamId(1_726_000_000_000L, 7);
        var msg = EntryCodec.Decode(id, entries, partition: 3);

        msg.Body.ToArray().Should().Equal(body);
        msg.Type.Should().Be("Orange.Contracts.BetPlaced");
        msg.PartitionKey.Should().Be("cust-42");
        msg.CorrelationId.Should().Be("corr-99");
        msg.TraceParent.Should().Be("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        msg.Id.Should().Be(id);
        msg.Partition.Should().Be(3);
        msg.EnqueuedTime.Should().Be(id.Timestamp);
        msg.Headers.ToDictionary().Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["brand"] = "orange",
            ["attempt"] = "3",
        });
    }

    /// <summary>
    /// <c>TraceParent</c> is a positional parameter, not an initialiser: the codec must pass it
    /// through the constructor for the processor to be able to restore the activity.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamMsg_PositionalConstructor_CarriesTraceParentAndComputesEnqueuedTime()
    {
        var id = new StreamId(1_726_000_000_123L, 4);

        var msg = new StreamMsg(
            ReadOnlyMemory<byte>.Empty,
            "T",
            id,
            2,
            "cust-1",
            "corr-1",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            HeaderBlock.Empty);

        msg.TraceParent.Should().Be("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        msg.PartitionKey.Should().Be("cust-1");
        msg.CorrelationId.Should().Be("corr-1");
        msg.Partition.Should().Be(2);

        // EnqueuedTime is computed from the id, never stored — this guards against it coming back.
        msg.EnqueuedTime.Should().Be(id.Timestamp);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void EntryCodec_Encode_AlwaysWritesBodyTypeAndKey()
    {
        var entries = EntryCodec.Encode(default, type: "T");

        entries.Should().HaveCount(3);
        entries.Select(e => (string?)e.Name).Should().BeEquivalentTo(
            [EntryCodec.BodyField, EntryCodec.TypeField, EntryCodec.PartitionKeyField]);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void EntryCodec_EmptyBody_RoundTrips()
    {
        var entries = EntryCodec.Encode(ReadOnlyMemory<byte>.Empty, type: "T");

        var msg = EntryCodec.Decode(StreamId.Min, entries, partition: 0);

        msg.Body.Length.Should().Be(0);
        msg.BodySpan.Length.Should().Be(0);
        msg.Type.Should().Be("T");
        msg.PartitionKey.Should().BeEmpty();
        msg.CorrelationId.Should().BeEmpty();
        msg.TraceParent.Should().BeNull();
        msg.Headers.IsEmpty.Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void EntryCodec_OneMebibyteBody_RoundTripsByteForByte()
    {
        var body = new byte[1024 * 1024];
        new Random(1234).NextBytes(body);

        var entries = EntryCodec.Encode(body, type: "Big");
        var msg = EntryCodec.Decode(new StreamId(9, 9), entries, partition: 1);

        msg.Body.Length.Should().Be(body.Length);
        msg.Body.Span.SequenceEqual(body).Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void EntryCodec_NoHeaders_OmitsHeaderField()
    {
        var entries = EntryCodec.Encode(Encoding.UTF8.GetBytes("x"), "T", headers: []);

        entries.Should().NotContain(e => (string?)e.Name == EntryCodec.HeadersField);
        EntryCodec.Decode(StreamId.Min, entries, 0).Headers.CountHeaders().Should().Be(0);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void EntryCodec_SingleHeader_RoundTrips()
    {
        var entries = EntryCodec.Encode(
            Encoding.UTF8.GetBytes("x"),
            "T",
            headers: [new KeyValuePair<string, string>("only", "one")]);

        var headers = EntryCodec.Decode(StreamId.Min, entries, 0).Headers;

        headers.CountHeaders().Should().Be(1);
        headers.ContainsKey("only").Should().BeTrue();
        headers.GetValueOrDefault("only").Should().Be("one");
        headers.GetValueOrDefault("missing").Should().BeNull();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void EntryCodec_SixtyFourHeaders_RoundTrip()
    {
        var headers = Enumerable.Range(0, HeaderBlock.MaxCount)
            .Select(i => new KeyValuePair<string, string>($"k{i}", $"v{i}"))
            .ToList();

        var entries = EntryCodec.Encode(Encoding.UTF8.GetBytes("x"), "T", headers: headers);
        var block = EntryCodec.Decode(StreamId.Min, entries, 0).Headers;

        block.CountHeaders().Should().Be(HeaderBlock.MaxCount);
        block.ToDictionary().Should().BeEquivalentTo(headers.ToDictionary(h => h.Key, h => h.Value));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void HeaderBlock_Pack_OverCountCap_Throws()
    {
        var headers = Enumerable.Range(0, HeaderBlock.MaxCount + 1)
            .Select(i => new KeyValuePair<string, string>($"k{i}", $"v{i}"))
            .ToList();

        var act = () => HeaderBlock.Pack(headers);

        act.Should().Throw<ArgumentException>().WithMessage("*64*");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void HeaderBlock_Pack_OverByteCap_Throws()
    {
        // 8 headers x ~1 KiB values comfortably exceeds the 8 KiB block cap.
        var headers = Enumerable.Range(0, 9)
            .Select(i => new KeyValuePair<string, string>($"k{i}", new string('v', 1024)))
            .ToList();

        var act = () => HeaderBlock.Pack(headers);

        act.Should().Throw<ArgumentException>().WithMessage($"*{HeaderBlock.MaxBytes}*");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void HeaderBlock_Pack_NullKeyOrValue_Throws()
    {
        var nullValue = () => HeaderBlock.Pack([new KeyValuePair<string, string>("k", null!)]);
        var nullKey = () => HeaderBlock.Pack([new KeyValuePair<string, string>(null!, "v")]);

        nullValue.Should().Throw<ArgumentException>();
        nullKey.Should().Throw<ArgumentException>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void HeaderBlock_Pack_Null_IsEmpty()
    {
        HeaderBlock.Pack(null).IsEmpty.Should().BeTrue();
        HeaderBlock.Pack([]).IsEmpty.Should().BeTrue();
        HeaderBlock.Empty.CountHeaders().Should().Be(0);
        HeaderBlock.Empty.ToString().Should().BeEmpty();
        HeaderBlock.Empty.TryGetValue("anything", out var missing).Should().BeFalse();
        missing.Should().BeEmpty();
    }

    /// <summary>
    /// The packing format is <c>len ':' bytes</c>, so a payload that itself contains ':' or starts
    /// with digits is the sharpest edge in the codec — the length prefix must win over the content.
    /// </summary>
    [Theory]
    [InlineData("a:b", "c:d")]
    [InlineData(":", ":")]
    [InlineData("::::", "5:notalength")]
    [InlineData("12:34", "56:78")]
    [InlineData("3:abc", "3:abc")]                     // looks exactly like a well-formed field
    [InlineData("0:", "0:")]
    [InlineData("999999999:x", "1:y")]                 // a length prefix that would run off the end
    [InlineData("42", "7")]                            // pure digits, no delimiter
    [InlineData("", "")]                               // empty key and value
    [InlineData("", "value-for-empty-key")]
    [InlineData("key-with-empty-value", "")]
    [InlineData("héllo:wörld", "ünïcode:2")]           // multi-byte UTF-8 around a delimiter
    [Trait("TestType", "UnitTest")]
    public void HeaderBlock_DelimiterAndDigitsInKeysAndValues_RoundTrip(string key, string value)
    {
        var block = HeaderBlock.Pack([new KeyValuePair<string, string>(key, value)]);

        var decoded = HeaderBlock.FromPacked(block.Packed);

        decoded.CountHeaders().Should().Be(1);
        decoded.TryGetValue(key, out var read).Should().BeTrue();
        read.Should().Be(value);
        decoded.ToDictionary().Should().BeEquivalentTo(new Dictionary<string, string> { [key] = value });
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void HeaderBlock_AdversarialHeaders_SurviveTheFullEntryRoundTrip()
    {
        var headers = new List<KeyValuePair<string, string>>
        {
            new("3:abc", "3:abc"),
            new("", ""),
            new("12", "34:56"),
            new(":leading", "trailing:"),
        };

        var entries = EntryCodec.Encode(Encoding.UTF8.GetBytes("body"), "T", headers: headers);
        var block = EntryCodec.Decode(new StreamId(5, 5), entries, 0).Headers;

        block.CountHeaders().Should().Be(headers.Count);
        block.ToDictionary().Should().BeEquivalentTo(headers.ToDictionary(h => h.Key, h => h.Value));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void HeaderBlock_Enumerator_YieldsKeysAndValuesInOrder()
    {
        var block = HeaderBlock.Pack(
        [
            new KeyValuePair<string, string>("a", "1"),
            new KeyValuePair<string, string>("b", "2"),
        ]);

        var seen = new List<string>();
        foreach (var header in block)
        {
            seen.Add($"{header.Key}={header.Value}");
        }

        seen.Should().ContainInOrder("a=1", "b=2");
        block.ToString().Should().Be("a=1; b=2");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void HeaderBlock_Malformed_Throws()
    {
        // A key field with no matching value.
        var truncated = HeaderBlock.FromPacked(Encoding.UTF8.GetBytes("1:a"));
        var act = () => truncated.CountHeaders();
        act.Should().Throw<StreamTransportException>();

        // A non-digit length prefix.
        var garbage = HeaderBlock.FromPacked(Encoding.UTF8.GetBytes("x:a1:b"));
        var act2 = () => garbage.CountHeaders();
        act2.Should().Throw<StreamTransportException>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void HeaderBlock_Equality_IsByPackedBytes()
    {
        var a = HeaderBlock.Pack([new KeyValuePair<string, string>("k", "v")]);
        var b = HeaderBlock.Pack([new KeyValuePair<string, string>("k", "v")]);
        var c = HeaderBlock.Pack([new KeyValuePair<string, string>("k", "w")]);

        (a == b).Should().BeTrue();
        a.GetHashCode().Should().Be(b.GetHashCode());
        (a != c).Should().BeTrue();
        a.Equals((object)b).Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void EntryCodec_Decode_UnknownCodecVersion_Throws()
    {
        var entries = EntryCodec.Encode(Encoding.UTF8.GetBytes("x"), "T");

        var act = () => EntryCodec.Decode(StreamId.Min, entries, 0, codecVersion: 2);

        act.Should().Throw<StreamTransportException>().WithMessage("*version 2*");
        EntryCodec.IsSupportedVersion(2).Should().BeFalse();
        EntryCodec.IsSupportedVersion(EntryCodec.CodecVersion).Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void EntryCodec_Decode_IgnoresUnknownFieldsWithinTheSameVersion()
    {
        var entries = EntryCodec.Encode(Encoding.UTF8.GetBytes("x"), "T")
            .Append(new NameValueEntry("z", "future"))
            .ToArray();

        var msg = EntryCodec.Decode(StreamId.Min, entries, 0);

        msg.Type.Should().Be("T");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void EntryCodec_Encode_NullType_Throws()
    {
        var act = () => EntryCodec.Encode(default, null!);
        act.Should().Throw<ArgumentNullException>();
    }

    // ---------------------------------------------------------- PartitionRouter

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void PartitionRouter_ForKey_IsDeterministic()
    {
        var key = Encoding.UTF8.GetBytes("customer-12345");

        var first = PartitionRouter.ForKey(key, 8);
        for (var i = 0; i < 1000; i++)
        {
            PartitionRouter.ForKey(key, 8).Should().Be(first);
        }

        // A different key count changes the answer, but the same key/count pair never does.
        PartitionRouter.ForKey(Encoding.UTF8.GetBytes("customer-12345"), 8).Should().Be(first);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-4)]
    [Trait("TestType", "UnitTest")]
    public void PartitionRouter_SinglePartition_AlwaysZero(int partitions)
    {
        PartitionRouter.ForKey(Encoding.UTF8.GetBytes("anything"), partitions).Should().Be(0);
        PartitionRouter.ForKey([], partitions).Should().Be(0);

        var counter = 0u;
        for (var i = 0; i < 10; i++)
        {
            PartitionRouter.RoundRobin(ref counter, partitions).Should().Be(0);
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(3)]   // non power of two takes the modulo path
    [InlineData(10)]
    [Trait("TestType", "UnitTest")]
    public void PartitionRouter_ForKey_StaysInRange(int partitions)
    {
        for (var i = 0; i < 5_000; i++)
        {
            var p = PartitionRouter.ForKey(Encoding.UTF8.GetBytes($"key-{i}"), partitions);
            p.Should().BeInRange(0, partitions - 1);
        }
    }

    [Theory]
    [InlineData(8)]
    [InlineData(16)]
    [Trait("TestType", "UnitTest")]
    public void PartitionRouter_ForKey_DistributesWithinTenPercent(int partitions)
    {
        const int keys = 100_000;
        var counts = new int[partitions];

        for (var i = 0; i < keys; i++)
        {
            counts[PartitionRouter.ForKey(Encoding.UTF8.GetBytes($"customer-{i:D6}"), partitions)]++;
        }

        var expected = (double)keys / partitions;
        var tolerance = expected * 0.10;

        counts.Should().OnlyContain(c => c > 0);
        foreach (var count in counts)
        {
            Math.Abs(count - expected).Should().BeLessThan(tolerance,
                "partition counts must stay within 10% of {0}", expected);
        }
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void PartitionRouter_RoundRobin_SpreadsEvenlyAndNeverGoesNegative()
    {
        const int partitions = 8;
        var counter = 0u;
        var counts = new int[partitions];

        for (var i = 0; i < 8_000; i++)
        {
            var p = PartitionRouter.RoundRobin(ref counter, partitions);
            p.Should().BeInRange(0, partitions - 1);
            counts[p]++;
        }

        counts.Should().OnlyContain(c => c == 1000);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void PartitionRouter_RoundRobin_WrapsAtUintOverflow()
    {
        const int partitions = 8;
        var counter = uint.MaxValue - 2;

        var observed = new List<int>();
        for (var i = 0; i < 6; i++)
        {
            var p = PartitionRouter.RoundRobin(ref counter, partitions);
            p.Should().BeGreaterThanOrEqualTo(0).And.BeLessThan(partitions);
            observed.Add(p);
        }

        // uint.MaxValue is 0xFFFFFFFF, so the sequence steps ...FE, FF, 00, 01, 02, 03 -> 6,7,0,1,2,3.
        observed.Should().ContainInOrder(6, 7, 0, 1, 2, 3);
        counter.Should().Be(3);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void PartitionRouter_RoundRobin_NonPowerOfTwo_NeverNegativeAcrossOverflow()
    {
        const int partitions = 10;
        var counter = uint.MaxValue - 3;

        for (var i = 0; i < 20; i++)
        {
            PartitionRouter.RoundRobin(ref counter, partitions).Should().BeInRange(0, partitions - 1);
        }
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void PartitionRouter_ForKey_EmptyKey_IsStableButNotSpread()
    {
        // An empty key hashes like any other span; spreading empty keys is the publisher's job
        // (RoundRobin), not ForKey's.
        var first = PartitionRouter.ForKey([], 8);
        PartitionRouter.ForKey([], 8).Should().Be(first);
        first.Should().BeInRange(0, 7);
    }

    // ----------------------------------------------------------------- StreamKeys

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamKeys_UseLiteralBracesAroundTheTopicAsHashTag()
    {
        ((string?)StreamKeys.Stream("offer_odds", 0)).Should().Be("s:{offer_odds}:0");
        ((string?)StreamKeys.Stream("offer_odds", 15)).Should().Be("s:{offer_odds}:15");
        ((string?)StreamKeys.Positions("offer_odds", "offer-products")).Should().Be("p:{offer_odds}:offer-products");
        ((string?)StreamKeys.PositionsMeta("offer_odds", "offer-products")).Should().Be("p:{offer_odds}:offer-products:meta");
        ((string?)StreamKeys.Ownership("offer_odds", "offer-products")).Should().Be("o:{offer_odds}:offer-products");
        ((string?)StreamKeys.TopicMeta("offer_odds")).Should().Be("m:{offer_odds}");
        StreamKeys.PositionsPattern("offer_odds").Should().Be("p:{offer_odds}:*");
        StreamKeys.StreamPattern("offer_odds").Should().Be("s:{offer_odds}:*");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamKeys_CoLocateFalse_DropsTheHashTag()
    {
        ((string?)StreamKeys.Stream("offer_odds", 3, coLocate: false)).Should().Be("s:offer_odds:3");
        StreamKeys.StreamPattern("offer_odds", coLocate: false).Should().Be("s:offer_odds:*");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamKeys_ColocatedKeysShareOneHashTag()
    {
        var stream = (string?)StreamKeys.Stream("bet_state", 1) ?? string.Empty;
        var positions = (string?)StreamKeys.Positions("bet_state", "bet-processing") ?? string.Empty;
        var ownership = (string?)StreamKeys.Ownership("bet_state", "bet-processing") ?? string.Empty;
        var meta = (string?)StreamKeys.TopicMeta("bet_state") ?? string.Empty;

        static string Tag(string key) => key[(key.IndexOf('{') + 1)..key.IndexOf('}')];

        Tag(stream).Should().Be("bet_state");
        Tag(positions).Should().Be("bet_state");
        Tag(ownership).Should().Be("bet_state");
        Tag(meta).Should().Be("bet_state");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void StreamKeys_LongTopicName_ExceedingTheStackBuffer_StillBuilds()
    {
        var topic = new string('t', 400);

        ((string?)StreamKeys.Stream(topic, 7)).Should().Be($"s:{{{topic}}}:7");
    }
}
