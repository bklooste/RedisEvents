using FluentAssertions;

using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Errors;

using StackExchange.Redis;

namespace Orange.Lib.Streams.UnitTests;

/// <summary>
/// R-21 (P3 test gap 3). <see cref="BlockFetch"/>'s hand-written <c>XREAD</c> reply parser had no
/// unit tests at all, despite being the highest-risk code in the library: it is the only place the
/// library decodes RESP itself, it runs on the default read path, and every mistake in it is silent
/// — a mis-parsed reply either loses entries or advances a cursor past entries nobody handled.
/// </summary>
/// <remarks>
/// <para>
/// The reply shapes are built by hand from <see cref="RedisResult"/> rather than captured from a
/// server, so both wire layouts are covered from one test assembly: the RESP2
/// <c>[[key, entries], …]</c> nesting and the flattened RESP3 map <c>key, entries, key, entries…</c>
/// that a connection negotiating <c>HELLO 3</c> receives. The fixture is <c>redis:8</c> on RESP2,
/// so the RESP3 branch of <c>StreamWalk</c> is unreachable from the service tests and this is the
/// only coverage it has.
/// </para>
/// <para>
/// Every "malformed" case asserts a throw rather than an empty batch on purpose. An empty batch
/// leaves the read cursor where it was, and the read loop comes straight back around: a silent
/// mis-parse is an infinite spin at full speed against a stream that has data.
/// </para>
/// </remarks>
public class BlockFetchParseTests
{
    private static byte[] Bytes(string key) => (byte[]?)(RedisValue)key ?? [];

    // ------------------------------------------------------------------ Parse: idle and empty

    /// <summary>The ordinary idle path: <c>BLOCK</c> expired, Redis sent a nil reply.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_nil_reply_is_the_idle_case_not_an_error()
    {
        BlockFetch.Parse(null, Bytes("s:t:0")).IsEmpty.Should().BeTrue();
        BlockFetch.Parse(Reply.Nil, Bytes("s:t:0")).IsEmpty.Should().BeTrue();
        BlockFetch.Parse(Reply.Array(), Bytes("s:t:0")).IsEmpty.Should().BeTrue("some server versions send an empty array where others send nil");
        BlockFetch.Parse(Reply.Value("+OK"), Bytes("s:t:0")).IsEmpty.Should().BeTrue("a non-array reply carries no entries");
    }

    /// <summary>A stream that is present in the reply but carries no entries is not an error either.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_stream_with_an_empty_entries_array_parses_as_empty()
    {
        var reply = Reply.Array(Reply.Resp2Stream("s:t:0", Reply.Array()));

        BlockFetch.Parse(reply, Bytes("s:t:0")).IsEmpty.Should().BeTrue();
    }

    // ------------------------------------------------------------------ Parse: RESP2 / RESP3

    /// <summary>The RESP2 layout: entries, ids and field/value pairs all survive the walk.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_resp2_reply_yields_every_entry_with_its_fields()
    {
        var reply = Reply.Array(
            Reply.Resp2Stream(
                "s:t:0",
                Reply.Array(
                    Reply.Entry("100-0", ("b", "one"), ("t", "Order")),
                    Reply.Entry("100-1", ("b", "two"), ("t", "Order")))));

        var batch = BlockFetch.Parse(reply, Bytes("s:t:0"));

        batch.Count.Should().Be(2);
        batch.Span[0].Id.Should().Be((RedisValue)"100-0");
        batch.Span[1].Id.Should().Be((RedisValue)"100-1");
        batch.Span[0].Values.Should().HaveCount(2);
        batch.Span[0].Values[0].Name.Should().Be((RedisValue)"b");
        batch.Span[0].Values[0].Value.Should().Be((RedisValue)"one");
        batch.Span[1].Values[0].Value.Should().Be((RedisValue)"two");
    }

    /// <summary>
    /// The RESP3 layout — the same content as a flattened map, <c>key, entries, key, entries</c> —
    /// is told apart from RESP2 by shape, and the requested stream is picked out of it.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_resp3_map_reply_is_walked_by_shape_and_the_right_stream_is_picked()
    {
        var reply = Reply.Array(
            Reply.Value("s:t:0"),
            Reply.Array(Reply.Entry("1-0", ("b", "mine"))),
            Reply.Value("s:t:1"),
            Reply.Array(Reply.Entry("2-0", ("b", "theirs"))));

        var batch = BlockFetch.Parse(reply, Bytes("s:t:1"));

        batch.Count.Should().Be(1);
        batch.Span[0].Id.Should().Be((RedisValue)"2-0");
        batch.Span[0].Values[0].Value.Should().Be((RedisValue)"theirs", "the key match must pick the stream, not the position in the reply");
    }

    /// <summary>A RESP3 map whose last key has no value element is malformed, not silently dropped.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_resp3_key_with_no_entries_after_it_throws()
    {
        var reply = Reply.Array(
            Reply.Value("s:t:0"),
            Reply.Array(Reply.Entry("1-0", ("b", "x"))),
            Reply.Value("s:t:1"));

        var act = () => BlockFetch.Parse(reply, Bytes("s:t:0"));

        act.Should().Throw<StreamTransportException>().WithMessage("*no entries element*");
    }

    // ------------------------------------------------------------------ Parse: stream selection

    /// <summary>
    /// A multi-stream reply is matched on key bytes: the requested stream's entries come back even
    /// when it is not the first one in the reply.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_multi_stream_reply_returns_the_requested_stream()
    {
        var reply = Reply.Array(
            Reply.Resp2Stream("s:t:0", Reply.Array(Reply.Entry("1-0", ("b", "zero")))),
            Reply.Resp2Stream("s:t:1", Reply.Array(Reply.Entry("9-9", ("b", "one")))));

        var batch = BlockFetch.Parse(reply, Bytes("s:t:1"));

        batch.Count.Should().Be(1);
        batch.Span[0].Id.Should().Be((RedisValue)"9-9");
    }

    /// <summary>
    /// A single-stream reply is accepted whatever its key text says: a key prefix configured on the
    /// connection rewrites the key on the wire, and one key was requested, so it can only be ours.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_single_stream_reply_is_accepted_even_when_the_key_text_differs()
    {
        var reply = Reply.Array(Reply.Resp2Stream("prefix:s:t:0", Reply.Array(Reply.Entry("4-0", ("b", "x")))));

        BlockFetch.Parse(reply, Bytes("s:t:0")).Count.Should().Be(1);
    }

    /// <summary>
    /// Several streams and none of them ours is not a stream we can attribute. Guessing would record
    /// another partition's last id as this one's position and skip everything in between.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Several_streams_and_none_requested_throws_rather_than_guessing()
    {
        var reply = Reply.Array(
            Reply.Resp2Stream("s:other:0", Reply.Array(Reply.Entry("1-0", ("b", "x")))),
            Reply.Resp2Stream("s:other:1", Reply.Array(Reply.Entry("2-0", ("b", "y")))));

        var act = () => BlockFetch.Parse(reply, Bytes("s:t:0"));

        act.Should().Throw<StreamTransportException>().WithMessage("*2 streams*");
    }

    // ------------------------------------------------------------------ Parse: entry shapes

    /// <summary>
    /// A nil entry — trimmed or deleted between the read and the reply — is dropped, and the entries
    /// around it still arrive.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_nil_entry_is_dropped_and_its_neighbours_survive()
    {
        var reply = Reply.Array(
            Reply.Resp2Stream(
                "s:t:0",
                Reply.Array(
                    Reply.Nil,
                    Reply.Entry("7-0", ("b", "kept")),
                    Reply.Nil)));

        var batch = BlockFetch.Parse(reply, Bytes("s:t:0"));

        batch.Count.Should().Be(1);
        batch.Span[0].Id.Should().Be((RedisValue)"7-0");
    }

    /// <summary>
    /// Every entry nil is different in kind: there is no id to advance the cursor to, so returning an
    /// empty batch would spin the read loop against an unchanged cursor at full speed.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_all_nil_entry_list_throws_because_the_cursor_could_not_advance()
    {
        var reply = Reply.Array(Reply.Resp2Stream("s:t:0", Reply.Array(Reply.Nil, Reply.Nil)));

        var act = () => BlockFetch.Parse(reply, Bytes("s:t:0"));

        act.Should().Throw<StreamTransportException>().WithMessage("*nowhere to advance*");
    }

    /// <summary>An entry that is not <c>[id, fields]</c>, an entry with no id, and an odd field list.</summary>
    [Theory]
    [Trait("TestType", "UnitTest")]
    [InlineData("arity")]
    [InlineData("no-id")]
    [InlineData("odd-fields")]
    [InlineData("pair-arity")]
    public void A_malformed_entry_throws_and_names_what_was_wrong(string shape)
    {
        var entries = shape switch
        {
            "arity" => Reply.Array(Reply.Array(Reply.Value("1-0"), Reply.Array(), Reply.Value("extra"))),
            "no-id" => Reply.Array(Reply.Array(Reply.Nil, Reply.Array(Reply.Value("b"), Reply.Value("x")))),
            _ => Reply.Array(Reply.Array(Reply.Value("1-0"), Reply.Array(Reply.Value("b")))),
        };

        var reply = shape == "pair-arity"
            ? Reply.Array(Reply.Array(Reply.Value("s:t:0"), entries, Reply.Value("surplus")))
            : Reply.Array(Reply.Resp2Stream("s:t:0", entries));

        var act = () => BlockFetch.Parse(reply, Bytes("s:t:0"));

        var thrown = act.Should().Throw<StreamTransportException>().Which;
        thrown.Message.Should().Contain("XREAD reply was not in the expected shape");
    }

    /// <summary>An entry whose field list is nil carries no headers and no body — but it is an entry.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_entry_with_a_nil_field_list_still_arrives()
    {
        var reply = Reply.Array(
            Reply.Resp2Stream("s:t:0", Reply.Array(Reply.Array(Reply.Value("3-0"), Reply.Nil))));

        var batch = BlockFetch.Parse(reply, Bytes("s:t:0"));

        batch.Count.Should().Be(1);
        batch.Span[0].Values.Should().BeEmpty();
    }

    // ------------------------------------------------------------------ ParseAll: the co-located split

    /// <summary>
    /// The multi-stream split is index-aligned by key, not by reply order, and every slice carries
    /// the <em>requested</em> key so the read loop's slot lookup compares like with like.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ParseAll_splits_a_reply_by_key_whatever_order_the_streams_arrive_in()
    {
        RedisKey[] keys = [(RedisKey)"s:t:0", (RedisKey)"s:t:1", (RedisKey)"s:t:2"];
        byte[][] keyBytes = [Bytes("s:t:0"), Bytes("s:t:1"), Bytes("s:t:2")];

        var reply = Reply.Array(
            Reply.Resp2Stream("s:t:2", Reply.Array(Reply.Entry("30-0", ("b", "two")))),
            Reply.Resp2Stream("s:t:0", Reply.Array(Reply.Entry("10-0", ("b", "zero")), Reply.Entry("10-1", ("b", "zero-b")))),
            Reply.Resp2Stream("s:t:1", Reply.Array(Reply.Entry("20-0", ("b", "one")))));

        var slices = BlockFetch.ParseAll(reply, keys, keyBytes);

        slices.Should().HaveCount(3);
        slices[0].Key.Should().Be(keys[2]);
        slices[0].Count.Should().Be(1);
        slices[1].Key.Should().Be(keys[0]);
        slices[1].Count.Should().Be(2, "a stream's own entries must not leak into a neighbour's slice");
        slices[2].Key.Should().Be(keys[1]);
        slices[2].Span[0].Values[0].Value.Should().Be((RedisValue)"one");
    }

    /// <summary>
    /// Streams with nothing new are omitted by the server, and a stream whose entries array is empty
    /// is dropped here — the returned array is trimmed to what actually carried entries.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ParseAll_returns_only_the_streams_that_carried_entries()
    {
        RedisKey[] keys = [(RedisKey)"s:t:0", (RedisKey)"s:t:1"];
        byte[][] keyBytes = [Bytes("s:t:0"), Bytes("s:t:1")];

        var reply = Reply.Array(
            Reply.Resp2Stream("s:t:0", Reply.Array()),
            Reply.Resp2Stream("s:t:1", Reply.Array(Reply.Entry("5-0", ("b", "x")))));

        var slices = BlockFetch.ParseAll(reply, keys, keyBytes);

        slices.Should().HaveCount(1, "an empty slice would be handed to the loop as a partition with no work");
        slices[0].Key.Should().Be(keys[1]);
    }

    /// <summary>The RESP3 map splits the same way; the two layouts share one walk on purpose.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ParseAll_handles_the_resp3_map_layout()
    {
        RedisKey[] keys = [(RedisKey)"s:t:0", (RedisKey)"s:t:1"];
        byte[][] keyBytes = [Bytes("s:t:0"), Bytes("s:t:1")];

        var reply = Reply.Array(
            Reply.Value("s:t:0"),
            Reply.Array(Reply.Entry("1-0", ("b", "zero"))),
            Reply.Value("s:t:1"),
            Reply.Array(Reply.Entry("2-0", ("b", "one"))));

        var slices = BlockFetch.ParseAll(reply, keys, keyBytes);

        slices.Should().HaveCount(2);
        slices[0].Key.Should().Be(keys[0]);
        slices[1].Key.Should().Be(keys[1]);
        slices[1].Span[0].Id.Should().Be((RedisValue)"2-0");
    }

    /// <summary>Nil and empty replies are the idle case for the co-located read too.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ParseAll_treats_a_nil_or_empty_reply_as_idle()
    {
        RedisKey[] keys = [(RedisKey)"s:t:0"];
        byte[][] keyBytes = [Bytes("s:t:0")];

        BlockFetch.ParseAll(null, keys, keyBytes).Should().BeEmpty();
        BlockFetch.ParseAll(Reply.Nil, keys, keyBytes).Should().BeEmpty();
        BlockFetch.ParseAll(Reply.Array(), keys, keyBytes).Should().BeEmpty();
        BlockFetch.ParseAll(Reply.Value("+OK"), keys, keyBytes).Should().BeEmpty();
    }

    /// <summary>
    /// When several keys were requested there is no single-stream rescue to fall back on, so an
    /// unmatched key is carried through with the key exactly as it arrived rather than being
    /// attributed to one of the requested slots.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ParseAll_never_attributes_an_unmatched_key_to_a_requested_slot()
    {
        RedisKey[] keys = [(RedisKey)"s:t:0", (RedisKey)"s:t:1"];
        byte[][] keyBytes = [Bytes("s:t:0"), Bytes("s:t:1")];

        var reply = Reply.Array(
            Reply.Resp2Stream("s:t:0", Reply.Array(Reply.Entry("1-0", ("b", "mine")))),
            Reply.Resp2Stream("s:elsewhere:4", Reply.Array(Reply.Entry("999-0", ("b", "foreign")))));

        var slices = BlockFetch.ParseAll(reply, keys, keyBytes);

        slices.Should().HaveCount(2);
        slices[0].Key.Should().Be(keys[0]);
        slices[1].Key.Should().Be((RedisKey)"s:elsewhere:4", "the foreign key is reported as it arrived, so the loop can warn");
        slices[1].Key.Should().NotBe(keys[1], "attributing it to partition 1 would record 999-0 as that partition's position");
    }

    /// <summary>
    /// The slot search starts from a rotating hint, so a reply whose streams arrive in a different
    /// order from the request — or twice around the ring — still lands in the right slots.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ParseAll_matches_slots_after_the_hint_has_wrapped()
    {
        RedisKey[] keys = [(RedisKey)"s:t:0", (RedisKey)"s:t:1", (RedisKey)"s:t:2"];
        byte[][] keyBytes = [Bytes("s:t:0"), Bytes("s:t:1"), Bytes("s:t:2")];

        // 2 sets the hint to 0; 1 then has to be found by the wrapped second pass, and 0 after it.
        var reply = Reply.Array(
            Reply.Resp2Stream("s:t:2", Reply.Array(Reply.Entry("3-0", ("b", "c")))),
            Reply.Resp2Stream("s:t:1", Reply.Array(Reply.Entry("2-0", ("b", "b")))),
            Reply.Resp2Stream("s:t:0", Reply.Array(Reply.Entry("1-0", ("b", "a")))));

        var slices = BlockFetch.ParseAll(reply, keys, keyBytes);

        slices.Select(s => s.Key).Should().Equal(keys[2], keys[1], keys[0]);
    }

    /// <summary>A malformed entry inside one stream of a multi-stream reply fails the whole parse.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ParseAll_throws_when_any_stream_in_the_reply_is_malformed()
    {
        RedisKey[] keys = [(RedisKey)"s:t:0", (RedisKey)"s:t:1"];
        byte[][] keyBytes = [Bytes("s:t:0"), Bytes("s:t:1")];

        var reply = Reply.Array(
            Reply.Resp2Stream("s:t:0", Reply.Array(Reply.Entry("1-0", ("b", "ok")))),
            Reply.Resp2Stream("s:t:1", Reply.Array(Reply.Array(Reply.Value("2-0"), Reply.Array(Reply.Value("b"))))));

        var act = () => BlockFetch.ParseAll(reply, keys, keyBytes);

        act.Should().Throw<StreamTransportException>().WithMessage("*even field/value list*");
    }
}

/// <summary>
/// Builds <c>XREAD</c> replies out of <see cref="RedisResult"/>, in both wire layouts.
/// </summary>
/// <remarks>
/// Deliberately separate from the reply builder in <c>ConsumerCoreUnitTests</c>, which only needs a
/// one-entry RESP2 stream: these tests need nils, arities and the RESP3 map, and a shared builder
/// that grew all of those options would obscure the shape each test is actually asserting on.
/// </remarks>
internal static class Reply
{
    /// <summary>A nil reply — a block that expired, or an entry trimmed under the read.</summary>
    internal static RedisResult Nil => RedisResult.Create(RedisValue.Null);

    /// <summary>A single (non-array) reply element.</summary>
    internal static RedisResult Value(string text) => RedisResult.Create((RedisValue)text);

    /// <summary>An array reply.</summary>
    internal static RedisResult Array(params RedisResult[] items) => RedisResult.Create(items);

    /// <summary>The RESP2 <c>[key, entries]</c> pair.</summary>
    internal static RedisResult Resp2Stream(string key, RedisResult entries)
        => Array(Value(key), entries);

    /// <summary>One entry: <c>[id, [field, value, …]]</c>.</summary>
    internal static RedisResult Entry(string id, params (string Field, string Value)[] fields)
    {
        var flat = new RedisResult[fields.Length * 2];
        for (var i = 0; i < fields.Length; i++)
        {
            flat[i * 2] = Value(fields[i].Field);
            flat[(i * 2) + 1] = Value(fields[i].Value);
        }

        return Array(Value(id), Array(flat));
    }
}
