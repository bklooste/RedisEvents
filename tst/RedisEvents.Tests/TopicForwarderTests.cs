using FluentAssertions;

using RedisEvents.Config;
using RedisEvents.Producer;
using RedisEvents.Wire;

namespace RedisEvents.Tests;

/// <summary>
/// <see cref="TopicForwarder"/> against a real Redis: each guard on its own (the high-water mark, the
/// dedupe marker), the numeric position comparison, independent sources, and a multi-message forward
/// landing as one transaction.
/// </summary>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class TopicForwarderTests(RedisStreamsFixture fixture)
{
    [Fact]
    public async Task A_forward_publishes_and_the_same_position_again_does_not()
    {
        var (forwarder, length) = this.Forwarder();

        (await forwarder.ForwardAsync("src", "tx-1", new StreamId(5, 0), [Message("a")])).Should().BeTrue();
        (await forwarder.ForwardAsync("src", "tx-1", new StreamId(5, 0), [Message("a")])).Should().BeFalse();

        (await length()).Should().Be(1);
    }

    [Fact]
    public async Task A_replay_below_the_high_water_mark_is_skipped_even_after_its_marker_expired()
    {
        var (forwarder, length) = this.Forwarder(markerTtl: TimeSpan.FromMilliseconds(50));

        await forwarder.ForwardAsync("src", "tx-1", new StreamId(5, 0), [Message("a")]);
        await forwarder.ForwardAsync("src", "tx-2", new StreamId(6, 0), [Message("b")]);
        await Task.Delay(200);

        // A lost consumer position replays the source from the start; the markers are long gone.
        (await forwarder.ForwardAsync("src", "tx-1", new StreamId(5, 0), [Message("a")])).Should().BeFalse();
        (await forwarder.ForwardAsync("src", "tx-2", new StreamId(6, 0), [Message("b")])).Should().BeFalse();

        (await length()).Should().Be(2);
    }

    [Fact]
    public async Task Positions_compare_as_stream_ids_not_as_text()
    {
        var (forwarder, length) = this.Forwarder();

        await forwarder.ForwardAsync("src", "tx-9", new StreamId(9, 0), [Message("a")]);

        // "10-0" < "9-0" as text; as a stream id it is later and must be forwarded.
        (await forwarder.ForwardAsync("src", "tx-10", new StreamId(10, 0), [Message("b")])).Should().BeTrue();
        (await length()).Should().Be(2);
    }

    [Fact]
    public async Task A_marked_dedupe_id_is_skipped_at_a_new_position()
    {
        var (forwarder, length) = this.Forwarder();

        await forwarder.ForwardAsync("src", "tx-1", new StreamId(5, 0), [Message("a")]);

        (await forwarder.ForwardAsync("src", "tx-1", new StreamId(7, 0), [Message("a")])).Should().BeFalse();
        (await length()).Should().Be(1);
    }

    [Fact]
    public async Task Each_source_keeps_its_own_high_water_mark()
    {
        var (forwarder, length) = this.Forwarder();

        await forwarder.ForwardAsync("wallet", "w:tx-1", new StreamId(100, 0), [Message("a")]);

        (await forwarder.ForwardAsync("payments", "p:tx-1", new StreamId(1, 0), [Message("b")])).Should().BeTrue(
            "a low position on another source is not below the wallet's mark");
        (await length()).Should().Be(2);
    }

    [Fact]
    public async Task Every_message_of_one_forward_is_published_in_order()
    {
        var (forwarder, length) = this.Forwarder();

        (await forwarder.ForwardAsync("src", "tx-1", new StreamId(1, 0), [Message("a"), Message("b"), Message("c")])).Should().BeTrue();

        (await length()).Should().Be(3);
    }

    [Fact]
    public async Task An_empty_forward_is_rejected()
    {
        var (forwarder, _) = this.Forwarder();

        var act = () => forwarder.ForwardAsync("src", "tx-1", new StreamId(1, 0), []);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    private static ForwardedMessage Message(string text) => new("customer-1", System.Text.Encoding.UTF8.GetBytes(text), "Forwarded");

    /// <summary>A forwarder onto a fresh one-partition topic, and a way to read how much landed on it.</summary>
    private (TopicForwarder Forwarder, Func<Task<long>> Length) Forwarder(
        TimeSpan? markerTtl = null,
        [System.Runtime.CompilerServices.CallerMemberName] string hint = "forward")
    {
        var topic = fixture.NewTopic(hint);
        var options = new StreamOptions();
        options.Topics[topic] = new TopicOptions { Partitions = 1, Trim = TrimMode.None };

        var forwarder = new TopicForwarder(fixture.Db, topic, options, markerTtl: markerTtl);
        return (forwarder, () => fixture.Db.StreamLengthAsync(StreamKeys.Stream(topic, 0)));
    }
}
