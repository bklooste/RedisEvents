using System.Globalization;

using FluentAssertions;

using RedisEvents.Admin;
using RedisEvents.Config;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>
/// <see cref="StreamAdmin.ReadTopicTailAsync"/> against real Redis: an empty topic, a trimmed one,
/// a namespaced one and a spread (non co-located) one.
/// </summary>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class TopicTailTests(RedisStreamsFixture fixture)
{
    [Fact]
    public async Task A_topic_that_was_never_written_reports_0_0_on_every_partition()
    {
        var topic = fixture.NewTopic();

        var tails = await StreamAdmin.ReadTopicTailAsync(fixture.Redis, topic, new TopicOptions { Partitions = 3 }, TestContext.Current.CancellationToken);

        tails.Should().Equal(StreamId.Min, StreamId.Min, StreamId.Min);
    }

    [Fact]
    public async Task An_ensured_but_empty_topic_reports_0_0()
    {
        var topic = fixture.NewTopic();
        var options = new TopicOptions { Partitions = 2 };
        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, options, ct: TestContext.Current.CancellationToken);

        var tails = await StreamAdmin.ReadTopicTailAsync(fixture.Redis, topic, options, TestContext.Current.CancellationToken);

        tails.Should().Equal(StreamId.Min, StreamId.Min);
    }

    [Fact]
    public async Task Each_partition_reports_its_own_last_id()
    {
        var topic = fixture.NewTopic();
        await AddAsync(Key(topic, 0), "5-1");
        await AddAsync(Key(topic, 0), "7-0");
        await AddAsync(Key(topic, 2), "9-3");

        var tails = await StreamAdmin.ReadTopicTailAsync(fixture.Redis, topic, new TopicOptions { Partitions = 3 }, TestContext.Current.CancellationToken);

        tails.Should().Equal(new StreamId(7, 0), StreamId.Min, new StreamId(9, 3));
    }

    [Fact]
    public async Task A_trimmed_stream_still_reports_the_last_id_ever_added()
    {
        var topic = fixture.NewTopic();
        var key = Key(topic, 0);
        await AddAsync(key, "10-0");
        await AddAsync(key, "11-0");
        await fixture.Db.StreamTrimAsync(key, 0);
        (await fixture.Db.StreamLengthAsync(key)).Should().Be(0);

        var tails = await StreamAdmin.ReadTopicTailAsync(fixture.Redis, topic, new TopicOptions { Partitions = 1 }, TestContext.Current.CancellationToken);

        tails.Should().Equal(new StreamId(11, 0));
    }

    [Fact]
    public async Task Keys_carry_the_namespace_prefix()
    {
        var topic = fixture.NewTopic();
        KeyNamespace.Prefix().Should().NotBeEmpty();

        // The same shape without the prefix: another environment's stream, which must not be read.
        await AddAsync((RedisKey)string.Create(CultureInfo.InvariantCulture, $"s:{{{topic}}}:0"), "99-0");
        await AddAsync(Key(topic, 0), "3-0");

        var tails = await StreamAdmin.ReadTopicTailAsync(fixture.Redis, topic, new TopicOptions { Partitions = 1 }, TestContext.Current.CancellationToken);

        tails.Should().Equal(new StreamId(3, 0));
    }

    [Fact]
    public async Task A_spread_topic_reads_the_untagged_keys()
    {
        var topic = fixture.NewTopic();
        await AddAsync((RedisKey)string.Create(CultureInfo.InvariantCulture, $"{KeyNamespace.Prefix()}s:{topic}:1"), "4-2");

        var tails = await StreamAdmin.ReadTopicTailAsync(
            fixture.Redis, topic, new TopicOptions { Partitions = 2, CoLocatePartitions = false }, TestContext.Current.CancellationToken);

        tails.Should().Equal(StreamId.Min, new StreamId(4, 2));
    }

    [Fact]
    public async Task Fewer_than_one_partition_is_refused()
    {
        var act = () => StreamAdmin.ReadTopicTailAsync(fixture.Redis, "t", new TopicOptions { Partitions = 0 }, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
    }

    private static RedisKey Key(string topic, int partition)
        => (RedisKey)string.Create(CultureInfo.InvariantCulture, $"{KeyNamespace.Prefix()}s:{{{topic}}}:{partition}");

    private Task AddAsync(RedisKey key, string id)
        => fixture.Db.StreamAddAsync(key, "f", "v", messageId: id);
}
