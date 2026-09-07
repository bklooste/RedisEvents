using System.Text;

using FluentAssertions;

using Microsoft.AspNetCore.Http;

using Orange.Lib.Streams.Admin;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Positions;
using Orange.Lib.Streams.Web;
using Orange.Lib.Streams.Wire;

using StackExchange.Redis;

namespace Orange.Lib.Streams.Tests;

/// <summary>
/// The admin endpoints against a real Redis: the ownership SCAN, the key layout a reset reads with,
/// the running-consumer guard, and partition-range validation.
/// </summary>
/// <remarks>
/// <para>
/// R-11 in <c>docs/plans/2026-09-06-orange-lib-streams/12-remediation.md</c>. Three of the four
/// defects here are invisible without a server: a glob that matches nothing still returns 200, a
/// preview against the wrong key shape still reports a tidy <c>Length 0</c>, and a reset to the end
/// of a stream it cannot see still writes <c>0-0</c> and calls it success.
/// </para>
/// <para>
/// The handlers are invoked directly with a <see cref="DefaultHttpContext"/> rather than through a
/// test server. That keeps the suite free of a web host and a TestHost package reference while still
/// exercising the real status codes and the real response bodies; the routing and authorization half
/// is covered by the unit tests, which assert on the mapped endpoints' metadata.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class AdminEndpointServiceTests(RedisStreamsFixture fixture)
{
    private static readonly StreamAdminEndpointOptions Options = new();

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Ownership_route_finds_the_consumers_of_a_topic()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();

        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, new TopicOptions { Partitions = 2 }, null, ct);
        await fixture.Db.HashSetAsync(StreamKeys.Ownership(topic, consumer), "0", "pod-0");

        var (status, body) = await OwnershipAsync(topic, ct);

        status.Should().Be(StatusCodes.Status200OK);
        body.Should().NotContain(
            "No consumers found",
            "the SCAN glob has to carry the literal braces of o:{topic}:*; interpolating the topic into them matches nothing");
        body.Should().Contain("Consumer: " + consumer);
        body.Should().Contain("Owned: 1");
        body.Should().Contain("Unowned partitions: 1");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Ownership_route_refuses_a_topic_name_that_would_re_tag_the_key()
    {
        var ct = TestContext.Current.CancellationToken;

        var (status, body) = await OwnershipAsync("orders}:{other", ct);

        status.Should().Be(StatusCodes.Status400BadRequest);
        body.Should().Contain("topic");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Preview_reads_a_non_co_located_topic_with_the_layout_it_actually_has()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();

        var options = new TopicOptions { Partitions = 1, CoLocatePartitions = false };
        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, options, null, ct);
        var ids = await SeedAsync(topic, options, count: 3, ct);

        // The endpoint has no configuration to hand, which is exactly the case that used to report
        // Length 0 / Entries 0 because the layout was assumed to be co-located.
        var (status, body) = await ResetAsync(topic, consumer, new StreamAdminEndpoints.ResetRequest(
            From: null, Id: null, To: "end", Partition: null, MaxCountScan: null, WhenMissing: null, Force: false),
            apply: false, ct);

        status.Should().Be(StatusCodes.Status200OK);
        body.Should().Contain("of 3", "the partition really holds three entries");
        body.Should().Contain(ids[^1].Format(), "the target of a reset to the end is the partition's own tail");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Reset_to_end_on_a_non_co_located_topic_writes_the_tail_not_zero()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();

        var options = new TopicOptions { Partitions = 1, CoLocatePartitions = false };
        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, options, null, ct);
        var ids = await SeedAsync(topic, options, count: 3, ct);

        var (status, _) = await ResetAsync(topic, consumer, new StreamAdminEndpoints.ResetRequest(
            From: null, Id: null, To: "end", Partition: null, MaxCountScan: null, WhenMissing: null, Force: false),
            apply: true, ct);

        status.Should().Be(StatusCodes.Status200OK);

        var stored = await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, ct);
        stored.Should().ContainKey(0).WhoseValue.Should().Be(
            ids[^1],
            "0-0 here would be a rewind to the beginning — the opposite of skipping the backlog");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_reset_to_the_end_of_a_stream_that_is_not_there_is_refused_rather_than_writing_zero()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();

        // Metadata but no streams: the shape a wrong topic name, or the wrong layout, produces.
        await fixture.Db.HashSetAsync(StreamKeys.TopicMeta(topic), StreamAdmin.MetaPartitionsField, 1);

        var act = () => StreamAdmin.ResetPositionToEndAsync(
            fixture.Redis, topic, consumer, partition: null, new TopicOptions { Partitions = 1 }, null, ct);

        await act.Should().ThrowAsync<Errors.StreamConfigurationException>();

        var stored = await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, ct);
        stored.Should().BeEmpty("nothing is written when the target cannot be established");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task An_apply_is_refused_while_the_consumer_still_holds_claims()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();

        var options = new TopicOptions { Partitions = 1 };
        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, options, null, ct);
        await SeedAsync(topic, options, count: 2, ct);
        await fixture.Db.HashSetAsync(StreamKeys.Ownership(topic, consumer), "0", "pod-0");

        var request = new StreamAdminEndpoints.ResetRequest(
            From: null, Id: null, To: "start", Partition: null, MaxCountScan: null, WhenMissing: null, Force: false);

        var (status, body) = await ResetAsync(topic, consumer, request, apply: true, ct);

        status.Should().Be(StatusCodes.Status409Conflict);
        body.Should().Contain("scale to zero");

        var stored = await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, ct);
        stored.Should().BeEmpty("a refused reset writes nothing at all");

        // A preview is never refused; it says so and carries on.
        var (previewStatus, previewBody) = await ResetAsync(topic, consumer, request, apply: false, ct);
        previewStatus.Should().Be(StatusCodes.Status200OK);
        previewBody.Should().Contain("Warning:");

        var (forcedStatus, _) = await ResetAsync(topic, consumer, request with { Force = true }, apply: true, ct);
        forcedStatus.Should().Be(StatusCodes.Status200OK);

        stored = await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, ct);
        stored.Should().ContainKey(0).WhoseValue.Should().Be(StreamId.Min);
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_partition_past_the_end_of_the_topic_is_refused()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();

        var options = new TopicOptions { Partitions = 2 };
        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, options, null, ct);

        var (status, body) = await ResetAsync(topic, consumer, new StreamAdminEndpoints.ResetRequest(
            From: null, Id: null, To: "start", Partition: 7, MaxCountScan: null, WhenMissing: null, Force: false),
            apply: true, ct);

        status.Should().Be(StatusCodes.Status400BadRequest);
        body.Should().Contain("does not exist");

        var fields = await fixture.Db.HashGetAllAsync(StreamKeys.Positions(topic, consumer));
        fields.Should().BeEmpty("an out-of-range reset used to write a position and a marker nobody reads");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task The_core_reset_refuses_a_partition_past_the_end_too()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();

        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, new TopicOptions { Partitions = 2 }, null, ct);

        var act = () => StreamAdmin.ResetPositionToStartAsync(
            fixture.Redis, topic, consumer, partition: 9, options: null, logger: null, ct: ct);

        await act.Should().ThrowAsync<Errors.StreamConfigurationException>()
            .WithMessage("*partition 9 does not exist*");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task An_unknown_topic_answers_404()
    {
        var ct = TestContext.Current.CancellationToken;

        var (status, body) = await ResetAsync(fixture.NewTopic(), fixture.NewConsumer(), new StreamAdminEndpoints.ResetRequest(
            From: null, Id: null, To: "start", Partition: null, MaxCountScan: null, WhenMissing: null, Force: false),
            apply: false, ct);

        status.Should().Be(StatusCodes.Status404NotFound);
        body.Should().Contain("never been created");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_missing_or_doubled_target_is_refused()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();

        var (none, noneBody) = await ResetAsync(topic, consumer, new StreamAdminEndpoints.ResetRequest(
            From: null, Id: null, To: null, Partition: null, MaxCountScan: null, WhenMissing: null, Force: false),
            apply: false, ct);

        none.Should().Be(StatusCodes.Status400BadRequest);
        noneBody.Should().Contain("exactly one");

        var (both, _) = await ResetAsync(topic, consumer, new StreamAdminEndpoints.ResetRequest(
            From: "2026-09-01T00:00:00Z", Id: null, To: "end", Partition: null, MaxCountScan: null, WhenMissing: null, Force: false),
            apply: false, ct);

        both.Should().Be(StatusCodes.Status400BadRequest);
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_preview_with_no_stored_position_says_what_it_assumed()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();

        var options = new TopicOptions { Partitions = 1 };
        await StreamAdmin.EnsureTopicAsync(fixture.Redis, topic, options, null, ct);
        var ids = await SeedAsync(topic, options, count: 4, ct);

        var from = DateTimeOffset.FromUnixTimeMilliseconds(ids[1].Ms);

        // StartFromWhenMissing = Beginning: the consumer would have started at 0-0, so moving to the
        // second entry skips one.
        var beginning = await StreamAdmin.PreviewResetAsync(
            fixture.Redis, topic, consumer, from, StartFrom.Beginning, partition: null, options, ct: ct);

        beginning.Partitions[0].Current.Should().BeNull();
        beginning.Partitions[0].AssumedStart.Should().Be(StreamId.Min);
        beginning.Partitions[0].Direction.Should().Be(ResetDirection.Skip);
        beginning.TotalToSkip.Should().Be(1);
        beginning.Describe().Should().Contain("assuming 0-0");

        // StartFromWhenMissing = Now: it would have started at the tail, so the same request is a
        // rewind over three entries — the opposite direction and a different number.
        var now = await StreamAdmin.PreviewResetAsync(
            fixture.Redis, topic, consumer, from, StartFrom.Now, partition: null, options, ct: ct);

        now.Partitions[0].AssumedStart.Should().Be(ids[^1]);
        now.Partitions[0].Direction.Should().Be(ResetDirection.Rewind);
        now.TotalToReprocess.Should().Be(3);
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task The_layout_probe_reads_back_what_redis_holds()
    {
        var ct = TestContext.Current.CancellationToken;
        var spread = fixture.NewTopic();
        var tagged = fixture.NewTopic();

        await StreamAdmin.EnsureTopicAsync(fixture.Redis, spread, new TopicOptions { Partitions = 2, CoLocatePartitions = false }, null, ct);
        await StreamAdmin.EnsureTopicAsync(fixture.Redis, tagged, new TopicOptions { Partitions = 3 }, null, ct);

        var spreadLayout = await StreamAdmin.ReadTopicLayoutAsync(fixture.Redis, spread, ct);
        spreadLayout.CoLocatePartitions.Should().BeFalse();
        spreadLayout.Partitions.Should().Be(2);

        var taggedLayout = await StreamAdmin.ReadTopicLayoutAsync(fixture.Redis, tagged, ct);
        taggedLayout.CoLocatePartitions.Should().BeTrue();
        taggedLayout.Partitions.Should().Be(3);

        var unknown = await StreamAdmin.ReadTopicLayoutAsync(fixture.Redis, fixture.NewTopic(), ct);
        unknown.Partitions.Should().Be(0, "0 is how a caller tells 'no such topic' from 'one partition'");
    }

    // ------------------------------------------------------------ helpers

    private async Task<StreamId[]> SeedAsync(string topic, TopicOptions options, int count, CancellationToken ct)
    {
        var key = StreamKeys.Stream(topic, 0, options.CoLocatePartitions);
        var first = DateTimeOffset.UtcNow.AddMinutes(-count).ToUnixTimeMilliseconds();
        var ids = new StreamId[count];

        for (var i = 0; i < count; i++)
        {
            ct.ThrowIfCancellationRequested();

            var id = new StreamId(first + (i * 1000), 0);
            await fixture.Db.StreamAddAsync(key, "b", i, id.Format());
            ids[i] = id;
        }

        return ids;
    }

    private Task<(int Status, string Body)> ResetAsync(
        string topic,
        string consumer,
        StreamAdminEndpoints.ResetRequest request,
        bool apply,
        CancellationToken ct)
        => InvokeAsync(ctx => StreamAdminEndpoints.ResetHandler(
            _ => fixture.Redis, topic, consumer, request, Options, apply, ctx, ct));

    private Task<(int Status, string Body)> OwnershipAsync(string topic, CancellationToken ct)
        => InvokeAsync(ctx => StreamAdminEndpoints.GetOwnershipHandler(_ => fixture.Redis, topic, ctx, ct));

    private static async Task<(int Status, string Body)> InvokeAsync(Func<HttpContext, Task> handler)
    {
        var ctx = new DefaultHttpContext();
        var body = new MemoryStream();
        ctx.Response.Body = body;

        await handler(ctx);

        return (ctx.Response.StatusCode, Encoding.UTF8.GetString(body.ToArray()));
    }
}
