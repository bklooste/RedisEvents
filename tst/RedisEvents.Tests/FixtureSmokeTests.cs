using System.Text;

using FluentAssertions;

using RedisEvents.Wire;

namespace RedisEvents.Tests;

/// <summary>
/// Proves the shared fixture itself works: the container starts, Redis answers, the flush really
/// flushes, topic names are unique, and <see cref="TestHandler"/> completes its wait.
/// </summary>
/// <remarks>
/// Every other service test in this project rests on these five facts, so when a whole file starts
/// failing at once this is the file that says whether the cause is the fixture or the library.
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class FixtureSmokeTests(RedisStreamsFixture fixture)
{
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Redis_answers_ping_and_round_trips_a_value()
    {
        var latency = await fixture.Db.PingAsync();
        latency.Should().BePositive("a PING that never reached a server would not have a latency");

        var key = $"smoke:{Guid.NewGuid():N}";
        _ = await fixture.Db.StringSetAsync(key, "pong");

        ((string?)await fixture.Db.StringGetAsync(key)).Should().Be("pong");
        fixture.Endpoint.Should().Contain(":", "the endpoint is host:port for a library connection string");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Redis_is_at_least_7_4_so_hexpire_exists()
    {
        var info = (string?)await fixture.Db.ExecuteAsync("INFO", "server") ?? string.Empty;
        var line = info.Split('\n').First(l => l.StartsWith("redis_version:", StringComparison.Ordinal));
        var version = Version.Parse(line["redis_version:".Length..].Trim());

        version.Should().BeGreaterThanOrEqualTo(
            new Version(7, 4),
            "the ownership registry expires hash fields with HEXPIRE, which Redis 7.0 does not have");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task FlushAll_removes_keys_written_by_an_earlier_test()
    {
        var key = $"smoke:{Guid.NewGuid():N}";
        _ = await fixture.Db.StringSetAsync(key, "value");

        await fixture.FlushAllAsync();

        (await fixture.Db.KeyExistsAsync(key)).Should().BeFalse();
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public void NewTopic_is_unique_per_call_and_key_safe()
    {
        var names = Enumerable.Range(0, 50).Select(_ => fixture.NewTopic()).ToArray();

        names.Should().OnlyHaveUniqueItems();
        names.Should().AllSatisfy(n => n.Should().MatchRegex("^[a-z0-9-]+$"));
        names[0].Should().StartWith("newtopic-is-unique-per-call-and-key-safe");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task TestHandler_completes_its_wait_when_the_messages_arrive()
    {
        var handler = new TestHandler();
        var waiting = handler.WaitForAsync(3, TimeSpan.FromSeconds(10));

        await ((Consumer.IBatchHandler)handler).HandleAsync(Batch("a", "b"), CancellationToken.None);
        waiting.IsCompleted.Should().BeFalse("only two of the three messages have been delivered");

        await ((Consumer.IBatchHandler)handler).HandleAsync(Batch("c"), CancellationToken.None);
        await waiting;

        handler.Count.Should().Be(3);
        handler.Batches.Should().Be(2);
        handler.Bodies.Should().Equal("a", "b", "c");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task TestHandler_wait_times_out_with_a_useful_message()
    {
        var handler = new TestHandler();

        var act = async () => await handler.WaitForAsync(1, TimeSpan.FromMilliseconds(200));

        (await act.Should().ThrowAsync<TimeoutException>()).WithMessage("*saw 0*");
    }

    private static ReadOnlyMemory<StreamMsg> Batch(params string[] bodies)
        => bodies
            .Select((b, i) => new StreamMsg(
                Body: Encoding.UTF8.GetBytes(b),
                Type: "smoke",
                Id: new StreamId(i + 1, 0),
                Partition: 0,
                PartitionKey: string.Empty,
                CorrelationId: string.Empty,
                TraceParent: null,
                Headers: HeaderBlock.Empty))
            .ToArray()
            .AsMemory();
}
