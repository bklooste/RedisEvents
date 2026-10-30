using System.Globalization;
using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Hosting;

using RedisEvents.Admin;
using RedisEvents.Config;
using RedisEvents.Extensions;
using RedisEvents.Producer;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>
/// Topic and consumer names are opaque, and they are the only thing that isolates one consumer from
/// another: two consumers with different names on one topic in one Redis — the way an application
/// runs several environments against one shared feed (<c>dev.probe</c>, <c>tst.probe</c> on
/// <c>np.feed</c>) — each receive every message, keep their own positions, own their own work-queue
/// group, and an admin reset of one leaves the other alone.
/// </summary>
/// <remarks>
/// Nothing here sets an environment: the library derives no key from one. If any per-consumer key
/// (position, lease, group) were shared between the two names, one consumer would miss messages or
/// replay the other's — which is what these assertions catch.
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class ConsumerIsolationTests(RedisStreamsFixture fixture)
{
    private const int Partitions = 2;

    /// <summary>
    /// Ordered delivery: both consumers receive all twenty messages, each from its own position; a
    /// later run of one resumes from its own cursor; an admin reset of one moves only that one.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Two_consumer_names_on_one_topic_get_every_message_and_independent_positions()
    {
        var topic = "np." + fixture.NewTopic();
        var (dev, tst) = Names();
        var db = fixture.Db;

        var first = await this.PublishAsync(topic, from: 0, count: 20);

        var runs = await Task.WhenAll(
            this.ConsumeAsync(topic, dev, DeliveryMode.Ordered, expect: first.Length),
            this.ConsumeAsync(topic, tst, DeliveryMode.Ordered, expect: first.Length));

        runs[0].Should().BeEquivalentTo(first, "dev must receive every message on the shared topic");
        runs[1].Should().BeEquivalentTo(first, "tst must receive every message on the shared topic, too");

        // One stream, one position hash per consumer name, and nothing else carrying a cursor.
        (await StreamLengthAsync(db, topic)).Should().Be(first.Length);
        (await this.PositionKeysAsync(topic)).Should().BeEquivalentTo(
            [PositionsKey(topic, dev), PositionsKey(topic, tst)],
            "positions are keyed by consumer name");

        var tstAfterFirst = await ReadPositionsAsync(db, PositionsKey(topic, tst));
        tstAfterFirst.Should().NotBeEmpty();

        // Five more; dev alone resumes from its own position, and tst's does not move.
        var second = await this.PublishAsync(topic, from: first.Length, count: 5);

        (await this.ConsumeAsync(topic, dev, DeliveryMode.Ordered, expect: second.Length))
            .Should().BeEquivalentTo(second, "dev resumes after the twenty it already processed");

        (await ReadPositionsAsync(db, PositionsKey(topic, tst)))
            .Should().BeEquivalentTo(tstAfterFirst, "dev's progress must not move tst's cursor");

        // An admin reset of dev to the start touches dev's positions only.
        var devBeforeReset = await ReadPositionsAsync(db, PositionsKey(topic, dev));
        await StreamAdmin.ResetPositionToStartAsync(fixture.Redis, topic, dev, options: TopicOptions());

        (await ReadPositionsAsync(db, PositionsKey(topic, dev)))
            .Should().NotBeEquivalentTo(devBeforeReset, "the reset moved dev");
        (await ReadPositionsAsync(db, PositionsKey(topic, tst)))
            .Should().BeEquivalentTo(tstAfterFirst, "a reset of dev must not touch tst");

        // tst, still behind, gets exactly the five it has not seen; dev, reset, replays all twenty-five.
        var both = await Task.WhenAll(
            this.ConsumeAsync(topic, tst, DeliveryMode.Ordered, expect: second.Length),
            this.ConsumeAsync(topic, dev, DeliveryMode.Ordered, expect: first.Length + second.Length));

        both[0].Should().BeEquivalentTo(second);
        both[1].Should().BeEquivalentTo(first.Concat(second));
    }

    /// <summary>
    /// Work-queue delivery: a group lives on the stream key and is named by the consumer, so two
    /// consumer names are two groups and each is handed every message. Were the group shared, the two
    /// would split the stream between them.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Two_consumer_names_in_work_queue_mode_each_get_their_own_group()
    {
        var topic = "np." + fixture.NewTopic();
        var (dev, tst) = Names();
        var db = fixture.Db;

        var published = await this.PublishAsync(topic, from: 0, count: 20);

        var runs = await Task.WhenAll(
            this.ConsumeAsync(topic, dev, DeliveryMode.WorkQueue, expect: published.Length),
            this.ConsumeAsync(topic, tst, DeliveryMode.WorkQueue, expect: published.Length));

        runs[0].Should().BeEquivalentTo(published, "dev's group must be handed every message");
        runs[1].Should().BeEquivalentTo(published, "tst's group must be handed every message, not the other half");

        for (var p = 0; p < Partitions; p++)
        {
            var groups = await db.StreamGroupInfoAsync(StreamKey(topic, p));

            groups.Select(static g => g.Name).Should().BeEquivalentTo(
                [dev, tst],
                $"partition {p} carries exactly one group per consumer name");
            groups.Should().OnlyContain(static g => g.PendingMessageCount == 0, "every delivered entry was acknowledged");
        }
    }

    private static (string Dev, string Tst) Names()
    {
        var suffix = Guid.NewGuid().ToString("N")[..6];
        return ($"dev.probe-{suffix}", $"tst.probe-{suffix}");
    }

    private static TopicOptions TopicOptions() => new() { Partitions = Partitions };

    private static RedisKey StreamKey(string topic, int partition)
        => string.Create(CultureInfo.InvariantCulture, $"{KeyNamespace.Prefix()}s:{{{topic}}}:{partition}");

    private static string PositionsKey(string topic, string consumer)
        => $"{KeyNamespace.Prefix()}p:{{{topic}}}:{consumer}";

    private static async Task<long> StreamLengthAsync(IDatabase db, string topic)
    {
        var total = 0L;
        for (var p = 0; p < Partitions; p++)
        {
            total += await db.StreamLengthAsync(StreamKey(topic, p));
        }

        return total;
    }

    private static async Task<Dictionary<string, string>> ReadPositionsAsync(IDatabase db, string key)
        => (await db.HashGetAllAsync(key)).ToDictionary(static e => e.Name.ToString(), static e => e.Value.ToString());

    /// <summary>Every positions hash on the topic (not the <c>:meta</c> companions).</summary>
    private async Task<string[]> PositionKeysAsync(string topic)
    {
        var keys = new List<string>();
        foreach (var endpoint in fixture.Redis.GetEndPoints())
        {
            await foreach (var key in fixture.Redis.GetServer(endpoint).KeysAsync(pattern: $"*p:{{{topic}}}:*"))
            {
                var name = key.ToString();
                if (!name.EndsWith(":meta", StringComparison.Ordinal))
                {
                    keys.Add(name);
                }
            }
        }

        return [.. keys];
    }

    private async Task<string[]> PublishAsync(string topic, int from, int count)
    {
        var publisher = new StreamPublisher(fixture.Redis, topic, TopicOptions());

        var bodies = new string[count];
        for (var i = 0; i < count; i++)
        {
            var n = from + i;
            bodies[i] = string.Create(CultureInfo.InvariantCulture, $"m{n}");
            await publisher.PublishAsync(
                string.Create(CultureInfo.InvariantCulture, $"k{n % 7}"),
                Encoding.UTF8.GetBytes(bodies[i]),
                "test.event");
        }

        return bodies;
    }

    /// <summary>
    /// Runs one consumer, configured the way a service is (a <c>Streams</c> section and
    /// <c>AddStream</c>), until it has received <paramref name="expect"/> messages plus a short settle
    /// in which an extra delivery would show up; then stops it, which flushes its positions.
    /// </summary>
    private async Task<string[]> ConsumeAsync(string topic, string consumer, DeliveryMode delivery, int expect)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = "Production" });
        builder.Configuration["Streams:ConnectionString"] = fixture.ConnectionString;
        builder.Configuration["Streams:Consumer"] = consumer;
        builder.Configuration[$"Streams:Topics:{topic}:Partitions"] = Partitions.ToString(CultureInfo.InvariantCulture);
        builder.Configuration["Streams:Consumers:0:Topic"] = topic;
        builder.Configuration["Streams:Consumers:0:Delivery"] = delivery.ToString();
        builder.Configuration["Streams:Consumers:0:PersistIntervalMs"] = "100";
        builder.Configuration["Streams:Consumers:0:BlockMs"] = "200";

        var received = new List<string>();
        var gate = new Lock();

        builder.AddStream(topic, (batch, _) =>
        {
            lock (gate)
            {
                foreach (var message in batch.Span)
                {
                    received.Add(Encoding.UTF8.GetString(message.BodySpan));
                }
            }

            return ValueTask.CompletedTask;
        });

        using var host = builder.Build();
        await host.StartAsync();
        try
        {
            await RedisStreamsFixture.WaitUntilAsync(
                () => { lock (gate) { return received.Count >= expect; } },
                TimeSpan.FromSeconds(30),
                $"{consumer} to receive {expect} message(s)");

            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }
        finally
        {
            await host.StopAsync();
        }

        lock (gate)
        {
            return [.. received];
        }
    }
}
