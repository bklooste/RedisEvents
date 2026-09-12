using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Diagnostics;
using RedisEvents.Positions;
using RedisEvents.Producer;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>
/// R-07 — consumer-group mode against a real group: ack ordering when a claim sweep interleaves
/// with live reads, recovery from a group that disappears underneath the reader, and the reset that
/// used to be a silent no-op in this mode.
/// </summary>
/// <remarks>
/// Driven at the <see cref="ConsumerGroupFetch"/> seam rather than through hosts, for the reason
/// S18b gives: the claim threshold is a constructor argument, and at its 30-second production
/// default these tests would spend half a minute each proving something they can prove in one.
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class ConsumerGroupServiceTests(RedisStreamsFixture fixture)
{
    private const string MessageType = "test.event";
    private const int ClaimMinIdleMs = 300;

    /// <summary>
    /// R-07(b), end to end. An <c>XAUTOCLAIM</c> sweep hands this instance entries whose ids are
    /// OLDER than the live batch it fetched a moment earlier, so the pending queue is not ordered by
    /// id. Acknowledging the live batch must leave the claimed one in the pending-entries list: the
    /// handler has not seen it yet, and an entry acknowledged before it is processed is an entry
    /// this process loses if it dies.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Acknowledging_a_live_batch_leaves_a_later_claimed_batch_pending()
    {
        await this.ResetAsync();

        var topic = fixture.NewTopic();
        var group = fixture.NewConsumer();
        var key = StreamKeys.Stream(topic, 0);
        var db = fixture.Db;

        var dying = this.Member(key, group, "instance-a");
        await dying.EnsureGroupAsync(CancellationToken.None);

        var publisher = new StreamPublisher(db, topic, new TopicOptions { Partitions = 1 });

        // Two old entries, delivered to the instance that is about to die without acknowledging.
        var old = new[]
        {
            await Publish(publisher, "old-0"),
            await Publish(publisher, "old-1"),
        };

        (await dying.FetchAsync(CancellationToken.None)).Count.Should().Be(2);

        // Two newer entries, which the survivor reads live: its pending queue is now
        // [live 300-0..], and the claim sweep will push [claimed 100-0..] in BEHIND them.
        var fresh = new[]
        {
            await Publish(publisher, "new-0"),
            await Publish(publisher, "new-1"),
        };

        var survivor = this.Member(key, group, "instance-b");

        var live = await survivor.FetchAsync(CancellationToken.None);

        live.Span.ToArray().Select(static e => (string?)e.Id).Should().BeEquivalentTo(Format(fresh));
        survivor.PendingBatches.Should().Be(1);

        // Now the sweep: idle reads climb the backoff ladder to its cap and then claim. What comes
        // back starts with the abandoned entries and may also include this instance's OWN live
        // entries, which have been idle just as long — XAUTOCLAIM does not care who holds them. So
        // the claimed batch's ids overlap the live batch's, which is a second reason the old
        // "everything at or below the reported id" rule could not be right.
        StreamEntryBatch claimed = default;

        await RedisStreamsFixture.WaitUntilAsync(
            async () =>
            {
                claimed = await survivor.FetchAsync(CancellationToken.None);
                return !claimed.IsEmpty;
            },
            TimeSpan.FromSeconds(30),
            "the survivor to claim the abandoned entries");

        claimed.Span.ToArray().Select(static e => (string?)e.Id).Should().Contain(
            Format(old),
            "the sweep recovers the entries the first instance abandoned");

        survivor.PendingBatches.Should().Be(2, "the claimed batch is queued behind the live one");

        // The handler finishes the LIVE batch and reports its last id. Under the old
        // "Last <= processedThrough" rule this acknowledged all four entries, removing the claimed
        // pair from the PEL before the handler had been given it.
        await survivor.AckAsync(LastOf(live), CancellationToken.None);

        var afterAck = await db.StreamPendingMessagesAsync(key, group, count: 10, RedisValue.Null);

        afterAck.Select(static m => (string?)m.MessageId).Should().BeEquivalentTo(
            Format(old),
            "only the batch the handler processed may be acknowledged");

        survivor.PendingBatches.Should().Be(1);

        // ...and the claimed batch is acknowledged when its own turn through the channel comes,
        // reported by its own last id the way the processing loop reports every batch.
        await survivor.AckAsync(LastOf(claimed), CancellationToken.None);

        (await db.StreamPendingAsync(key, group)).PendingMessageCount.Should().Be(0);
        survivor.PendingBatches.Should().Be(0);
    }

    /// <summary>
    /// The group is destroyed underneath a running reader — a manual <c>XGROUP DESTROY</c>, a
    /// flushed database, an expired key. The reader recreates it at its start position, says so
    /// once, and carries on; the batches it was holding unacknowledged are dropped, because they
    /// name a pending-entries list that no longer exists.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_destroyed_group_is_recreated_and_the_stale_pending_batches_are_dropped()
    {
        await this.ResetAsync();

        var topic = fixture.NewTopic();
        var group = fixture.NewConsumer();
        var key = StreamKeys.Stream(topic, 0);
        var db = fixture.Db;
        var log = new CapturingLog();

        var member = this.Member(key, group, "instance-a", log);
        await member.EnsureGroupAsync(CancellationToken.None);

        var publisher = new StreamPublisher(db, topic, new TopicOptions { Partitions = 1 });
        await Publish(publisher, "m0");
        await Publish(publisher, "m1");

        (await member.FetchAsync(CancellationToken.None)).Count.Should().Be(2);
        member.PendingBatches.Should().Be(1);

        (await db.StreamDeleteConsumerGroupAsync(key, group)).Should().BeTrue();

        await Publish(publisher, "m2");

        // The next read answers NOGROUP; the fetch recreates the group and returns empty, so the
        // read after it sees the whole stream again (the group was recreated at the beginning).
        StreamEntryBatch redelivered = default;

        await RedisStreamsFixture.WaitUntilAsync(
            async () =>
            {
                redelivered = await member.FetchAsync(CancellationToken.None);
                return !redelivered.IsEmpty;
            },
            TimeSpan.FromSeconds(30),
            "the recreated group to redeliver the stream");

        redelivered.Count.Should().Be(3, "the group was recreated at its configured start position");

        member.PendingBatches.Should().Be(
            1,
            "the batch held against the destroyed group was dropped rather than left to accumulate");

        log.At(LogLevel.Warning).Should().Contain(m => m.Contains("is missing on", StringComparison.Ordinal));

        // The redelivered batch acknowledges normally against the new group.
        await member.AckAsync(LastOf(redelivered), CancellationToken.None);

        (await db.StreamPendingAsync(key, group)).PendingMessageCount.Should().Be(0);
    }

    /// <summary>
    /// R-07(c). There is no position hash in group mode, so <c>StreamAdmin</c>'s reset family moves
    /// nothing here; <c>XGROUP SETID</c> is the reset, and this is it working — rewind replays,
    /// skip-to-the-end skips, and a live member's pending entries go with the consumer that held
    /// them.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Resetting_a_group_rewinds_it_and_purges_the_pending_entries()
    {
        await this.ResetAsync();

        var topic = fixture.NewTopic();
        var group = fixture.NewConsumer();
        var key = StreamKeys.Stream(topic, 0);
        var db = fixture.Db;

        var member = this.Member(key, group, "instance-a");
        await member.EnsureGroupAsync(CancellationToken.None);

        var publisher = new StreamPublisher(db, topic, new TopicOptions { Partitions = 1 });
        var ids = new List<StreamId>();

        for (var i = 0; i < 5; i++)
        {
            ids.Add(await Publish(publisher, $"m{i}"));
        }

        var first = await member.FetchAsync(CancellationToken.None);
        first.Count.Should().Be(5);

        // Deliberately NOT acknowledged: the reset has to deal with a live pending-entries list,
        // which is the case that makes "skip to the end" mean anything.
        (await db.StreamPendingAsync(key, group)).PendingMessageCount.Should().Be(5);

        await member.ResetToAsync(StreamId.Min, purgePending: true, CancellationToken.None);

        (await db.StreamPendingAsync(key, group)).PendingMessageCount.Should().Be(
            0,
            "the consumers holding those entries were deleted with them");

        member.PendingBatches.Should().Be(0, "the ids held for the old cursor are meaningless after a reset");

        var replayed = await member.FetchAsync(CancellationToken.None);

        replayed.Span.ToArray().Select(static e => (string?)e.Id).Should().BeEquivalentTo(
            ids.Select(static id => id.Format()),
            "a rewind to 0-0 redelivers the whole retained stream");

        await member.AckAsync(LastOf(replayed), CancellationToken.None);

        // ...and the other direction: skip everything that exists today.
        await member.ResetToAsync(ids[^1], purgePending: true, CancellationToken.None);

        (await member.FetchAsync(CancellationToken.None)).IsEmpty.Should().BeTrue(
            "reading resumes after the id the group was set to");

        var after = await Publish(publisher, "after-the-reset");
        var tail = await member.FetchAsync(CancellationToken.None);

        tail.Count.Should().Be(1);
        ((string?)tail.Span[0].Id).Should().Be(after.Format());
    }

    /// <summary>A reset against a group that does not exist yet creates it at the target.</summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Resetting_a_group_that_does_not_exist_creates_it_at_the_target()
    {
        await this.ResetAsync();

        var topic = fixture.NewTopic();
        var group = fixture.NewConsumer();
        var key = StreamKeys.Stream(topic, 0);
        var db = fixture.Db;

        var publisher = new StreamPublisher(db, topic, new TopicOptions { Partitions = 1 });
        await Publish(publisher, "before");

        await ConsumerGroupFetch.SetGroupPositionAsync(
            db,
            key,
            group,
            StreamId.Min,
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            purgePending: true,
            CancellationToken.None);

        var member = this.Member(key, group, "instance-a");

        (await member.FetchAsync(CancellationToken.None)).Count.Should().Be(
            1,
            "the group was created at 0-0 by the reset, so the existing entry is delivered");
    }

    private static async Task<StreamId> Publish(StreamPublisher publisher, string body)
        => await publisher.PublishAsync(
            "k",
            Encoding.UTF8.GetBytes(body),
            MessageType,
            new PublishOptions(Partition: 0));

    private static IEnumerable<string> Format(IEnumerable<StreamId> ids) => ids.Select(static id => id.Format());

    private static StreamId LastOf(StreamEntryBatch batch)
        => StreamId.TryParse(((string?)batch.Span[^1].Id).AsSpan(), out var parsed)
            ? parsed
            : throw new InvalidOperationException("Redis returned an unparsable entry id.");

    private ConsumerGroupFetch Member(RedisKey key, string group, string instance, ILogger? log = null) => new(
        fixture.Db,
        key,
        group,
        instance,
        StartPosition.Beginning,
        batchSize: 10,
        maxIdleDelayMs: 10,
        log ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
        ClaimMinIdleMs,
        claimIntervalMs: 10);

    private async Task ResetAsync()
    {
        await fixture.FlushAllAsync();
        StreamLag.Clear();
    }

    /// <summary>Captures log entries so a test can assert the recovery path reported itself.</summary>
    private sealed class CapturingLog : ILogger
    {
        private readonly List<(LogLevel Level, string Message)> entries = [];

        internal IReadOnlyList<string> At(LogLevel level)
        {
            lock (this.entries)
            {
                return this.entries.Where(e => e.Level == level).Select(e => e.Message).ToArray();
            }
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (this.entries)
            {
                this.entries.Add((logLevel, formatter(state, exception)));
            }
        }
    }
}
