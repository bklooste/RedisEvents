using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Orange.Lib.Streams.Admin;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Positions;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Trimming;
using Orange.Lib.Streams.Wire;

using StackExchange.Redis;

namespace Orange.Lib.Streams.Tests;

/// <summary>
/// S15 / S15b / S15c — the trimming behaviours, against real Redis.
/// </summary>
/// <remarks>
/// <para>
/// Three separate mechanisms share this file because they are three halves of one memory bound, and
/// getting any of them wrong is an out-of-memory Redis rather than a wrong answer:
/// </para>
/// <list type="number">
///   <item><description>
///   <b>S15 — inline <c>MAXLEN</c></b> rides on every <c>XADD</c> and is the only thing that actually
///   bounds memory. It is blind: it never looks at a consumer's position.
///   </description></item>
///   <item><description>
///   <b>S15 — the background time trim</b> expresses retention, which <c>XADD</c> cannot, and clamps
///   its <c>MINID</c> to the slowest stored consumer position so it never silently deletes
///   unprocessed entries.
///   </description></item>
///   <item><description>
///   <b>S15b — the clamp release</b>, which is the one that matters. The clamp above is itself an OOM
///   path: a consumer blocked on a <see cref="DontIgnoreException"/> holds its position <em>by
///   design</em> and would hold the stream forever. Past
///   <see cref="TopicOptions.ClampReleaseThreshold"/> the trimmer overrides the clamp and deletes
///   that consumer's backlog on purpose. This file asserts the trade actually happens, because the
///   failure mode if it silently does not is an OOM that takes down every stream, every position and
///   every other service on the instance.
///   </description></item>
/// </list>
/// <para>
/// S15c then pins the behaviour at the far end of that story — Redis already at <c>maxmemory</c> with
/// <c>noeviction</c> — where publishing must fail loudly to its caller while consumers keep working
/// through a checkpoint outage.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class TrimmingTests(RedisStreamsFixture fixture)
{
    /// <summary>Type stamped on every published entry; the trimming tests never filter on it.</summary>
    private const string MessageType = "trim-test";

    private const string MeterName = "Orange.Lib.Streams";

    // ------------------------------------------------------------------ S15: inline MAXLEN

    /// <summary>
    /// S15. <c>MaxLen</c> 1000, publish 5000, <c>XLEN</c> ≈ 1000.
    /// </summary>
    /// <remarks>
    /// The slack is not sloppiness, it is the contract of <c>MAXLEN ~</c>: approximate trimming only
    /// ever drops <em>whole macro nodes</em> (<c>stream-node-max-entries</c>, 100 by default), so the
    /// stream is guaranteed to hold at least <c>MaxLen</c> and overshoots by at most about a node.
    /// The assertion is therefore two-sided — a lower bound proves nothing unprocessed was thrown
    /// away early, and the upper bound proves the trim is really running rather than silently absent.
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S15_inline_approximate_trim_bounds_the_stream_at_maxlen()
    {
        var topic = fixture.NewTopic();
        var options = new TopicOptions { Partitions = 1, MaxLen = 1_000, Trim = TrimMode.Approx };

        await using var provider = this.Provider(topic, options);
        var publisher = await this.PublisherAsync(provider, topic, options);

        _ = await PublishAsync(publisher, 5_000);

        var length = await this.LengthAsync(topic);

        length.Should().BeGreaterThanOrEqualTo(
            1_000L,
            "MAXLEN ~ guarantees at least MaxLen entries survive — trimming below it would delete entries a consumer may not have read");

        length.Should().BeLessThanOrEqualTo(
            1_300L,
            "approximate trimming overshoots by at most one macro node (~100 entries); 5000 published against MaxLen 1000 " +
            "must not leave the stream anywhere near unbounded");
    }

    /// <summary>
    /// S15. The exact form of the same clause holds <c>MaxLen</c> precisely, which is what makes the
    /// approximate test's slack a property of <c>~</c> rather than an excuse.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S15_inline_exact_trim_holds_maxlen_precisely()
    {
        var topic = fixture.NewTopic();
        var options = new TopicOptions { Partitions = 1, MaxLen = 1_000, Trim = TrimMode.Exact };

        await using var provider = this.Provider(topic, options);
        var publisher = await this.PublisherAsync(provider, topic, options);

        _ = await PublishAsync(publisher, 2_000);

        (await this.LengthAsync(topic)).Should().Be(
            1_000L,
            "TrimMode.Exact sends MAXLEN without the ~, so Redis trims to the ceiling entry by entry");
    }

    // ------------------------------------------------------------------ S15: background time trim

    /// <summary>
    /// S15. The background trimmer applies time-based retention and clamps the <c>MINID</c> it sends
    /// to the <b>slowest</b> stored consumer position, never past it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Entries are written with explicit ids an hour in the past rather than published live, so the
    /// retention cutoff is a fixed point rather than a race with the test's own runtime: every
    /// assertion below is an exact entry count, not an approximation.
    /// </para>
    /// <para>
    /// Three sweeps, removing one consumer each time, show the clamp is <c>min</c> over consumers and
    /// not "the first one found": with a fast and a slow consumer it stops at the slow one; with only
    /// the fast one it moves up to that; with nothing claiming to read the topic at all, retention
    /// applies in full.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S15_background_time_trim_clamps_at_the_slowest_stored_position()
    {
        var topic = fixture.NewTopic();
        var options = new TopicOptions
        {
            Partitions = 1,
            MaxLen = 100_000,
            Trim = TrimMode.Exact,
            RetentionSeconds = 1_800,
        };

        await using var provider = this.Provider(topic, options);
        var db = provider.Connection.GetDatabase();
        var key = StreamKeys.Stream(topic, 0, options.CoLocatePartitions);

        // An hour old, so "now - 30 minutes" is unambiguously after every one of them.
        var ids = await WriteDatedEntriesAsync(db, key, DateTimeOffset.UtcNow.AddHours(-1), 200);

        var slow = fixture.NewConsumer("slow");
        var fast = fixture.NewConsumer("fast");
        var store = new RedisPositionStore(provider.Connection);

        await store.SaveAsync(topic, slow, new[] { (0, ids[50]) }.AsSpan(), CancellationToken.None);
        await store.SaveAsync(topic, fast, new[] { (0, ids[150]) }.AsSpan(), CancellationToken.None);

        var log = new CapturingLogger();
        await using var trimmer = new BackgroundTrimmer(this.RootOptions(topic, options), provider.Connection, log);

        var held = await trimmer.TrimTopicOnceAsync(topic);
        var partition = held.Partitions.Should().ContainSingle().Subject;

        partition.HeldBy.Should().Be(slow, "the clamp is the minimum over every consumer's stored position");
        partition.Clamp.Should().Be(ids[50]);
        partition.MinId.Should().Be(ids[50], "the sweep must not send a MINID past the slowest consumer");
        partition.ClampReleased.Should().BeFalse("200 entries is far below 80% of MaxLen 100000");
        partition.Cutoff.Should().BeGreaterThan(ids[199], "every entry is an hour old and retention is 30 minutes");

        (await this.LengthAsync(topic)).Should().Be(150L, "entries 50..199 sit at or after the clamp");
        (await FirstIdAsync(db, key)).Should().Be(ids[50], "trimming stopped exactly at the slow consumer's position");

        log.Messages(LogLevel.Warning).Should().Contain(
            m => m.Contains("held back by consumer", StringComparison.Ordinal) && m.Contains(slow, StringComparison.Ordinal),
            "an operator has to be able to see which consumer is holding retention back");

        // Remove the slow consumer: the clamp moves up to the next-slowest, and no further.
        _ = await db.KeyDeleteAsync(StreamKeys.Positions(topic, slow));

        var movedUp = await trimmer.TrimTopicOnceAsync(topic);

        movedUp.Partitions[0].HeldBy.Should().Be(fast);
        movedUp.Partitions[0].MinId.Should().Be(ids[150]);
        (await this.LengthAsync(topic)).Should().Be(50L, "entries 150..199 remain");

        // Nothing claims to read the topic any more, so retention applies in full.
        _ = await db.KeyDeleteAsync(StreamKeys.Positions(topic, fast));

        var unclamped = await trimmer.TrimTopicOnceAsync(topic);

        unclamped.Partitions[0].HeldBy.Should().BeNull();
        unclamped.Partitions[0].MinId.Should().Be(unclamped.Partitions[0].Cutoff);
        (await this.LengthAsync(topic)).Should().Be(0L, "every entry is older than the retention cutoff");
    }

    /// <summary>
    /// S15. The hosted-service form sweeps on its own timer — the same code path
    /// <see cref="BackgroundTrimmer.TrimTopicOnceAsync"/> exercises, but reached through
    /// <see cref="BackgroundTrimmer.StartAsync"/> as the generic host would.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S15_background_trimmer_sweeps_on_its_own_timer()
    {
        var topic = fixture.NewTopic();
        var options = new TopicOptions
        {
            Partitions = 1,
            MaxLen = 100_000,
            Trim = TrimMode.Exact,
            RetentionSeconds = 1_800,
            BackgroundTrimIntervalSeconds = 1,
        };

        BackgroundTrimmer.IsEnabled(options).Should().BeTrue(
            "an interval, a retention window and a trim mode that deletes are all present");

        await using var provider = this.Provider(topic, options);
        var db = provider.Connection.GetDatabase();
        var key = StreamKeys.Stream(topic, 0, options.CoLocatePartitions);

        _ = await WriteDatedEntriesAsync(db, key, DateTimeOffset.UtcNow.AddHours(-1), 100);
        (await this.LengthAsync(topic)).Should().Be(100L);

        await using var trimmer = new BackgroundTrimmer(this.RootOptions(topic, options), provider.Connection, new CapturingLogger());

        trimmer.Topics.Should().ContainSingle().Which.Should().Be(topic);

        await trimmer.StartAsync(CancellationToken.None);
        trimmer.IsRunning.Should().BeTrue();

        await RedisStreamsFixture.WaitUntilAsync(
            async () => await this.LengthAsync(topic) == 0L,
            TimeSpan.FromSeconds(20),
            "the timer loop to sweep the topic at its 1s interval");

        await trimmer.StopAsync(CancellationToken.None);
        trimmer.IsRunning.Should().BeFalse();
    }

    /// <summary>
    /// R-26. The trimmer runs because the <b>generic host</b> started it — not because a test
    /// constructed one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the assertion the whole suite was missing. <see cref="BackgroundTrimmer"/> was
    /// complete, correct and covered by every other test in this file, and <b>nothing in the library
    /// ever built one</b>: a service that set <c>BackgroundTrimIntervalSeconds</c> and
    /// <c>RetentionSeconds</c> got no time-based retention whatsoever, only the inline <c>MAXLEN</c>
    /// on <c>XADD</c>. Hand-construction is exactly what hid that, so nothing here constructs a
    /// trimmer: the topic is configured in <c>IConfiguration</c>, the registration goes through
    /// <c>AddStreamPublisher</c>, and the sweep is triggered by <c>host.StartAsync</c>.
    /// </para>
    /// <para>
    /// A publisher rather than a consumer, deliberately: retention belongs to the topic, so a
    /// publish-only pod must trim too, and this keeps the test off the consumer machinery entirely.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S15_background_trimmer_runs_when_the_generic_host_starts_it()
    {
        var topic = fixture.NewTopic();
        var options = new TopicOptions
        {
            Partitions = 1,
            MaxLen = 100_000,
            Trim = TrimMode.Exact,
            RetentionSeconds = 1_800,
            BackgroundTrimIntervalSeconds = 1,
        };

        var key = StreamKeys.Stream(topic, 0, options.CoLocatePartitions);
        _ = await WriteDatedEntriesAsync(fixture.Db, key, DateTimeOffset.UtcNow.AddHours(-1), 100);
        (await this.LengthAsync(topic)).Should().Be(100L);

        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
        {
            EnvironmentName = "Production",
        });

        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["Streams:ConnectionString"] = fixture.ConnectionString,
            [$"Streams:Topics:{topic}:Partitions"] = "1",
            [$"Streams:Topics:{topic}:MaxLen"] = "100000",
            [$"Streams:Topics:{topic}:Trim"] = "Exact",
            [$"Streams:Topics:{topic}:RetentionSeconds"] = "1800",
            [$"Streams:Topics:{topic}:BackgroundTrimIntervalSeconds"] = "1",
        });

        _ = builder.AddStreamPublisher(topic);

        using var host = builder.Build();

        host.Services.GetServices<IHostedService>().OfType<BackgroundTrimmer>().Should().ContainSingle(
            "the trimmer must be a hosted service of the process, not something a test builds")
            .Which.Topics.Should().ContainSingle().Which.Should().Be(topic);

        await host.StartAsync(CancellationToken.None);

        try
        {
            await RedisStreamsFixture.WaitUntilAsync(
                async () => await this.LengthAsync(topic) == 0L,
                TimeSpan.FromSeconds(20),
                "the host-started trimmer to apply the configured 30-minute retention window");
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    // ------------------------------------------------------------------ S15b: the clamp release

    /// <summary>
    /// S15b — the OOM valve, on a <b>production-shaped</b> retention window. A consumer stuck since
    /// well before the window holds its position while publishing continues; below the release
    /// threshold the trimmer honours that clamp, and above it the trimmer overrides the clamp and
    /// deletes that consumer's unprocessed backlog on purpose so Redis does not run out of memory.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why the retention is two hours and the position is aged by hand.</b> This test used to run
    /// with <c>RetentionSeconds = 1</c>, and that one second was hiding the defect it exists to catch.
    /// With a realistic window the release cannot work the way the code used to assume: the stuck
    /// consumer's position is <em>old</em> — that is what being stuck means — while every entry still
    /// in the stream was published minutes ago and is therefore <b>newer</b> than the retention
    /// cutoff. <c>XTRIM MINID &lt;cutoff&gt;</c> deletes nothing at all in that state, so a release
    /// that trims only to the cutoff frees no memory while logging an Error and incrementing
    /// <c>streams.clamp.released</c> on every single sweep. That is asserted below, explicitly, as
    /// "the cutoff is older than the oldest surviving entry".
    /// </para>
    /// <para>
    /// The consumer really is blocked on a <see cref="DontIgnoreException"/> and really does refuse to
    /// advance — that half is exercised against a live host. Its recorded position is then aged to
    /// yesterday, because the alternative way to have a position two hours behind the cutoff is to
    /// wait two hours.
    /// </para>
    /// <para>
    /// <b>Why the numbers are what they are.</b> <c>MaxLen</c> 1000 with the default 0.8 threshold
    /// releases at 800 entries. The test never lets the stream reach 1000, so inline <c>MAXLEN</c>
    /// never fires: every deletion observed below is the <em>background trimmer's</em> doing and
    /// cannot be confused with the blind inline window. That isolation is the whole point.
    /// </para>
    /// <para>
    /// The last phase pins the reporting contract: the Error and the metric fire once per
    /// <em>transition</em> into the released state. A partition that stays released for a day is one
    /// alertable event, not one per sweep, and an alert built on the old behaviour would have been
    /// counting sweeps rather than incidents.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S15b_clamp_release_frees_memory_when_every_entry_is_newer_than_the_cutoff()
    {
        const int MaxLen = 1_000;
        const int ReleaseAt = 800;
        const int BlockAt = 10;
        const int RetentionSeconds = 2 * 60 * 60;

        var topic = fixture.NewTopic();
        var consumerName = fixture.NewConsumer();
        var options = new TopicOptions
        {
            Partitions = 1,
            MaxLen = MaxLen,
            Trim = TrimMode.Exact,
            RetentionSeconds = RetentionSeconds,
            ClampReleaseThreshold = 0.8,
        };

        await using var provider = this.Provider(topic, options);
        var db = provider.Connection.GetDatabase();
        var key = StreamKeys.Stream(topic, 0, options.CoLocatePartitions);
        var positions = StreamKeys.Positions(topic, consumerName);

        var publisher = await this.PublisherAsync(provider, topic, options);
        var ids = await PublishAsync(publisher, 100);

        var handler = new BlockingHandler(BlockAt);
        var consumerLog = new CapturingLogger();

        await using (var host = new StreamConsumerHost(
            this.RootOptions(topic, options),
            new ConsumerOptions
            {
                Topic = topic,
                Consumer = consumerName,
                BatchSize = 1,
                PersistIntervalMs = 50,
            },
            consumerName,
            handler.HandleAsync,
            provider,
            consumerLog))
        {
            await host.StartAsync(CancellationToken.None);

            try
            {
                await handler.Blocked.WaitAsync(TimeSpan.FromSeconds(30));

                await RedisStreamsFixture.WaitUntilAsync(
                    async () => await ReadPositionAsync(db, positions, 0) == ids[BlockAt - 1],
                    TimeSpan.FromSeconds(30),
                    "the blocked consumer to flush the position of the last batch it did handle");

                (await ReadPositionAsync(db, positions, 0)).Should().Be(
                    ids[BlockAt - 1],
                    "the position must not advance past a batch the handler never completed");

                await RedisStreamsFixture.WaitUntilAsync(
                    () => handler.Attempts > 1,
                    TimeSpan.FromSeconds(30),
                    "the blocked partition to retry the same batch — a clamp only holds while the consumer really is stuck");

                handler.Processed.Should().Equal(Enumerable.Range(0, BlockAt), "nothing past the poison batch may be handled");
            }
            finally
            {
                await host.StopAsync(CancellationToken.None);
            }
        }

        // The same consumer, still stuck, but stuck since yesterday — which is the state a two-hour
        // retention window describes and the one a live host cannot reach inside a test.
        var stuckSince = StreamId.FromDate(DateTimeOffset.UtcNow.AddDays(-1));
        await db.HashSetAsync(positions, RedisPositionStore.Field(0), PositionRecord.Format(stuckSince, Guid.NewGuid()));

        var trimLog = new CapturingLogger();
        using var released = new CounterProbe("streams.clamp.released");
        await using var trimmer = new BackgroundTrimmer(this.RootOptions(topic, options), provider.Connection, trimLog);

        // ---- Below the threshold: the clamp is honoured, and honouring it here deletes nothing at
        // all, because the position it clamps to is older than every entry in the stream.
        var honoured = (await trimmer.TrimTopicOnceAsync(topic)).Partitions.Should().ContainSingle().Subject;

        honoured.ClampReleased.Should().BeFalse("100 entries is below the 800-entry release point");
        honoured.HeldBy.Should().Be(consumerName);
        honoured.MinId.Should().Be(stuckSince, "retention wanted more; the clamp stopped it at the stuck position");
        honoured.Removed.Should().Be(0L, "nothing in the stream is older than a position from yesterday");
        (await this.LengthAsync(topic)).Should().Be(100L);
        (await FirstIdAsync(db, key)).Should().Be(ids[0], "nothing at or after the stuck consumer's position may be deleted");
        released.TotalFor("topic", topic).Should().Be(0L, "no backlog has been sacrificed yet");

        // ---- Push the partition over the release point, while staying under MaxLen so that inline
        // MAXLEN trimming cannot be what deletes the backlog.
        _ = await PublishAsync(publisher, 800, from: 100);

        var atThreshold = await this.LengthAsync(topic);
        atThreshold.Should().BeGreaterThanOrEqualTo(ReleaseAt, "the release point is 0.8 x MaxLen");
        atThreshold.Should().BeLessThan(MaxLen, "inline MAXLEN must not fire, or it — not the trimmer — would be doing the deleting");

        // ---- Above the threshold: the valve opens, and it has to free memory rather than merely say so.
        var sacrificed = (await trimmer.TrimTopicOnceAsync(topic)).Partitions.Should().ContainSingle().Subject;

        sacrificed.ClampReleased.Should().BeTrue(
            "holding the clamp above the release point would let one stuck consumer run Redis out of memory and " +
            "lose every stream, position and service on the instance — the clamp must yield");
        sacrificed.HeldBy.Should().Be(consumerName, "the sacrificed consumer has to be named, not just counted");
        sacrificed.Clamp.Should().Be(stuckSince);
        sacrificed.Length.Should().Be(atThreshold);

        // The assertion this test exists for. Trimming to the retention cutoff cannot remove a single
        // entry in this state, so a release that only sends MINID <cutoff> frees nothing while
        // reporting a sacrifice it never made.
        sacrificed.Cutoff.Should().BeLessThan(
            ids[0],
            "every entry in the stream is newer than a two-hour cutoff, so MINID alone deletes nothing");

        var afterRelease = await this.LengthAsync(topic);

        afterRelease.Should().BeLessThan(atThreshold, "the sweep has to actually shrink the stream, not merely report that it would");
        afterRelease.Should().BeLessThanOrEqualTo(ReleaseAt, "a release drops the partition back to the release threshold or below");
        sacrificed.Removed.Should().Be(atThreshold - afterRelease, "the reported count is what XTRIM really deleted");

        var head = await FirstIdAsync(db, key);

        head.Should().BeGreaterThan(
            ids[0],
            "the entries the stuck consumer had not processed are gone — that is the trade being made");
        sacrificed.MinId.Should().Be(head, "a released clamp reports the floor now in force, not the cutoff it could not use");

        released.TotalFor("topic", topic).Should().Be(
            1L,
            "streams.clamp.released is the only signal that a consumer silently lost data; it must fire once per release");

        var releaseTags = released.TagsFor("topic", topic).Should().ContainSingle().Which;

        releaseTags["consumer"].Should().Be(consumerName, "the metric has to identify who lost data, not just that someone did");
        releaseTags["partition"].Should().Be("0");

        trimLog.Messages(LogLevel.Error).Should().ContainSingle(
            m => m.Contains("RELEASED", StringComparison.Ordinal) && m.Contains(consumerName, StringComparison.Ordinal),
            "the Error log is what an operator greps for after a consumer reports missing messages");

        // ---- Still released on the next sweep: one incident, not one per sweep.
        _ = await PublishAsync(publisher, 200, from: 900);

        var stillReleased = (await trimmer.TrimTopicOnceAsync(topic)).Partitions.Should().ContainSingle().Subject;

        stillReleased.ClampReleased.Should().BeTrue("the consumer is still stuck and the partition is still over the line");
        released.TotalFor("topic", topic).Should().Be(
            1L,
            "the metric counts transitions into the released state, not sweeps taken while in it — an alert on the " +
            "old behaviour was counting timer ticks");
        trimLog.Messages(LogLevel.Error).Should().ContainSingle(m => m.Contains("RELEASED", StringComparison.Ordinal));

        // ---- Back under the line, then over it again: that is a second incident and it is reported.
        var recovered = (await trimmer.TrimTopicOnceAsync(topic)).Partitions.Should().ContainSingle().Subject;
        recovered.ClampReleased.Should().BeFalse("the partition is back below the release point");

        _ = await PublishAsync(publisher, 200, from: 1_100);
        _ = await trimmer.TrimTopicOnceAsync(topic);

        released.TotalFor("topic", topic).Should().Be(2L, "a fresh release after a recovery is a fresh incident");
    }

    // ------------------------------------------------------------------ S15c: noeviction

    /// <summary>
    /// S15c. With Redis at <c>maxmemory</c> under <c>noeviction</c>, <c>XADD</c> throws to the
    /// caller, position flushes fail and are retried without failing a batch, and the consumer keeps
    /// processing throughout.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the state <c>maxmemory</c> exists to produce: a visible, localised write failure
    /// instead of a silent instance-wide OOM kill. Three separate behaviours have to hold at once, and
    /// they pull in different directions, which is why they are asserted together rather than in three
    /// tests:
    /// </para>
    /// <list type="bullet">
    ///   <item><description><b>Publishing must fail loudly.</b> A publisher that swallowed this would
    ///   drop messages silently; the caller is the only one who can decide what to do.</description></item>
    ///   <item><description><b>Consuming must not fail at all.</b> Reads are not rejected at
    ///   <c>maxmemory</c>, so the only thing that can break a consumer here is the library treating a
    ///   failed checkpoint as a failed batch.</description></item>
    ///   <item><description><b>The checkpoint must be retried, not abandoned.</b> Once memory frees
    ///   up, the position has to land on its own — otherwise the outage silently converts into a
    ///   replay on the next restart.</description></item>
    /// </list>
    /// <para>
    /// The container's <c>maxmemory</c> is set below live usage and restored in a <c>finally</c>: the
    /// fixture's Redis is shared with the rest of the collection, and leaving it wedged would fail
    /// every test after this one for a reason that has nothing to do with them.
    /// </para>
    /// </remarks>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task S15c_at_maxmemory_publishing_throws_while_the_consumer_keeps_processing()
    {
        const int Published = 200;

        var topic = fixture.NewTopic();
        var consumerName = fixture.NewConsumer();
        var options = new TopicOptions { Partitions = 1, MaxLen = 100_000, Trim = TrimMode.Approx };

        await using var provider = this.Provider(topic, options);
        var db = provider.Connection.GetDatabase();
        var positions = StreamKeys.Positions(topic, consumerName);

        var publisher = await this.PublisherAsync(provider, topic, options);
        var ids = await PublishAsync(publisher, Published);

        // The gate keeps every entry unprocessed — and so the position hash unwritten — until Redis is
        // already at maxmemory, so every checkpoint this consumer ever attempts happens under OOM.
        var handler = new GatedHandler();
        var consumerLog = new CapturingLogger();

        await using var host = new StreamConsumerHost(
            this.RootOptions(topic, options),
            new ConsumerOptions
            {
                Topic = topic,
                Consumer = consumerName,
                BatchSize = 50,
                PersistIntervalMs = 50,
            },
            consumerName,
            handler.HandleAsync,
            provider,
            consumerLog);

        await host.StartAsync(CancellationToken.None);

        using var flushFailures = new CounterProbe("streams.positions.flush_failures");
        var server = fixture.Redis.GetServer(fixture.Redis.GetEndPoints()[0]);

        try
        {
            (await ReadRawPositionAsync(db, positions, 0)).Should().BeNull(
                "nothing has been handled yet, so there is no checkpoint to write");

            await SetMaxMemoryBelowUsageAsync(server);

            // 1. XADD fails, and it fails to the caller rather than being swallowed or retried.
            StreamTransportException? rejected = null;

            try
            {
                _ = await publisher.PublishAsync(string.Empty, Encoding.UTF8.GetBytes("rejected"), MessageType);
            }
            catch (StreamTransportException ex)
            {
                rejected = ex;
            }

            rejected.Should().NotBeNull(
                "the publisher does not retry and must not hide an OOM from the caller — dropping the message silently " +
                "is the one outcome nobody can recover from");

            rejected!.InnerException.Should().BeAssignableTo<RedisException>(
                "the transport failure is wrapped, not replaced: the caller still needs the server's own reason");

            rejected.InnerException!.Message.Should().Contain(
                "OOM",
                "Redis rejects denyoom commands such as XADD once used memory passes maxmemory");

            (await this.LengthAsync(topic)).Should().Be((long)Published, "the rejected entry was never appended");

            // 2. The consumer keeps processing the whole backlog while checkpoints are impossible.
            handler.Open();

            await RedisStreamsFixture.WaitUntilAsync(
                () => handler.Count >= Published,
                TimeSpan.FromSeconds(30),
                "the consumer to work through its backlog while Redis refuses writes");

            handler.Bodies.Should().Equal(
                Enumerable.Range(0, Published),
                "a failed checkpoint must not fail, reorder or drop a batch");

            // 3. The flush fails, is counted, is logged as retryable, and never reaches the handler.
            await RedisStreamsFixture.WaitUntilAsync(
                () => flushFailures.TotalFor("topic", topic) > 0,
                TimeSpan.FromSeconds(30),
                "the position flush to fail and be counted on streams.positions.flush_failures");

            (await ReadRawPositionAsync(db, positions, 0)).Should().BeNull(
                "the checkpoint cannot land while Redis is refusing writes");

            consumerLog.Messages(LogLevel.Warning).Should().Contain(
                m => m.Contains("position flush failed", StringComparison.Ordinal) && m.Contains("retrying", StringComparison.Ordinal),
                "a checkpoint outage is degraded-but-recoverable and must say so, not fail the batch");

            handler.Faults.Should().Be(0, "nothing about a failed position write is the handler's problem");

            // 4. Memory frees up: the flusher's own retry lands the checkpoint with no restart.
            await RestoreMaxMemoryAsync(server);

            await RedisStreamsFixture.WaitUntilAsync(
                async () => await ReadPositionAsync(db, positions, 0) == ids[^1],
                TimeSpan.FromSeconds(30),
                "the re-queued position flush to succeed once Redis accepts writes again");

            // 5. And the publisher recovers too — the failure was the server's state, not a poisoned client.
            var recovered = await PublishAsync(publisher, 5, from: Published);

            await RedisStreamsFixture.WaitUntilAsync(
                () => handler.Count >= Published + 5,
                TimeSpan.FromSeconds(30),
                "the consumer to pick up entries published after the outage");

            await RedisStreamsFixture.WaitUntilAsync(
                async () => await ReadPositionAsync(db, positions, 0) >= recovered[^1],
                TimeSpan.FromSeconds(30),
                "the consumer to check point the entries published after the outage");
        }
        finally
        {
            await RestoreMaxMemoryAsync(server);
            await host.StopAsync(CancellationToken.None);
        }
    }

    // ------------------------------------------------------------------ helpers

    /// <summary>Root options naming exactly one topic, pointed at the fixture's container.</summary>
    private StreamOptions RootOptions(string topic, TopicOptions topicOptions) => new()
    {
        ConnectionString = fixture.ConnectionString,
        Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
    };

    /// <summary>
    /// A connection provider over the fixture's Redis, deliberately built from the <em>non</em>-admin
    /// connection string so that library code reaching for an admin command shows up as a failure here.
    /// </summary>
    private StreamsConnectionProvider Provider(string topic, TopicOptions topicOptions)
        => new(this.RootOptions(topic, topicOptions), services: null, logger: null);

    /// <summary>Reconciles the topic, then returns a publisher bound to it.</summary>
    private async Task<StreamPublisher> PublisherAsync(
        StreamsConnectionProvider provider,
        string topic,
        TopicOptions topicOptions)
    {
        await StreamAdmin.EnsureTopicAsync(provider.Connection, topic, topicOptions, null, CancellationToken.None);
        return new StreamPublisher(provider.Connection.GetDatabase(), topic, topicOptions);
    }

    /// <summary><c>XLEN</c> of partition 0, read on the fixture's own connection.</summary>
    private async Task<long> LengthAsync(string topic)
        => await fixture.Db.StreamLengthAsync(StreamKeys.Stream(topic, 0));

    /// <summary>Publishes <paramref name="count"/> entries whose bodies are their ordinal.</summary>
    private static async Task<StreamId[]> PublishAsync(IStreamPublisher publisher, int count, int from = 0)
    {
        var ids = new StreamId[count];

        for (var i = 0; i < count; i++)
        {
            ids[i] = await publisher.PublishAsync(
                string.Empty,
                Encoding.UTF8.GetBytes((from + i).ToString(CultureInfo.InvariantCulture)),
                MessageType);
        }

        return ids;
    }

    /// <summary>
    /// Appends entries at explicit, consecutive-millisecond ids starting at <paramref name="start"/>.
    /// </summary>
    /// <remarks>
    /// Nothing decodes these, so they carry a bare field rather than the wire codec's: the point is to
    /// put entries at a <em>known instant</em>, which auto-ids cannot do, so that a retention cutoff is
    /// a fixed point rather than a race with how long the test itself takes to run.
    /// </remarks>
    private static async Task<StreamId[]> WriteDatedEntriesAsync(
        IDatabase db,
        RedisKey key,
        DateTimeOffset start,
        int count)
    {
        var baseMs = start.ToUnixTimeMilliseconds();
        var ids = new StreamId[count];

        for (var i = 0; i < count; i++)
        {
            ids[i] = new StreamId(baseMs + i, 0);
            _ = await db.StreamAddAsync(key, "b", i, ids[i].Format());
        }

        return ids;
    }

    /// <summary>The id of the oldest entry still in the stream.</summary>
    private static async Task<StreamId> FirstIdAsync(IDatabase db, RedisKey key)
    {
        var entries = await db.StreamRangeAsync(key, count: 1);

        entries.Should().NotBeEmpty("the stream was expected to still hold entries");

        return StreamId.Parse(entries[0].Id.ToString());
    }

    /// <summary>The raw positions-hash field for a partition, or <see langword="null"/>.</summary>
    private static async Task<string?> ReadRawPositionAsync(IDatabase db, RedisKey positions, int partition)
        => (string?)await db.HashGetAsync(positions, RedisPositionStore.Field(partition));

    /// <summary>The stored position for a partition, or <see cref="StreamId.Min"/> when unwritten.</summary>
    private static async Task<StreamId> ReadPositionAsync(IDatabase db, RedisKey positions, int partition)
    {
        var raw = await ReadRawPositionAsync(db, positions, partition);

        return raw is not null && PositionRecord.TryParse(raw, out var record) ? record.Id : StreamId.Min;
    }

    /// <summary>
    /// Drives the container below its own memory usage, so every <c>denyoom</c> command is rejected.
    /// </summary>
    /// <remarks>
    /// The ceiling is derived from live <c>used_memory</c> rather than hard-coded, so the test does not
    /// quietly stop reproducing the state it is named after when the image or the dataset changes.
    /// </remarks>
    private static async Task SetMaxMemoryBelowUsageAsync(IServer server)
    {
        var info = await server.InfoRawAsync("memory") ?? string.Empty;

        var line = info
            .Split('\n')
            .FirstOrDefault(l => l.StartsWith("used_memory:", StringComparison.Ordinal))
            ?? throw new InvalidOperationException("INFO memory did not report used_memory.");

        var used = long.Parse(line["used_memory:".Length..].Trim(), CultureInfo.InvariantCulture);

        await server.ConfigSetAsync("maxmemory-policy", "noeviction");
        await server.ConfigSetAsync("maxmemory", (used / 2).ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>Lifts the memory ceiling again. Safe to call twice.</summary>
    private static async Task RestoreMaxMemoryAsync(IServer server)
    {
        await server.ConfigSetAsync("maxmemory", "0");
        await server.ConfigSetAsync("maxmemory-policy", "noeviction");
    }

    /// <summary>A concrete <see cref="DontIgnoreException"/> — the base class is abstract.</summary>
    private sealed class StuckDownstreamException(string message) : DontIgnoreException(message);

    /// <summary>
    /// Handles every entry below <c>blockAt</c> and then throws a <see cref="DontIgnoreException"/>
    /// forever, which is the shape of a consumer wedged on a dead downstream.
    /// </summary>
    private sealed class BlockingHandler(int blockAt)
    {
        private readonly Lock gate = new();
        private readonly List<int> processed = [];
        private readonly TaskCompletionSource blocked = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int attempts;

        /// <summary>Completes the first time the handler refuses a batch.</summary>
        internal Task Blocked => this.blocked.Task;

        /// <summary>How many times the poison batch has been attempted.</summary>
        internal int Attempts => Volatile.Read(ref this.attempts);

        /// <summary>Ordinals of every entry handled successfully, in order.</summary>
        internal IReadOnlyList<int> Processed
        {
            get
            {
                lock (this.gate)
                {
                    return this.processed.ToArray();
                }
            }
        }

        internal ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            _ = ct;
            var span = batch.Span;

            for (var i = 0; i < span.Length; i++)
            {
                var ordinal = Ordinal(span[i]);

                if (ordinal >= blockAt)
                {
                    _ = Interlocked.Increment(ref this.attempts);
                    _ = this.blocked.TrySetResult();

                    throw new StuckDownstreamException(
                        $"entry {ordinal} must not be skipped; the downstream this handler needs is unavailable.");
                }

                lock (this.gate)
                {
                    this.processed.Add(ordinal);
                }
            }

            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Holds every batch until <see cref="Open"/> is called, then records what it is given. It exists
    /// so a test can put Redis into a state <em>before</em> the consumer has written anything.
    /// </summary>
    private sealed class GatedHandler
    {
        private readonly TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Lock guard = new();
        private readonly List<int> bodies = [];

        private int faults;

        /// <summary>Entries handled so far.</summary>
        internal int Count
        {
            get
            {
                lock (this.guard)
                {
                    return this.bodies.Count;
                }
            }
        }

        /// <summary>Ordinals handled, in order.</summary>
        internal IReadOnlyList<int> Bodies
        {
            get
            {
                lock (this.guard)
                {
                    return this.bodies.ToArray();
                }
            }
        }

        /// <summary>Batches the handler itself saw fail — always zero unless the library leaks one in.</summary>
        internal int Faults => Volatile.Read(ref this.faults);

        /// <summary>Releases every parked and future batch.</summary>
        internal void Open() => this.gate.TrySetResult();

        internal async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            try
            {
                await this.gate.Task.WaitAsync(ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception)
            {
                _ = Interlocked.Increment(ref this.faults);
                throw;
            }

            var span = batch.Span;

            lock (this.guard)
            {
                for (var i = 0; i < span.Length; i++)
                {
                    this.bodies.Add(Ordinal(span[i]));
                }
            }
        }
    }

    /// <summary>The ordinal a test publisher put in the body.</summary>
    private static int Ordinal(in StreamMsg msg)
        => int.Parse(Encoding.UTF8.GetString(msg.Body.Span), CultureInfo.InvariantCulture);

    /// <summary>
    /// Records every measurement on one Orange.Lib.Streams counter, with its tags.
    /// </summary>
    /// <remarks>
    /// Measurements are filtered by the <c>topic</c> tag at assertion time, so a counter incremented
    /// by an unrelated test running in parallel can never be mistaken for this test's.
    /// </remarks>
    private sealed class CounterProbe : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly Lock gate = new();
        private readonly List<(long Value, Dictionary<string, string> Tags)> measurements = [];

        internal CounterProbe(string instrument)
        {
            this.listener.InstrumentPublished = (published, listening) =>
            {
                if (published.Meter.Name == MeterName &&
                    string.Equals(published.Name, instrument, StringComparison.Ordinal))
                {
                    listening.EnableMeasurementEvents(published);
                }
            };

            this.listener.SetMeasurementEventCallback<long>(this.OnMeasurement);
            this.listener.Start();
        }

        /// <summary>The sum of every measurement carrying the given tag value.</summary>
        internal long TotalFor(string tag, string value)
        {
            lock (this.gate)
            {
                return this.measurements
                    .Where(m => m.Tags.TryGetValue(tag, out var actual) && string.Equals(actual, value, StringComparison.Ordinal))
                    .Sum(m => m.Value);
            }
        }

        /// <summary>The tag sets of every measurement carrying the given tag value.</summary>
        internal IReadOnlyList<IReadOnlyDictionary<string, string>> TagsFor(string tag, string value)
        {
            lock (this.gate)
            {
                return this.measurements
                    .Where(m => m.Tags.TryGetValue(tag, out var actual) && string.Equals(actual, value, StringComparison.Ordinal))
                    .Select(m => (IReadOnlyDictionary<string, string>)m.Tags)
                    .ToArray();
            }
        }

        /// <inheritdoc />
        public void Dispose() => this.listener.Dispose();

        private void OnMeasurement(
            Instrument instrument,
            long measurement,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state)
        {
            _ = instrument;
            _ = state;

            var copied = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var tag in tags)
            {
                copied[tag.Key] = tag.Value?.ToString() ?? string.Empty;
            }

            lock (this.gate)
            {
                this.measurements.Add((measurement, copied));
            }
        }
    }

    /// <summary>
    /// An <see cref="ILogger"/> that keeps every formatted message, so a test can assert on the line
    /// an operator would actually grep for.
    /// </summary>
    private sealed class CapturingLogger : ILogger
    {
        private readonly Lock gate = new();
        private readonly List<(LogLevel Level, string Message)> entries = [];

        /// <summary>Formatted messages at one level, in order.</summary>
        internal IReadOnlyList<string> Messages(LogLevel level)
        {
            lock (this.gate)
            {
                return this.entries.Where(e => e.Level == level).Select(e => e.Message).ToArray();
            }
        }

        /// <inheritdoc />
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => NullScope.Instance;

        /// <inheritdoc />
        public bool IsEnabled(LogLevel logLevel) => true;

        /// <inheritdoc />
        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);

            var message = formatter(state, exception);

            lock (this.gate)
            {
                this.entries.Add((logLevel, message));
            }
        }

        private sealed class NullScope : IDisposable
        {
            internal static readonly NullScope Instance = new();

            public void Dispose()
            {
            }
        }
    }
}
