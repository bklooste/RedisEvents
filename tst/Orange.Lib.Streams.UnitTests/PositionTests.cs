using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Threading.Channels;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orange.Lib.Streams.Admin;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Ownership;
using Orange.Lib.Streams.Positions;
using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.UnitTests;

/// <summary>
/// Unit tests for the position and replay half of the library: <see cref="PositionFlusher"/>'s
/// coalescing, the per-<see cref="PersistMode"/> write counts, <see cref="StartPosition"/>'s
/// resolution table, the reset-marker protocol live and cold, and second-writer detection.
/// </summary>
/// <remarks>
/// No Docker and no Redis. The two things that would need one — the multiplexer and the database —
/// are <see cref="DispatchProxy"/> fakes over an in-memory hash, which is enough because the flusher
/// only ever issues <c>HMGET</c>, <c>HDEL</c>, <c>HGETALL</c> and <c>HSET</c> against it.
/// </remarks>
public class PositionTests
{
    private const string Topic = "orders";
    private const string Consumer = "svc";

    private static string PositionsKey => StreamKeys.Positions(Topic, Consumer).ToString()!;

    private static string OwnershipKey => StreamKeys.Ownership(Topic, Consumer).ToString()!;

    /// <summary>Two fixed identities, so a tiebreak assertion is not a coin toss.</summary>
    private static readonly Guid Lower = new("11111111-1111-4111-8111-111111111111");

    /// <inheritdoc cref="Lower"/>
    private static readonly Guid Higher = new("22222222-2222-4222-8222-222222222222");

    private static StreamId Id(long ms, long seq = 0) => new(ms, seq);

    // ------------------------------------------------------------ flusher: coalescing

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Flusher_ThousandRecords_CoalesceToOneWrite()
    {
        var store = new CountingStore();
        await using var flusher = NewFlusher(store, partitions: 4);

        for (var i = 1; i <= 1000; i++)
        {
            flusher.Record(0, Id(i));
        }

        flusher.DirtyCount.Should().Be(1, "a thousand records on one partition are one dirty slot");

        await flusher.FlushAsync();

        store.Writes.Should().Be(1);
        store.Saved.Should().ContainSingle().Which.Should().Equal((0, Id(1000)));
        flusher.Flushes.Should().Be(1);
        flusher.DirtyCount.Should().Be(0);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Flusher_SecondFlushWithNothingNew_WritesNothing()
    {
        var store = new CountingStore();
        await using var flusher = NewFlusher(store, partitions: 4);

        flusher.Record(0, Id(10));
        await flusher.FlushAsync();
        await flusher.FlushAsync();

        store.Writes.Should().Be(1, "the second flush found nothing dirty");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Flusher_OnlyDirtyPartitions_AreWritten()
    {
        var store = new CountingStore();
        await using var flusher = NewFlusher(store, partitions: 8);

        flusher.Record(1, Id(100, 3));
        flusher.Record(5, Id(200, 7));

        flusher.DirtyCount.Should().Be(2);

        await flusher.FlushAsync();

        store.Writes.Should().Be(1, "every dirty partition rides one HSET");
        store.Saved.Should().ContainSingle().Which.Should().Equal((1, Id(100, 3)), (5, Id(200, 7)));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Flusher_Idle_WritesNothingAtAll()
    {
        var store = new CountingStore();
        await using var flusher = NewFlusher(store, partitions: 4, interval: TimeSpan.FromMilliseconds(15));

        flusher.Start();
        await Task.Delay(250);

        store.Writes.Should().Be(0, "an idle consumer generates no position traffic at all");
        store.Calls.Should().Be(0, "not even an empty SaveAsync is issued");
        flusher.Flushes.Should().Be(0);

        await flusher.StopAsync();

        store.Writes.Should().Be(0, "the final flush of an idle flusher is also a no-op");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Flusher_RunningTimer_WritesRoughlyOncePerInterval()
    {
        var store = new CountingStore();
        var interval = TimeSpan.FromMilliseconds(100);
        await using var flusher = NewFlusher(store, partitions: 2, interval: interval);

        flusher.Start();

        // Far more records than intervals: the whole point is that the write count tracks elapsed
        // intervals, not message count.
        for (var i = 1; i <= 500; i++)
        {
            flusher.Record(0, Id(i));
            flusher.Record(1, Id(i));
        }

        await Task.Delay(350);
        await flusher.StopAsync();

        store.Writes.Should().BeGreaterThan(0);
        store.Writes.Should().BeLessThan(10, "500 records over ~3 intervals must not cost 500 writes");
        var everything = store.Saved.SelectMany(w => w).ToArray();
        everything.Should().Contain((0, Id(500))).And.Contain((1, Id(500)),
            "whenever the last tick fell, both partitions' final positions reached the store");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Flusher_StopAsync_FlushesTheLastInterval()
    {
        var store = new CountingStore();
        await using var flusher = NewFlusher(store, partitions: 2, interval: TimeSpan.FromMinutes(5));

        flusher.Start();
        flusher.Record(0, Id(42));

        await flusher.StopAsync();

        store.Writes.Should().Be(1, "a clean shutdown must not redeliver the last interval");
        store.Last!.Should().Equal((0, Id(42)));
    }

    // ------------------------------------------------------------ flusher: failure handling

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Flusher_FlushFailure_DoesNotThrowAndRetriesNextTime()
    {
        var store = new CountingStore { Throw = new InvalidOperationException("redis is down") };
        var log = new CapturingLogger();
        await using var flusher = NewFlusher(store, partitions: 4, log: log);

        flusher.Record(2, Id(7));

        var flush = async () => await flusher.FlushAsync();
        await flush.Should().NotThrowAsync("a position write is an optimisation, never a batch failure");

        flusher.Failures.Should().Be(1);
        flusher.Flushes.Should().Be(0);
        flusher.DirtyCount.Should().Be(1, "the drained partition is re-queued for the next tick");
        log.Entries.Should().Contain(e => e.Level == LogLevel.Warning);

        store.Throw = null;
        await flusher.FlushAsync();

        flusher.Flushes.Should().Be(1);
        store.Last!.Should().Equal((2, Id(7)));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Flusher_FlushFailureFromCancellation_IsNotLoggedButIsCounted()
    {
        var store = new CountingStore { Throw = new OperationCanceledException() };
        var log = new CapturingLogger();
        await using var flusher = NewFlusher(store, partitions: 2, log: log);

        flusher.Record(0, Id(3));
        await flusher.FlushAsync();

        flusher.Failures.Should().Be(1);
        log.Entries.Should().NotContain(e => e.Level == LogLevel.Warning);
    }

    // ------------------------------------------------------------ flusher: monotonicity assert

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Flusher_BackwardsId_TripsTheMonotonicityAssert()
    {
#if !DEBUG
        // The guard is [Conditional("DEBUG")]; in a release build there is nothing to observe.
        await Task.CompletedTask;
#else
        var store = new CountingStore();
        await using var flusher = NewFlusher(store, partitions: 2);

        flusher.Record(0, Id(100, 5));

        using var assertions = AssertionTrap.Install();

        flusher.Record(0, Id(100, 4));

        assertions.Failures.Should().Be(1, "positions are monotonic per partition");
        assertions.LastMessage.Should().Contain("moved backwards");
#endif
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Flusher_ForwardIds_DoNotTripTheAssert()
    {
        var store = new CountingStore();
        await using var flusher = NewFlusher(store, partitions: 2);

        using var assertions = AssertionTrap.Install();

        flusher.Record(0, Id(100, 1));
        flusher.Record(0, Id(100, 2));
        flusher.Record(0, Id(101, 0));
        flusher.Record(0, Id(101, 0));

        assertions.Failures.Should().Be(0, "equal or increasing ids are legal");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Flusher_RecordForUnknownPartition_IsIgnoredNotThrown()
    {
        var store = new CountingStore();
        await using var flusher = NewFlusher(store, partitions: 2);

        using var assertions = AssertionTrap.Install();

        var record = () => flusher.Record(9, Id(1));
        record.Should().NotThrow("a wiring bug must not fail the processing path");

        flusher.DirtyCount.Should().Be(0);

        await flusher.FlushAsync();
        store.Writes.Should().Be(0);
    }

    // ------------------------------------------------------------ persist modes

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Persist_AsyncBatch_RecordsPerBatchAndNeverFlushesInline()
    {
        var run = await RunProcessLoopAsync(PersistMode.AsyncBatch, batches: 5, messagesPerBatch: 3);

        run.Records.Should().Be(5, "one position per batch");
        run.Flushes.Should().Be(0, "AsyncBatch leaves persistence entirely to the flusher's timer");
        run.LastRecorded.Should().Be(Id(5, 2));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Persist_SyncBatch_FlushesOncePerBatch()
    {
        var run = await RunProcessLoopAsync(PersistMode.SyncBatch, batches: 5, messagesPerBatch: 3);

        run.Records.Should().Be(5);
        run.Flushes.Should().Be(5, "one HSET per batch, awaited before the next batch");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Persist_SyncMessage_FlushesOncePerMessage()
    {
        var run = await RunProcessLoopAsync(PersistMode.SyncMessage, batches: 5, messagesPerBatch: 3);

        run.Records.Should().Be(15, "a position per message");
        run.Flushes.Should().Be(15, "one round trip per message — the money-movement setting");
        run.HandlerCalls.Should().Be(15, "the handler sees one message at a time");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Persist_None_RecordsNothingAndWritesNothing()
    {
        var run = await RunProcessLoopAsync(PersistMode.None, batches: 5, messagesPerBatch: 3);

        run.Records.Should().Be(0, "None does not even keep the in-memory record");
        run.Flushes.Should().Be(0);
        run.HandlerCalls.Should().Be(5, "messages are still delivered; only the position is dropped");
    }

    [Theory]
    [InlineData(PersistMode.SyncBatch)]
    [InlineData(PersistMode.SyncMessage)]
    [Trait("TestType", "UnitTest")]
    public async Task Persist_SyncModeWithoutFlush_IsAConfigurationError(PersistMode persist)
    {
        var channel = Channel.CreateUnbounded<StreamBatch>();
        channel.Writer.Complete();

        var start = () => PartitionWorker.ProcessLoopAsync(
            NewContext(),
            (_, _) => ValueTask.CompletedTask,
            (_, _) => { },
            CancellationToken.None,
            channel.Reader,
            persist,
            flush: null);

        await start.Should().ThrowAsync<StreamConfigurationException>()
            .Where(e => e.Message.Contains("silently behave like AsyncBatch"));
    }

    // ------------------------------------------------------------ start-position resolution

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_Stored_WithStoredPosition_ResumesAfterIt()
    {
        var resolved = StartPosition.Resolve(StartFrom.Stored, StartFrom.Beginning, date: null, stored: Id(500, 2));

        resolved.Should().Be(new StartPosition(Id(500, 2), FromNow: false));
        resolved.ReadFrom(lastRead: null).ToString().Should().Be("500-2");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_Stored_WhenMissing_FallsBackToBeginningByDefault()
    {
        var resolved = StartPosition.Resolve(StartFrom.Stored, StartFrom.Beginning, date: null, stored: null);

        resolved.Should().Be(StartPosition.Beginning);
        resolved.After.Should().Be(StreamId.Min);
        resolved.FromNow.Should().BeFalse();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_Stored_WhenMissing_HonoursNowFallback()
    {
        var resolved = StartPosition.Resolve(StartFrom.Stored, StartFrom.Now, date: null, stored: null);

        resolved.Should().Be(StartPosition.Now);
        resolved.FromNow.Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_Stored_WhenMissing_HonoursDateFallback()
    {
        var when = DateTimeOffset.Parse("2026-09-01T00:00:00Z", CultureInfo.InvariantCulture);

        var resolved = StartPosition.Resolve(StartFrom.Stored, StartFrom.Date, when, stored: null);

        resolved.Should().Be(StartPosition.FromDate(when));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_Stored_WhenMissingIsAlsoStored_IsCircularAndThrows()
    {
        var act = () => StartPosition.Resolve(StartFrom.Stored, StartFrom.Stored, date: null, stored: null, Topic);

        act.Should().Throw<StreamConfigurationException>()
            .Where(e => e.Message.Contains("circular") && e.Message.Contains(Topic));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_Beginning_IgnoresAStoredPosition()
    {
        StartPosition.Resolve(StartFrom.Beginning, StartFrom.Now, date: null, stored: Id(999))
            .Should().Be(StartPosition.Beginning);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_Now_IgnoresAStoredPosition()
    {
        StartPosition.Resolve(StartFrom.Now, StartFrom.Beginning, date: null, stored: Id(999))
            .Should().Be(StartPosition.Now);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_Date_PlacesThePositionImmediatelyBeforeThatMillisecond()
    {
        var when = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000);

        var resolved = StartPosition.Resolve(StartFrom.Date, StartFrom.Beginning, when, stored: Id(1));

        resolved.FromNow.Should().BeFalse();
        resolved.After.Should().Be(new StreamId(1_699_999_999_999, long.MaxValue));
        StartPosition.Before(StreamId.FromDate(when)).Should().Be(resolved.After,
            "an entry written exactly on that millisecond must be delivered, not skipped");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_DateWithoutADate_Throws()
    {
        var act = () => StartPosition.Resolve(StartFrom.Date, StartFrom.Beginning, date: null, stored: null, Topic);

        act.Should().Throw<StreamConfigurationException>()
            .Where(e => e.Message.Contains("StartFromDate"));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_UnrecognisedMode_Throws()
    {
        var act = () => StartPosition.Resolve((StartFrom)99, StartFrom.Beginning, date: null, stored: null, Topic);

        act.Should().Throw<StreamConfigurationException>()
            .Where(e => e.Message.Contains("unrecognised"));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_FromConsumerOptions_UsesItsThreeSettings()
    {
        var options = new ConsumerOptions
        {
            Topic = Topic,
            StartFrom = StartFrom.Stored,
            StartFromWhenMissing = StartFrom.Now,
        };

        StartPosition.Resolve(options, stored: null).Should().Be(StartPosition.Now);
        StartPosition.Resolve(options, stored: Id(4, 1)).Should().Be(StartPosition.Resume(Id(4, 1)));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ReadFrom_Now_UsesDollarOnlyForTheVeryFirstRead()
    {
        var now = StartPosition.Now;

        // "$" is resolved by the server, at the moment the read arrives — correct exactly once.
        now.ReadFrom(lastRead: null).Should().Be(StreamPosition.NewMessages);

        // After the first fetch the worker passes a concrete id, so a reconnect resumes rather than
        // skipping everything published while the read was in flight.
        now.ReadFrom(Id(10, 4)).ToString().Should().Be("10-4");
        now.ReadFrom(Id(10, 4)).Should().NotBe(StreamPosition.NewMessages);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ReadFrom_NonNowModes_NeverUseDollar()
    {
        StartPosition.Beginning.ReadFrom(lastRead: null).ToString().Should().Be("0-0");
        StartPosition.Resume(Id(7, 2)).ReadFrom(lastRead: null).ToString().Should().Be("7-2");
        StartPosition.Resume(Id(7, 2)).ReadFrom(Id(9)).ToString().Should().Be("9-0");
    }

    [Theory]
    [InlineData(5, 3, 5, 2)]
    [InlineData(5, 0, 4, long.MaxValue)]
    [InlineData(0, 0, 0, 0)]
    [Trait("TestType", "UnitTest")]
    public void Before_IsTheLargestIdStrictlyBelow(long ms, long seq, long expectedMs, long expectedSeq)
        => StartPosition.Before(new StreamId(ms, seq)).Should().Be(new StreamId(expectedMs, expectedSeq));

    // ------------------------------------------------------------ reset markers: live

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Reset_Live_IsNoticedOnAFlusherTickHandedOverAndCleared()
    {
        var redis = new FakeRedis();
        var key = PositionsKey;
        var target = Id(1_000, 4);
        redis.Set(key, StreamAdmin.ResetMarkerField(0), StreamAdmin.FormatResetMarker(target, DateTimeOffset.UtcNow));

        var store = new CountingStore();
        var signal = new ResetSignal([0, 1]);
        var log = new CapturingLogger();

        await using var flusher = new PositionFlusher(
            store,
            Topic,
            Consumer,
            partitionCount: 2,
            interval: TimeSpan.FromMilliseconds(15),
            log: log,
            redis: redis.Multiplexer,
            instanceId: Guid.NewGuid(),
            resets: signal);

        flusher.Start();

        // The flusher notices the marker on its tick and publishes it to the signal.
        await Wait(() => signal.HasPending, "the reset became pending");
        log.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("reset marker found"));

        // Standing in for the read loop, which is the only thing that can actually rewind a cursor.
        signal.TryTake(0, out var taken).Should().BeTrue();
        taken.Target.Should().Be(target);
        signal.HasPending.Should().BeFalse();
        signal.IsTaken(0).Should().BeTrue();

        // Only now, on a later tick, is the marker deleted from Redis and the slot cleared.
        await Wait(() => flusher.ResetsApplied == 1, "the marker was cleared");

        redis.Get(key, StreamAdmin.ResetMarkerField(0)).Should().BeNull("the marker is deleted once acted on");
        signal.IsTaken(0).Should().BeFalse();
        flusher.DirtyCount.Should().Be(0, "the pending pre-reset position was dropped");
        log.Entries.Should().Contain(e => e.Level == LogLevel.Information && e.Message.Contains("reset marker cleared"));

        await flusher.StopAsync();

        store.Writes.Should().Be(0, "a rewind writes nothing — Redis already holds the target");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Reset_Live_MarkerStaysInRedisUntilAReadLoopTakesIt()
    {
        var redis = new FakeRedis();
        var key = PositionsKey;
        redis.Set(key, StreamAdmin.ResetMarkerField(0), StreamAdmin.FormatResetMarker(Id(9), DateTimeOffset.UtcNow));

        var signal = new ResetSignal([0]);

        await using var flusher = new PositionFlusher(
            new CountingStore(),
            Topic,
            Consumer,
            partitionCount: 1,
            interval: TimeSpan.FromMilliseconds(10),
            redis: redis.Multiplexer,
            instanceId: Guid.NewGuid(),
            resets: signal);

        flusher.Start();
        await Task.Delay(150);

        redis.Get(key, StreamAdmin.ResetMarkerField(0)).Should().NotBeNull(
            "a reader parked in a blocking XREAD must not lose the reset");
        flusher.ResetsApplied.Should().Be(0);
        signal.HasPending.Should().BeTrue();

        await flusher.StopAsync();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Reset_Live_AfterRewind_ABackwardsRecordIsForgivenExactlyOnce()
    {
#if !DEBUG
        await Task.CompletedTask;
#else
        var redis = new FakeRedis();
        var key = PositionsKey;
        redis.Set(key, StreamAdmin.ResetMarkerField(0), StreamAdmin.FormatResetMarker(Id(10), DateTimeOffset.UtcNow));

        var signal = new ResetSignal([0]);

        await using var flusher = new PositionFlusher(
            new CountingStore(),
            Topic,
            Consumer,
            partitionCount: 1,
            interval: TimeSpan.FromMilliseconds(10),
            redis: redis.Multiplexer,
            instanceId: Guid.NewGuid(),
            resets: signal);

        flusher.Record(0, Id(9_000));
        flusher.Start();

        await Wait(() => signal.HasPending, "the reset became pending");
        signal.TryTake(0, out _).Should().BeTrue();
        await Wait(() => flusher.ResetsApplied == 1, "the marker was cleared");
        await flusher.StopAsync();

        using var assertions = AssertionTrap.Install();

        // Rewind cleared the pending value outright, so the first replayed record has nothing to be
        // compared against and the forgiveness token is still armed.
        flusher.Record(0, Id(11));
        assertions.Failures.Should().Be(0);

        // This one really is backwards. It is the replay the operator asked for, so the token is
        // spent on it rather than an assertion firing.
        flusher.Record(0, Id(10, 5));
        assertions.Failures.Should().Be(0, "the reset's own replay must not trip the guard");

        // Token spent: anything backwards after that is a real wiring bug again.
        flusher.Record(0, Id(10, 4));
        assertions.Failures.Should().Be(1, "forgiveness is granted exactly once per reset");
#endif
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResetSignal_RepublishingTheSameMarker_IsANoOp()
    {
        var signal = new ResetSignal([0, 2]);
        var marker = new ResetMarker(0, Id(4), DateTimeOffset.UnixEpoch);

        signal.Request(in marker).Should().BeTrue("the first sighting is the one worth logging");
        signal.Request(in marker).Should().BeFalse("the flusher re-reads it on every tick until it is deleted");
        signal.HasPending.Should().BeTrue();

        var foreign = new ResetMarker(1, Id(4), DateTimeOffset.UnixEpoch);
        signal.Request(in foreign).Should().BeFalse("a marker for a partition we do not own is left for its owner");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResetSignal_NewerMarker_ReplacesATakenOne()
    {
        var signal = new ResetSignal([0]);
        var first = new ResetMarker(0, Id(4), DateTimeOffset.UnixEpoch);

        signal.Request(in first).Should().BeTrue();
        signal.TryTake(0, out _).Should().BeTrue();
        signal.IsTaken(0).Should().BeTrue();

        var second = new ResetMarker(0, Id(9), DateTimeOffset.UnixEpoch);
        signal.Request(in second).Should().BeTrue("the newest instruction wins");
        signal.HasPending.Should().BeTrue();
        signal.IsTaken(0).Should().BeFalse();

        signal.TryTake(0, out var taken).Should().BeTrue();
        taken.Target.Should().Be(Id(9));
        signal.TryTake(0, out _).Should().BeFalse("a taken reset is not handed out twice");

        signal.TryClearTaken(0, out var applied).Should().BeTrue();
        applied.Target.Should().Be(Id(9));
        signal.TryClearTaken(0, out _).Should().BeFalse();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResetSignal_WithNoOwnedPartitions_Throws()
    {
        var act = () => new ResetSignal([]);

        act.Should().Throw<ArgumentException>();
    }

    // ------------------------------------------------------------ reset markers: cold

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Reset_Cold_IsAppliedAtStartupAndCleared()
    {
        var redis = new FakeRedis();
        var key = PositionsKey;
        var issued = DateTimeOffset.UtcNow;

        redis.Set(key, "0", PositionRecord.Format(Id(50), Guid.NewGuid()));
        redis.Set(key, StreamAdmin.ResetMarkerField(0), StreamAdmin.FormatResetMarker(Id(10), issued));
        redis.Set(key, StreamAdmin.ResetMarkerField(3), StreamAdmin.FormatResetMarker(Id(20), issued));

        var log = new CapturingLogger();

        var markers = await ResetMarkers.TakeAllAsync(
            redis.Multiplexer.GetDatabase(),
            Topic,
            Consumer,
            log,
            CancellationToken.None);

        markers.Should().HaveCount(2);
        markers[0].Target.Should().Be(Id(10));
        markers[3].Target.Should().Be(Id(20));
        log.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains("at startup"));

        redis.Get(key, StreamAdmin.ResetMarkerField(0)).Should().BeNull();
        redis.Get(key, StreamAdmin.ResetMarkerField(3)).Should().BeNull();
        redis.Get(key, "0").Should().NotBeNull("the position itself is left exactly as the admin call wrote it");

        // A marker beats every StartFrom mode, StartFrom.Now included.
        ResetMarkers.ApplyToStart(StartPosition.Now, markers, 0).Should().Be(StartPosition.Resume(Id(10)));
        ResetMarkers.ApplyToStart(StartPosition.Now, markers, 1).Should().Be(StartPosition.Now,
            "a partition with no marker keeps its configured start");
        ResetMarkers.ApplyToStart(StartPosition.Beginning, markers: null, 0).Should().Be(StartPosition.Beginning);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Reset_Cold_WithNoMarkers_ReadsNothingBack()
    {
        var redis = new FakeRedis();
        redis.Set(PositionsKey, "0", PositionRecord.Format(Id(50), Guid.Empty));

        var markers = await ResetMarkers.TakeAllAsync(
            redis.Multiplexer.GetDatabase(),
            Topic,
            Consumer,
            log: null,
            CancellationToken.None);

        markers.Should().BeEmpty();
        redis.Deletes.Should().Be(0, "there is nothing to acknowledge");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResetMarkers_Read_SkipsPositionsAndUnparseableValues()
    {
        var entries = new[]
        {
            new HashEntry("0", "12-3|00000000-0000-0000-0000-000000000001"),
            new HashEntry("0:updatedUtc", "2026-09-06T00:00:00.0000000Z"),
            new HashEntry(StreamAdmin.ResetMarkerField(1), StreamAdmin.FormatResetMarker(Id(8, 1), DateTimeOffset.UnixEpoch)),
            new HashEntry(StreamAdmin.ResetMarkerField(2), "not-an-id"),
            new HashEntry("__reset:notanumber", "1-0"),
        };

        var markers = ResetMarkers.Read(entries);

        markers.Should().ContainSingle();
        markers[1].Should().Be(new ResetMarker(1, Id(8, 1), DateTimeOffset.UnixEpoch));

        // The same hash read as positions must see the position and none of the markers.
        var positions = RedisPositionStore.Read(entries);
        positions.Should().ContainSingle();
        positions[0].Id.Should().Be(Id(12, 3));
    }

    // ------------------------------------------------------------ second-writer detection

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task SecondWriter_LiveForeignInstanceId_LogsErrorCountsContestedAndStopsThePartition()
    {
        var redis = new FakeRedis();
        var key = PositionsKey;

        // The tiebreak is "lowest id keeps the partition", so for this instance to be the one that
        // stands down its id has to be the higher of the two. Fixed, not random, or the test would
        // assert a stand-down half the time and a hold the other half.
        var mine = Higher;
        var theirs = Lower;

        redis.Set(key, RedisPositionStore.Field(0).ToString(), PositionRecord.Format(Id(77, 1), theirs));

        // A foreign id only counts as a rival while its owner still holds a live ownership claim.
        MarkLive(redis, theirs);

        var store = new CountingStore();
        var log = new CapturingLogger();
        var contested = new List<(int Partition, Guid Mine, Guid Theirs)>();

        await using var flusher = new PositionFlusher(
            store,
            Topic,
            Consumer,
            partitionCount: 2,
            interval: TimeSpan.FromMinutes(5),
            log: log,
            redis: redis.Multiplexer,
            instanceId: mine,
            onContested: (partition, m, t) => contested.Add((partition, m, t)));

        flusher.Record(0, Id(100));
        flusher.Record(1, Id(100));
        await flusher.FlushAsync();

        contested.Should().ContainSingle().Which.Should().Be((0, mine, theirs));
        flusher.ContestedCount.Should().Be(1);
        log.Entries.Should().Contain(e => e.Level == LogLevel.Error && e.Message.Contains("StatefulSet"));

        // The stood-down partition stops writing; the uncontested one carries on.
        store.Writes.Should().Be(1);
        flusher.Record(0, Id(200));
        flusher.Record(1, Id(200));
        flusher.DirtyCount.Should().Be(1, "partition 0 is stood down, partition 1 is not");

        await flusher.FlushAsync();
        store.Writes.Should().Be(2);
        store.Last!.Should().Equal((1, Id(200)));

        // Latched: a second sighting neither logs nor counts twice.
        contested.Should().ContainSingle();
        flusher.ContestedCount.Should().Be(1);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task SecondWriter_OurOwnOrUnattributedValues_AreNotContention()
    {
        var redis = new FakeRedis();
        var key = PositionsKey;
        var mine = Guid.NewGuid();

        redis.Set(key, RedisPositionStore.Field(0).ToString(), PositionRecord.Format(Id(1), mine));
        redis.Set(key, RedisPositionStore.Field(1).ToString(), "12-3");        // legacy, unattributed
        redis.Set(key, RedisPositionStore.Field(2).ToString(), "garbage");     // unparseable

        var contested = 0;

        await using var flusher = new PositionFlusher(
            new CountingStore(),
            Topic,
            Consumer,
            partitionCount: 4,
            interval: TimeSpan.FromMinutes(5),
            redis: redis.Multiplexer,
            instanceId: mine,
            onContested: (_, _, _) => contested++);

        flusher.Record(0, Id(100));
        flusher.Record(1, Id(100));
        flusher.Record(2, Id(100));
        flusher.Record(3, Id(100));   // no previous value at all
        await flusher.FlushAsync();

        contested.Should().Be(0);
        flusher.ContestedCount.Should().Be(0);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task SecondWriter_ForeignIdWithNoLiveClaim_IsNotContention()
    {
        // R-01. This is what a pod restart and an out-of-process reset both look like from here: a
        // stranger's id in the field, and nobody behind it. Treating that as a rival stood every
        // restarting pod's own partitions down on its first flush.
        var redis = new FakeRedis();
        var mine = Higher;
        var theirs = Lower;

        redis.Set(PositionsKey, RedisPositionStore.Field(0).ToString(), PositionRecord.Format(Id(77, 1), theirs));

        var store = new CountingStore();
        var log = new CapturingLogger();
        var contested = 0;

        await using var flusher = new PositionFlusher(
            store,
            Topic,
            Consumer,
            partitionCount: 1,
            interval: TimeSpan.FromMinutes(5),
            log: log,
            redis: redis.Multiplexer,
            instanceId: mine,
            onContested: (_, _, _) => contested++);

        flusher.Record(0, Id(100));
        await flusher.FlushAsync();

        contested.Should().Be(0);
        flusher.ContestedCount.Should().Be(0);
        flusher.ContentionCount.Should().Be(0);
        log.Entries.Should().NotContain(e => e.Level == LogLevel.Error);

        // And it keeps writing, which is the whole point.
        flusher.Record(0, Id(200));
        await flusher.FlushAsync();
        store.Writes.Should().Be(2);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task SecondWriter_AdminReset_IsNotContention()
    {
        // A reset is an operator overruling the consumer. It is stamped with a well-known identity
        // precisely so the flusher does not read it as a rival and stop the partition being rewound.
        var redis = new FakeRedis();

        redis.Set(
            PositionsKey,
            RedisPositionStore.Field(0).ToString(),
            PositionRecord.Format(Id(9, 0), RedisPositionStore.AdminInstanceId));

        MarkLive(redis, RedisPositionStore.AdminInstanceId);

        var contested = 0;

        await using var flusher = new PositionFlusher(
            new CountingStore(),
            Topic,
            Consumer,
            partitionCount: 1,
            interval: TimeSpan.FromMinutes(5),
            redis: redis.Multiplexer,
            instanceId: Guid.NewGuid(),
            onContested: (_, _, _) => contested++);

        flusher.Record(0, Id(100));
        await flusher.FlushAsync();

        contested.Should().Be(0);
        flusher.ContestedCount.Should().Be(0);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task SecondWriter_LowerIdWinsTheTiebreakAndKeepsThePartition()
    {
        // Two genuine contenders used to stand each other down, leaving the partition consumed by
        // nobody. Exactly one gives way now, and both sides compute the same answer.
        var redis = new FakeRedis();
        var mine = Lower;
        var theirs = Higher;

        redis.Set(PositionsKey, RedisPositionStore.Field(0).ToString(), PositionRecord.Format(Id(77, 1), theirs));
        MarkLive(redis, theirs);

        var store = new CountingStore();
        var log = new CapturingLogger();
        var contested = 0;

        await using var flusher = new PositionFlusher(
            store,
            Topic,
            Consumer,
            partitionCount: 1,
            interval: TimeSpan.FromMinutes(5),
            log: log,
            redis: redis.Multiplexer,
            instanceId: mine,
            onContested: (_, _, _) => contested++);

        flusher.Record(0, Id(100));
        await flusher.FlushAsync();

        contested.Should().Be(0, "the lower id keeps the partition");
        flusher.ContestedCount.Should().Be(0);
        flusher.ContentionCount.Should().Be(1, "the overlap is still real and still on the gauge");
        log.Entries.Should().Contain(e => e.Level == LogLevel.Error && e.Message.Contains("keeps the", StringComparison.Ordinal));

        // Still writing.
        flusher.Record(0, Id(200));
        await flusher.FlushAsync();
        store.Writes.Should().Be(2);

        // Logged once, not on every flush.
        log.Entries.Count(e => e.Level == LogLevel.Error).Should().Be(1);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task SecondWriter_WithoutAMultiplexer_IsNotChecked()
    {
        var store = new CountingStore();
        var contested = 0;

        await using var flusher = new PositionFlusher(
            store,
            Topic,
            Consumer,
            partitionCount: 2,
            interval: TimeSpan.FromMinutes(5),
            redis: null,
            instanceId: Guid.NewGuid(),
            onContested: (_, _, _) => contested++);

        flusher.Record(0, Id(1));
        await flusher.FlushAsync();

        contested.Should().Be(0, "the field layout is not ours to interpret for a non-Redis store");
        store.Writes.Should().Be(1);
    }

    // ------------------------------------------------------------ PositionRecord round trip

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void PositionRecord_RoundTrips()
    {
        var instance = Guid.NewGuid();
        var formatted = PositionRecord.Format(Id(123, 45), instance);

        formatted.Should().Be($"123-45|{instance:D}");

        PositionRecord.TryParse(formatted, out var parsed).Should().BeTrue();
        parsed.Should().Be(new PositionRecord(Id(123, 45), instance));
        parsed.ToString().Should().Be(formatted);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void PositionRecord_BareIdParsesAsUnattributed()
    {
        PositionRecord.TryParse("77-2", out var parsed).Should().BeTrue();
        parsed.Should().Be(new PositionRecord(Id(77, 2), Guid.Empty));

        PositionRecord.TryParse("77-2|not-a-guid", out var loose).Should().BeTrue();
        loose.InstanceId.Should().Be(Guid.Empty, "a bad guid must not lose a usable position");

        PositionRecord.TryParse("nonsense", out _).Should().BeFalse();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task NullPositionStore_LoadsNothingAndWritesNothing()
    {
        var store = NullPositionStore.Instance;

        (await store.LoadAsync(Topic, Consumer, CancellationToken.None)).Should().BeEmpty();

        var save = async () => await store.SaveAsync(Topic, Consumer, new[] { (0, Id(1)) }.AsSpan(), CancellationToken.None);
        await save.Should().NotThrowAsync();

        var reset = async () => await store.ResetAsync(Topic, Consumer, Id(1), 0, CancellationToken.None);
        await reset.Should().NotThrowAsync();
    }

    // ------------------------------------------------------------ helpers

    private static PositionFlusher NewFlusher(
        IPositionStore store,
        int partitions,
        TimeSpan? interval = null,
        ILogger? log = null)
        => new(
            store,
            Topic,
            Consumer,
            partitions,
            interval ?? TimeSpan.FromMinutes(5),
            log,
            redis: null,
            instanceId: Guid.NewGuid());

    private static PartitionContext NewContext()
        => new(
            Db: null!,
            StreamKey: "s:{orders}:0",
            Partition: 0,
            Topic: Topic,
            Consumer: Consumer,
            BatchSize: 16,
            Filter: null,
            OnError: ErrorPolicy.BestEffort,
            Writer: null,
            Log: NullLogger.Instance);

    /// <summary>Polls until <paramref name="until"/> holds, or fails the test.</summary>
    private static async Task Wait(Func<bool> until, string what, int timeoutMs = 5000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;

        while (Environment.TickCount64 < deadline)
        {
            if (until())
            {
                return;
            }

            await Task.Delay(5);
        }

        Assert.Fail($"Timed out after {timeoutMs}ms waiting for: {what}.");
    }

    /// <summary>What one <see cref="PartitionWorker.ProcessLoopAsync"/> run did.</summary>
    private sealed record ProcessRun(int Records, int Flushes, int HandlerCalls, StreamId LastRecorded);

    /// <summary>
    /// Runs the processing loop over synthetic batches under one persist mode and counts the
    /// position records and synchronous flushes it produced.
    /// </summary>
    private static async Task<ProcessRun> RunProcessLoopAsync(PersistMode persist, int batches, int messagesPerBatch)
    {
        var channel = Channel.CreateUnbounded<StreamBatch>();

        for (var b = 1; b <= batches; b++)
        {
            var items = StreamBatch.Rent(messagesPerBatch);

            for (var i = 0; i < messagesPerBatch; i++)
            {
                items[i] = new StreamMsg(
                    ReadOnlyMemory<byte>.Empty,
                    "test",
                    new StreamId(b, i),
                    0,
                    string.Empty,
                    string.Empty,
                    null,
                    HeaderBlock.Empty);
            }

            await channel.Writer.WriteAsync(new StreamBatch(items, messagesPerBatch, new StreamId(b, messagesPerBatch - 1)));
        }

        channel.Writer.Complete();

        var records = 0;
        var flushes = 0;
        var handlerCalls = 0;
        var last = default(StreamId);

        PositionFlush? flush = null;
        if (persist is PersistMode.SyncBatch or PersistMode.SyncMessage)
        {
            flush = _ =>
            {
                flushes++;
                return ValueTask.CompletedTask;
            };
        }

        await PartitionWorker.ProcessLoopAsync(
            NewContext(),
            (_, _) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return ValueTask.CompletedTask;
            },
            (_, id) =>
            {
                records++;
                last = id;
            },
            CancellationToken.None,
            channel.Reader,
            persist,
            flush);

        return new ProcessRun(records, flushes, handlerCalls, last);
    }

    /// <summary>An <see cref="IPositionStore"/> that counts writes and remembers what it was given.</summary>
    private sealed class CountingStore : IPositionStore
    {
        private readonly List<(int Partition, StreamId Id)[]> saved = [];

        /// <summary>Set to make the next <see cref="SaveAsync"/> throw.</summary>
        internal Exception? Throw { get; set; }

        /// <summary>Every <see cref="SaveAsync"/> call, including the empty ones.</summary>
        internal int Calls { get; private set; }

        /// <summary>Writes that actually carried a position.</summary>
        internal int Writes => this.saved.Count;

        /// <summary>What each write carried, in order.</summary>
        internal IReadOnlyList<(int Partition, StreamId Id)[]> Saved => this.saved;

        /// <summary>The most recent write, or <see langword="null"/> when there has been none.</summary>
        internal (int Partition, StreamId Id)[]? Last => this.saved.Count == 0 ? null : this.saved[^1];

        public ValueTask<IReadOnlyDictionary<int, StreamId>> LoadAsync(string topic, string consumer, CancellationToken ct)
            => NullPositionStore.Instance.LoadAsync(topic, consumer, ct);

        public ValueTask SaveAsync(
            string topic,
            string consumer,
            ReadOnlySpan<(int Partition, StreamId Id)> positions,
            CancellationToken ct)
        {
            this.Calls++;

            if (this.Throw is { } boom)
            {
                return ValueTask.FromException(boom);
            }

            if (!positions.IsEmpty)
            {
                this.saved.Add(positions.ToArray());
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask ResetAsync(string topic, string consumer, StreamId to, int? partition, CancellationToken ct)
            => ValueTask.CompletedTask;
    }

    /// <summary>Captures log entries so the tests can assert on level and text.</summary>
    private sealed class CapturingLogger : ILogger
    {
        private readonly List<(LogLevel Level, string Message)> entries = [];

        internal IReadOnlyList<(LogLevel Level, string Message)> Entries
        {
            get
            {
                lock (this.entries)
                {
                    return this.entries.ToArray();
                }
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

    /// <summary>
    /// Catches <see cref="Debug.Fail"/> so a <c>DEBUG</c>-only assert can be asserted <em>on</em>
    /// rather than crashing the run. <see cref="Debug"/> routes failures through
    /// <see cref="Trace.Listeners"/>, so swapping the listener is enough.
    /// </summary>
    private sealed class AssertionTrap : TraceListener
    {
        private static readonly Lock Gate = new();

        private TraceListener[] previous = [];
        private bool installed;

        /// <summary>How many assertions fired while this trap was installed.</summary>
        internal int Failures { get; private set; }

        /// <summary>The most recent assertion message.</summary>
        internal string? LastMessage { get; private set; }

        /// <summary>Installs the trap; disposing restores the listeners that were there before.</summary>
        internal static AssertionTrap Install()
        {
            var trap = new AssertionTrap();

            // The listener collection is process-wide, so installing and restoring is serialised.
            Gate.Enter();

            var existing = new TraceListener[Trace.Listeners.Count];
            Trace.Listeners.CopyTo(existing, 0);
            trap.previous = existing;

            Trace.Listeners.Clear();
            Trace.Listeners.Add(trap);
            trap.installed = true;

            return trap;
        }

        public override void Fail(string? message) => this.Fail(message, null);

        public override void Fail(string? message, string? detailMessage)
        {
            this.Failures++;
            this.LastMessage = string.Concat(message, " ", detailMessage);
        }

        public override void Write(string? message)
        {
        }

        public override void WriteLine(string? message)
        {
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && this.installed)
            {
                this.installed = false;
                Trace.Listeners.Clear();
                Trace.Listeners.AddRange(this.previous);
                this.previous = [];
                Gate.Exit();
            }

            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// An in-memory stand-in for the two Redis interfaces the flusher touches, built with
    /// <see cref="DispatchProxy"/> so no mocking package is needed.
    /// </summary>
    /// <remarks>
    /// Only the four commands the position path issues are implemented — <c>HMGET</c>,
    /// <c>HGETALL</c>, <c>HDEL</c> and <c>HSET</c>. Anything else throws, deliberately: a test that
    /// starts needing a fifth command is a test that has drifted away from what it is checking.
    /// </remarks>
    /// <summary>
    /// Gives an instance a live ownership claim, so the flusher's liveness cross-check sees it as a
    /// contender rather than as a ghost.
    /// </summary>
    private static void MarkLive(FakeRedis redis, Guid instanceId)
        => redis.Set(OwnershipKey, OwnershipRegistry.PresenceField(instanceId).ToString()!, $"{instanceId:D}|pod");

    public sealed class FakeRedis
    {
        private readonly Dictionary<string, Dictionary<string, string>> hashes = [];
        private readonly Lock gate = new();

        internal FakeRedis()
        {
            var db = DispatchProxy.Create<IDatabase, RedisProxy>();
            ((RedisProxy)db).Owner = this;

            var multiplexer = DispatchProxy.Create<IConnectionMultiplexer, RedisProxy>();
            ((RedisProxy)multiplexer).Owner = this;
            ((RedisProxy)multiplexer).Database = db;

            this.Multiplexer = multiplexer;
        }

        /// <summary>The fake multiplexer to hand to the code under test.</summary>
        internal IConnectionMultiplexer Multiplexer { get; }

        /// <summary>How many fields have been deleted across every <c>HDEL</c>.</summary>
        internal int Deletes { get; private set; }

        /// <summary>How many <c>HMGET</c>s have been issued.</summary>
        internal int Gets { get; private set; }

        /// <summary>Seeds or overwrites one hash field.</summary>
        internal void Set(string key, string field, string value)
        {
            lock (this.gate)
            {
                if (!this.hashes.TryGetValue(key, out var hash))
                {
                    hash = [];
                    this.hashes[key] = hash;
                }

                hash[field] = value;
            }
        }

        /// <summary>Reads one hash field, or <see langword="null"/> when it is absent.</summary>
        internal string? Get(string key, string field)
        {
            lock (this.gate)
            {
                return this.hashes.TryGetValue(key, out var hash) && hash.TryGetValue(field, out var value)
                    ? value
                    : null;
            }
        }

        private object Invoke(MethodInfo method, object?[] args)
        {
            switch (method.Name)
            {
                case "HashGetAsync" when args.Length == 3 && args[1] is RedisValue[] fields:
                    return Task.FromResult(this.HashGet(Key(args[0]), fields));

                case "HashGetAllAsync" when args.Length == 2:
                    return Task.FromResult(this.HashGetAll(Key(args[0])));

                case "HashDeleteAsync" when args.Length == 3 && args[1] is RedisValue[] dead:
                    return Task.FromResult(this.HashDelete(Key(args[0]), dead));

                case "HashSetAsync" when args.Length == 3 && args[1] is HashEntry[] entries:
                    this.HashSet(Key(args[0]), entries);
                    return Task.CompletedTask;

                default:
                    throw new NotSupportedException(
                        $"FakeRedis does not implement {method.Name}; the position path should not be calling it.");
            }
        }

        private static string Key(object? key) => ((RedisKey)key!).ToString()!;

        private RedisValue[] HashGet(string key, RedisValue[] fields)
        {
            lock (this.gate)
            {
                this.Gets++;
                this.hashes.TryGetValue(key, out var hash);

                var values = new RedisValue[fields.Length];
                for (var i = 0; i < fields.Length; i++)
                {
                    values[i] = hash is not null && hash.TryGetValue(fields[i].ToString()!, out var value)
                        ? value
                        : RedisValue.Null;
                }

                return values;
            }
        }

        private HashEntry[] HashGetAll(string key)
        {
            lock (this.gate)
            {
                if (!this.hashes.TryGetValue(key, out var hash))
                {
                    return [];
                }

                var entries = new HashEntry[hash.Count];
                var at = 0;
                foreach (var pair in hash)
                {
                    entries[at++] = new HashEntry(pair.Key, pair.Value);
                }

                return entries;
            }
        }

        private long HashDelete(string key, RedisValue[] fields)
        {
            lock (this.gate)
            {
                if (!this.hashes.TryGetValue(key, out var hash))
                {
                    return 0;
                }

                long removed = 0;
                foreach (var field in fields)
                {
                    if (hash.Remove(field.ToString()!))
                    {
                        removed++;
                        this.Deletes++;
                    }
                }

                return removed;
            }
        }

        private void HashSet(string key, HashEntry[] entries)
        {
            foreach (var entry in entries)
            {
                this.Set(key, entry.Name.ToString()!, entry.Value.ToString()!);
            }
        }

        /// <summary>
        /// The generated proxy's base. Public because <see cref="DispatchProxy"/> emits a subclass of
        /// it into a dynamic assembly.
        /// </summary>
        public class RedisProxy : DispatchProxy
        {
            internal FakeRedis Owner { get; set; } = null!;

            internal IDatabase? Database { get; set; }

            /// <inheritdoc />
            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            {
                ArgumentNullException.ThrowIfNull(targetMethod);

                if (targetMethod.Name == "GetDatabase")
                {
                    return this.Database
                        ?? throw new NotSupportedException("GetDatabase was called on the database proxy itself.");
                }

                return this.Owner.Invoke(targetMethod, args ?? []);
            }
        }
    }
}
