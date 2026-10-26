using FluentAssertions;

using RedisEvents.Admin;
using RedisEvents.Positions;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>
/// R-28 — the flusher deletes a taken reset marker only while the field still holds the value that
/// was taken, against a real Redis (the delete is a Lua compare-and-delete, which a fake cannot vouch
/// for).
/// </summary>
/// <remarks>
/// The race: a read loop takes marker A, an operator then issues a newer reset B for the same
/// partition (the admin write replaces the field's value), and only then does the flusher delete.
/// A plain HDEL removed B with A and that reset was silently lost.
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class ResetMarkerCompareAndDeleteTests(RedisStreamsFixture fixture)
{
    private static StreamId Id(long ms) => new(ms, 0);

    private static string Format(StreamId target, int issuedSeconds)
        => StreamAdmin.FormatResetMarker(target, new DateTimeOffset(2026, 1, 1, 0, 0, issuedSeconds, TimeSpan.Zero));

    private async Task<(PositionFlusher Flusher, ResetSignal Signal, RedisKey Key, string Field)> ArrangeAsync(
        string topic, string consumer, TimeSpan interval, ResetSignal? shared = null)
    {
        var signal = shared ?? new ResetSignal([0]);
        var flusher = new PositionFlusher(
            new RedisPositionStore(fixture.Redis),
            topic,
            consumer,
            partitionCount: 1,
            interval,
            instanceId: Guid.NewGuid(),
            redis: fixture.Redis,
            resets: signal);

        await Task.CompletedTask;
        return (flusher, signal, StreamKeys.Positions(topic, consumer), StreamAdmin.ResetMarkerField(0));
    }

    private static void Take(ResetSignal signal, string raw)
    {
        ResetMarkers.TryParse(0, raw, out var marker).Should().BeTrue();
        signal.Request(in marker).Should().BeTrue();
        signal.TryTake(0, out _).Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_newer_marker_written_after_the_take_survives_the_flush_and_is_applied_afterwards()
    {
        var topic = fixture.NewTopic("cad-tick");
        var consumer = fixture.NewConsumer("cad-tick");
        var db = fixture.Redis.GetDatabase();
        var (flusher, signal, key, field) = await ArrangeAsync(topic, consumer, TimeSpan.FromHours(1));
        await using var _ = flusher;

        var older = Format(Id(100), 1);
        var newer = Format(Id(50), 2);
        await db.HashSetAsync(key, field, older);
        Take(signal, older);

        await db.HashSetAsync(key, field, newer);
        await flusher.FlushAsync(TestContext.Current.CancellationToken);

        ((string?)await db.HashGetAsync(key, field)).Should().Be(newer, "the newer reset must not be deleted with the taken one");
        signal.IsTaken(0).Should().BeFalse("the taken slot for the old marker is cleared");

        // Afterwards: the next poll finds the newer marker and hands it to the read loop.
        var ticking = (await ArrangeAsync(topic, consumer, TimeSpan.FromMilliseconds(20), signal)).Flusher;
        await using var _ticking = ticking;
        ticking.Start();

        var deadline = DateTime.UtcNow.AddSeconds(10);
        ResetMarker taken = default;
        while (!signal.TryTake(0, out taken) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }

        taken.Target.Should().Be(Id(50), "the newer reset is applied, not lost");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_newer_marker_survives_the_final_flush_on_stop()
    {
        var topic = fixture.NewTopic("cad-stop");
        var consumer = fixture.NewConsumer("cad-stop");
        var db = fixture.Redis.GetDatabase();
        var (flusher, signal, key, field) = await ArrangeAsync(topic, consumer, TimeSpan.FromHours(1));
        await using var _ = flusher;

        var older = Format(Id(100), 1);
        var newer = Format(Id(50), 2);
        await db.HashSetAsync(key, field, older);
        flusher.Start();
        Take(signal, older);
        flusher.Rewind(0);

        await db.HashSetAsync(key, field, newer);
        await flusher.StopAsync();

        ((string?)await db.HashGetAsync(key, field)).Should().Be(newer, "the final flush must not delete a newer reset");
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task The_plain_case_still_deletes_the_taken_marker()
    {
        var topic = fixture.NewTopic("cad-plain");
        var consumer = fixture.NewConsumer("cad-plain");
        var db = fixture.Redis.GetDatabase();
        var (flusher, signal, key, field) = await ArrangeAsync(topic, consumer, TimeSpan.FromHours(1));
        await using var _ = flusher;

        var only = Format(Id(100), 1);
        await db.HashSetAsync(key, field, only);
        Take(signal, only);

        await flusher.FlushAsync(TestContext.Current.CancellationToken);

        (await db.HashExistsAsync(key, field)).Should().BeFalse();
        signal.IsTaken(0).Should().BeFalse();
        flusher.ResetsApplied.Should().Be(1);
    }
}
