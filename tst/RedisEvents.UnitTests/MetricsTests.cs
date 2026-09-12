using System.Diagnostics.Metrics;
using FluentAssertions;
using RedisEvents.Diagnostics;

namespace RedisEvents.UnitTests;

/// <summary>
/// R-16. <c>streams.blocked</c> is specified as a 0/1 gauge, and it removed its series at 0 instead
/// of reporting 0. The difference matters on the only day it is read: a series that disappears looks
/// exactly like a pod that died, so an alert on <c>streams.blocked == 1</c> resolved identically
/// whether the partition recovered or the process was OOM-killed, and a recovery left no datapoint
/// to show that it had happened.
/// </summary>
public class MetricsTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Blocked_ReportsOneThenZero_AndOnlyStopsWhenThePartitionIsReleased()
    {
        var topic = $"metrics-blocked-{Guid.NewGuid():N}";
        var key = $"{topic}:0:reader";

        StreamsDiagnostics.SetBlockedGauge(key, 1);
        Observe("streams.blocked", topic).Should().Equal([1L]);

        StreamsDiagnostics.SetBlockedGauge(key, 0);
        Observe("streams.blocked", topic).Should().Equal(
            [0L],
            "an unblocked partition reports 0; vanishing would be indistinguishable from a dead pod");

        StreamsDiagnostics.ClearPartition(key);
        Observe("streams.blocked", topic).Should().BeEmpty(
            "a partition this process no longer consumes stops publishing, which is what absence means");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void BlockDuration_FallsBackToZeroWithItsBlockedGauge()
    {
        var topic = $"metrics-duration-{Guid.NewGuid():N}";
        var key = $"{topic}:1:reader";

        StreamsDiagnostics.SetBlockDurationMs(key, 4200);
        ObserveDouble("streams.block.duration_ms", topic).Should().Equal([4200d]);

        StreamsDiagnostics.SetBlockDurationMs(key, 0);
        ObserveDouble("streams.block.duration_ms", topic).Should().Equal(
            [0d],
            "the pair has to rise and fall together, or a dashboard shows a blocked partition with no duration");

        StreamsDiagnostics.ClearPartition(key);
        ObserveDouble("streams.block.duration_ms", topic).Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void BlockedGauge_CarriesTopicPartitionAndConsumerTags()
    {
        var topic = $"metrics-tags-{Guid.NewGuid():N}";
        StreamsDiagnostics.SetBlockedGauge($"{topic}:3:orders-reader", 1);

        var tags = Tags("streams.blocked", topic);

        tags.Should().Contain(new KeyValuePair<string, object?>("topic", topic));
        tags.Should().Contain(new KeyValuePair<string, object?>("partition", 3));
        tags.Should().Contain(new KeyValuePair<string, object?>("consumer", "orders-reader"));

        StreamsDiagnostics.ClearPartition($"{topic}:3:orders-reader");
    }

    /// <summary>Scrapes one observable gauge, keeping only the series tagged with <paramref name="topic"/>.</summary>
    private static List<long> Observe(string instrument, string topic)
    {
        var values = new List<long>();

        using var listener = Listen(instrument);
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            if (HasTopic(tags, topic))
                values.Add(value);
        });

        listener.Start();
        listener.RecordObservableInstruments();

        return values;
    }

    private static List<double> ObserveDouble(string instrument, string topic)
    {
        var values = new List<double>();

        using var listener = Listen(instrument);
        listener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
        {
            if (HasTopic(tags, topic))
                values.Add(value);
        });

        listener.Start();
        listener.RecordObservableInstruments();

        return values;
    }

    private static List<KeyValuePair<string, object?>> Tags(string instrument, string topic)
    {
        var found = new List<KeyValuePair<string, object?>>();

        using var listener = Listen(instrument);
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            if (HasTopic(tags, topic))
                found.AddRange(tags.ToArray());
        });

        listener.Start();
        listener.RecordObservableInstruments();

        return found;
    }

    private static MeterListener Listen(string instrument)
    {
        var listener = new MeterListener();

        listener.InstrumentPublished = (published, l) =>
        {
            if (published.Meter.Name == "RedisEvents" && published.Name == instrument)
                l.EnableMeasurementEvents(published);
        };

        return listener;
    }

    private static bool HasTopic(ReadOnlySpan<KeyValuePair<string, object?>> tags, string topic)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == "topic" && Equals(tag.Value, topic))
                return true;
        }

        return false;
    }
}
