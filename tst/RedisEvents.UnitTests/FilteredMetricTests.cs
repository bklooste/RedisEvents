using System.Diagnostics.Metrics;
using System.Threading.Channels;

using FluentAssertions;

using Microsoft.Extensions.Logging.Abstractions;

using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Diagnostics;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// R-16 (the metric half). <c>streams.filtered</c> was declared in
/// <see cref="StreamsDiagnostics"/> and nothing ever called <c>Add</c> on it.
/// </summary>
/// <remarks>
/// The consequence is not cosmetic: a consumer with a type filter reports
/// <c>streams.consumed</c> far below the topic's <c>streams.published</c>, and with
/// <c>streams.filtered</c> flat at zero there is nothing on the dashboard to say the difference was
/// deliberate. "The filter dropped them" and "we lost them" looked identical, which is the worst
/// property a delivery metric can have.
/// </remarks>
public class FilteredMetricTests
{
    /// <summary>
    /// The read path counts what the filter dropped, tagged topic and consumer per the metrics table
    /// in <c>06-errors-and-observability.md</c> — and counts the drops, not the whole batch.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_filtered_batch_records_what_it_dropped()
    {
        var topic = $"filtered-{Guid.NewGuid():N}";
        using var rig = new CounterRig("streams.filtered", topic);

        var channel = PartitionWorker.CreateChannel(4);

        // Four entries, one of which the filter keeps.
        var batch = Pipeline.Batch(
            Pipeline.Entry(1, 0, "Keep"),
            Pipeline.Entry(2, 0, "Drop"),
            Pipeline.Entry(3, 0, "Drop"),
            Pipeline.Entry(4, 0, "Drop"));

        using var cts = new CancellationTokenSource();

        var read = PartitionWorker.ReadLoopAsync(
            Context(topic, channel.Writer, ["Keep"]),
            StreamId.Min,
            Pipeline.Script([batch]),
            cts.Token);

        var delivered = await channel.Reader.ReadAsync(TestTimeout());
        delivered.Count.Should().Be(1, "only the one matching entry survives");
        delivered.Return();

        await cts.CancelAsync();
        await Swallow(read);

        rig.Total().Should().Be(3, "three entries were dropped by the filter, not four and not one");
        rig.Tags().Should().Contain(("topic", topic)).And.Contain(("consumer", "filtered-consumer"));
    }

    /// <summary>
    /// A consumer with no filter never touches the counter. The instrument is on the read path, and
    /// the common case must not pay for it or report a phantom series.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_unfiltered_batch_records_nothing()
    {
        var topic = $"unfiltered-{Guid.NewGuid():N}";
        using var rig = new CounterRig("streams.filtered", topic);

        var channel = PartitionWorker.CreateChannel(4);
        var batch = Pipeline.Batch(Pipeline.Entry(1, 0, "A"), Pipeline.Entry(2, 0, "B"));

        using var cts = new CancellationTokenSource();

        var read = PartitionWorker.ReadLoopAsync(
            Context(topic, channel.Writer, filter: null),
            StreamId.Min,
            Pipeline.Script([batch]),
            cts.Token);

        var delivered = await channel.Reader.ReadAsync(TestTimeout());
        delivered.Count.Should().Be(2);
        delivered.Return();

        await cts.CancelAsync();
        await Swallow(read);

        rig.Total().Should().Be(0, "nothing was filtered, so no series should exist for this topic at all");
    }

    private static PartitionContext Context(string topic, ChannelWriter<StreamBatch> writer, string[]? filter)
        => new(
            Db: null!,
            StreamKey: (RedisKey)$"s:{{{topic}}}:0",
            Partition: 0,
            Topic: topic,
            Consumer: "filtered-consumer",
            BatchSize: 100,
            Filter: filter,
            OnError: ErrorPolicy.BestEffort,
            Writer: writer,
            Log: NullLogger.Instance);

    private static CancellationToken TestTimeout() => new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token;

    private static async Task Swallow(Task loop)
    {
        try
        {
            await loop;
        }
        catch (OperationCanceledException)
        {
            // Cancelling the loop is how the test ends it.
        }
    }

    /// <summary>
    /// Collects one counter's measurements for one topic tag. Scoped by topic so tests in this
    /// assembly — which share one static <see cref="Meter"/> — cannot see each other's counts.
    /// </summary>
    private sealed class CounterRig : IDisposable
    {
        private readonly Lock gate = new();
        private readonly List<(string Key, string Value)> tags = [];
        private readonly MeterListener listener;
        private readonly string instrument;
        private readonly string topic;

        private long total;

        internal CounterRig(string instrument, string topic)
        {
            this.instrument = instrument;
            this.topic = topic;

            this.listener = new MeterListener
            {
                InstrumentPublished = (i, l) =>
                {
                    if (i.Meter.Name == StreamsDiagnostics.SourceName && i.Name == this.instrument)
                    {
                        l.EnableMeasurementEvents(i);
                    }
                },
            };

            this.listener.SetMeasurementEventCallback<long>(this.OnMeasurement);
            this.listener.Start();
        }

        internal long Total()
        {
            lock (this.gate)
            {
                return this.total;
            }
        }

        internal IReadOnlyList<(string Key, string Value)> Tags()
        {
            lock (this.gate)
            {
                return [.. this.tags];
            }
        }

        public void Dispose() => this.listener.Dispose();

        private void OnMeasurement(
            Instrument instrument,
            long measurement,
            ReadOnlySpan<KeyValuePair<string, object?>> measurementTags,
            object? state)
        {
            var mine = false;

            foreach (var tag in measurementTags)
            {
                if (tag.Key == "topic" && string.Equals(tag.Value?.ToString(), this.topic, StringComparison.Ordinal))
                {
                    mine = true;
                    break;
                }
            }

            if (!mine)
            {
                return;
            }

            lock (this.gate)
            {
                this.total += measurement;

                foreach (var tag in measurementTags)
                {
                    this.tags.Add((tag.Key, tag.Value?.ToString() ?? string.Empty));
                }
            }
        }
    }
}
