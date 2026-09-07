using System.Diagnostics.Metrics;

using FluentAssertions;

using Microsoft.Extensions.Logging;

using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;

using StackExchange.Redis;

namespace Orange.Lib.Streams.UnitTests;

/// <summary>
/// R-21 (P3 test gap 12). Nothing observed <c>streams.errors</c>. The counter is the one that every
/// error dashboard and every alert is built on, and it is only useful because of its tags: the
/// policy says whether the message was skipped, the partition stopped or the pod failed, and
/// <c>exception.type</c> is what turns "errors are up" into a cause. A dropped or renamed tag breaks
/// every grouping downstream and changes nothing that any other test can see.
/// </summary>
/// <remarks>
/// Each test uses a topic name of its own and the listener keeps only that topic's measurements, so
/// the assertions cannot pick up a neighbouring test's counter on the shared static
/// <see cref="Meter"/> while the suite runs in parallel.
/// </remarks>
public class ErrorMetricTests
{
    /// <summary>The ordinary failure: logged, skipped, counted as best effort with its exception type.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_best_effort_failure_is_counted_with_its_policy_and_exception_type()
    {
        const string topic = "errors-best-effort";
        using var errors = new ErrorProbe(topic);

        var channel = PartitionWorker.CreateChannel(4);
        var ctx = Context(topic, channel.Writer, ErrorPolicy.BestEffort);

        ErrorHandling.Queue(channel, 1, 0);
        channel.Writer.Complete();

        await PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) => throw new InvalidOperationException("boom"),
            (_, _) => { },
            CancellationToken.None,
            channel.Reader);

        var measurement = errors.Measurements.Should().ContainSingle().Subject;

        measurement.Value.Should().Be(1);
        measurement.Consumer.Should().Be("svc");
        measurement.Policy.Should().Be("best_effort");
        measurement.ExceptionType.Should().Be(nameof(InvalidOperationException));
    }

    /// <summary>
    /// The policy tag is the whole point: the same exception counted under <c>stop_partition</c>
    /// means a partition is down, not a message skipped.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_stopped_partition_is_counted_under_its_own_policy()
    {
        const string topic = "errors-stop-partition";
        using var errors = new ErrorProbe(topic);

        var channel = PartitionWorker.CreateChannel(4);
        var ctx = Context(topic, channel.Writer, ErrorPolicy.StopPartition);

        ErrorHandling.Queue(channel, 1, 0);
        channel.Writer.Complete();

        await PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) => throw new TimeoutException("downstream"),
            (_, _) => { },
            CancellationToken.None,
            channel.Reader);

        var measurement = errors.Measurements.Should().ContainSingle().Subject;

        measurement.Policy.Should().Be("stop_partition");
        measurement.ExceptionType.Should().Be(nameof(TimeoutException));
    }

    /// <summary>
    /// A <see cref="Orange.Lib.Streams.Errors.DontIgnoreException"/> is not an error policy outcome
    /// at all — the partition blocks and retries — so it is counted under <c>block</c>. Counting it
    /// as best effort would say a message had been skipped when it explicitly was not.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_blocked_batch_is_counted_under_the_block_policy()
    {
        const string topic = "errors-block";
        using var errors = new ErrorProbe(topic);

        var channel = PartitionWorker.CreateChannel(4);
        var ctx = Context(topic, channel.Writer, ErrorPolicy.BestEffort);
        using var cts = new CancellationTokenSource();

        // The backoff seam, so the 1 s → 30 s ladder costs nothing and the loop ends deterministically.
        var delays = new ScriptedDelay(cancelAfter: 2, cts);
        PartitionWorker.BlockDelay = delays.DelayAsync;

        ErrorHandling.Queue(channel, 1, 0);

        await PartitionWorker.ProcessLoopAsync(
            ctx,
            (_, _) => throw new MandatoryDownstreamDownException("ledger is down"),
            (_, _) => { },
            cts.Token,
            channel.Reader);

        errors.Measurements.Should().NotBeEmpty();
        errors.Measurements.Should().OnlyContain(m => m.Policy == "block");
        errors.Measurements[0].ExceptionType.Should().Be(nameof(MandatoryDownstreamDownException));
    }

    private static PartitionContext Context(string topic, System.Threading.Channels.ChannelWriter<StreamBatch> writer, ErrorPolicy onError)
        => new(
            Db: null!,
            StreamKey: (RedisKey)$"s:{{{topic}}}:0",
            Partition: 0,
            Topic: topic,
            Consumer: "svc",
            BatchSize: 100,
            Filter: null,
            OnError: onError,
            Writer: writer,
            Log: Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

    /// <summary>One <c>streams.errors</c> measurement, with the tags the dashboards group by.</summary>
    internal sealed record ErrorMeasurement(long Value, string Consumer, string Policy, string ExceptionType);

    /// <summary>Collects <c>streams.errors</c> for one topic only.</summary>
    private sealed class ErrorProbe : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly List<ErrorMeasurement> measurements = [];
        private readonly string topic;

        internal ErrorProbe(string topic)
        {
            this.topic = topic;

            this.listener.InstrumentPublished = (instrument, listening) =>
            {
                if (instrument.Meter.Name == "Orange.Lib.Streams" &&
                    string.Equals(instrument.Name, "streams.errors", StringComparison.Ordinal))
                {
                    listening.EnableMeasurementEvents(instrument);
                }
            };

            this.listener.SetMeasurementEventCallback<long>(this.OnMeasurement);
            this.listener.Start();
        }

        internal IReadOnlyList<ErrorMeasurement> Measurements
        {
            get
            {
                lock (this.measurements)
                {
                    return this.measurements.ToArray();
                }
            }
        }

        public void Dispose() => this.listener.Dispose();

        private void OnMeasurement(
            Instrument instrument,
            long value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state)
        {
            _ = instrument;
            _ = state;

            string? measuredTopic = null;
            string? consumer = null;
            string? policy = null;
            string? exceptionType = null;

            foreach (var tag in tags)
            {
                switch (tag.Key)
                {
                    case "topic":
                        measuredTopic = tag.Value?.ToString();
                        break;
                    case "consumer":
                        consumer = tag.Value?.ToString();
                        break;
                    case "policy":
                        policy = tag.Value?.ToString();
                        break;
                    case "exception.type":
                        exceptionType = tag.Value?.ToString();
                        break;
                    default:
                        break;
                }
            }

            if (!string.Equals(measuredTopic, this.topic, StringComparison.Ordinal))
            {
                return;
            }

            lock (this.measurements)
            {
                this.measurements.Add(new ErrorMeasurement(value, consumer ?? string.Empty, policy ?? string.Empty, exceptionType ?? string.Empty));
            }
        }
    }
}
