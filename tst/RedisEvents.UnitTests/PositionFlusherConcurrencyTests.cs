using System.Diagnostics;

using FluentAssertions;

using RedisEvents.Positions;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// R-21 (P3 test gap 14). <see cref="PositionFlusher"/>'s coalescing is tested by a "thousand
/// records" test that records a thousand positions and <em>then</em> flushes — which is sequential,
/// so it exercises none of the concurrency the code is written for. Recording happens on the
/// processor loop and draining happens on the flush timer, at the same time, with no lock between
/// them: the three ordered stores in <c>Record</c> and the seq/ms/seq re-read in
/// <c>TryReadPending</c> exist precisely to make that safe.
/// </summary>
/// <remarks>
/// <para>
/// The detection trick is the recorded id itself. Every position is written as <c>(i, i)</c> — the
/// millisecond and the sequence always equal — so a torn read is not a subtle statistical claim: it
/// is a stored position whose two halves disagree, and the store fails the test the moment it sees
/// one. That is the exact failure the store ordering prevents: publishing <c>(newMs, oldSeq)</c> is
/// a position slightly <em>ahead</em> of what was processed, which skips messages rather than
/// replaying them.
/// </para>
/// <para>
/// No Redis: the store is an in-memory <see cref="IPositionStore"/>, and the multiplexer is left
/// null so no second-writer <c>HMGET</c> is issued.
/// </para>
/// </remarks>
public class PositionFlusherConcurrencyTests
{
    private const int Partitions = 4;
    private const int PerPartition = 20_000;

    /// <summary>
    /// How long a writer keeps recording, past <see cref="PerPartition"/>, waiting for the drain side
    /// to have raced it at least 3 times before giving up on that precondition and just stopping.
    /// </summary>
    /// <remarks>
    /// Wall-clock, not a multiple of <see cref="PerPartition"/>: the earlier fixed-iteration bailout
    /// (20x) is a fixed amount of CPU work, and how long that takes to burn through depends on how many
    /// cores the four writers actually get. On a contended runner the drain side's own thread can go
    /// unscheduled for a while — <c>Recording_while_the_flusher_drains...</c>'s hand-driven
    /// <c>await FlushAsync()</c> loop needs a thread-pool thread for its continuation, and
    /// <c>The_running_timer_loop...</c>'s real <c>PeriodicTimer</c> needs one for its callback — so a
    /// CPU-work bailout can be exhausted before the drain side gets scheduled even once, which is a
    /// false failure of the precondition check, not the race it exists to test. Ten seconds is far more
    /// than any healthy run needs (the precondition is normally satisfied in well under one), and gives
    /// the drain side room to actually get a timeslice under real contention.
    /// </remarks>
    private static readonly TimeSpan RaceDeadline = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Four partitions recorded from four threads while a fifth drains them in a tight loop. Nothing
    /// torn is ever stored, positions never go backwards, and the last record of every partition has
    /// reached the store once the flusher has stopped.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Recording_while_the_flusher_drains_never_stores_a_torn_or_backwards_position()
    {
        var store = new CheckingStore();

        await using var flusher = new PositionFlusher(
            store,
            topic: "concurrency",
            consumer: "svc",
            partitionCount: Partitions,
            interval: TimeSpan.FromMinutes(5));

        using var writersDone = new CancellationTokenSource();

        // Each writer keeps recording until the drain side has actually landed writes while it was
        // running, so the interleaving is a precondition of the test rather than a hope about the
        // scheduler. The last id recorded is whatever it reached, which is what the store must hold.
        var lastRecorded = new StreamId[Partitions];
        var writers = new Task[Partitions];
        for (var p = 0; p < Partitions; p++)
        {
            var partition = p;
            writers[p] = Task.Run(() =>
            {
                var i = 0;
                var elapsed = Stopwatch.StartNew();
                while (true)
                {
                    i++;
                    flusher.Record(partition, new StreamId(i, i));

                    if (i >= PerPartition && (store.Writes >= 3 || elapsed.Elapsed >= RaceDeadline))
                    {
                        break;
                    }
                }

                lastRecorded[partition] = new StreamId(i, i);
            });
        }

        // The drain side: flush as fast as it can for as long as anything is still recording, which
        // is what puts a Drain in the middle of a Record rather than after it.
        var drainer = Task.Run(async () =>
        {
            while (!writersDone.IsCancellationRequested)
            {
                await flusher.FlushAsync().ConfigureAwait(false);
            }
        });

        await Task.WhenAll(writers);
        await writersDone.CancelAsync();
        await drainer;

        // The final flush is the one that has to land the last record of every partition.
        await flusher.FlushAsync();

        store.Torn.Should().BeEmpty("a half-written (ms, seq) pair must never reach the store");
        store.Backwards.Should().BeEmpty("a partition's stored position must never move backwards");
        store.Writes.Should().BeGreaterThan(1, "the drain loop must actually have raced the writers, not just run after them");

        for (var p = 0; p < Partitions; p++)
        {
            store.Last(p).Should().Be(lastRecorded[p], "partition {0}'s last record must reach the store", p);
        }

        flusher.DirtyCount.Should().Be(0, "a record that raced a drain re-marks itself, so nothing is left behind");
        flusher.Failures.Should().Be(0);
    }

    /// <summary>
    /// The same race against the real timer loop rather than a hand-driven flush, because the loop
    /// is what runs in production: it drains on its own thread while the processor records, and
    /// <c>StopAsync</c>'s final flush is what stops a clean shutdown redelivering the last interval.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_running_timer_loop_races_the_recorder_safely()
    {
        var store = new CheckingStore();

        await using var flusher = new PositionFlusher(
            store,
            topic: "concurrency-timer",
            consumer: "svc",
            partitionCount: Partitions,
            interval: TimeSpan.FromMilliseconds(1));

        flusher.Start();

        var lastRecorded = new StreamId[Partitions];
        var writers = new Task[Partitions];
        for (var p = 0; p < Partitions; p++)
        {
            var partition = p;
            writers[p] = Task.Run(() =>
            {
                var i = 0;
                var elapsed = Stopwatch.StartNew();
                while (true)
                {
                    i++;
                    flusher.Record(partition, new StreamId(i, i));

                    // Keep going until the timer has ticked and written under the recorder, so the
                    // test cannot pass by having the loop only ever run after the writers finished.
                    if (i >= PerPartition && (store.Writes >= 3 || elapsed.Elapsed >= RaceDeadline))
                    {
                        break;
                    }
                }

                lastRecorded[partition] = new StreamId(i, i);
            });
        }

        await Task.WhenAll(writers);
        await flusher.StopAsync();

        store.Torn.Should().BeEmpty();
        store.Backwards.Should().BeEmpty();
        store.Writes.Should().BeGreaterThan(1, "the timer loop must have drained while the recorder was still running");

        for (var p = 0; p < Partitions; p++)
        {
            store.Last(p).Should().Be(lastRecorded[p], "StopAsync flushes what the last tick did not");
        }
    }

    /// <summary>
    /// An <see cref="IPositionStore"/> that checks every write as it arrives: the two halves of a
    /// recorded id must still agree, and a partition's position must never go backwards.
    /// </summary>
    private sealed class CheckingStore : IPositionStore
    {
        private readonly Lock gate = new();
        private readonly Dictionary<int, StreamId> last = [];
        private readonly List<string> torn = [];
        private readonly List<string> backwards = [];
        private long writes;

        /// <summary>Positions whose millisecond and sequence disagreed — a pair caught mid-write.</summary>
        internal IReadOnlyList<string> Torn
        {
            get
            {
                lock (this.gate)
                {
                    return this.torn.ToArray();
                }
            }
        }

        /// <summary>Positions that moved a partition backwards.</summary>
        internal IReadOnlyList<string> Backwards
        {
            get
            {
                lock (this.gate)
                {
                    return this.backwards.ToArray();
                }
            }
        }

        /// <summary>How many writes carried at least one position.</summary>
        internal long Writes
        {
            get
            {
                lock (this.gate)
                {
                    return this.writes;
                }
            }
        }

        internal StreamId Last(int partition)
        {
            lock (this.gate)
            {
                return this.last.TryGetValue(partition, out var id) ? id : default;
            }
        }

        public ValueTask<IReadOnlyDictionary<int, StreamId>> LoadAsync(string topic, string consumer, CancellationToken ct)
            => NullPositionStore.Instance.LoadAsync(topic, consumer, ct);

        public ValueTask SaveAsync(
            string topic,
            string consumer,
            ReadOnlySpan<(int Partition, StreamId Id)> positions,
            CancellationToken ct)
        {
            if (positions.IsEmpty)
            {
                return ValueTask.CompletedTask;
            }

            lock (this.gate)
            {
                this.writes++;

                foreach (var (partition, id) in positions)
                {
                    if (id.Ms != id.Seq)
                    {
                        this.torn.Add($"partition {partition} stored {id.Format()}");
                    }

                    if (this.last.TryGetValue(partition, out var previous) && id.Ms < previous.Ms)
                    {
                        this.backwards.Add($"partition {partition} went from {previous.Format()} to {id.Format()}");
                    }

                    this.last[partition] = id;
                }
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask ResetAsync(string topic, string consumer, StreamId to, int? partition, CancellationToken ct)
            => ValueTask.CompletedTask;
    }
}
