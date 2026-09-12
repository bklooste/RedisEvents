using FluentAssertions;

using RedisEvents.Consumer;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// R-07 — the consumer-group ack queue: who may touch it, and which batch one reported position
/// actually acknowledges.
/// </summary>
/// <remarks>
/// <para>
/// These drive <see cref="PendingAcks"/> directly rather than through
/// <see cref="ConsumerGroupFetch"/>, because both defects live in the bookkeeping and not in the
/// transport: the fetch would need an <see cref="IDatabase"/> to build, and the interesting cases
/// (a claimed batch queued behind a live one; a reader and a processor racing) are about the queue
/// alone. The end-to-end proof against a real group is in the service tests.
/// </para>
/// <para>
/// The queue was an unsynchronised <c>Queue&lt;PendingAck&gt;</c> and acknowledged "every batch
/// whose last id is at or below the reported id", which is the pair of bugs the P0 row for R-07
/// names.
/// </para>
/// </remarks>
public sealed class ConsumerGroupAckTests
{
    /// <summary>Builds one batch's worth of ids, contiguous from <paramref name="firstMs"/>.</summary>
    private static (RedisValue[] Ids, StreamId First, StreamId Last) Batch(long firstMs, int count)
    {
        var ids = new RedisValue[count];

        for (var i = 0; i < count; i++)
        {
            ids[i] = new StreamId(firstMs + i, 0).Format();
        }

        return (ids, new StreamId(firstMs, 0), new StreamId(firstMs + count - 1, 0));
    }

    private static PendingAcks WithBatches(params (RedisValue[] Ids, StreamId First, StreamId Last)[] batches)
    {
        var pending = new PendingAcks();

        foreach (var batch in batches)
        {
            pending.Add(batch.Ids, batch.First, batch.Last).Should().Be(0);
        }

        return pending;
    }

    /// <summary>
    /// The R-07(b) regression. An <c>XAUTOCLAIM</c> sweep returns entries OLDER than the live batch
    /// fetched before it, so acknowledging the live batch must not sweep the claimed one out of the
    /// pending-entries list — the handler has not seen it yet.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_claimed_batch_with_older_ids_is_not_acknowledged_by_the_live_batch_ahead_of_it()
    {
        var live = Batch(firstMs: 500, count: 3);
        var claimed = Batch(firstMs: 5, count: 2);

        var pending = WithBatches(live, claimed);

        // The handler finishes the live batch and reports its last id. Under the old
        // "Last <= processedThrough" rule this returned all five ids.
        var acked = pending.Take(live.Last);

        acked.Should().BeEquivalentTo(live.Ids, "only the batch the handler actually processed is acknowledged");
        pending.Count.Should().Be(1, "the claimed batch is still waiting for its turn through the channel");

        // ...and when the handler gets to it, it is acknowledged in its own right.
        pending.Take(claimed.Last).Should().BeEquivalentTo(claimed.Ids);
        pending.Count.Should().Be(0);
    }

    /// <summary>
    /// The ordinary case: one fetch, one handler call, one acknowledgement, and the fetch's own id
    /// array is handed back without a copy.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void One_batch_is_acknowledged_by_its_own_last_id()
    {
        var only = Batch(firstMs: 10, count: 4);
        var pending = WithBatches(only);

        pending.Take(only.Last).Should().BeSameAs(only.Ids);
        pending.Count.Should().Be(0);
    }

    /// <summary>
    /// An id that reaches into a batch without reaching its end is per-message progress, not a
    /// finished batch: nothing of that batch is acknowledged, though anything queued ahead of it is.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_id_inside_an_unfinished_batch_acknowledges_only_the_batches_ahead_of_it()
    {
        var first = Batch(firstMs: 100, count: 2);
        var second = Batch(firstMs: 200, count: 3);

        var pending = WithBatches(first, second);

        // 201-0 is the middle entry of the second batch.
        var acked = pending.Take(new StreamId(201, 0));

        acked.Should().BeEquivalentTo(first.Ids, "the earlier batch was handled before this one");
        pending.Count.Should().Be(1, "the batch still being processed keeps its ids");
    }

    /// <summary>
    /// A reported id belonging to no pending batch — a duplicate flush after the batch was already
    /// acknowledged — acknowledges nothing rather than guessing at the head of the queue.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_unknown_id_acknowledges_nothing()
    {
        var batch = Batch(firstMs: 100, count: 2);
        var pending = WithBatches(batch);

        pending.Take(new StreamId(99, 0)).Should().BeNull("that id is older than anything held");
        pending.Take(new StreamId(4_000, 0)).Should().BeNull("that id was never fetched by this instance");
        pending.Count.Should().Be(1);

        pending.Take(batch.Last).Should().BeEquivalentTo(batch.Ids);
    }

    /// <summary>
    /// When an earlier acknowledgement never landed — its <c>XACK</c> threw, say — the next one
    /// covers both batches in one call, in fetch order.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_unacknowledged_batch_ahead_is_joined_into_the_next_acknowledgement()
    {
        var first = Batch(firstMs: 100, count: 2);
        var second = Batch(firstMs: 200, count: 2);
        var third = Batch(firstMs: 300, count: 1);

        var pending = WithBatches(first, second, third);

        var acked = pending.Take(second.Last);

        acked.Should().NotBeNull();
        acked!.Should().Equal(first.Ids[0], first.Ids[1], second.Ids[0], second.Ids[1]);
        pending.Count.Should().Be(1);
    }

    /// <summary>A cleared queue — the group was destroyed or reset — acknowledges nothing.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Clear_forgets_every_pending_batch()
    {
        var batch = Batch(firstMs: 100, count: 2);
        var pending = WithBatches(batch);

        pending.Clear();

        pending.Count.Should().Be(0);
        pending.Take(batch.Last).Should().BeNull();
    }

    /// <summary>
    /// The unhealthy case is bounded rather than unbounded: acknowledgements that never arrive drop
    /// the oldest batches and say so, instead of growing the queue until the pod dies. Nothing is
    /// lost — the ids stay in this consumer's PEL and come back through the claim sweep.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_queue_is_bounded_and_reports_what_it_dropped()
    {
        var pending = new PendingAcks();
        var dropped = 0;

        for (var i = 0; i < PendingAcks.MaxPendingBatches + 3; i++)
        {
            var batch = Batch(firstMs: (i + 1) * 10, count: 1);
            dropped += pending.Add(batch.Ids, batch.First, batch.Last);
        }

        dropped.Should().Be(3);
        pending.Count.Should().Be(PendingAcks.MaxPendingBatches);
        pending.Take(new StreamId(10, 0)).Should().BeNull("the oldest batches were dropped, not acknowledged");
    }

    /// <summary>
    /// The R-07(a) regression, and the one a green suite could never have caught: the reader loop
    /// enqueues while the processor loop dequeues. With a bare <see cref="Queue{T}"/> this loses
    /// ids, returns the same ids twice, or throws out of one of the two loops.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_reader_and_the_processor_may_use_the_queue_at_the_same_time()
    {
        const int batches = 10_000;

        // The reader stops fetching once this many batches are unacknowledged, exactly as the real
        // one stops on a full channel. Without it the reader would run past MaxPendingBatches and
        // the queue would drop the very batches the processor is waiting for.
        const int inFlightCap = 32;

        var pending = new PendingAcks();
        var acked = new List<string>(batches * 2);
        var dropped = 0;

        // The reader: one batch per fetch, ids increasing, exactly as XREADGROUP delivers them.
        var reader = Task.Run(() =>
        {
            for (var i = 0; i < batches; i++)
            {
                while (pending.Count >= inFlightCap)
                {
                    Thread.Yield();
                }

                var batch = Batch(firstMs: (i + 1) * 10, count: 2);
                dropped += pending.Add(batch.Ids, batch.First, batch.Last);
            }
        });

        // The processor: acknowledges the same batches, in the same order, as they show up.
        var processor = Task.Run(() =>
        {
            for (var i = 0; i < batches; i++)
            {
                var last = new StreamId(((i + 1) * 10) + 1, 0);
                RedisValue[]? ids;

                // Take returns null while the reader is still ahead of us; that is the shape the
                // real loops have too, minus the spin.
                while ((ids = pending.Take(last)) is null)
                {
                    Thread.Yield();
                }

                foreach (var id in ids)
                {
                    acked.Add(id.ToString());
                }
            }
        });

        await Task.WhenAll(reader, processor).WaitAsync(TimeSpan.FromSeconds(60));

        dropped.Should().Be(0, "the reader never outran the cap, so nothing was pushed out of the queue");
        acked.Should().HaveCount(batches * 2, "every fetched id is acknowledged exactly once");
        acked.Should().OnlyHaveUniqueItems();
        pending.Count.Should().Be(0);
    }
}
