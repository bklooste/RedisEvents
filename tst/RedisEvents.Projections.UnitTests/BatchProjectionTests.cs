using System.Diagnostics;
using System.Text.Json.Serialization;

using FluentAssertions;

using RedisEvents.Errors;
using RedisEvents.Projections;
using RedisEvents.Projections.Diagnostics;
using RedisEvents.Wire;

namespace RedisEvents.Projections.UnitTests;

/// <summary>
/// Covers <see cref="IBatchProjection"/> and <see cref="ProjectionBatch"/>: a batch projection buffers
/// into the batch scope its events carry and is flushed once, after the last event and before
/// <see cref="EventProjector.HandleAsync"/> returns (so before core can move the position), and a flush
/// that throws is a batch that threw. The real-Redis proof that core then blocks or skips is
/// <c>BatchProjectionEndToEndTests</c>.
/// </summary>
public class BatchProjectionTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_flush_runs_once_after_every_event_is_dispatched_and_sees_them_all_in_order()
    {
        var registry = Registry();
        var projection = new BufferingProjection();
        var projector = new EventProjector(registry, [projection], logger: null, topic: "orders");

        await projector.HandleAsync(Batch(("a", "1"), ("a", "2"), ("a", "3")), CancellationToken.None);

        projection.Timeline.Should().Equal("handle 1", "handle 2", "handle 3", "flush 1,2,3");
        projection.Flushed.Should().ContainSingle().Which.Should().Equal("1", "2", "3");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Every_event_in_one_batch_carries_the_same_open_scope_and_the_next_batch_gets_a_new_one()
    {
        var registry = Registry();
        var projection = new BufferingProjection();
        var projector = new EventProjector(registry, [projection], logger: null, topic: "orders");

        await projector.HandleAsync(Batch(("a", "1"), ("a", "2")), CancellationToken.None);
        await projector.HandleAsync(Batch(("a", "3")), CancellationToken.None);

        projection.Scopes.Should().HaveCount(3);
        projection.Scopes[0].Should().BeSameAs(projection.Scopes[1]);
        projection.Scopes[2].Should().NotBeSameAs(projection.Scopes[0]);
        projection.Scopes[0].Topic.Should().Be("orders");
        projection.Scopes[0].Partition.Should().Be(3);
        projection.Flushed.Should().HaveCount(2);
        projection.Flushed[1].Should().Equal("3");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_scope_is_closed_when_HandleAsync_returns_so_a_kept_reference_cannot_buffer_past_the_flush()
    {
        var registry = Registry();
        var projection = new BufferingProjection();
        var projector = new EventProjector(registry, [projection]);

        await projector.HandleAsync(Batch(("a", "1")), CancellationToken.None);

        var kept = projection.Scopes.Single();
        kept.IsClosed.Should().BeTrue();

        var buffer = () => kept.GetOrAdd(projection, () => new List<string>());
        buffer.Should().Throw<InvalidOperationException>().WithMessage("*closed*");
        var read = () => kept.TryGet<List<string>>(projection, out _);
        read.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_projection_that_does_not_opt_in_is_dispatched_as_before_and_is_never_flushed()
    {
        var registry = Registry();
        var plain = new PlainProjection();
        var projector = new EventProjector(registry, [plain]);

        await projector.HandleAsync(Batch(("a", "1"), ("a", "2")), CancellationToken.None);

        plain.Received.Should().Equal("1", "2");
        plain.SawBatch.Should().OnlyContain(b => b, "the scope is set on every dispatched event, whether or not anyone buffers");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_batch_projection_handed_no_event_in_the_batch_is_not_flushed()
    {
        var registry = Registry();
        var notesOnly = new NotesOnlyProjection();
        var projector = new EventProjector(registry, [notesOnly]);

        await projector.HandleAsync(Batch(("a", "1"), ("a", "2")), CancellationToken.None);

        notesOnly.Flushes.Should().Be(0, "nothing in the batch bound to it");

        await projector.HandleAsync(Batch(("a", "3"), ("b", "x")), CancellationToken.None);

        notesOnly.Flushes.Should().Be(1);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Two_batch_projections_are_each_flushed_once_with_only_their_own_state()
    {
        var registry = Registry();
        var first = new BufferingProjection();
        var second = new BufferingProjection();
        var projector = new EventProjector(registry, [first, second]);

        await projector.HandleAsync(Batch(("a", "1"), ("b", "x"), ("a", "2")), CancellationToken.None);

        first.Flushed.Should().ContainSingle().Which.Should().Equal("1", "x", "2");
        second.Flushed.Should().ContainSingle().Which.Should().Equal("1", "x", "2");
        first.Timeline.Last().Should().StartWith("flush");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Concurrent_batches_through_two_projectors_sharing_one_projection_keep_separate_state()
    {
        // One singleton shared by every topic's projector, called concurrently, is the normal shape.
        var registry = Registry();
        using var gate = new SemaphoreSlim(0);
        var projection = new BufferingProjection(gate);
        var left = new EventProjector(registry, [projection], logger: null, topic: "left");
        var right = new EventProjector(registry, [projection], logger: null, topic: "right");

        var l = left.HandleAsync(Batch(("a", "L1"), ("a", "L2")), CancellationToken.None).AsTask();
        var r = right.HandleAsync(Batch(("a", "R1"), ("a", "R2")), CancellationToken.None).AsTask();
        projection.Gate!.Release(4);
        await Task.WhenAll(l, r);

        projection.Flushed.Should().HaveCount(2);
        projection.Flushed.Should().ContainEquivalentOf(new[] { "L1", "L2" }, o => o.WithStrictOrdering());
        projection.Flushed.Should().ContainEquivalentOf(new[] { "R1", "R2" }, o => o.WithStrictOrdering());
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_DontIgnoreException_from_the_flush_propagates_unchanged_so_core_blocks_the_batch()
    {
        var registry = Registry();
        var projection = new BufferingProjection { FlushThrows = new StoreDownException() };
        var projector = new EventProjector(registry, [projection]);

        var act = async () => await projector.HandleAsync(Batch(("a", "1"), ("a", "2")), CancellationToken.None);

        await act.Should().ThrowExactlyAsync<StoreDownException>();
        projection.Scopes.Distinct().Single().IsClosed.Should().BeTrue("a retried batch starts over in a new scope");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_ordinary_exception_from_one_flush_still_flushes_the_others_then_propagates_unchanged()
    {
        var registry = Registry();
        var failing = new BufferingProjection { FlushThrows = new InvalidOperationException("row rejected") };
        var healthy = new BufferingProjection();
        var projector = new EventProjector(registry, [failing, healthy]);

        var act = async () => await projector.HandleAsync(Batch(("a", "1")), CancellationToken.None);

        (await act.Should().ThrowExactlyAsync<InvalidOperationException>()).WithMessage("row rejected");
        healthy.Flushed.Should().ContainSingle("core skips the batch and advances past the other projection's work too");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_DontIgnoreException_from_one_flush_skips_the_remaining_flushes_because_the_batch_redelivers_whole()
    {
        var registry = Registry();
        var failing = new BufferingProjection { FlushThrows = new StoreDownException() };
        var later = new BufferingProjection();
        var projector = new EventProjector(registry, [failing, later]);

        var act = async () => await projector.HandleAsync(Batch(("a", "1")), CancellationToken.None);

        await act.Should().ThrowExactlyAsync<StoreDownException>();
        later.Flushed.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_ordinary_exception_from_an_event_flushes_the_events_before_it_then_rethrows_the_original()
    {
        // Core skips the batch on an ordinary exception. Per-event projections have already written the
        // events before the failing one; a batch projection must not lose them either.
        var registry = Registry();
        var projection = new BufferingProjection { ThrowOn = ("2", new FormatException("bad body")) };
        var projector = new EventProjector(registry, [projection]);

        var act = async () => await projector.HandleAsync(Batch(("a", "1"), ("a", "2"), ("a", "3")), CancellationToken.None);

        (await act.Should().ThrowExactlyAsync<FormatException>()).WithMessage("bad body");
        projection.Flushed.Should().ContainSingle().Which.Should().Equal("1");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_DontIgnoreException_from_an_event_is_rethrown_without_a_flush()
    {
        var registry = Registry();
        var projection = new BufferingProjection { ThrowOn = ("2", new StoreDownException()) };
        var projector = new EventProjector(registry, [projection]);

        var act = async () => await projector.HandleAsync(Batch(("a", "1"), ("a", "2")), CancellationToken.None);

        await act.Should().ThrowExactlyAsync<StoreDownException>();
        projection.Flushed.Should().BeEmpty("the whole batch is redelivered, so flushing part of it is wasted work against a store that is down");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Shutdown_cancellation_from_an_event_is_rethrown_without_a_flush()
    {
        using var cts = new CancellationTokenSource();
        var registry = Registry();
        var projection = new BufferingProjection { CancelOn = ("2", cts) };
        var projector = new EventProjector(registry, [projection]);

        var act = async () => await projector.HandleAsync(Batch(("a", "1"), ("a", "2")), cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        projection.Flushed.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task When_an_event_fails_ordinarily_and_the_flush_then_finds_the_store_down_the_batch_blocks_instead_of_skipping()
    {
        var registry = Registry();
        var projection = new BufferingProjection
        {
            ThrowOn = ("2", new FormatException("bad body")),
            FlushThrows = new StoreDownException(),
        };
        var projector = new EventProjector(registry, [projection]);

        var act = async () => await projector.HandleAsync(Batch(("a", "1"), ("a", "2")), CancellationToken.None);

        await act.Should().ThrowExactlyAsync<StoreDownException>("skipping would advance past event 1's unwritten rows");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task When_an_event_fails_ordinarily_and_the_flush_also_fails_ordinarily_the_original_is_rethrown()
    {
        var registry = Registry();
        var projection = new BufferingProjection
        {
            ThrowOn = ("2", new FormatException("bad body")),
            FlushThrows = new InvalidOperationException("row rejected"),
        };
        var projector = new EventProjector(registry, [projection]);

        var act = async () => await projector.HandleAsync(Batch(("a", "1"), ("a", "2")), CancellationToken.None);

        (await act.Should().ThrowExactlyAsync<FormatException>()).WithMessage("bad body");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_batch_of_only_a_malformed_entry_is_skipped_without_dispatch_or_flush()
    {
        var registry = Registry();
        var projection = new BufferingProjection();
        var projector = new EventProjector(registry, [projection]);
        var poison = new StreamMsg(
            Body: "not json"u8.ToArray(),
            Type: "a",
            Id: new StreamId(1, 0),
            Partition: 3,
            PartitionKey: "k",
            CorrelationId: string.Empty,
            TraceParent: null,
            Headers: default);

        var act = async () => await projector.HandleAsync(new[] { poison }, CancellationToken.None);

        await act.Should().NotThrowAsync("a malformed entry is skipped per entry, not thrown for the batch");
        projection.Timeline.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Each_flush_gets_its_own_span_named_for_the_projection()
    {
        var topic = "flush-span-" + Guid.NewGuid().ToString("N");
        var spans = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == ProjectionsDiagnostics.SourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a =>
            {
                lock (spans)
                {
                    spans.Add(a);
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        var registry = Registry();
        var projector = new EventProjector(registry, [new BufferingProjection()], logger: null, topic: topic);

        await projector.HandleAsync(Batch(("a", "1"), ("a", "2")), CancellationToken.None);

        Activity[] mine;
        lock (spans)
        {
            mine = spans.Where(s => Equals(s.GetTagItem(ProjectionsSpans.TopicTagKey), topic)).ToArray();
        }

        mine.Should().ContainSingle().Which.OperationName.Should().Be("projections.flush BufferingProjection");
        mine[0].GetTagItem(ProjectionsSpans.PartitionTagKey).Should().Be(3);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void GetOrAdd_creates_once_per_owner_and_keeps_owners_apart()
    {
        var batch = new ProjectionBatch("t", 0);
        var ownerA = new object();
        var ownerB = new object();
        var created = 0;

        var a1 = batch.GetOrAdd(ownerA, () => { created++; return new List<int>(); });
        var a2 = batch.GetOrAdd(ownerA, () => { created++; return new List<int>(); });
        var b = batch.GetOrAdd(ownerB, () => new List<int>());

        a1.Should().BeSameAs(a2);
        b.Should().NotBeSameAs(a1);
        created.Should().Be(1);
        batch.TryGet<List<int>>(ownerA, out var found).Should().BeTrue();
        found.Should().BeSameAs(a1);
        batch.TryGet<List<int>>(new object(), out _).Should().BeFalse();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Asking_for_an_owners_state_as_another_type_throws_rather_than_returning_null()
    {
        var batch = new ProjectionBatch("t", 0);
        var owner = new object();
        batch.GetOrAdd(owner, () => new List<int>());

        var act = () => batch.GetOrAdd(owner, () => new List<string>());

        act.Should().Throw<InvalidOperationException>().WithMessage("*List`1*");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_malformed_entry_is_skipped_and_the_rest_of_the_batch_is_still_projected_and_flushed()
    {
        var registry = Registry();
        var buffering = new BufferingProjection();
        var plain = new PlainProjection();
        var projector = new EventProjector(registry, [buffering, plain], logger: null, topic: "orders");
        var batch = Batch(("a", "1"), ("a", "2"), ("a", "3"), ("a", "4"));
        batch[1] = batch[1] with { Body = "{not json"u8.ToArray() };

        long malformed = 0;
        using var listener = new System.Diagnostics.Metrics.MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "RedisEvents.Projections" && instrument.Name == "redisevents.projections.events.malformed")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref malformed, value));
        listener.Start();

        await projector.HandleAsync(batch, CancellationToken.None);

        buffering.Flushed.Should().ContainSingle().Which.Should().Equal("1", "3", "4");
        plain.Received.Should().Equal("1", "3", "4");
        Interlocked.Read(ref malformed).Should().BeGreaterThanOrEqualTo(1);
    }

    private static EventTypeRegistry Registry() => new EventTypeRegistry()
        .RegisterJson("a", BatchProjectionTestsJson.Default.OrderPlaced)
        .RegisterJson("b", BatchProjectionTestsJson.Default.OrderNoted);

    private static StreamMsg[] Batch(params (string Type, string Id)[] events)
    {
        var batch = new StreamMsg[events.Length];
        for (var i = 0; i < events.Length; i++)
        {
            var (type, id) = events[i];
            var body = type == "a"
                ? System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new OrderPlaced(id), BatchProjectionTestsJson.Default.OrderPlaced)
                : System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new OrderNoted(id), BatchProjectionTestsJson.Default.OrderNoted);

            batch[i] = new StreamMsg(
                Body: body,
                Type: type,
                Id: new StreamId(100 + i, 0),
                Partition: 3,
                PartitionKey: "order-" + id,
                CorrelationId: string.Empty,
                TraceParent: null,
                Headers: default);
        }

        return batch;
    }

    /// <summary>Buffers each event's id in the batch scope and records the order of calls.</summary>
    private sealed class BufferingProjection(SemaphoreSlim? gate = null)
        : IProjection<OrderPlaced>, IProjection<OrderNoted>, IBatchProjection
    {
        private readonly Lock sync = new();

        public SemaphoreSlim? Gate { get; } = gate;

        public List<string> Timeline { get; } = [];

        public List<List<string>> Flushed { get; } = [];

        public List<ProjectionBatch> Scopes { get; } = [];

        public Exception? FlushThrows { get; init; }

        public (string Id, Exception Error)? ThrowOn { get; init; }

        public (string Id, CancellationTokenSource Source)? CancelOn { get; init; }

        public ValueTask HandleAsync(OrderPlaced @event, EventMeta meta, CancellationToken ct) =>
            this.BufferAsync(@event.Id, meta, ct);

        public ValueTask HandleAsync(OrderNoted @event, EventMeta meta, CancellationToken ct) =>
            this.BufferAsync(@event.Id, meta, ct);

        public ValueTask FlushAsync(ProjectionBatch batch, CancellationToken ct)
        {
            if (this.FlushThrows is not null)
            {
                throw this.FlushThrows;
            }

            var rows = batch.TryGet<List<string>>(this, out var buffered) ? buffered : [];
            lock (this.sync)
            {
                this.Timeline.Add("flush " + string.Join(",", rows));
                this.Flushed.Add([.. rows]);
            }

            return ValueTask.CompletedTask;
        }

        private async ValueTask BufferAsync(string id, EventMeta meta, CancellationToken ct)
        {
            if (this.Gate is not null)
            {
                await this.Gate.WaitAsync(ct);
            }

            lock (this.sync)
            {
                this.Scopes.Add(meta.Batch!);
                this.Timeline.Add("handle " + id);
            }

            if (this.ThrowOn is { } failure && failure.Id == id)
            {
                throw failure.Error;
            }

            if (this.CancelOn is { } cancel && cancel.Id == id)
            {
                await cancel.Source.CancelAsync();
                ct.ThrowIfCancellationRequested();
            }

            meta.Batch!.GetOrAdd(this, () => new List<string>()).Add(id);
        }
    }

    private sealed class NotesOnlyProjection : IProjection<OrderNoted>, IBatchProjection
    {
        public int Flushes { get; private set; }

        public ValueTask HandleAsync(OrderNoted @event, EventMeta meta, CancellationToken ct) => ValueTask.CompletedTask;

        public ValueTask FlushAsync(ProjectionBatch batch, CancellationToken ct)
        {
            this.Flushes++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class PlainProjection : IProjection<OrderPlaced>
    {
        public List<string> Received { get; } = [];

        public List<bool> SawBatch { get; } = [];

        public ValueTask HandleAsync(OrderPlaced @event, EventMeta meta, CancellationToken ct)
        {
            this.Received.Add(@event.Id);
            this.SawBatch.Add(meta.Batch is not null);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class StoreDownException() : DontIgnoreException("the store is down");
}

internal sealed record OrderPlaced(string Id);

internal sealed record OrderNoted(string Id);

[JsonSerializable(typeof(OrderPlaced))]
[JsonSerializable(typeof(OrderNoted))]
internal sealed partial class BatchProjectionTestsJson : JsonSerializerContext;
