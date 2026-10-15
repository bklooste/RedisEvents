using System.Collections.Concurrent;
using System.Text.Json.Serialization;

using FluentAssertions;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RedisEvents.Consumer;
using RedisEvents.Errors;
using RedisEvents.Extensions;
using RedisEvents.Positions;
using RedisEvents.Producer;
using RedisEvents.Projections;
using RedisEvents.Wire;

namespace RedisEvents.Tests;

/// <summary>
/// Proves, against a real Redis and core's real consumer loop, that an <see cref="IBatchProjection"/>'s
/// flush is part of the batch: the position does not move until the flush returns, a flush that throws a
/// <see cref="DontIgnoreException"/> blocks the partition and redelivers the whole batch until it
/// succeeds, and an ordinary flush exception skips the batch as a handler throw does.
/// </summary>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class BatchProjectionEndToEndTests(RedisStreamsFixture fixture)
{
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_flush_that_finds_the_store_down_blocks_the_position_and_rewrites_the_whole_batch_when_it_returns()
    {
        // Same shortening ErrorServiceTests.S12b uses: the same retry path, without a real minute of backoff.
        PartitionWorker.BlockDelay = static (ms, ct) => Task.Delay(Math.Min(ms, 100), ct);

        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var store = new FakeStore { Down = true };

        using var host = this.BuildHost(topic, consumer, store);
        var publisher = host.Services.GetRequiredKeyedService<IStreamPublisher>(topic);

        // One key, so one partition, published before the consumer starts so they are read as one batch.
        var ids = new List<StreamId>();
        foreach (var name in new[] { "one", "two", "three" })
        {
            ids.Add(await publisher.PublishAsync("order-1", Json(name), "line"));
        }

        await host.StartAsync();
        try
        {
            await RedisStreamsFixture.WaitUntilAsync(
                () => store.FailedFlushes >= 3,
                TimeSpan.FromSeconds(30),
                "the blocked batch to be dispatched and flushed again");

            store.Attempts.Should().OnlyContain(
                attempt => attempt.SequenceEqual(store.Attempts.First()),
                "every retry re-dispatches the same batch from its first event into a new scope");
            store.Attempts.First().Should().StartWith("one");

            var during = await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, CancellationToken.None);
            during.Values.Intersect(ids).Should().BeEmpty("the position never moves while the flush is blocked");

            store.Down = false;

            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, CancellationToken.None))
                    .Values.Contains(ids[^1]),
                TimeSpan.FromSeconds(30),
                "the position to advance once the flush succeeds");

            store.Written.Should().Equal("one", "two", "three");
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task An_ordinary_flush_failure_skips_the_batch_like_a_handler_throw_and_the_next_batch_flows()
    {
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        var store = new FakeStore { RejectFirst = true };

        using var host = this.BuildHost(topic, consumer, store);
        var publisher = host.Services.GetRequiredKeyedService<IStreamPublisher>(topic);

        var first = await publisher.PublishAsync("order-2", Json("rejected"), "line");

        await host.StartAsync();
        try
        {
            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, CancellationToken.None))
                    .Values.Contains(first),
                TimeSpan.FromSeconds(30),
                "the position to advance past the skipped batch");

            var next = await publisher.PublishAsync("order-2", Json("accepted"), "line");

            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await new RedisPositionStore(fixture.Redis).LoadAsync(topic, consumer, CancellationToken.None))
                    .Values.Contains(next),
                TimeSpan.FromSeconds(30),
                "the next batch to be projected and flushed");

            store.Written.Should().Equal("accepted");
            store.Attempts.Count(a => a.Contains("rejected")).Should().Be(1, "an ordinary exception is not retried");
        }
        finally
        {
            await host.StopAsync();
        }
    }

    private IHost BuildHost(string topic, string consumer, FakeStore store)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
        {
            EnvironmentName = "Production",
        });

        builder.Configuration["Streams:ConnectionString"] = fixture.ConnectionString;
        builder.Configuration["Streams:Consumer"] = consumer;
        builder.Configuration["Streams:Consumers:0:Topic"] = topic;
        builder.Configuration["Streams:Consumers:0:BatchSize"] = "10";

        // SyncBatch: the position write lands before the next batch, so "not advanced" is observable.
        builder.Configuration["Streams:Consumers:0:Persist"] = "SyncBatch";

        builder.Services.AddSingleton(store);
        builder.AddStreamPublisher(topic)
               .AddEventProjector(topic, types => types.RegisterJson("line", LinesJson.Default.Line))
               .AddProjection<LineProjection>();

        return builder.Build();
    }

    private static byte[] Json(string name) =>
        System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new Line(name), LinesJson.Default.Line);

    /// <summary>A store a test can take down or make reject a write, recording every flush attempt.</summary>
    internal sealed class FakeStore
    {
        private int failedFlushes;

        public volatile bool Down;

        public bool RejectFirst { get; init; }

        public ConcurrentQueue<string[]> Attempts { get; } = new();

        public ConcurrentQueue<string> Written { get; } = new();

        public int FailedFlushes => Volatile.Read(ref this.failedFlushes);

        public void Write(string[] rows)
        {
            this.Attempts.Enqueue(rows);

            if (this.Down)
            {
                Interlocked.Increment(ref this.failedFlushes);
                throw new StoreUnavailableException();
            }

            if (this.RejectFirst && rows.Contains("rejected"))
            {
                throw new InvalidOperationException("the store rejected a row");
            }

            foreach (var row in rows)
            {
                this.Written.Enqueue(row);
            }
        }
    }

    /// <summary>Buffers each line in the batch scope and writes the batch's lines in one flush.</summary>
    internal sealed class LineProjection(FakeStore store) : IProjection<Line>, IBatchProjection
    {
        public ValueTask HandleAsync(Line @event, EventMeta meta, CancellationToken ct)
        {
            meta.Batch!.GetOrAdd(this, () => new List<string>()).Add(@event.Name);
            return ValueTask.CompletedTask;
        }

        public ValueTask FlushAsync(ProjectionBatch batch, CancellationToken ct)
        {
            if (batch.TryGet<List<string>>(this, out var rows))
            {
                store.Write([.. rows]);
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class StoreUnavailableException() : DontIgnoreException("the store is unavailable");
}

internal sealed record Line(string Name);

[JsonSerializable(typeof(Line))]
internal sealed partial class LinesJson : JsonSerializerContext;
