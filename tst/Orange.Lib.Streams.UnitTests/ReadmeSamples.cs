using System.Text;
using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orange.Lib.Streams.Admin;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.UnitTests;

/// <summary>
/// Every code sample printed in <c>library/src/Orange.Lib.Streams/README.md</c>, compiled.
///
/// The README is the page a handler author reads before writing a handler, so a sample in it that
/// no longer compiles is worse than no sample at all: it teaches an API that does not exist. Keeping
/// the samples here means the build is what enforces that, not a reviewer's memory.
///
/// <b>If you change a sample here, change the matching block in the README, and vice versa.</b>
/// The region names below match the README headings they appear under.
/// </summary>
public static class ReadmeSamples
{
    // ------------------------------------------------------------------ Quickstart

    #region quickstart-registration

    /// <summary>README — Quickstart. The zero-config registration; three lines, no appsettings.</summary>
    public static void Register(IHostApplicationBuilder builder)
    {
        builder.AddStream<OrderHandler>("orders");
    }

    /// <summary>README — Quickstart. A batch handler: one call per batch, not per message.</summary>
    public sealed class OrderHandler : IBatchHandler
    {
        public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            for (var i = 0; i < batch.Length; i++)
            {
                var msg = batch.Span[i];
                var order = JsonSerializer.Deserialize<Order>(msg.Body.Span);
                _ = order;
            }

            return ValueTask.CompletedTask;
        }
    }

    #endregion

    #region quickstart-delegate

    /// <summary>README — Quickstart. A delegate handler, for a consumer too small to justify a class.</summary>
    public static void RegisterDelegate(IHostApplicationBuilder builder)
    {
        builder.AddStream("orders", (batch, ct) =>
        {
            for (var i = 0; i < batch.Length; i++)
            {
                Handle(batch.Span[i]);
            }

            return ValueTask.CompletedTask;
        });
    }

    #endregion

    #region quickstart-message-handler

    /// <summary>README — Quickstart. A per-message handler, when the batch shape does not matter.</summary>
    public sealed class OneOrderHandler : IMessageHandler
    {
        public ValueTask HandleAsync(in StreamMsg msg, CancellationToken ct)
        {
            Handle(msg);
            return ValueTask.CompletedTask;
        }
    }

    #endregion

    // ------------------------------------------------------------- The error contract

    #region error-malformed

    /// <summary>README — Error contract, row 1. Malformed: let it throw, log and skip is correct.</summary>
    public sealed class MalformedRow : IBatchHandler
    {
        public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            for (var i = 0; i < batch.Length; i++)
            {
                // JsonException on a body that will never parse. Nothing is caught: the batch is
                // logged at Error, the position advances, and the partition keeps moving.
                var order = JsonSerializer.Deserialize<Order>(batch.Span[i].Body.Span)
                    ?? throw new JsonException("null order");
                _ = order;
            }

            return ValueTask.CompletedTask;
        }
    }

    #endregion

    #region error-transient

    /// <summary>README — Error contract, row 2. Transient: the handler retries; the library never does.</summary>
    public sealed class TransientRow(IPricingClient pricing) : IBatchHandler
    {
        public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            for (var i = 0; i < batch.Length; i++)
            {
                var msg = batch.Span[i];

                for (var attempt = 0; ; attempt++)
                {
                    try
                    {
                        await pricing.PriceAsync(msg.Body, ct).ConfigureAwait(false);
                        break;
                    }
                    catch (HttpRequestException) when (attempt < 3)
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(100 * (1 << attempt)), ct).ConfigureAwait(false);
                    }
                }
            }
        }
    }

    #endregion

    #region error-dont-ignore

    /// <summary>
    /// README — Error contract, row 3. A failure that must not be skipped. Deriving from
    /// <see cref="DontIgnoreException"/> is the whole opt-in: the partition blocks and retries this
    /// batch until it succeeds, and the position never advances past it.
    /// </summary>
    public sealed class LedgerUnavailableException(string message, Exception inner)
        : DontIgnoreException(message, inner);

    /// <summary>README — Error contract, row 3. Throwing it.</summary>
    public sealed class MustNotSkipRow(ILedger ledger) : IBatchHandler
    {
        public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            for (var i = 0; i < batch.Length; i++)
            {
                var msg = batch.Span[i];

                try
                {
                    await ledger.PostAsync(msg.Body, ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // Skipping this loses money, so block instead. Lag will grow while the ledger is
                    // down — that is the deliberate trade, and it is only correct because the
                    // alternative is a settlement that never happens.
                    throw new LedgerUnavailableException(
                        $"Ledger rejected entry {msg.Id.Format()}; blocking rather than skipping.", ex);
                }
            }
        }
    }

    #endregion

    #region error-park

    /// <summary>
    /// README — Error contract, row 4. Important but not urgent: park it somewhere durable and
    /// return normally, so the partition keeps moving and nothing is lost.
    /// </summary>
    public sealed class ParkingRow(IParkingLot parked) : IBatchHandler
    {
        public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            for (var i = 0; i < batch.Length; i++)
            {
                var msg = batch.Span[i];

                try
                {
                    Handle(msg);
                }
                catch (InvalidOperationException ex)
                {
                    // Copy: the batch array and the body both go back to their pools the moment this
                    // handler returns, so anything kept has to own its storage.
                    await parked.ParkAsync(msg.Copy(), ex, ct).ConfigureAwait(false);
                }
            }
        }
    }

    #endregion

    // ------------------------------------------------------------------ Publishing

    #region publishing

    /// <summary>README — Publishing. AddStreamPublisher registers a real publisher, keyed on topic.</summary>
    public static void RegisterPublisher(IHostApplicationBuilder builder)
    {
        builder.AddStreamPublisher("orders");
    }

    /// <summary>
    /// README — Publishing. The unkeyed resolution works with exactly one publisher registered; with
    /// two or more it throws rather than guessing, so name the topic.
    /// </summary>
    public sealed class OrderGateway([FromKeyedServices("orders")] IStreamPublisher publisher)
    {
        public ValueTask<StreamId> PlaceAsync(string orderId, ReadOnlyMemory<byte> body, CancellationToken ct)
            => publisher.PublishAsync(orderId, body, type: "OrderPlaced", ct: ct);
    }

    #endregion

    // ------------------------------------------------------------------ The outbox

    #region outbox

    /// <summary>
    /// README — The outbox. State write and publish in one MULTI/EXEC, so a crash cannot leave the
    /// state and the stream disagreeing.
    /// </summary>
    public static async Task<StreamId?> PlaceOrderAsync(
        IDatabase db,
        string orderId,
        ReadOnlyMemory<byte> body,
        TopicOptions topicOptions,
        CancellationToken ct)
    {
        var stateKey = Outbox.StateKey("orders", $"order:{orderId}");

        var id = await Outbox.WriteAndPublishAsync(
            db,
            tran => tran.HashSetAsync(stateKey, [new HashEntry("status", "placed")]),   // queue only — no await in here
            topic: "orders",
            partitionKey: orderId,
            body: body,
            type: "OrderPlaced",
            stateKeys: [stateKey],
            topicOptions: topicOptions,
            ct: ct).ConfigureAwait(false);

        return id;
    }

    /// <summary>
    /// README — The outbox. Optimistic concurrency: conditions map straight onto WATCH, so "publish
    /// only if the version is still N" needs no Lua.
    /// </summary>
    public static async Task<StreamId?> PlaceOrderIfUnchangedAsync(
        IDatabase db,
        string orderId,
        ReadOnlyMemory<byte> body,
        int expected,
        TopicOptions topicOptions,
        CancellationToken ct)
    {
        var stateKey = Outbox.StateKey("orders", $"order:{orderId}");

        var id = await Outbox.WriteAndPublishAsync(
            db,
            tran => tran.HashSetAsync(stateKey, [new HashEntry("version", expected + 1)]),
            topic: "orders", partitionKey: orderId, body: body, type: "OrderPlaced",
            conditions: [Condition.HashEqual(stateKey, "version", expected)],
            stateKeys: [stateKey],
            topicOptions: topicOptions,
            ct: ct).ConfigureAwait(false);

        if (id is null)
        {
            // A condition failed: someone else moved the version on and NOTHING was applied.
            // null is not a general failure signal — every other failure throws.
        }

        return id;
    }

    #endregion

    // ----------------------------------------------------------------- Idempotency

    #region idempotency

    /// <summary>
    /// README — Idempotency. <see cref="Idempotency.TryBeginAsync"/> is a one-round-trip claim
    /// (SET NX PX) for handlers with no better place to keep one.
    /// </summary>
    public sealed class DedupedHandler(IDatabase db, ILedger ledger) : IBatchHandler
    {
        private static readonly TimeSpan Window = TimeSpan.FromHours(24);

        public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            for (var i = 0; i < batch.Length; i++)
            {
                var msg = batch.Span[i];

                // false = seen before, within the window. Size the TTL above the replay window: a claim
                // that expires before a replay reaches it dedupes nothing.
                if (!await Idempotency.TryBeginAsync(db, "orders", msg.Id.Format(), Window, ct).ConfigureAwait(false))
                {
                    continue;
                }

                await ledger.PostAsync(msg.Body, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// README — Idempotency. Delivery is at-least-once, so a handler must tolerate seeing the same
    /// message twice. The stream entry id is a natural dedupe key: it is assigned by Redis, is unique
    /// within a partition, and is the same on every redelivery of the same entry. A store you own —
    /// rather than the TTL'd claim above — is what a handler needs when the claim must be made in
    /// the same write as the work.
    /// </summary>
    public sealed class IdempotentHandler(ISeenSet seen, ILedger ledger) : IBatchHandler
    {
        public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        {
            for (var i = 0; i < batch.Length; i++)
            {
                var msg = batch.Span[i];
                var key = $"{msg.Partition}:{msg.Id.Format()}";

                if (!await seen.TryClaimAsync(key, ct).ConfigureAwait(false))
                {
                    continue;
                }

                await ledger.PostAsync(msg.Body, ct).ConfigureAwait(false);
            }
        }
    }

    #endregion

    // ---------------------------------------------------------------- Sharp edges

    #region retain-copy

    /// <summary>
    /// README — Sharp edges. The batch is a window onto a pooled array and a borrowed read buffer.
    /// Keeping messages past the await needs <see cref="StreamBatchExtensions.Copy(ReadOnlyMemory{StreamMsg})"/>.
    /// </summary>
    public static StreamMsg[] KeepForLater(ReadOnlyMemory<StreamMsg> batch) => batch.Copy();

    #endregion

    #region configure-consumer

    /// <summary>
    /// README — Configuration. Registration with explicit options, when config is not the right place
    /// for them (a test, or a value derived at startup).
    /// </summary>
    public static void RegisterConfigured(IHostApplicationBuilder builder)
    {
        builder.AddStream<OrderHandler>(consumer => consumer with
        {
            Topic = "orders",
            BatchSize = 250,
            Persist = PersistMode.SyncBatch,
            OnError = ErrorPolicy.BestEffort,
        });
    }

    #endregion

    // ------------------------------------------------------------------- Runbooks

    #region runbook-replay

    /// <summary>
    /// README — Runbook, replay from a date. Preview first: it says how much would be reprocessed and
    /// whether the date has already been trimmed away. Reset only with the consumer scaled to zero.
    /// </summary>
    public static async Task<string> PreviewReplayAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        DateTimeOffset from,
        CancellationToken ct)
    {
        var preview = await StreamAdmin.PreviewResetAsync(redis, topic, consumer, from, ct: ct)
            .ConfigureAwait(false);

        return preview.Describe();
    }

    /// <summary>README — Runbook, replay from a date. The reset itself, once the preview looks right.</summary>
    public static async Task<int> ReplayAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        DateTimeOffset from,
        CancellationToken ct)
    {
        var written = await StreamAdmin.ResetPositionAsync(redis, topic, consumer, from, ct: ct)
            .ConfigureAwait(false);

        return written.Count;
    }

    #endregion

    #region runbook-ownership

    /// <summary>README — Runbook, who owns which partition. Unowned partitions are consumed by nobody.</summary>
    public static async Task<IReadOnlyList<int>> UnownedPartitionsAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        CancellationToken ct)
    {
        var map = await StreamAdmin.GetOwnershipAsync(redis, topic, consumer, ct: ct).ConfigureAwait(false);
        return map.Unowned;
    }

    #endregion

    // ------------------------------------------------------------ sample scaffolding

    /// <summary>The message the samples deserialise. Nothing in the library knows this type exists.</summary>
    public sealed record Order(string Id, decimal Stake);

    /// <summary>Stand-in for whatever the handler actually does with a message.</summary>
    private static void Handle(in StreamMsg msg)
    {
        if (msg.Body.Length == 0)
        {
            throw new InvalidOperationException($"empty body at {msg.Id.Format()}");
        }
    }

    /// <summary>A downstream that can fail transiently.</summary>
    public interface IPricingClient
    {
        ValueTask PriceAsync(ReadOnlyMemory<byte> body, CancellationToken ct);
    }

    /// <summary>A downstream where skipping loses money.</summary>
    public interface ILedger
    {
        ValueTask PostAsync(ReadOnlyMemory<byte> body, CancellationToken ct);
    }

    /// <summary>Durable somewhere-else for messages worth keeping but not worth blocking on.</summary>
    public interface IParkingLot
    {
        ValueTask ParkAsync(StreamMsg msg, Exception cause, CancellationToken ct);
    }

    /// <summary>A dedupe store — Redis SET NX, a unique index, whatever the service already has.</summary>
    public interface ISeenSet
    {
        ValueTask<bool> TryClaimAsync(string key, CancellationToken ct);
    }
}

/// <summary>
/// Exercises the README samples so they are more than a compile check: the ones with observable
/// behaviour are actually run.
/// </summary>
public class ReadmeSampleTests
{
    private static StreamMsg Msg(string body, int partition = 0, long ms = 1, int seq = 0)
        => new(
            Encoding.UTF8.GetBytes(body),
            typeof(ReadmeSamples.Order).FullName!,
            new StreamId(ms, seq),
            partition,
            PartitionKey: string.Empty,
            CorrelationId: string.Empty,
            TraceParent: null,
            HeaderBlock.Empty);

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ErrorContract_MalformedSample_Throws_SoTheBatchIsLoggedAndSkipped()
    {
        var handler = new ReadmeSamples.MalformedRow();
        ReadOnlyMemory<StreamMsg> batch = new[] { Msg("not json at all") };

        Assert.ThrowsAny<Exception>(() => handler.HandleAsync(batch, CancellationToken.None).AsTask().GetAwaiter().GetResult());
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ErrorContract_MustNotSkipSample_ThrowsADontIgnoreException()
    {
        var handler = new ReadmeSamples.MustNotSkipRow(new ThrowingLedger());
        ReadOnlyMemory<StreamMsg> batch = new[] { Msg("""{"Id":"o1","Stake":2}""") };

        var thrown = await Assert.ThrowsAsync<ReadmeSamples.LedgerUnavailableException>(
            async () => await handler.HandleAsync(batch, CancellationToken.None));

        thrown.Should().BeAssignableTo<DontIgnoreException>(
            "the partition only blocks for DontIgnoreException subclasses");
        thrown.InnerException.Should().BeOfType<TimeoutException>("the cause must survive for the operator");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task ErrorContract_ParkingSample_SwallowsTheFailureAndKeepsACopy()
    {
        var lot = new RecordingParkingLot();
        var handler = new ReadmeSamples.ParkingRow(lot);
        ReadOnlyMemory<StreamMsg> batch = new[] { Msg(string.Empty), Msg("fine") };

        await handler.HandleAsync(batch, CancellationToken.None);

        lot.Parked.Should().ContainSingle("only the empty-bodied message fails");
        lot.Parked[0].Cause.Should().BeOfType<InvalidOperationException>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Idempotency_Sample_ProcessesARedeliveredEntryOnlyOnce()
    {
        var seen = new InMemorySeenSet();
        var ledger = new CountingLedger();
        var handler = new ReadmeSamples.IdempotentHandler(seen, ledger);

        ReadOnlyMemory<StreamMsg> batch = new[] { Msg("""{"Id":"o1","Stake":2}""", ms: 7, seq: 0) };

        await handler.HandleAsync(batch, CancellationToken.None);
        await handler.HandleAsync(batch, CancellationToken.None);

        ledger.Posts.Should().Be(1, "the stream entry id is the same on every redelivery");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void SharpEdge_CopySample_OwnsItsStorage()
    {
        var buffer = Encoding.UTF8.GetBytes("payload");
        ReadOnlyMemory<StreamMsg> batch = new[] { Msg("payload") with { Body = buffer } };

        var kept = ReadmeSamples.KeepForLater(batch);

        // Overwrite the "read buffer" the library would have recycled. The copy is unaffected.
        buffer.AsSpan().Fill((byte)'!');

        Encoding.UTF8.GetString(kept[0].Body.Span).Should().Be("payload");
    }

    private sealed class ThrowingLedger : ReadmeSamples.ILedger
    {
        public ValueTask PostAsync(ReadOnlyMemory<byte> body, CancellationToken ct)
            => throw new TimeoutException("ledger is down");
    }

    private sealed class CountingLedger : ReadmeSamples.ILedger
    {
        public int Posts { get; private set; }

        public ValueTask PostAsync(ReadOnlyMemory<byte> body, CancellationToken ct)
        {
            this.Posts++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class InMemorySeenSet : ReadmeSamples.ISeenSet
    {
        private readonly HashSet<string> keys = new(StringComparer.Ordinal);

        public ValueTask<bool> TryClaimAsync(string key, CancellationToken ct)
            => ValueTask.FromResult(this.keys.Add(key));
    }

    private sealed class RecordingParkingLot : ReadmeSamples.IParkingLot
    {
        public List<(StreamMsg Msg, Exception Cause)> Parked { get; } = [];

        public ValueTask ParkAsync(StreamMsg msg, Exception cause, CancellationToken ct)
        {
            this.Parked.Add((msg, cause));
            return ValueTask.CompletedTask;
        }
    }
}
