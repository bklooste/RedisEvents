using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;

using FluentAssertions;

using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;

namespace Orange.Lib.Streams.UnitTests;

/// <summary>
/// R-21 (P3 test gap 7). <see cref="TypedPublishExtensions"/> had no tests at all, including the
/// two-pass offset arithmetic in <c>PublishBatchAsync&lt;T&gt;</c>: every body is serialised into
/// one pooled buffer, and the buffer can grow — and therefore move — while that is happening, so
/// the offsets are recorded first and sliced only once every body is in place. A one-pass version
/// hands the publisher slices of an array that is no longer the buffer.
/// </summary>
/// <remarks>
/// The publisher is a recording stand-in that copies each body <em>during</em> the call, exactly as
/// a real one must: the bodies alias the pooled buffer and are only valid until the returned task
/// completes.
/// </remarks>
public class TypedPublishTests
{
    /// <summary>Serialise-and-publish: the body is the JSON, and the type string defaults to the CLR name.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task PublishAsync_serialises_the_body_and_defaults_the_type_to_the_clr_name()
    {
        var publisher = new RecordingPublisher();

        var id = await publisher.PublishAsync(
            "customer-1",
            new TypedOrder(7, "seven"),
            TypedPublishJson.Default.TypedOrder,
            options: new PublishOptions(CorrelationId: "corr-1", Partition: 3));

        id.Should().Be(RecordingPublisher.AssignedId);

        var call = publisher.Singles.Should().ContainSingle().Subject;
        call.PartitionKey.Should().Be("customer-1");
        call.Type.Should().Be(typeof(TypedOrder).FullName);
        call.Options.CorrelationId.Should().Be("corr-1");
        call.Options.Partition.Should().Be(3);
        Decode(call.Body).Should().Be(new TypedOrder(7, "seven"));
    }

    /// <summary>An explicit type string wins over the CLR name — that is the migration escape hatch.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task PublishAsync_uses_an_explicit_type_string_when_one_is_given()
    {
        var publisher = new RecordingPublisher();

        await publisher.PublishAsync("k", new TypedOrder(1, "a"), TypedPublishJson.Default.TypedOrder, type: "OrderPlaced");

        publisher.Singles[0].Type.Should().Be("OrderPlaced");
    }

    /// <summary>Nothing to publish issues no command at all.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task PublishBatchAsync_with_no_messages_publishes_nothing()
    {
        var publisher = new RecordingPublisher();

        await publisher.PublishBatchAsync("k", System.Array.Empty<TypedOrder>(), TypedPublishJson.Default.TypedOrder);

        publisher.Batches.Should().BeEmpty();
        publisher.Singles.Should().BeEmpty();
    }

    /// <summary>Every argument that cannot be defaulted is rejected before anything is serialised.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Null_arguments_are_rejected()
    {
        var publisher = new RecordingPublisher();

        var nullPublisher = async () => await ((IStreamPublisher)null!).PublishAsync("k", new TypedOrder(1, "a"), TypedPublishJson.Default.TypedOrder);
        var nullInfo = async () => await publisher.PublishAsync("k", new TypedOrder(1, "a"), null!);
        var nullMessages = async () => await publisher.PublishBatchAsync<TypedOrder>("k", null!, TypedPublishJson.Default.TypedOrder);

        await nullPublisher.Should().ThrowAsync<ArgumentNullException>();
        await nullInfo.Should().ThrowAsync<ArgumentNullException>();
        await nullMessages.Should().ThrowAsync<ArgumentNullException>();
    }

    /// <summary>A batch keeps order, and every body is its own message rather than a shared prefix.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task PublishBatchAsync_keeps_order_and_gives_every_message_its_own_slice()
    {
        var publisher = new RecordingPublisher();
        var messages = new[] { new TypedOrder(1, "one"), new TypedOrder(2, "two"), new TypedOrder(3, "three") };

        await publisher.PublishBatchAsync("customer-1", messages, TypedPublishJson.Default.TypedOrder, type: "Order");

        var batch = publisher.Batches.Should().ContainSingle().Subject;
        batch.PartitionKey.Should().Be("customer-1");
        batch.Type.Should().Be("Order");
        batch.Bodies.Select(Decode).Should().Equal(messages);
    }

    /// <summary>
    /// The finding this file exists for. The bodies are sized so the pooled buffer must grow — and
    /// therefore reallocate — part way through the batch, which is exactly when offsets recorded as
    /// live slices stop pointing at the buffer. Recording integer offsets and slicing once, at the
    /// end, is what survives it.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task PublishBatchAsync_survives_the_buffer_growing_and_moving_mid_batch()
    {
        var publisher = new RecordingPublisher();

        // The pooled writer starts at 1 KiB. A batch that crosses that several times over — with the
        // growth landing in the middle rather than at a boundary — reallocates repeatedly.
        var messages = new TypedOrder[24];
        for (var i = 0; i < messages.Length; i++)
        {
            messages[i] = new TypedOrder(i, new string((char)('a' + (i % 26)), 37 + (i * 53)));
        }

        await publisher.PublishBatchAsync("k", messages, TypedPublishJson.Default.TypedOrder);

        var batch = publisher.Batches.Should().ContainSingle().Subject;
        batch.Bodies.Should().HaveCount(messages.Length);
        batch.Bodies.Select(Decode).Should().Equal(messages, "a slice taken before the buffer moved would carry another message's bytes, or none");
    }

    /// <summary>
    /// The documented mechanism, not just the outcome: one pooled buffer, sliced — no intermediate
    /// <c>byte[]</c> per message. Asserted because it is the reason the offset arithmetic exists;
    /// a version that allocated per message would pass the test above and lose the property.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task PublishBatchAsync_slices_one_contiguous_buffer()
    {
        var publisher = new RecordingPublisher();
        var messages = new[] { new TypedOrder(1, "one"), new TypedOrder(2, "two"), new TypedOrder(3, "three") };

        await publisher.PublishBatchAsync("k", messages, TypedPublishJson.Default.TypedOrder);

        var segments = publisher.Batches[0].Segments;
        segments.Should().HaveCount(3);
        segments.Select(s => s.Array).Distinct().Should().ContainSingle("every body is a window onto the one pooled buffer");

        for (var i = 1; i < segments.Count; i++)
        {
            segments[i].Offset.Should().Be(
                segments[i - 1].Offset + segments[i - 1].Count,
                "the slices are adjacent, in order, with no gap and no overlap");
        }
    }

    /// <summary>
    /// The pooled writer is reused by the next call on the same thread. A stale written-count or a
    /// writer that was not reset shows up as the second batch carrying the first batch's bytes.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_pooled_buffer_is_reusable_by_the_next_publish_on_the_same_thread()
    {
        var publisher = new RecordingPublisher();

        await publisher.PublishBatchAsync("k", new[] { new TypedOrder(1, "first"), new TypedOrder(2, "second") }, TypedPublishJson.Default.TypedOrder);
        await publisher.PublishBatchAsync("k", new[] { new TypedOrder(3, "third") }, TypedPublishJson.Default.TypedOrder);
        await publisher.PublishAsync("k", new TypedOrder(4, "fourth"), TypedPublishJson.Default.TypedOrder);

        publisher.Batches[1].Bodies.Select(Decode).Should().Equal(new TypedOrder(3, "third"));
        Decode(publisher.Singles[0].Body).Should().Be(new TypedOrder(4, "fourth"));
    }

    /// <summary>
    /// A publish that publishes again from inside the first call — the same thread, the same
    /// <c>[ThreadStatic]</c> pair — must not have its buffer written out from under it. The rent
    /// detaches the pooled pair for the duration, so the inner call gets a fresh one.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_reentrant_publish_on_the_same_thread_does_not_corrupt_the_outer_body()
    {
        var publisher = new RecordingPublisher();
        var reentered = false;

        publisher.BeforeReturn = async () =>
        {
            if (reentered)
            {
                return;
            }

            reentered = true;

            // Long enough to force the inner call to grow a buffer of its own.
            await publisher.PublishAsync("inner", new TypedOrder(99, new string('z', 4096)), TypedPublishJson.Default.TypedOrder);
        };

        await publisher.PublishBatchAsync(
            "outer",
            new[] { new TypedOrder(1, "one"), new TypedOrder(2, "two") },
            TypedPublishJson.Default.TypedOrder);

        reentered.Should().BeTrue();
        publisher.Batches[0].Bodies.Select(Decode).Should().Equal(new TypedOrder(1, "one"), new TypedOrder(2, "two"));
        Decode(publisher.Singles[0].Body).Id.Should().Be(99);
    }

    /// <summary>
    /// A body over the pooling ceiling is dropped rather than pinned, and the next publish on the
    /// thread still works — the drop path disposes the writer, so a reuse of it would throw.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_oversized_body_is_not_pooled_and_the_next_publish_still_works()
    {
        var publisher = new RecordingPublisher();

        // Comfortably past the 512 KiB pooling ceiling.
        await publisher.PublishAsync("k", new TypedOrder(1, new string('x', 700_000)), TypedPublishJson.Default.TypedOrder);
        await publisher.PublishAsync("k", new TypedOrder(2, "small"), TypedPublishJson.Default.TypedOrder);

        Decode(publisher.Singles[0].Body).Name.Should().HaveLength(700_000);
        Decode(publisher.Singles[1].Body).Should().Be(new TypedOrder(2, "small"));
    }

    private static TypedOrder Decode(byte[] body)
        => JsonSerializer.Deserialize(body, TypedPublishJson.Default.TypedOrder)!;

    /// <summary>One recorded single publish.</summary>
    internal sealed record SinglePublish(string PartitionKey, byte[] Body, string Type, PublishOptions Options);

    /// <summary>One recorded batch publish, with the raw segments the extension handed over.</summary>
    internal sealed record BatchPublish(
        string PartitionKey,
        IReadOnlyList<byte[]> Bodies,
        IReadOnlyList<ArraySegment<byte>> Segments,
        string Type,
        PublishOptions Options);

    /// <summary>
    /// Records what it was published, copying every body inside the call — the interface says a body
    /// is only valid until the returned task completes, and these all alias a pooled buffer.
    /// </summary>
    private sealed class RecordingPublisher : IStreamPublisher
    {
        internal static readonly StreamId AssignedId = new(1_700_000_000_000, 3);

        private readonly List<SinglePublish> singles = [];
        private readonly List<BatchPublish> batches = [];

        /// <summary>Runs inside a publish, before it returns — used for the reentrancy case.</summary>
        internal Func<Task>? BeforeReturn { get; set; }

        internal IReadOnlyList<SinglePublish> Singles => this.singles;

        internal IReadOnlyList<BatchPublish> Batches => this.batches;

        public async ValueTask<StreamId> PublishAsync(
            string partitionKey,
            ReadOnlyMemory<byte> body,
            string type,
            PublishOptions options = default,
            CancellationToken ct = default)
        {
            // The hook runs before the copy on purpose: a reentrant publish that stole this call's
            // pooled buffer would corrupt the bytes that are copied afterwards.
            if (this.BeforeReturn is { } hook)
            {
                await hook().ConfigureAwait(false);
            }

            this.singles.Add(new SinglePublish(partitionKey, body.ToArray(), type, options));

            return AssignedId;
        }

        public async ValueTask PublishBatchAsync(
            string partitionKey,
            ReadOnlyMemory<ReadOnlyMemory<byte>> bodies,
            string type,
            PublishOptions options = default,
            CancellationToken ct = default)
        {
            if (this.BeforeReturn is { } hook)
            {
                await hook().ConfigureAwait(false);
            }

            var copies = new List<byte[]>(bodies.Length);
            var segments = new List<ArraySegment<byte>>(bodies.Length);

            for (var i = 0; i < bodies.Length; i++)
            {
                var body = bodies.Span[i];
                copies.Add(body.ToArray());

                MemoryMarshal.TryGetArray(body, out var segment).Should().BeTrue("a pooled-buffer slice is always array-backed");
                segments.Add(segment);
            }

            this.batches.Add(new BatchPublish(partitionKey, copies, segments, type, options));
        }
    }
}

/// <summary>The payload the typed publish tests serialise.</summary>
public sealed record TypedOrder(int Id, string Name);

/// <summary>
/// Source-generated metadata, because <see cref="TypedPublishExtensions"/> deliberately takes only
/// <c>JsonTypeInfo&lt;T&gt;</c> — there is no reflection-based overload to test against.
/// </summary>
[JsonSerializable(typeof(TypedOrder))]
internal sealed partial class TypedPublishJson : JsonSerializerContext;
