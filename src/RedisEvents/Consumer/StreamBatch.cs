using System.Buffers;
using RedisEvents.Wire;

namespace RedisEvents.Consumer;

/// <summary>
/// One batch of decoded messages on its way from the reader loop to the handler, carried in an
/// array rented from <see cref="ArrayPool{T}"/>.
/// </summary>
/// <remarks>
/// <para>
/// This type is the reason the consumer path allocates nothing per message. A read of 250 entries
/// costs one rental — which is usually a recycled array and therefore no allocation at all —
/// instead of 250 objects for the garbage collector to walk. Everything else on the hot path is
/// slicing over buffers somebody else already owns.
/// </para>
/// <para>
/// <b>The lifetime contract.</b> The array belongs to the pool, not to the batch. Exactly one
/// <see cref="Return"/> must happen for every <see cref="Rent"/>, and it happens in the processor
/// loop's <c>finally</c>. Returning twice hands the same array to two future rentals at once, which
/// corrupts two unrelated batches and shows up somewhere else entirely — so a double return is
/// treated as a bug and thrown on rather than tolerated.
/// </para>
/// <para>
/// <b>The handler contract.</b> A handler gets <see cref="AsMemory"/>, valid for the duration of the
/// call and no longer. Retaining it past the await is the one genuinely dangerous mistake available
/// here: the array goes back to the pool, a later read fills it with different messages, and the
/// retained memory silently starts reading someone else's data. Handlers that need to keep messages
/// call <see cref="StreamBatchExtensions.Copy(ReadOnlyMemory{StreamMsg})"/>, which is the documented
/// escape hatch.
/// </para>
/// <para>
/// <b>How that mistake is made loud.</b> In <c>DEBUG</c> builds every rental is tracked by reference
/// and the array is poison-filled before it goes back to the pool, so a retaining handler reads
/// <see cref="PoisonType"/> rather than plausible-looking torn data, and a double return throws with
/// a message naming the fault. In release builds the array is returned cleared, so the same mistake
/// surfaces as a null <see cref="StreamMsg.Type"/> rather than as silent corruption.
/// </para>
/// </remarks>
/// <param name="Items">The pooled backing array. Only the first <paramref name="Count"/> slots are meaningful.</param>
/// <param name="Count">How many messages the batch actually holds.</param>
/// <param name="Last">
/// The id of the last entry read, including entries the filter dropped. Positions advance to this
/// after the handler succeeds, which is why a batch filtered down to nothing still checkpoints.
/// </param>
internal readonly record struct StreamBatch(StreamMsg[] Items, int Count, StreamId Last)
{
    /// <summary>
    /// The <see cref="StreamMsg.Type"/> a poisoned slot carries in <c>DEBUG</c> builds. Tests assert
    /// on it to prove a handler retained memory past its await.
    /// </summary>
    internal const string PoisonType = "!! RedisEvents: this batch array was returned to the pool !!";

#if DEBUG
    /// <summary>
    /// Written over every slot before the array goes back to the pool. Deliberately not
    /// <see langword="default"/>: a nulled-out message reads as "empty", whereas this reads as
    /// "you are looking at recycled memory" the moment anyone prints it.
    /// </summary>
    private static readonly StreamMsg Poison = new(
        ReadOnlyMemory<byte>.Empty,
        PoisonType,
        new StreamId(-1, -1),
        -1,
        PoisonType,
        PoisonType,
        PoisonType,
        HeaderBlock.Empty);
#endif

    /// <summary>
    /// Scopes a substituted pool to one async flow, so a test that installs a counting pool cannot
    /// hand its arrays to unrelated code running in parallel.
    /// </summary>
    private static readonly AsyncLocal<ArrayPool<StreamMsg>?> Substitute = new();

    /// <summary>
    /// The pool every batch array is rented from and returned to. Always
    /// <see cref="ArrayPool{T}.Shared"/> in production; the setter exists solely so a unit test can
    /// substitute a counting pool and prove that every rental is returned exactly once (P1-24),
    /// which cannot be observed from outside otherwise.
    /// </summary>
    /// <remarks>
    /// The substitution is <see cref="AsyncLocal{T}"/> rather than a plain static, and that is not
    /// fussiness: a process-global swap is visible to every other test running in parallel, so a
    /// batch rented from the shim can be returned to <see cref="ArrayPool{T}.Shared"/> after the
    /// shim is uninstalled — which throws "the buffer is not associated with this pool" in a test
    /// that never touched the seam. Scoping it to the installing flow (and everything that flow
    /// starts, <c>Task.Run</c> included) keeps the substitution where it was meant to be. Production
    /// never sets it, so the read is a null check against an uncontended slot.
    /// </remarks>
    internal static ArrayPool<StreamMsg> Pool
    {
        get => Substitute.Value ?? ArrayPool<StreamMsg>.Shared;
        set => Substitute.Value = ReferenceEquals(value, ArrayPool<StreamMsg>.Shared) ? null : value;
    }

    /// <summary>Whether the batch survived filtering with nothing in it.</summary>
    internal bool IsEmpty => Count == 0;

    /// <summary>
    /// Rents a backing array for at least <paramref name="minimumCapacity"/> messages. The array is
    /// usually larger than asked for; only <see cref="Count"/> ever matters.
    /// </summary>
    /// <remarks>
    /// Every rental must be paired with exactly one <see cref="Return"/>. Rent through here rather
    /// than through <see cref="ArrayPool{T}"/> directly — in <c>DEBUG</c> this is what registers the
    /// array so a double or foreign return can be caught.
    /// </remarks>
    internal static StreamMsg[] Rent(int minimumCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(minimumCapacity);

        var items = Pool.Rent(minimumCapacity);
#if DEBUG
        RentTracker.Track(items);
#endif
        return items;
    }

    /// <summary>
    /// The batch as memory sliced to <see cref="Count"/> — what the handler is handed.
    /// </summary>
    /// <remarks>
    /// Valid only for the duration of the handler call. See the type remarks; if you need it
    /// afterwards, <see cref="StreamBatchExtensions.Copy(ReadOnlyMemory{StreamMsg})"/> it.
    /// </remarks>
    internal ReadOnlyMemory<StreamMsg> AsMemory() => Items.AsMemory(0, Count);

    /// <summary>The batch as a span sliced to <see cref="Count"/>, for internal loops.</summary>
    internal ReadOnlySpan<StreamMsg> AsSpan() => Items.AsSpan(0, Count);

    /// <summary>
    /// Returns the backing array to the pool. Call this exactly once per <see cref="Rent"/>, from a
    /// <c>finally</c>, after the handler has finished with the batch.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The batch holds no rented array (a <see langword="default"/> <see cref="StreamBatch"/>), or —
    /// in <c>DEBUG</c> builds — the array has already been returned or did not come from
    /// <see cref="Rent"/>. All three are lifetime bugs, and letting them through would corrupt an
    /// unrelated batch later.
    /// </exception>
    internal void Return()
    {
        var items = Items;

        if (items is null)
        {
            throw new InvalidOperationException(
                "StreamBatch.Return() was called on a default StreamBatch, which holds no rented array. " +
                "A batch must be built from StreamBatch.Rent() and returned exactly once.");
        }

#if DEBUG
        RentTracker.Release(items);
        items.AsSpan().Fill(Poison);

        // Poison, not zeroes: the point is that a handler which retained the memory reads something
        // obviously wrong instead of something plausible. Clearing here would undo that.
        Pool.Return(items, clearArray: false);
#else
        // Clearing drops the references held in each StreamMsg — bodies alias the Redis read buffer,
        // and a pooled array holding the last batch alive would pin those buffers indefinitely.
        Pool.Return(items, clearArray: true);
#endif
    }

#if DEBUG
    /// <summary>
    /// Tracks outstanding rentals by reference so a double return, or a return of an array that
    /// never came from <see cref="Rent"/>, throws at the point of the mistake.
    /// </summary>
    /// <remarks>
    /// Debug-only and deliberately so: the set is small (partitions × channel capacity) but it does
    /// take a lock per batch, which has no business on a release hot path.
    /// </remarks>
    private static class RentTracker
    {
        private static readonly Lock Gate = new();
        private static readonly HashSet<object> Outstanding = new(ReferenceEqualityComparer.Instance);

        internal static void Track(StreamMsg[] items)
        {
            lock (Gate)
            {
                if (!Outstanding.Add(items))
                {
                    throw new InvalidOperationException(
                        "ArrayPool handed out a StreamBatch array that is already rented. " +
                        "That means an earlier batch was returned twice.");
                }
            }
        }

        internal static void Release(StreamMsg[] items)
        {
            lock (Gate)
            {
                if (!Outstanding.Remove(items))
                {
                    throw new InvalidOperationException(
                        "StreamBatch.Return() was called on an array that is not currently rented — " +
                        "either the batch has already been returned, or its array did not come from " +
                        "StreamBatch.Rent(). Returning a pooled array twice corrupts a later, unrelated batch.");
                }
            }
        }
    }
#endif
}

/// <summary>
/// The supported way for a handler to keep messages beyond the handler call.
/// </summary>
public static class StreamBatchExtensions
{
    /// <summary>
    /// Deep-copies a batch so it can safely outlive the handler call.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A batch handed to a handler is a window onto two pieces of borrowed memory: the pooled array
    /// of messages, and the read buffer each <see cref="StreamMsg.Body"/> aliases. Both are recycled
    /// as soon as the handler returns, so keeping the batch — queueing it, closing over it, storing
    /// it for a later flush — needs a copy of both. This makes that copy.
    /// </para>
    /// <para>
    /// It allocates: one message array, plus one byte array holding every body and header block back
    /// to back. That is the price of retention and is why it is opt-in rather than the default —
    /// the ordinary handler, which reads what it needs and returns, pays none of it.
    /// </para>
    /// </remarks>
    /// <param name="batch">The batch as handed to the handler.</param>
    /// <returns>Messages owning their own storage, safe to keep indefinitely.</returns>
    public static StreamMsg[] Copy(this ReadOnlyMemory<StreamMsg> batch)
    {
        var source = batch.Span;
        if (source.Length == 0)
        {
            return [];
        }

        var bytes = 0;
        for (var i = 0; i < source.Length; i++)
        {
            bytes += source[i].Body.Length + source[i].Headers.Packed.Length;
        }

        // One backing array for the whole batch's payload: copying is a bulk memcpy per message and
        // the collector sees one object rather than two per message.
        byte[] storage = bytes == 0 ? [] : new byte[bytes];
        var copies = new StreamMsg[source.Length];
        var at = 0;

        for (var i = 0; i < source.Length; i++)
        {
            copies[i] = CopyInto(source[i], storage, ref at);
        }

        return copies;
    }

    /// <summary>
    /// Deep-copies a single message so it can safely outlive the handler call.
    /// </summary>
    /// <remarks>
    /// The same contract as <see cref="Copy(ReadOnlyMemory{StreamMsg})"/>, for a handler that keeps
    /// one message out of a batch rather than the batch. Allocates one byte array for the body and
    /// packed headers; the strings are immutable and are shared rather than copied.
    /// </remarks>
    /// <param name="msg">The message to copy.</param>
    /// <returns>A message owning its own storage.</returns>
    public static StreamMsg Copy(this in StreamMsg msg)
    {
        var bytes = msg.Body.Length + msg.Headers.Packed.Length;
        byte[] storage = bytes == 0 ? [] : new byte[bytes];
        var at = 0;

        return CopyInto(msg, storage, ref at);
    }

    /// <summary>
    /// Copies one message's body and headers into <paramref name="storage"/> at
    /// <paramref name="at"/>, advancing it, and returns the message rebound to that storage.
    /// </summary>
    private static StreamMsg CopyInto(in StreamMsg msg, byte[] storage, ref int at)
    {
        var body = Take(storage, ref at, msg.Body.Span);
        var headers = msg.Headers.IsEmpty
            ? HeaderBlock.Empty
            : HeaderBlock.FromPacked(Take(storage, ref at, msg.Headers.Packed.Span));

        return msg with { Body = body, Headers = headers };
    }

    /// <summary>Copies <paramref name="source"/> into the next free slice of <paramref name="storage"/>.</summary>
    private static ReadOnlyMemory<byte> Take(byte[] storage, ref int at, ReadOnlySpan<byte> source)
    {
        if (source.Length == 0)
        {
            return ReadOnlyMemory<byte>.Empty;
        }

        source.CopyTo(storage.AsSpan(at, source.Length));
        var slice = storage.AsMemory(at, source.Length);
        at += source.Length;

        return slice;
    }
}
