using System.Diagnostics.CodeAnalysis;

using Orange.Lib.EventHubs.Consumer.BatchConsumer;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Wire;

namespace Orange.Lib.Streams.Compat;

/// <summary>
/// The one place the compatibility shim actually costs something: it turns the native
/// <c>ReadOnlyMemory&lt;StreamMsg&gt;</c> batch into the <see cref="EventMsg"/><c>[]</c> a Kafka-era
/// <see cref="IBatchConsumer"/> expects, and hands it over.
/// </summary>
/// <remarks>
/// <para>
/// It is registered as the handler class for a shim consumer, so the pipeline resolves it once at
/// startup — like any other <see cref="IBatchHandler"/> — and the per-batch path is one conversion
/// and one call.
/// </para>
/// <para>
/// <b>The conversion, and what it allocates.</b> Per batch: one <see cref="EventMsg"/> array, one
/// byte array holding every body back to back, one <see cref="EventMsg"/> per message, one string
/// per message for the formatted stream id, and one dictionary per message that carries headers.
/// The native path allocates none of this — a batch of 250 messages is one pooled-array rental —
/// which is exactly why the shim is temporary. Bodies are copied rather than aliased so that the
/// Kafka contract still holds: a handler may keep a message after <see cref="IBatchConsumer.Consume"/>
/// returns, where a native handler would have to call <c>Copy()</c>. That copy is one array and one
/// <c>memcpy</c> per message, not one allocation per message, and it buys away the worst mistake
/// available to a migrating handler — retaining a window onto a recycled read buffer, which corrupts
/// data silently and somewhere else entirely.
/// </para>
/// </remarks>
/// <typeparam name="T">The service's batch consumer.</typeparam>
internal sealed class CompatBatchHandler<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T> : IBatchHandler
    where T : class, IBatchConsumer
{
    private readonly T consumer;

    /// <summary>Wraps the service's consumer, which DI has already constructed.</summary>
    /// <param name="consumer">The service's batch consumer.</param>
    public CompatBatchHandler(T consumer)
    {
        ArgumentNullException.ThrowIfNull(consumer);
        this.consumer = consumer;
    }

    /// <inheritdoc />
    public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
        => new(this.consumer.Consume(ToEventMsgs(batch.Span), ct));

    /// <summary>
    /// Maps a native batch onto the Kafka-era shape. See the type remarks for the cost, and
    /// <see cref="EventMsg"/> for the per-field differences.
    /// </summary>
    /// <param name="batch">The batch as handed to the handler; valid only for this call.</param>
    /// <returns>Messages owning their own bodies, safe for the consumer to retain.</returns>
    internal static EventMsg[] ToEventMsgs(ReadOnlySpan<StreamMsg> batch)
    {
        if (batch.Length == 0)
        {
            // Empty batches do reach a handler — a batch filtered down to nothing still checkpoints —
            // and Kafka called Consume with an empty array in that case too, so the shim does the same
            // rather than inventing a "skipped" semantic no handler was written against.
            return [];
        }

        // One backing array for every body in the batch: the collector sees one object instead of one
        // per message, and each copy is a bulk memcpy.
        var bytes = 0;
        for (var i = 0; i < batch.Length; i++)
        {
            bytes += batch[i].Body.Length;
        }

        byte[] storage = bytes == 0 ? [] : new byte[bytes];
        var msgs = new EventMsg[batch.Length];
        var at = 0;

        for (var i = 0; i < batch.Length; i++)
        {
            ref readonly var msg = ref batch[i];

            ReadOnlyMemory<byte> body = ReadOnlyMemory<byte>.Empty;
            var length = msg.Body.Length;
            if (length > 0)
            {
                msg.Body.Span.CopyTo(storage.AsSpan(at, length));
                body = storage.AsMemory(at, length);
                at += length;
            }

            msgs[i] = new EventMsg(
                body,

                // Deliberately null, not empty: handlers branch on `msg.Type == null` to pick up
                // untyped payloads, and the Kafka path left it null for exactly the same entries.
                // The parameter is non-nullable only because the Kafka record declared it so.
                msg.Type.Length == 0 ? null! : msg.Type,
                msg.CorrelationId,
                msg.Id.Format(),
                msg.PartitionKey,
                msg.EnqueuedTime,
                msg.Headers.IsEmpty ? null : msg.Headers.ToDictionary());
        }

        return msgs;
    }
}
