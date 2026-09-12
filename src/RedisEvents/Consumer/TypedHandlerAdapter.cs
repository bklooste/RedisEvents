using System.Runtime.ExceptionServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using RedisEvents.Wire;

namespace RedisEvents.Consumer;

/// <summary>
/// Wraps a typed handler (<see cref="IMessageHandler{T}"/> / <see cref="IBatchHandler{T}"/>) into the
/// corresponding <b>non-typed</b> interface, so registration/dispatch — <c>StreamConsumerHost.ResolveHandler</c>
/// and <see cref="HandlerAdapter"/> — never need to know typed handlers exist.
/// </summary>
internal sealed class TypedMessageHandlerAdapter<T>(IMessageHandler<T> inner, JsonTypeInfo<T> typeInfo)
    : IMessageHandler
{
    public ValueTask HandleAsync(in StreamMsg msg, CancellationToken ct)
    {
        var typed = new StreamMsg<T>(JsonSerializer.Deserialize(msg.BodySpan, typeInfo), msg);
        return inner.HandleAsync(in typed, ct);
    }
}

/// <summary>
/// Wraps an <see cref="IBatchHandler{T}"/> into a plain <see cref="IBatchHandler"/>, deserialising
/// every body in the batch first.
/// </summary>
/// <remarks>
/// A small batch is deserialised in a plain loop; a batch bigger than <see cref="ChunkSize"/> is split
/// into chunks of that size and deserialised in parallel — below the threshold, the thread-hop and
/// <see cref="Parallel.For(int, int, Action{int})"/> overhead costs more than it saves, above it,
/// spreading JSON parsing across cores is worth it.
/// </remarks>
internal sealed class TypedBatchHandlerAdapter<T>(IBatchHandler<T> inner, JsonTypeInfo<T> typeInfo)
    : IBatchHandler
{
    /// <summary>The parallel/sequential cutover point, and the chunk size once parallel.</summary>
    internal const int ChunkSize = 10;

    public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        var typed = batch.Length <= ChunkSize
            ? DeserializeSequential(batch, typeInfo)
            : DeserializeChunkedParallel(batch, typeInfo);

        await inner.HandleAsync(typed, ct).ConfigureAwait(false);
    }

    private static StreamMsg<T>[] DeserializeSequential(ReadOnlyMemory<StreamMsg> batch, JsonTypeInfo<T> typeInfo)
    {
        var span = batch.Span;
        var result = new StreamMsg<T>[span.Length];
        for (var i = 0; i < span.Length; i++)
        {
            result[i] = new StreamMsg<T>(JsonSerializer.Deserialize(span[i].BodySpan, typeInfo), span[i]);
        }

        return result;
    }

    private static StreamMsg<T>[] DeserializeChunkedParallel(ReadOnlyMemory<StreamMsg> batch, JsonTypeInfo<T> typeInfo)
    {
        var result = new StreamMsg<T>[batch.Length];
        var chunkCount = (batch.Length + ChunkSize - 1) / ChunkSize;

        try
        {
            Parallel.For(0, chunkCount, chunkIndex =>
            {
                // Read fresh inside the delegate: a Span cannot be captured across the closure
                // boundary, but it can be read synchronously within one Parallel.For iteration like
                // this.
                var span = batch.Span;
                var start = chunkIndex * ChunkSize;
                var end = Math.Min(start + ChunkSize, span.Length);

                for (var i = start; i < end; i++)
                {
                    result[i] = new StreamMsg<T>(JsonSerializer.Deserialize(span[i].BodySpan, typeInfo), span[i]);
                }
            });
        }
        catch (AggregateException ex)
        {
            // Parallel.For always wraps, even a single failure — rethrow the original exception so a
            // large batch fails the same way a small (sequential) one does, indistinguishable to
            // ErrorPolicy from any other handler-thrown exception.
            ExceptionDispatchInfo.Capture(ex.Flatten().InnerExceptions[0]).Throw();
            throw; // unreachable; satisfies flow analysis.
        }

        return result;
    }
}
