using System.Runtime.ExceptionServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.Logging;
using RedisEvents.Wire;

namespace RedisEvents.Consumer;

/// <summary>
/// What the consumer host tells a typed adapter about where it sits, so a body that decodes to
/// <see langword="null"/> is reported with its topic and consumer and not handed on in silence.
/// </summary>
internal interface ITypedHandlerAdapter
{
    /// <summary>Names the stream this adapter decodes for and gives it a logger.</summary>
    /// <param name="log">The consumer's logger.</param>
    /// <param name="topic">The topic consumed.</param>
    /// <param name="consumer">The consumer name.</param>
    void Attach(ILogger log, string topic, string consumer);
}

/// <summary>The one Warning both typed adapters log for bodies that decoded to <see langword="null"/>.</summary>
internal static class NullDecodeLog
{
    internal static void Warn(ILogger? log, string topic, string consumer, Type messageType, int nullCount, int count, in StreamMsg first, in StreamMsg last)
    {
        log?.LogWarning(
            "Streams: {NullCount} of {Count} message bodies decoded to null {MessageClrType} for topic={Topic} partition={Partition} consumer={Consumer} ids={FirstId}..{LastId} type={MessageType}. " +
            "The handler is still called and sees Value == null for them; a deserializer that returns null (JSON literal null, or a decoder that swallows its own failure) hides a bad message.",
            nullCount, count, messageType.Name, topic, first.Partition, consumer, first.Id.Format(), last.Id.Format(), first.Type);
    }
}

/// <summary>
/// Wraps a typed handler (<see cref="IMessageHandler{T}"/> / <see cref="IBatchHandler{T}"/>) into the
/// corresponding <b>non-typed</b> interface, so registration/dispatch — <c>StreamConsumerHost.ResolveHandler</c>
/// and <see cref="HandlerAdapter"/> — never need to know typed handlers exist.
/// </summary>
/// <remarks>
/// Generic only in the deserialiser: <c>deserialize</c> is a plain
/// <c>Func&lt;ReadOnlyMemory&lt;byte&gt;, T?&gt;</c> rather than a <see cref="JsonTypeInfo{T}"/>, so any
/// serialisation format can plug into the same registration path
/// (<c>StreamsBuilderExtensions.AddStream{THandler,TMessage}(topic, Func&lt;...&gt;)</c>) with no
/// changes here. The <see cref="JsonTypeInfo{T}"/> constructor is the JSON-specific convenience kept
/// for the existing typed-publish-and-consume overloads and their tests.
/// </remarks>
internal sealed class TypedMessageHandlerAdapter<T> : IMessageHandler, ITypedHandlerAdapter
{
    private ILogger? log;
    private string topic = string.Empty;
    private string consumer = string.Empty;

    private readonly IMessageHandler<T> inner;
    private readonly Func<ReadOnlyMemory<byte>, T?> deserialize;

    internal TypedMessageHandlerAdapter(IMessageHandler<T> inner, Func<ReadOnlyMemory<byte>, T?> deserialize)
    {
        this.inner = inner;
        this.deserialize = deserialize;
    }

    internal TypedMessageHandlerAdapter(IMessageHandler<T> inner, JsonTypeInfo<T> typeInfo)
        : this(inner, body => JsonSerializer.Deserialize(body.Span, typeInfo))
    {
    }

    public ValueTask HandleAsync(in StreamMsg msg, CancellationToken ct)
    {
        var typed = new StreamMsg<T>(this.deserialize(msg.Body), msg);
        if (typed.Value is null)
        {
            NullDecodeLog.Warn(this.log, this.topic, this.consumer, typeof(T), 1, 1, in msg, in msg);
        }

        return this.inner.HandleAsync(in typed, ct);
    }

    /// <inheritdoc />
    public void Attach(ILogger log, string topic, string consumer)
    {
        this.log = log;
        this.topic = topic;
        this.consumer = consumer;
    }
}

/// <summary>
/// Wraps an <see cref="IBatchHandler{T}"/> into a plain <see cref="IBatchHandler"/>, deserialising
/// every body in the batch first.
/// </summary>
/// <remarks>
/// <para>
/// Generic only in the deserialiser — see the remarks on <see cref="TypedMessageHandlerAdapter{T}"/>;
/// the same <c>Func&lt;ReadOnlyMemory&lt;byte&gt;, T?&gt;</c> vs. <see cref="JsonTypeInfo{T}"/>
/// constructor split applies here.
/// </para>
/// <para>
/// A small batch is deserialised in a plain loop; a batch bigger than <see cref="ChunkSize"/> is split
/// into chunks of that size and deserialised in parallel — below the threshold, the thread-hop and
/// <see cref="Parallel.For(int, int, Action{int})"/> overhead costs more than it saves, above it,
/// spreading deserialisation across cores is worth it.
/// </para>
/// </remarks>
internal sealed class TypedBatchHandlerAdapter<T> : IBatchHandler, ITypedHandlerAdapter
{
    /// <summary>The parallel/sequential cutover point, and the chunk size once parallel.</summary>
    internal const int ChunkSize = 10;

    private ILogger? log;
    private string topic = string.Empty;
    private string consumer = string.Empty;

    private readonly IBatchHandler<T> inner;
    private readonly Func<ReadOnlyMemory<byte>, T?> deserialize;

    internal TypedBatchHandlerAdapter(IBatchHandler<T> inner, Func<ReadOnlyMemory<byte>, T?> deserialize)
    {
        this.inner = inner;
        this.deserialize = deserialize;
    }

    internal TypedBatchHandlerAdapter(IBatchHandler<T> inner, JsonTypeInfo<T> typeInfo)
        : this(inner, body => JsonSerializer.Deserialize(body.Span, typeInfo))
    {
    }

    public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        // failed[i] marks an entry whose body threw while decoding; it was logged per entry and is dropped
        // below, so one malformed entry no longer takes the rest of the batch down with it.
        var failed = new bool[batch.Length];
        var typed = batch.Length <= ChunkSize
            ? this.DeserializeSequential(batch, failed)
            : this.DeserializeChunkedParallel(batch, failed);

        var failures = 0;
        foreach (var f in failed)
        {
            failures += f ? 1 : 0;
        }

        if (failures > 0)
        {
            if (failures == typed.Length)
            {
                return;
            }

            var kept = new StreamMsg<T>[typed.Length - failures];
            var j = 0;
            for (var i = 0; i < typed.Length; i++)
            {
                if (!failed[i])
                {
                    kept[j++] = typed[i];
                }
            }

            batch = DropFailed(batch, failed, kept.Length);
            typed = kept;
        }

        this.WarnOnNulls(batch, typed);

        await this.inner.HandleAsync(typed, ct).ConfigureAwait(false);
    }

    private static StreamMsg[] DropFailed(ReadOnlyMemory<StreamMsg> batch, bool[] failed, int keptCount)
    {
        var span = batch.Span;
        var kept = new StreamMsg[keptCount];
        var j = 0;
        for (var i = 0; i < span.Length; i++)
        {
            if (!failed[i])
            {
                kept[j++] = span[i];
            }
        }

        return kept;
    }

    /// <summary>
    /// Decodes one entry. An ordinary failure is that entry's fault, not the batch's: it is logged at Error with
    /// the entry's id and type and the slot is marked failed, so the rest of the batch still reaches the handler.
    /// A <see cref="Errors.DontIgnoreException"/> from a decoder still propagates and blocks the partition.
    /// </summary>
    private StreamMsg<T> DecodeOne(in StreamMsg msg, int index, bool[] failed)
    {
        try
        {
            return new StreamMsg<T>(this.deserialize(msg.Body), msg);
        }
        catch (Exception ex) when (ex is not Errors.DontIgnoreException)
        {
            failed[index] = true;
            this.log?.LogError(
                ex,
                "Streams: skipping malformed message for topic={Topic} partition={Partition} consumer={Consumer} id={Id} type={MessageType}: its body failed to decode to {MessageClrType} with {ExceptionType}. " +
                "The rest of the batch is still handled.",
                this.topic, msg.Partition, this.consumer, msg.Id.Format(), msg.Type, typeof(T).Name, ex.GetType().Name);
            return default;
        }
    }

    /// <inheritdoc />
    public void Attach(ILogger log, string topic, string consumer)
    {
        this.log = log;
        this.topic = topic;
        this.consumer = consumer;
    }

    // One line per batch, never one per message: a topic of null bodies must not flood the log.
    private void WarnOnNulls(ReadOnlyMemory<StreamMsg> batch, StreamMsg<T>[] typed)
    {
        var nulls = 0;
        var first = -1;
        var last = -1;
        for (var i = 0; i < typed.Length; i++)
        {
            if (typed[i].Value is null)
            {
                nulls++;
                first = first < 0 ? i : first;
                last = i;
            }
        }

        if (nulls > 0)
        {
            var span = batch.Span;
            NullDecodeLog.Warn(this.log, this.topic, this.consumer, typeof(T), nulls, typed.Length, in span[first], in span[last]);
        }
    }

    private StreamMsg<T>[] DeserializeSequential(ReadOnlyMemory<StreamMsg> batch, bool[] failed)
    {
        var span = batch.Span;
        var result = new StreamMsg<T>[span.Length];
        for (var i = 0; i < span.Length; i++)
        {
            result[i] = this.DecodeOne(in span[i], i, failed);
        }

        return result;
    }

    private StreamMsg<T>[] DeserializeChunkedParallel(ReadOnlyMemory<StreamMsg> batch, bool[] failed)
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
                    result[i] = this.DecodeOne(in span[i], i, failed);
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
