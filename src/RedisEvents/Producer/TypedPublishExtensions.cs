using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using RedisEvents.Wire;

namespace RedisEvents.Producer;

/// <summary>
/// Typed convenience over <see cref="IStreamPublisher"/>: serialise and publish in one call.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why extensions rather than interface members.</b> Typed publishing is a convenience, not a
/// capability — it composes entirely from the two byte-oriented methods. Putting it here keeps the
/// interface at two methods, so a test double or a decorator implements two things and gets the
/// typed surface for free.
/// </para>
/// <para>
/// <b><see cref="JsonTypeInfo{T}"/> only.</b> There is deliberately no reflection-based overload.
/// A <c>JsonSerializerOptions</c> overload would have to carry
/// <c>RequiresUnreferencedCode</c>, and that attribute is viral: every caller would inherit the
/// warning, and the library would stop being honestly AOT-clean. Callers pass the context-generated
/// <c>MyJsonContext.Default.MyType</c> instead, which is a field access.
/// </para>
/// <para>
/// <b>One buffer, no intermediate array.</b> The body is serialised straight into a pooled
/// <see cref="ArrayBufferWriter{T}"/> and its <see cref="ArrayBufferWriter{T}.WrittenMemory"/> is
/// handed to <c>XADD</c> as-is — no <c>byte[]</c> from <c>JsonSerializer.SerializeToUtf8Bytes</c>,
/// and no copy into an intermediate array on the way to the wire. The buffer is returned
/// to the pool only after the publish completes, because StackExchange.Redis writes it to the
/// socket asynchronously and recycling it early would corrupt the entry.
/// </para>
/// </remarks>
public static class TypedPublishExtensions
{
    /// <summary>Buffers larger than this are dropped rather than pooled, so one huge message does not pin memory.</summary>
    private const int MaxPooledBufferBytes = 512 * 1024;

    private const int InitialBufferBytes = 1024;

    [ThreadStatic]
    private static ArrayBufferWriter<byte>? pooledBuffer;

    [ThreadStatic]
    private static Utf8JsonWriter? pooledWriter;

    /// <summary>
    /// Serialises <paramref name="message"/> as UTF-8 JSON and publishes it.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="publisher">The publisher.</param>
    /// <param name="partitionKey">The routing key; empty round-robins.</param>
    /// <param name="message">The message.</param>
    /// <param name="typeInfo">Source-generated metadata for <typeparamref name="T"/>.</param>
    /// <param name="type">
    /// The message type string consumers filter on. Defaults to <c>typeof(T).FullName</c>, which is
    /// resolved once per closed generic and never through reflection metadata that a trimmer would
    /// have to preserve.
    /// </param>
    /// <param name="options">Correlation, headers and an optional explicit partition.</param>
    /// <param name="ct">Cancellation, observed before the command is issued.</param>
    /// <returns>The id Redis assigned to the entry.</returns>
    public static async ValueTask<StreamId> PublishAsync<T>(
        this IStreamPublisher publisher,
        string partitionKey,
        T message,
        JsonTypeInfo<T> typeInfo,
        string? type = null,
        PublishOptions options = default,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(typeInfo);

        var (buffer, writer) = Rent();
        try
        {
            JsonSerializer.Serialize(writer, message, typeInfo);
            writer.Flush();

            return await publisher
                .PublishAsync(partitionKey, buffer.WrittenMemory, type ?? TypeName<T>.Value, options, ct)
                .ConfigureAwait(false);
        }
        finally
        {
            Return(buffer, writer);
        }
    }

    /// <summary>
    /// Serialises several messages sharing a partition key into <b>one</b> pooled buffer and
    /// publishes them as a single pipelined batch.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="publisher">The publisher.</param>
    /// <param name="partitionKey">The routing key shared by every message in the batch.</param>
    /// <param name="messages">The messages, in the order they should appear on the stream.</param>
    /// <param name="typeInfo">Source-generated metadata for <typeparamref name="T"/>.</param>
    /// <param name="type">The message type string; defaults to <c>typeof(T).FullName</c>.</param>
    /// <param name="options">Correlation, headers and an optional explicit partition, applied to every entry.</param>
    /// <param name="ct">Cancellation, observed before the commands are issued.</param>
    /// <returns>A task that completes when every entry in the batch has been accepted.</returns>
    public static async ValueTask PublishBatchAsync<T>(
        this IStreamPublisher publisher,
        string partitionKey,
        IReadOnlyList<T> messages,
        JsonTypeInfo<T> typeInfo,
        string? type = null,
        PublishOptions options = default,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentNullException.ThrowIfNull(typeInfo);

        if (messages.Count == 0)
        {
            return;
        }

        var (buffer, writer) = Rent();
        try
        {
            // Two passes, because the buffer may grow and move while it is being written: record the
            // offsets first, slice WrittenMemory only once every body is in place.
            var offsets = new int[messages.Count + 1];
            for (var i = 0; i < messages.Count; i++)
            {
                offsets[i] = buffer.WrittenCount;
                JsonSerializer.Serialize(writer, messages[i], typeInfo);
                writer.Flush();
                writer.Reset();
            }

            offsets[messages.Count] = buffer.WrittenCount;

            var written = buffer.WrittenMemory;
            var bodies = new ReadOnlyMemory<byte>[messages.Count];
            for (var i = 0; i < bodies.Length; i++)
            {
                bodies[i] = written[offsets[i]..offsets[i + 1]];
            }

            await publisher
                .PublishBatchAsync(partitionKey, bodies, type ?? TypeName<T>.Value, options, ct)
                .ConfigureAwait(false);
        }
        finally
        {
            Return(buffer, writer);
        }
    }

    /// <summary>
    /// Serialises <paramref name="message"/> as UTF-8 JSON and enqueues it for the next batch flush.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="publisher">The buffered publisher.</param>
    /// <param name="partitionKey">The routing key; empty round-robins.</param>
    /// <param name="message">The message.</param>
    /// <param name="typeInfo">Source-generated metadata for <typeparamref name="T"/>.</param>
    /// <param name="type">The message type string; defaults to <c>typeof(T).FullName</c>.</param>
    /// <param name="options">Correlation, headers and an optional explicit partition.</param>
    /// <param name="ct">Cancellation, observed only while waiting for queue space.</param>
    /// <returns>A task that completes once the message is in the buffer — not once it is in Redis.</returns>
    public static ValueTask EnqueueAsync<T>(
        this IStreamBufferedPublisher publisher,
        string partitionKey,
        T message,
        JsonTypeInfo<T> typeInfo,
        string? type = null,
        PublishOptions options = default,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(typeInfo);

        var (buffer, writer) = Rent();
        try
        {
            JsonSerializer.Serialize(writer, message, typeInfo);
            writer.Flush();

            // EnqueueAsync copies the body into its own buffer before this call returns (see its
            // contract), so unlike the direct PublishAsync<T> above there is nothing to await here
            // before this method's own buffer goes back to the pool.
            return publisher.EnqueueAsync(partitionKey, buffer.WrittenMemory, type ?? TypeName<T>.Value, options, ct);
        }
        finally
        {
            Return(buffer, writer);
        }
    }

    /// <summary>
    /// Takes the calling thread's writer pair, detaching it for the duration so a reentrant publish
    /// on the same thread gets a fresh one instead of corrupting this one's buffer.
    /// </summary>
    private static (ArrayBufferWriter<byte> Buffer, Utf8JsonWriter Writer) Rent()
    {
        var buffer = pooledBuffer;
        var writer = pooledWriter;

        if (buffer is null || writer is null)
        {
            buffer = new ArrayBufferWriter<byte>(InitialBufferBytes);
            writer = new Utf8JsonWriter(buffer);
        }
        else
        {
            pooledBuffer = null;
            pooledWriter = null;
        }

        return (buffer, writer);
    }

    /// <summary>Resets the pair and offers it back to the thread, dropping oversized buffers.</summary>
    private static void Return(ArrayBufferWriter<byte> buffer, Utf8JsonWriter writer)
    {
        if (buffer.Capacity > MaxPooledBufferBytes)
        {
            writer.Dispose();
            return;
        }

        buffer.ResetWrittenCount();
        writer.Reset(buffer);

        pooledBuffer = buffer;
        pooledWriter = writer;
    }

    /// <summary>
    /// <c>typeof(T).FullName</c>, resolved once per closed generic. A generic static holds it, so
    /// the default type string costs a static field read per publish rather than a type lookup.
    /// </summary>
    private static class TypeName<T>
    {
        internal static readonly string Value = typeof(T).FullName ?? typeof(T).Name;
    }
}
