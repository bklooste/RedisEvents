using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Orange.Lib.Streams.Consumer;
using Orange.Lib.Streams.Producer;
using Orange.Lib.Streams.Wire;

namespace Orange.Lib.Streams.Testing;

/// <summary>
/// A test harness for driving message handlers directly, without a consumer host or Redis.
/// </summary>
/// <remarks>
/// R-20: the id counter is now advanced with <see cref="Interlocked"/>, so two tests running in
/// parallel (xUnit's default across collections) cannot be handed the same id, and there are
/// overloads for the two handler interfaces the library actually dispatches to —
/// <see cref="IMessageHandler"/> and <see cref="IBatchHandler"/> — rather than only the delegate
/// shape, which <see cref="IMessageHandler"/> does not match (its parameter is <c>in StreamMsg</c>).
/// </remarks>
public static class StreamTestHarness
{
    private static long nextId;

    /// <summary>
    /// Sends a single message to a handler and awaits its completion.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Serializes the message using the provided <see cref="JsonTypeInfo{T}"/>, wraps it in a
    /// <see cref="StreamMsg"/>, and invokes the handler. The StreamMsg is populated with sensible
    /// defaults:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>Type is set to <c>typeof(T).FullName</c>.</description></item>
    ///   <item><description>Id is a synthetic stream id with incrementing millisecond component.</description></item>
    ///   <item><description>Partition is 0.</description></item>
    ///   <item><description>PartitionKey is empty.</description></item>
    ///   <item><description>CorrelationId is empty.</description></item>
    ///   <item><description>TraceParent is null.</description></item>
    ///   <item><description>Headers are empty.</description></item>
    /// </list>
    /// <para>
    /// Example:
    /// <code>
    /// var handler = new MyHandler();
    /// await StreamTestHarness.SendAsync(
    ///     handler.HandleAsync,
    ///     new BetPlaced { BetId = "bet-123", Amount = 100 },
    ///     SerializerContext.Default.BetPlaced,
    ///     ct);
    /// </code>
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="handler">The handler function; typically <c>myHandler.HandleAsync</c>.</param>
    /// <param name="message">The message to send.</param>
    /// <param name="typeInfo">The <see cref="JsonTypeInfo{T}"/> for serializing the message.</param>
    /// <param name="options">Optional partition, key, correlation id and headers to stamp on the message.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="handler"/> or <paramref name="typeInfo"/> is null.</exception>
    public static async ValueTask SendAsync<T>(
        Func<StreamMsg, CancellationToken, ValueTask> handler,
        T message,
        JsonTypeInfo<T> typeInfo,
        PublishOptions options = default,
        CancellationToken ct = default)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(typeInfo);

        await handler(Build(message, typeInfo, options), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Sends a single message to an <see cref="IMessageHandler"/> and awaits its completion.
    /// </summary>
    /// <remarks>
    /// The delegate overload cannot take an <see cref="IMessageHandler"/> directly: its
    /// <c>HandleAsync</c> takes <c>in StreamMsg</c>, which no <c>Func&lt;StreamMsg, …&gt;</c>
    /// matches. This is the overload to use when the handler under test implements the interface
    /// the consumer host dispatches to.
    /// </remarks>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="handler">The handler under test.</param>
    /// <param name="message">The message to send.</param>
    /// <param name="typeInfo">The <see cref="JsonTypeInfo{T}"/> for serializing the message.</param>
    /// <param name="options">Optional partition, key, correlation id and headers to stamp on the message.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="handler"/> or <paramref name="typeInfo"/> is null.</exception>
    public static async ValueTask SendAsync<T>(
        IMessageHandler handler,
        T message,
        JsonTypeInfo<T> typeInfo,
        PublishOptions options = default,
        CancellationToken ct = default)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(typeInfo);

        var msg = Build(message, typeInfo, options);
        await handler.HandleAsync(in msg, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Sends a batch of messages to an <see cref="IBatchHandler"/> as one call, the way the
    /// consumer host would.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every message is serialized with the same <paramref name="typeInfo"/> and stamped with the
    /// same options, and the ids increase across the batch — so a handler that asserts on batch
    /// ordering, or records the last id it saw as a position, sees what it would see live.
    /// </para>
    /// <para>
    /// The array backing the batch is <b>not</b> pooled here, so a handler that wrongly retains the
    /// memory past the await will not be caught by this harness. That contract is enforced by the
    /// real pipeline; this overload exists to exercise the handler's logic, not its discipline.
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="handler">The handler under test.</param>
    /// <param name="messages">The messages in the batch, in order. An empty batch still calls the handler.</param>
    /// <param name="typeInfo">The <see cref="JsonTypeInfo{T}"/> for serializing the messages.</param>
    /// <param name="options">Optional partition, key, correlation id and headers, applied to every message.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="ArgumentNullException"><paramref name="handler"/>, <paramref name="messages"/> or <paramref name="typeInfo"/> is null.</exception>
    public static async ValueTask SendBatchAsync<T>(
        IBatchHandler handler,
        IReadOnlyList<T> messages,
        JsonTypeInfo<T> typeInfo,
        PublishOptions options = default,
        CancellationToken ct = default)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentNullException.ThrowIfNull(typeInfo);

        var batch = new StreamMsg[messages.Count];
        for (var i = 0; i < messages.Count; i++)
        {
            batch[i] = Build(messages[i], typeInfo, options);
        }

        await handler.HandleAsync(batch, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds the <see cref="StreamMsg"/> the harness would hand a handler, without invoking one.
    /// Useful for a test that drives the handler itself.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="message">The message to wrap.</param>
    /// <param name="typeInfo">The <see cref="JsonTypeInfo{T}"/> for serializing the message.</param>
    /// <param name="options">Optional partition, key, correlation id and headers to stamp on the message.</param>
    /// <exception cref="ArgumentNullException"><paramref name="typeInfo"/> is null.</exception>
    public static StreamMsg Build<T>(T message, JsonTypeInfo<T> typeInfo, PublishOptions options = default)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(typeInfo);

        var json = JsonSerializer.SerializeToUtf8Bytes(message, typeInfo);

        return new StreamMsg(
            Body: new ReadOnlyMemory<byte>(json),
            Type: typeof(T).FullName ?? "unknown",
            Id: NextStreamId(),
            Partition: options.Partition ?? 0,
            PartitionKey: string.Empty,
            CorrelationId: options.CorrelationId ?? string.Empty,
            TraceParent: null,
            Headers: HeaderBlock.Pack(options.Headers));
    }

    /// <summary>
    /// Generates a synthetic stream id for test messages. Unique and increasing across every
    /// caller in the process.
    /// </summary>
    private static StreamId NextStreamId() => new(Interlocked.Increment(ref nextId), 0);
}
