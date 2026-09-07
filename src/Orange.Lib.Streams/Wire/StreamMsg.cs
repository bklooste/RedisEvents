namespace Orange.Lib.Streams.Wire;

/// <summary>
/// One decoded message read from a partition stream.
/// </summary>
/// <remarks>
/// <para>
/// A struct, and deliberately so: batches are handed to handlers as a <c>ReadOnlyMemory&lt;StreamMsg&gt;</c>
/// over a pooled array, so a batch of 250 messages costs one rental rather than 250 allocations.
/// </para>
/// <para>
/// <see cref="Body"/> aliases the buffer the Redis client handed us. It is valid for the duration of
/// the handler call and no longer — a handler that needs the bytes afterwards must copy them, the
/// same contract that applies to the batch array itself.
/// </para>
/// <para>
/// There is no envelope: the body is exactly the bytes the publisher passed in. Deserialising is
/// the caller's job, which is what keeps the library free of any dependency on the message model.
/// </para>
/// <para>
/// <b>Why some things are fields and everything else is a header.</b> A header is promoted to a
/// field iff the library itself reads it and it has a dedicated entry field: <see cref="Type"/> for
/// filtering, <see cref="Id"/> and <see cref="Partition"/> for positions, <see cref="TraceParent"/>
/// for activity restoration, <see cref="CorrelationId"/> for the log scope, and
/// <see cref="PartitionKey"/> for routing. Everything else is a custom header in
/// <see cref="Headers"/>. The split is not taste — it is exactly the set the library acts on.
/// </para>
/// <para>
/// <b>Equality is inconsistent by nature.</b> <see cref="Body"/> is a
/// <see cref="ReadOnlyMemory{T}"/> and so compares by buffer segment identity, while
/// <see cref="Headers"/> compares by content. Two messages carrying the same bytes in different
/// buffers are unequal, and a message read from a pooled batch may compare equal to a later one
/// that happens to alias the same segment. Equality is therefore not a meaningful operation on a
/// live batch message; it exists only because the type is a record struct.
/// </para>
/// </remarks>
/// <param name="Body">The raw message body, aliasing the read buffer.</param>
/// <param name="Type">The message type string, by convention <c>typeof(T).FullName</c>. Consumers filter on it.</param>
/// <param name="Id">The Redis stream entry id this message was read at.</param>
/// <param name="Partition">The partition this message was read from.</param>
/// <param name="PartitionKey">The key the message was routed on; empty for round-robin publishes.</param>
/// <param name="CorrelationId">The publisher's correlation id, or empty when none was set.</param>
/// <param name="TraceParent">
/// The W3C <c>traceparent</c> the publisher stamped on the entry, or <see langword="null"/>.
/// The processor rebuilds the ambient <see cref="System.Diagnostics.Activity"/> from this, so
/// trace context follows the message rather than the thread.
/// </param>
/// <param name="Headers">A lazy view over the packed custom headers; nothing is decoded until asked for.</param>
public readonly record struct StreamMsg(
    // Payload.
    ReadOnlyMemory<byte> Body,
    string Type,
    // Where it sits on the wire.
    StreamId Id,
    int Partition,
    // What the publisher stamped on it.
    string PartitionKey,
    string CorrelationId,
    string? TraceParent,
    HeaderBlock Headers)
{
    /// <summary>
    /// When Redis accepted the entry, taken from <see cref="StreamId.Ms"/>. It is the broker's clock,
    /// not the producer's, so it is consistent across services with no clock-skew caveat.
    /// </summary>
    /// <remarks>
    /// Computed, never stored: it is exactly <see cref="StreamId.Timestamp"/>, and a stored copy
    /// would sit redundantly in every slot of every pooled batch array.
    /// </remarks>
    public DateTimeOffset EnqueuedTime => Id.Timestamp;

    /// <summary>The body as a span, for handlers that deserialise straight off the buffer.</summary>
    public ReadOnlySpan<byte> BodySpan => Body.Span;
}
