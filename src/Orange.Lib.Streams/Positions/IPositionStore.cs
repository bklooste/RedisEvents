using System.Globalization;
using Orange.Lib.Streams.Wire;

namespace Orange.Lib.Streams.Positions;

/// <summary>
/// One partition's recorded position, together with the instance that wrote it.
/// </summary>
/// <remarks>
/// The writer's instance id rides along with the position so that a second instance writing the
/// same partition — a partitioned consumer accidentally deployed as a Deployment, or a lease that
/// expired on a live-but-paused pod — is <i>detectable</i>. It is deliberately detection and not
/// fencing: a position moving backwards replays already-processed messages, which is the
/// at-least-once behaviour every handler must already tolerate.
/// </remarks>
/// <param name="Id">The stream id of the last processed entry.</param>
/// <param name="InstanceId">The instance that recorded it, or <see cref="Guid.Empty"/> when the
/// stored value carried no instance id (a legacy or hand-written value).</param>
public readonly record struct PositionRecord(StreamId Id, Guid InstanceId)
{
    /// <summary>Separator between the stream id and the instance id in the stored value.</summary>
    public const char Separator = '|';

    /// <summary>Longest rendering: a 40-char id, the separator, and a 36-char GUID.</summary>
    private const int MaxFormattedLength = 96;

    /// <summary>The stored hash value: <c>&lt;streamId&gt;|&lt;instanceId&gt;</c>.</summary>
    /// <returns>The value to write into the positions hash.</returns>
    public string Format() => Format(this.Id, this.InstanceId);

    /// <summary>Renders <c>&lt;streamId&gt;|&lt;instanceId&gt;</c> without an intermediate allocation.</summary>
    /// <param name="id">The position.</param>
    /// <param name="instanceId">The writing instance, already rendered in <c>"D"</c> form.</param>
    /// <returns>The value to write into the positions hash.</returns>
    public static string Format(StreamId id, string instanceId)
    {
        ArgumentNullException.ThrowIfNull(instanceId);

        Span<char> buffer = stackalloc char[MaxFormattedLength];
        if (id.TryFormat(buffer, out var written) && written + 1 + instanceId.Length <= buffer.Length)
        {
            buffer[written] = Separator;
            instanceId.CopyTo(buffer[(written + 1)..]);
            return new string(buffer[..(written + 1 + instanceId.Length)]);
        }

        return string.Concat(id.Format(), Separator.ToString(), instanceId);
    }

    /// <summary>Renders <c>&lt;streamId&gt;|&lt;instanceId&gt;</c>.</summary>
    /// <param name="id">The position.</param>
    /// <param name="instanceId">The writing instance.</param>
    /// <returns>The value to write into the positions hash.</returns>
    public static string Format(StreamId id, Guid instanceId)
        => Format(id, instanceId.ToString("D", CultureInfo.InvariantCulture));

    /// <summary>
    /// Parses a stored value tolerantly. <c>&lt;id&gt;|&lt;guid&gt;</c> yields both halves; a bare
    /// <c>&lt;id&gt;</c> — a legacy value, or one an operator typed by hand — yields the id with
    /// <see cref="Guid.Empty"/>, because an unattributed position is still a usable position.
    /// </summary>
    /// <param name="value">The raw hash value.</param>
    /// <param name="record">The parsed record when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the value contained a well-formed stream id.</returns>
    public static bool TryParse(ReadOnlySpan<char> value, out PositionRecord record)
    {
        record = default;

        var at = value.IndexOf(Separator);
        var idText = at < 0 ? value : value[..at];

        if (!StreamId.TryParse(idText, out var id))
        {
            return false;
        }

        var instance = Guid.Empty;
        if (at >= 0 && Guid.TryParseExact(value[(at + 1)..], "D", out var parsed))
        {
            instance = parsed;
        }

        record = new PositionRecord(id, instance);
        return true;
    }

    /// <inheritdoc />
    public override string ToString() => this.Format();
}

/// <summary>
/// Where a consumer's per-partition positions are read from and written to.
/// </summary>
/// <remarks>
/// This is the only seam in the design that admits a non-Redis store without touching the pipeline.
/// Positions are recorded when a message is <i>processed</i>, never when it is read, and they are an
/// optimisation against redelivery rather than a delivery guarantee — a store that loses a write
/// causes replay, not loss. Implementations must therefore never throw a transport failure into the
/// processing path in a way that fails a batch; the flusher logs and retries.
/// </remarks>
public interface IPositionStore
{
    /// <summary>
    /// Reads every recorded position for one consumer, in a single round trip.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Partition to last processed id, for every partition that has one. Partitions with no
    /// stored position are absent, and the caller resolves them from <c>StartFromWhenMissing</c>.</returns>
    ValueTask<IReadOnlyDictionary<int, StreamId>> LoadAsync(string topic, string consumer, CancellationToken ct);

    /// <summary>
    /// Persists every supplied partition position in one write.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="positions">The dirty partitions and their positions. May be empty, in which case
    /// nothing is written — an idle consumer generates no traffic.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when the write has landed.</returns>
    ValueTask SaveAsync(
        string topic,
        string consumer,
        ReadOnlySpan<(int Partition, StreamId Id)> positions,
        CancellationToken ct);

    /// <summary>
    /// Moves a consumer's position, for replay or for skipping a backlog.
    /// </summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="to">The id to resume from; the next read starts strictly after it.</param>
    /// <param name="partition">The single partition to reset, or <see langword="null"/> for every
    /// partition that currently has a recorded position.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when the reset has landed.</returns>
    /// <remarks>
    /// Resetting a <i>running</i> consumer races its flusher: the safe operator path is scale to
    /// zero, reset, scale up. The reset-marker protocol layered on top of this store is what makes a
    /// live reset stick.
    /// </remarks>
    ValueTask ResetAsync(string topic, string consumer, StreamId to, int? partition, CancellationToken ct);
}
