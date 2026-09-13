using RedisEvents.Errors;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Producer;

/// <summary>
/// A stream of state — an aggregate's own history — that is written in the same
/// <c>MULTI</c>/<c>EXEC</c> as the topic publish announcing it.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two methods, and the second one is the whole point.</b> This is the outbox
/// (<see cref="Outbox.WriteAndPublishManyAsync"/>) narrowed to the one state shape that needs no
/// delegate: another stream. <see cref="AppendAndPublishAsync"/> appends every event to the named
/// state stream <em>and</em> publishes every event to the topic in one transaction, guarded by a
/// length check on the state stream — which is exactly an event store's append-with-expected-version
/// and an event store's projection feed, with no second system and no relay process.
/// </para>
/// <para>
/// <b>Keys.</b> The name is not a Redis key; it is the tail of one. The store builds
/// <c>{topic}:state:&lt;name&gt;</c> through <see cref="Outbox.StateKey"/>, so the state stream carries the
/// topic's hash tag and one transaction over both streams is legal on Redis Cluster. A topic with
/// <see cref="Config.TopicOptions.CoLocatePartitions"/> set to <see langword="false"/> spreads its
/// partitions across slots and therefore cannot have a state store at all — that is refused at
/// registration, not at the first append.
/// </para>
/// <para>
/// <b>Version is length.</b> <c>XLEN</c> of the state stream is the only version definition
/// <see cref="Condition.StreamLengthEqual"/> can enforce in one round trip, with no second key to
/// keep in sync — and <c>XLEN</c> of a missing key is <c>0</c>, so creating a stream is
/// <c>expectedLength: 0</c> with no special case. The corollary is an invariant a caller must not
/// break: <b>never trim a state stream.</b> Trim one and every later concurrency check compares
/// against a number that no longer means "how many events have happened", silently. Nothing in this
/// library trims them — the background trimmer and <c>StreamAdmin.Reset</c> only ever build
/// <c>s:*</c> stream keys — and the appends issued here carry no <c>MAXLEN</c>.
/// </para>
/// <para>
/// <b>No retries, no fallback.</b> A failure throws, exactly as <see cref="IStreamPublisher"/> does.
/// A concurrency loss is not a failure: it is <see langword="null"/>, and nothing was written.
/// </para>
/// </remarks>
public interface IStreamStore
{
    /// <summary>
    /// Reads a page of a state stream, oldest first, decoded with the topic's wire codec.
    /// </summary>
    /// <param name="name">
    /// The state stream's name, co-located with the topic by <see cref="Outbox.StateKey"/> — e.g.
    /// <c>es:Inventory:42</c> for key <c>{topic}:state:es:Inventory:42</c>.
    /// </param>
    /// <param name="after">
    /// Exclusive lower bound: only entries with an id strictly greater than this are returned. Pass
    /// <see cref="StreamId.Min"/> to read from the beginning, or the id of the last entry of the
    /// previous page to continue.
    /// </param>
    /// <param name="max">The maximum number of entries to return. A short page means the end of the stream.</param>
    /// <param name="ct">Cancellation, observed before the command is issued.</param>
    /// <returns>
    /// The decoded entries in stream order, or an empty list when there are none. A missing stream
    /// reads as empty — "no such aggregate" and "an aggregate with no events" are the same thing.
    /// <para>
    /// Unlike the consumer's batches, these bodies do not alias a pooled buffer: each one owns its
    /// bytes and stays valid after the call, so a caller may deserialise at its leisure.
    /// </para>
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is null, empty or whitespace.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="max"/> is not positive.</exception>
    /// <exception cref="StreamTransportException">An entry is malformed, or was written by a codec version this build cannot read.</exception>
    ValueTask<IReadOnlyList<StreamMsg>> ReadAsync(
        string name,
        StreamId after,
        int max,
        CancellationToken ct = default);

    /// <summary>
    /// One <c>MULTI</c>/<c>EXEC</c>: appends every event to the named state stream and publishes
    /// every event to the topic, both in the order given, if and only if the state stream's length is
    /// still <paramref name="expectedLength"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The condition compiles to <c>WATCH</c> plus a pre-<c>EXEC</c> check, so this is real optimistic
    /// concurrency with no Lua script. Redis applies the queued commands in order inside <c>EXEC</c>:
    /// the state appends in event order, then the topic publishes in event order.
    /// </para>
    /// <para>
    /// <b>Three outcomes, no fourth.</b> The condition held and everything applied; the condition
    /// failed and <b>nothing</b> applied (<see langword="null"/>); or the call threw, in which case
    /// the transaction either never ran or ran completely — a connection lost around <c>EXEC</c>
    /// leaves an outcome that is unknown but never torn. Retrying with the same
    /// <paramref name="expectedLength"/> therefore tells you which it was: <see langword="null"/>
    /// means the first attempt landed, a non-null result means it did not.
    /// </para>
    /// </remarks>
    /// <param name="name">The state stream's name, as in <see cref="ReadAsync"/>.</param>
    /// <param name="expectedLength">
    /// The length the state stream must still have, i.e. the version the caller decided against.
    /// <c>0</c> creates the stream.
    /// </param>
    /// <param name="partitionKey">
    /// The routing key for the topic publishes, and the key stamped on both copies of every event.
    /// One state stream should always use one key — the aggregate's id — so that all of its events
    /// land on one partition and stay ordered for the projection side.
    /// </param>
    /// <param name="events">
    /// The events, appended and published in this order. Must not be empty: a transaction that
    /// publishes nothing is not an append.
    /// </param>
    /// <param name="ct">Cancellation, observed before the transaction is built. Once <c>EXEC</c> is on the wire there is nothing left to cancel.</param>
    /// <returns>
    /// The ids Redis assigned to the <b>state-stream</b> entries, in event order — not the topic
    /// publishes' ids — or <see langword="null"/> when the length check failed and nothing was
    /// written.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is blank, <paramref name="events"/> is empty, or an event names no type.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="expectedLength"/> is negative.</exception>
    /// <exception cref="StreamConfigurationException">The topic's partitions are not co-located, or its options are unusable.</exception>
    /// <exception cref="StreamTransportException">Redis rejected or failed the transaction.</exception>
    ValueTask<StreamId[]?> AppendAndPublishAsync(
        string name,
        long expectedLength,
        string partitionKey,
        IReadOnlyList<StateEvent> events,
        CancellationToken ct = default);

    /// <summary>
    /// One <c>MULTI</c>/<c>EXEC</c>: appends every event to the named state stream and publishes
    /// every event to the topic, both in the order given, with <b>no</b> optimistic-concurrency
    /// check — no <c>WATCH</c>, no length comparison, no chance of a lost-condition <see
    /// langword="null"/> return.
    /// </summary>
    /// <remarks>
    /// This is <see cref="AppendAndPublishAsync(string, long, string, IReadOnlyList{StateEvent}, CancellationToken)"/>
    /// with the version check removed and nothing else changed: same keys, same transaction shape,
    /// same wire format. It exists for callers that do not need per-aggregate conflict detection —
    /// e.g. a single-writer aggregate, or a benchmark isolating the cost of the <c>WATCH</c> round
    /// trip itself — and must not be reached for by a caller that does, since two concurrent callers
    /// can silently interleave their appends with no error and no way to tell afterwards.
    /// </remarks>
    /// <param name="name">The state stream's name, as in <see cref="ReadAsync"/>.</param>
    /// <param name="partitionKey">
    /// The routing key for the topic publishes, and the key stamped on both copies of every event.
    /// </param>
    /// <param name="events">
    /// The events, appended and published in this order. Must not be empty: a transaction that
    /// publishes nothing is not an append.
    /// </param>
    /// <param name="ct">Cancellation, observed before the transaction is built. Once <c>EXEC</c> is on the wire there is nothing left to cancel.</param>
    /// <returns>The ids Redis assigned to the <b>state-stream</b> entries, in event order.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is blank, <paramref name="events"/> is empty, or an event names no type.</exception>
    /// <exception cref="StreamConfigurationException">The topic's partitions are not co-located, or its options are unusable.</exception>
    /// <exception cref="StreamTransportException">Redis rejected or failed the transaction.</exception>
    ValueTask<StreamId[]> AppendAndPublishAsync(
        string name,
        string partitionKey,
        IReadOnlyList<StateEvent> events,
        CancellationToken ct = default);
}
