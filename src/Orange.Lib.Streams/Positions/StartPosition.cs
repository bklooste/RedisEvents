using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Positions;

/// <summary>
/// Where one partition's reader starts, resolved once at worker start.
/// </summary>
/// <remarks>
/// <para>
/// A position is always "the last id already dealt with"; a read resumes <em>strictly after</em> it.
/// That is why <see cref="After"/> is the whole story for three of the four <see cref="StartFrom"/>
/// modes, and why <see cref="StartFrom.Now"/> needs a flag of its own.
/// </para>
/// <para>
/// <b>The <c>$</c> subtlety.</b> <see cref="StartFrom.Now"/> means "only entries added after we
/// connect", and Redis spells that <c>$</c> — resolved by the <i>server</i>, at the moment the read
/// arrives. It is therefore correct exactly once: on the first <c>XREAD</c>. If the worker kept
/// passing <c>$</c>, every reconnect, every block timeout and every retry would re-resolve it to
/// whatever the tail is <em>now</em>, silently skipping everything published while the read was in
/// flight. So after the first fetch the worker tracks the last id it actually saw and passes that,
/// which is what <see cref="ReadFrom"/> encodes: hand it the last id read, and it returns <c>$</c>
/// only while there is not one yet.
/// </para>
/// </remarks>
/// <param name="After">The id to read strictly after. Ignored while <paramref name="FromNow"/> is
/// <see langword="true"/> and no entry has been read yet.</param>
/// <param name="FromNow">
/// <see langword="true"/> when the first read must use the literal <c>$</c> rather than an id.
/// </param>
internal readonly record struct StartPosition(StreamId After, bool FromNow)
{
    /// <summary>The whole stream: <c>0-0</c>.</summary>
    public static StartPosition Beginning => new(StreamId.Min, FromNow: false);

    /// <summary>Only entries added after the first read reaches the server.</summary>
    public static StartPosition Now => new(StreamId.Min, FromNow: true);

    /// <summary>Resume strictly after a known id.</summary>
    /// <param name="id">The last id already processed.</param>
    /// <returns>The corresponding start position.</returns>
    public static StartPosition Resume(StreamId id) => new(id, FromNow: false);

    /// <summary>
    /// The first entry at or after <paramref name="when"/> — the position is placed immediately
    /// <i>before</i> <c>&lt;unixMillis&gt;-0</c> so that an entry written exactly on that millisecond
    /// is delivered rather than skipped.
    /// </summary>
    /// <param name="when">The wall-clock instant to start from.</param>
    /// <returns>The corresponding start position.</returns>
    public static StartPosition FromDate(DateTimeOffset when) => new(Before(StreamId.FromDate(when)), FromNow: false);

    /// <summary>
    /// The largest id strictly less than <paramref name="first"/> — i.e. the position to store so
    /// that <paramref name="first"/> is the next entry delivered.
    /// </summary>
    /// <param name="first">The first entry that should be (re)processed.</param>
    /// <returns>The position immediately before it, clamped at <see cref="StreamId.Min"/>.</returns>
    /// <remarks>
    /// Sequence numbers are exhausted within a millisecond before the millisecond advances, so
    /// <c>(ms-1, long.MaxValue)</c> has no successor inside <c>ms-1</c> and a read after it lands on
    /// the first entry of <c>ms</c>. The arithmetic is exact; no probing read is needed.
    /// </remarks>
    public static StreamId Before(StreamId first)
    {
        if (first.Seq > 0)
        {
            return new StreamId(first.Ms, first.Seq - 1);
        }

        return first.Ms > 0 ? new StreamId(first.Ms - 1, long.MaxValue) : StreamId.Min;
    }

    /// <summary>
    /// Resolves the start position for one partition from the consumer's configuration and whatever
    /// the position store had for it.
    /// </summary>
    /// <param name="options">The consumer's options.</param>
    /// <param name="stored">The stored position for this partition, or <see langword="null"/> when
    /// the partition has none.</param>
    /// <returns>The resolved start position.</returns>
    /// <exception cref="StreamConfigurationException">
    /// <see cref="StartFrom.Date"/> was selected without a <see cref="ConsumerOptions.StartFromDate"/>.
    /// </exception>
    public static StartPosition Resolve(ConsumerOptions options, StreamId? stored)
    {
        ArgumentNullException.ThrowIfNull(options);

        return Resolve(options.StartFrom, options.StartFromWhenMissing, options.StartFromDate, stored, options.Topic);
    }

    /// <summary>
    /// Resolves the start position from the individual settings, for callers that do not have a
    /// whole <see cref="ConsumerOptions"/> — the reset paths and the tests among them.
    /// </summary>
    /// <param name="startFrom">The configured mode.</param>
    /// <param name="whenMissing">The fallback used when <paramref name="startFrom"/> is
    /// <see cref="StartFrom.Stored"/> and <paramref name="stored"/> is <see langword="null"/>.</param>
    /// <param name="date">The date for <see cref="StartFrom.Date"/>.</param>
    /// <param name="stored">The stored position for this partition, if any.</param>
    /// <param name="topic">Topic name, used only to make a configuration error legible.</param>
    /// <returns>The resolved start position.</returns>
    /// <exception cref="StreamConfigurationException">
    /// <see cref="StartFrom.Date"/> was selected without a date, or the missing-position fallback is
    /// itself <see cref="StartFrom.Stored"/>, which would be circular.
    /// </exception>
    public static StartPosition Resolve(
        StartFrom startFrom,
        StartFrom whenMissing,
        DateTimeOffset? date,
        StreamId? stored,
        string? topic = null)
    {
        switch (startFrom)
        {
            case StartFrom.Stored:
                if (stored is { } id)
                {
                    return Resume(id);
                }

                if (whenMissing == StartFrom.Stored)
                {
                    throw new StreamConfigurationException(
                        $"Consumer{Describe(topic)} has StartFrom=Stored and StartFromWhenMissing=Stored, which is circular. " +
                        "Set StartFromWhenMissing to Beginning (the default), Now or Date.");
                }

                // One level only: the fallback is never itself Stored, so this cannot recurse further.
                return Resolve(whenMissing, StartFrom.Beginning, date, stored: null, topic);

            case StartFrom.Beginning:
                return Beginning;

            case StartFrom.Now:
                return Now;

            case StartFrom.Date:
                if (date is { } when)
                {
                    return FromDate(when);
                }

                throw new StreamConfigurationException(
                    $"Consumer{Describe(topic)} has StartFrom=Date but no StartFromDate. " +
                    "Set StartFromDate to an ISO-8601 instant, for example 2026-09-01T00:00:00Z.");

            default:
                throw new StreamConfigurationException(
                    $"Consumer{Describe(topic)} has an unrecognised StartFrom value '{startFrom}'.");
        }
    }

    /// <summary>
    /// The id to pass to the next <c>XREAD</c>.
    /// </summary>
    /// <param name="lastRead">The last id this worker has actually read, or <see langword="null"/>
    /// when it has not read anything yet.</param>
    /// <returns><c>$</c> only for the very first read of a <see cref="StartFrom.Now"/> consumer;
    /// otherwise a concrete id, so a reconnect resumes rather than skips.</returns>
    public RedisValue ReadFrom(StreamId? lastRead)
    {
        if (lastRead is { } id)
        {
            return id.Format();
        }

        return this.FromNow ? StreamPosition.NewMessages : this.After.Format();
    }

    private static string Describe(string? topic)
        => string.IsNullOrWhiteSpace(topic) ? string.Empty : $" for topic '{topic}'";
}
