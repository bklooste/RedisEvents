using Orange.Lib.Streams.Errors;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Wire;

/// <summary>
/// Encodes and decodes a single Redis stream entry.
/// </summary>
/// <remarks>
/// <para>
/// Field names are one byte each. That looks like premature miserliness until you multiply it out:
/// at 50k messages a second the field names alone would be several megabytes a second of RESP
/// traffic, on both the wire and Redis's parser.
/// </para>
/// <list type="table">
///   <listheader><term>Field</term><description>Meaning</description></listheader>
///   <item><term><c>b</c></term><description>body, raw bytes — always present</description></item>
///   <item><term><c>t</c></term><description>message type string — always present</description></item>
///   <item><term><c>k</c></term><description>partition key — always present, may be empty</description></item>
///   <item><term><c>c</c></term><description>correlation id — optional</description></item>
///   <item><term><c>p</c></term><description>W3C <c>traceparent</c> — optional</description></item>
///   <item><term><c>h</c></term><description>packed custom headers — optional</description></item>
/// </list>
/// <para>
/// There is no envelope and no timestamp field. The body is the publisher's bytes verbatim, and the
/// enqueue time comes out of the stream id's millisecond component — the broker's clock, free.
/// </para>
/// <para>
/// Decoding allocates only the strings the shape of <see cref="StreamMsg"/> forces: the body aliases
/// the read buffer and the headers stay packed until something asks for them.
/// </para>
/// </remarks>
internal static class EntryCodec
{
    /// <summary>
    /// The codec version this build writes and reads. Recorded once per topic in <c>m:{topic}</c>
    /// under <c>v</c>, not on every entry — it is a property of the topic's layout, and paying for
    /// it per entry would defeat the point of one-byte field names.
    /// </summary>
    public const int CodecVersion = 1;

    /// <summary>Entry field carrying the raw body.</summary>
    public const string BodyField = "b";

    /// <summary>Entry field carrying the message type string.</summary>
    public const string TypeField = "t";

    /// <summary>Entry field carrying the partition key.</summary>
    public const string PartitionKeyField = "k";

    /// <summary>Entry field carrying the correlation id.</summary>
    public const string CorrelationIdField = "c";

    /// <summary>Entry field carrying the W3C <c>traceparent</c>.</summary>
    public const string TraceParentField = "p";

    /// <summary>Entry field carrying the packed headers.</summary>
    public const string HeadersField = "h";

    /// <summary>Longest possible stream id rendering: 19 digits, a dash, 19 digits.</summary>
    private const int MaxIdBytes = 40;

    private static readonly RedisValue Body = BodyField;
    private static readonly RedisValue Type = TypeField;
    private static readonly RedisValue PartitionKey = PartitionKeyField;
    private static readonly RedisValue CorrelationId = CorrelationIdField;
    private static readonly RedisValue TraceParent = TraceParentField;
    private static readonly RedisValue Headers = HeadersField;

    /// <summary>
    /// Builds the field set for an <c>XADD</c>. Optional fields are omitted entirely rather than
    /// written empty, so an entry with no correlation id, trace or headers costs three fields.
    /// </summary>
    /// <param name="body">The message body. Passed to Redis by reference — not copied.</param>
    /// <param name="type">The message type string; consumers filter on it, so it is required.</param>
    /// <param name="partitionKey">The routing key, or <see langword="null"/> for a round-robin publish.</param>
    /// <param name="correlationId">Optional correlation id, carried through to the consumer's log scope.</param>
    /// <param name="traceParent">Optional W3C <c>traceparent</c> so the consumer can continue the trace.</param>
    /// <param name="headers">Optional custom headers, packed into one field.</param>
    /// <exception cref="ArgumentException">
    /// The headers breach the <see cref="HeaderBlock.MaxCount"/> or <see cref="HeaderBlock.MaxBytes"/>
    /// cap. That is a bug in the publishing service rather than a runtime condition, so it throws here
    /// instead of being silently truncated onto the wire.
    /// </exception>
    public static NameValueEntry[] Encode(
        ReadOnlyMemory<byte> body,
        string type,
        string? partitionKey = null,
        string? correlationId = null,
        string? traceParent = null,
        IReadOnlyList<KeyValuePair<string, string>>? headers = null)
    {
        ArgumentNullException.ThrowIfNull(type);

        var packed = HeaderBlock.Pack(headers);

        var count = 3;
        if (!string.IsNullOrEmpty(correlationId))
        {
            count++;
        }

        if (!string.IsNullOrEmpty(traceParent))
        {
            count++;
        }

        if (!packed.IsEmpty)
        {
            count++;
        }

        var entries = new NameValueEntry[count];
        var at = 0;

        entries[at++] = new NameValueEntry(Body, body);
        entries[at++] = new NameValueEntry(Type, type);
        entries[at++] = new NameValueEntry(PartitionKey, partitionKey ?? string.Empty);

        if (!string.IsNullOrEmpty(correlationId))
        {
            entries[at++] = new NameValueEntry(CorrelationId, correlationId);
        }

        if (!string.IsNullOrEmpty(traceParent))
        {
            entries[at++] = new NameValueEntry(TraceParent, traceParent);
        }

        if (!packed.IsEmpty)
        {
            entries[at] = new NameValueEntry(Headers, packed.Packed);
        }

        return entries;
    }

    /// <summary>Decodes one entry as returned by <c>XREAD</c> / <c>XRANGE</c>.</summary>
    /// <param name="entry">The entry.</param>
    /// <param name="partition">The partition the entry was read from — the stream key knows it, the entry does not.</param>
    /// <param name="codecVersion">The version recorded in <c>m:{topic}</c>. Defaults to what this build writes.</param>
    /// <exception cref="StreamTransportException">
    /// The codec version is not one this build understands, or the entry is malformed.
    /// </exception>
    public static StreamMsg Decode(in StreamEntry entry, int partition, int codecVersion = CodecVersion)
    {
        EnsureSupportedVersion(codecVersion);
        return Decode(ParseId(entry.Id), entry.Values, partition, codecVersion);
    }

    /// <summary>
    /// Decodes an entry from its parts, for the read loop, which splits a raw <c>XREAD</c> reply
    /// itself and already holds the id.
    /// </summary>
    /// <param name="id">The entry's stream id.</param>
    /// <param name="values">The entry's field/value pairs.</param>
    /// <param name="partition">The partition the entry was read from.</param>
    /// <param name="codecVersion">The version recorded in <c>m:{topic}</c>.</param>
    /// <exception cref="StreamTransportException">The codec version is not one this build understands.</exception>
    public static StreamMsg Decode(
        StreamId id,
        ReadOnlySpan<NameValueEntry> values,
        int partition,
        int codecVersion = CodecVersion)
    {
        EnsureSupportedVersion(codecVersion);

        ReadOnlyMemory<byte> body = default;
        var type = string.Empty;
        var key = string.Empty;
        var correlationId = string.Empty;
        string? traceParent = null;
        var headers = HeaderBlock.Empty;

        for (var i = 0; i < values.Length; i++)
        {
            ref readonly var field = ref values[i];
            var name = field.Name;

            // Ordered by how often each field appears, so the common entry falls out early.
            if (name == Body)
            {
                body = field.Value;
            }
            else if (name == Type)
            {
                type = (string?)field.Value ?? string.Empty;
            }
            else if (name == PartitionKey)
            {
                key = (string?)field.Value ?? string.Empty;
            }
            else if (name == CorrelationId)
            {
                correlationId = (string?)field.Value ?? string.Empty;
            }
            else if (name == TraceParent)
            {
                traceParent = (string?)field.Value;
            }
            else if (name == Headers)
            {
                headers = HeaderBlock.FromPacked(field.Value);
            }

            // Anything else is a field a newer writer added within the same codec version. Ignore it
            // rather than throwing: unknown fields are additive, an unknown version is not.
        }

        return new StreamMsg(body, type, id, partition, key, correlationId, traceParent, headers);
    }

    /// <summary>Whether this build can decode entries written under <paramref name="codecVersion"/>.</summary>
    public static bool IsSupportedVersion(int codecVersion) => codecVersion == CodecVersion;

    /// <summary>
    /// Guards the codec version. An unknown version means the entry's layout is not the one this
    /// build assumes, so refusing is the only safe answer — a best-effort parse would hand a handler
    /// plausible-looking nonsense.
    /// </summary>
    /// <exception cref="StreamTransportException">The version is not supported.</exception>
    public static void EnsureSupportedVersion(int codecVersion)
    {
        if (!IsSupportedVersion(codecVersion))
        {
            throw new StreamTransportException(
                $"Stream entries are stamped with codec version {codecVersion}, but this build only understands version {CodecVersion}. " +
                "Refusing to decode rather than mis-parse the entry; upgrade the service or check the 'v' field of the topic's m:{topic} hash.");
        }
    }

    /// <summary>Parses an entry id straight out of its UTF-8 bytes, with no intermediate string.</summary>
    private static StreamId ParseId(RedisValue id)
    {
        if (id.IsNullOrEmpty)
        {
            return StreamId.Min;
        }

        var byteCount = id.GetByteCount();
        if (byteCount > MaxIdBytes)
        {
            throw MalformedId(id);
        }

        Span<byte> utf8 = stackalloc byte[MaxIdBytes];
        id.CopyTo(utf8);

        // Stream ids are ASCII digits and a dash, so the widening is exact.
        Span<char> chars = stackalloc char[MaxIdBytes];
        for (var i = 0; i < byteCount; i++)
        {
            chars[i] = (char)utf8[i];
        }

        return StreamId.TryParse(chars[..byteCount], out var parsed) ? parsed : throw MalformedId(id);
    }

    private static StreamTransportException MalformedId(RedisValue id)
        => new($"'{id}' is not a well-formed Redis stream id; expected '<ms>-<seq>'.");
}
