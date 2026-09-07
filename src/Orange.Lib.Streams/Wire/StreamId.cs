using System.Globalization;

namespace Orange.Lib.Streams.Wire;

/// <summary>
/// A Redis stream entry id in its native <c>&lt;unix-millis&gt;-&lt;seq&gt;</c> form.
/// </summary>
/// <remarks>
/// Keeping the two halves separate makes date math exact: an id's millisecond component is a real
/// unix timestamp, so replay-by-date is a straight <see cref="FromDate"/> and a range read.
/// Parsing and formatting are allocation-free apart from the single string <see cref="Format"/> returns.
/// </remarks>
/// <param name="Ms">Unix time in milliseconds.</param>
/// <param name="Seq">Sequence number within that millisecond.</param>
public readonly record struct StreamId(long Ms, long Seq) : IComparable<StreamId>
{
    /// <summary>The lowest possible id, <c>"0-0"</c>. Reads from here see the whole stream.</summary>
    public static readonly StreamId Min = new(0, 0);

    /// <summary>The highest possible id. Useful as an exclusive upper bound for range reads.</summary>
    public static readonly StreamId Max = new(long.MaxValue, long.MaxValue);

    /// <summary>Longest possible rendering: 19 digits, a separator, 19 digits.</summary>
    private const int MaxFormattedLength = 40;

    /// <summary>The id's millisecond component as a point in time.</summary>
    public DateTimeOffset Timestamp => DateTimeOffset.FromUnixTimeMilliseconds(Ms);

    /// <summary>The first id that could have been written at or after <paramref name="when"/>.</summary>
    public static StreamId FromDate(DateTimeOffset when) => new(when.ToUnixTimeMilliseconds(), 0);

    /// <summary>Parses <c>"&lt;ms&gt;-&lt;seq&gt;"</c>.</summary>
    /// <exception cref="FormatException">The text is not a well-formed stream id.</exception>
    public static StreamId Parse(ReadOnlySpan<char> s)
    {
        if (!TryParse(s, out var id))
        {
            throw new FormatException($"'{s}' is not a valid Redis stream id; expected '<ms>-<seq>'.");
        }

        return id;
    }

    /// <summary>Parses <c>"&lt;ms&gt;-&lt;seq&gt;"</c>, returning <see langword="false"/> rather than throwing.</summary>
    public static bool TryParse(ReadOnlySpan<char> s, out StreamId id)
    {
        id = default;

        var dash = s.IndexOf('-');
        if (dash <= 0 || dash == s.Length - 1)
        {
            return false;
        }

        if (!long.TryParse(s[..dash], NumberStyles.None, CultureInfo.InvariantCulture, out var ms) ||
            !long.TryParse(s[(dash + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out var seq))
        {
            return false;
        }

        id = new StreamId(ms, seq);
        return true;
    }

    /// <summary>Renders the id, allocating exactly one string.</summary>
    public string Format()
    {
        Span<char> buffer = stackalloc char[MaxFormattedLength];
        _ = TryFormat(buffer, out var written);
        return new string(buffer[..written]);
    }

    /// <summary>Writes the id into <paramref name="destination"/> without allocating.</summary>
    public bool TryFormat(Span<char> destination, out int charsWritten)
    {
        charsWritten = 0;

        if (!Ms.TryFormat(destination, out var msLength, default, CultureInfo.InvariantCulture) ||
            destination.Length <= msLength)
        {
            return false;
        }

        destination[msLength] = '-';

        if (!Seq.TryFormat(destination[(msLength + 1)..], out var seqLength, default, CultureInfo.InvariantCulture))
        {
            return false;
        }

        charsWritten = msLength + 1 + seqLength;
        return true;
    }

    /// <inheritdoc />
    public override string ToString() => Format();

    /// <inheritdoc />
    public int CompareTo(StreamId other)
    {
        var byMs = Ms.CompareTo(other.Ms);
        return byMs != 0 ? byMs : Seq.CompareTo(other.Seq);
    }

    public static bool operator <(StreamId left, StreamId right) => left.CompareTo(right) < 0;

    public static bool operator <=(StreamId left, StreamId right) => left.CompareTo(right) <= 0;

    public static bool operator >(StreamId left, StreamId right) => left.CompareTo(right) > 0;

    public static bool operator >=(StreamId left, StreamId right) => left.CompareTo(right) >= 0;
}
