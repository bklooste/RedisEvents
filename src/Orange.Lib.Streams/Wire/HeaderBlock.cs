using System.Globalization;
using System.IO.Hashing;
using System.Text;
using Orange.Lib.Streams.Errors;

namespace Orange.Lib.Streams.Wire;

/// <summary>
/// A lazy, allocation-free view over the packed <c>h</c> field of a stream entry.
/// </summary>
/// <remarks>
/// <para>
/// Custom headers are rare and small, so rather than spending a Redis stream field on each one —
/// which would bloat every entry whether it carries headers or not — they are packed into a single
/// field as a run of length-prefixed byte strings:
/// </para>
/// <code>
/// len(key) ':' key len(val) ':' val  len(key) ':' key len(val) ':' val  …
/// </code>
/// <para>
/// Lengths are ASCII decimal counts of <em>UTF-8 bytes</em>, so keys and values may themselves
/// contain <c>':'</c> and digits without ambiguity: the length prefix, never a delimiter search,
/// decides where a field ends.
/// </para>
/// <para>
/// Decoding is a single forward scan with span slicing. Nothing is allocated until a caller
/// actually asks for a header, and even then only the requested string is materialised. A message
/// whose headers are never read costs nothing beyond carrying the packed bytes around.
/// </para>
/// <para>
/// Caps are enforced when packing, not when reading: over-cap is a bug in the publishing service
/// rather than a runtime condition, so <see cref="Pack"/> throws rather than truncating.
/// </para>
/// </remarks>
public readonly struct HeaderBlock : IEquatable<HeaderBlock>
{
    /// <summary>Most headers one message may carry. Exceeding this throws at publish time.</summary>
    public const int MaxCount = 64;

    /// <summary>Largest packed size in bytes (8 KiB). Exceeding this throws at publish time.</summary>
    public const int MaxBytes = 8 * 1024;

    private const byte Separator = (byte)':';

    /// <summary>Header keys at or below this UTF-8 length are encoded on the stack for lookups.</summary>
    private const int KeyStackBytes = 256;

    private readonly ReadOnlyMemory<byte> _packed;

    private HeaderBlock(ReadOnlyMemory<byte> packed) => _packed = packed;

    /// <summary>A block carrying no headers.</summary>
    public static HeaderBlock Empty => default;

    /// <summary>The raw packed bytes, exactly as they sit in the entry's <c>h</c> field.</summary>
    public ReadOnlyMemory<byte> Packed => _packed;

    /// <summary>Whether the block carries no headers at all.</summary>
    public bool IsEmpty => _packed.IsEmpty;

    /// <summary>Wraps already-packed bytes — the decode path, which never validates eagerly.</summary>
    public static HeaderBlock FromPacked(ReadOnlyMemory<byte> packed) => new(packed);

    /// <summary>
    /// Packs headers for publication, enforcing the <see cref="MaxCount"/> and <see cref="MaxBytes"/> caps.
    /// </summary>
    /// <param name="headers">The headers, or <see langword="null"/> / empty for none.</param>
    /// <returns>The packed block; <see cref="Empty"/> when there is nothing to pack.</returns>
    /// <exception cref="ArgumentException">
    /// More than <see cref="MaxCount"/> headers, a packed size above <see cref="MaxBytes"/>, or a
    /// <see langword="null"/> key or value. All three are publisher bugs, not runtime conditions.
    /// </exception>
    public static HeaderBlock Pack(IReadOnlyList<KeyValuePair<string, string>>? headers)
    {
        if (headers is null || headers.Count == 0)
        {
            return Empty;
        }

        if (headers.Count > MaxCount)
        {
            throw new ArgumentException(
                $"A message may carry at most {MaxCount} headers; {headers.Count} were supplied.",
                nameof(headers));
        }

        long total = 0;
        for (var i = 0; i < headers.Count; i++)
        {
            var (key, value) = (headers[i].Key, headers[i].Value);
            if (key is null || value is null)
            {
                throw new ArgumentException(
                    $"Header at index {i} has a null {(key is null ? "key" : "value")}.",
                    nameof(headers));
            }

            total += FieldSize(key) + FieldSize(value);
            if (total > MaxBytes)
            {
                break;
            }
        }

        if (total > MaxBytes)
        {
            throw new ArgumentException(
                $"Packed headers exceed the {MaxBytes} byte cap.",
                nameof(headers));
        }

        var buffer = new byte[(int)total];
        var span = buffer.AsSpan();
        var at = 0;

        for (var i = 0; i < headers.Count; i++)
        {
            at += WriteField(span[at..], headers[i].Key);
            at += WriteField(span[at..], headers[i].Value);
        }

        return new HeaderBlock(buffer);
    }

    /// <summary>
    /// Looks a header up by key, allocating a string only for the value that is actually found.
    /// </summary>
    public bool TryGetValue(string key, out string value)
    {
        ArgumentNullException.ThrowIfNull(key);

        Span<byte> scratch = stackalloc byte[KeyStackBytes];
        var found = Encoding.UTF8.GetByteCount(key) <= KeyStackBytes
            ? TryGetValueUtf8(scratch[..Encoding.UTF8.GetBytes(key.AsSpan(), scratch)], out var bytes)
            : TryGetValueUtf8(Encoding.UTF8.GetBytes(key), out bytes);

        if (found)
        {
            value = Encoding.UTF8.GetString(bytes);
            return true;
        }

        value = string.Empty;
        return false;
    }

    /// <summary>
    /// Looks a header up by its UTF-8 key and returns a slice of the packed block — no allocation
    /// at all. The returned span is only valid while the underlying buffer is.
    /// </summary>
    public bool TryGetValueUtf8(ReadOnlySpan<byte> keyUtf8, out ReadOnlySpan<byte> value)
    {
        var data = _packed.Span;
        var at = 0;

        while (TryReadField(data, ref at, out var keyStart, out var keyLength))
        {
            if (!TryReadField(data, ref at, out var valueStart, out var valueLength))
            {
                throw Malformed("the block ends after a key with no matching value");
            }

            if (data.Slice(keyStart, keyLength).SequenceEqual(keyUtf8))
            {
                value = data.Slice(valueStart, valueLength);
                return true;
            }
        }

        value = default;
        return false;
    }

    /// <summary>Walks the headers in packed order without allocating.</summary>
    public Enumerator GetEnumerator() => new(_packed);

    /// <inheritdoc />
    public bool Equals(HeaderBlock other) => _packed.Span.SequenceEqual(other._packed.Span);

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is HeaderBlock other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => (int)XxHash3.HashToUInt64(_packed.Span);

    public static bool operator ==(HeaderBlock left, HeaderBlock right) => left.Equals(right);

    public static bool operator !=(HeaderBlock left, HeaderBlock right) => !left.Equals(right);

    /// <summary>Human-readable rendering for logs and <c>redis-cli</c> spelunking.</summary>
    public override string ToString()
    {
        if (IsEmpty)
        {
            return string.Empty;
        }

        var builder = new StringBuilder();
        foreach (var header in this)
        {
            if (builder.Length > 0)
            {
                builder.Append("; ");
            }

            builder.Append(header.Key).Append('=').Append(header.Value);
        }

        return builder.ToString();
    }

    /// <summary>Bytes one length-prefixed field occupies once packed.</summary>
    private static int FieldSize(string text)
    {
        var length = Encoding.UTF8.GetByteCount(text);
        return DigitCount(length) + 1 + length;
    }

    private static int WriteField(Span<byte> destination, string text)
    {
        var length = Encoding.UTF8.GetByteCount(text);

        _ = length.TryFormat(destination, out var digits, default, CultureInfo.InvariantCulture);
        destination[digits] = Separator;
        _ = Encoding.UTF8.GetBytes(text.AsSpan(), destination[(digits + 1)..]);

        return digits + 1 + length;
    }

    private static int DigitCount(int value)
    {
        var digits = 1;
        while (value >= 10)
        {
            value /= 10;
            digits++;
        }

        return digits;
    }

    /// <summary>
    /// Reads one length-prefixed field, advancing <paramref name="at"/> past it.
    /// Returns <see langword="false"/> only at a clean end of block.
    /// </summary>
    private static bool TryReadField(ReadOnlySpan<byte> data, ref int at, out int start, out int length)
    {
        start = 0;
        length = 0;

        if (at >= data.Length)
        {
            return false;
        }

        long declared = 0;
        var digits = 0;
        var cursor = at;

        while (cursor < data.Length && data[cursor] != Separator)
        {
            var digit = data[cursor];
            if (digit is < (byte)'0' or > (byte)'9')
            {
                throw Malformed("a length prefix contains a non-digit byte");
            }

            declared = (declared * 10) + (digit - '0');
            if (declared > data.Length)
            {
                throw Malformed("a length prefix runs past the end of the block");
            }

            cursor++;
            digits++;
        }

        if (digits == 0 || cursor >= data.Length)
        {
            throw Malformed("a length prefix is empty or unterminated");
        }

        cursor++;

        if (cursor + declared > data.Length)
        {
            throw Malformed("a field's declared length runs past the end of the block");
        }

        start = cursor;
        length = (int)declared;
        at = cursor + length;
        return true;
    }

    private static StreamTransportException Malformed(string detail)
        => new($"Packed stream headers are malformed: {detail}. The entry was written by an incompatible codec or the value has been corrupted.");

    /// <summary>Allocation-free forward walk over a packed block.</summary>
    public struct Enumerator
    {
        private readonly ReadOnlyMemory<byte> _packed;
        private int _at;
        private Header _current;

        internal Enumerator(ReadOnlyMemory<byte> packed)
        {
            _packed = packed;
            _at = 0;
            _current = default;
        }

        /// <summary>The header at the current position.</summary>
        public readonly Header Current => _current;

        /// <summary>Advances to the next header.</summary>
        /// <exception cref="StreamTransportException">The packed bytes are malformed.</exception>
        public bool MoveNext()
        {
            var data = _packed.Span;

            if (!TryReadField(data, ref _at, out var keyStart, out var keyLength))
            {
                return false;
            }

            if (!TryReadField(data, ref _at, out var valueStart, out var valueLength))
            {
                throw Malformed("the block ends after a key with no matching value");
            }

            _current = new Header(_packed.Slice(keyStart, keyLength), _packed.Slice(valueStart, valueLength));
            return true;
        }
    }

    /// <summary>One header, held as slices of the packed block until a string is asked for.</summary>
    public readonly struct Header
    {
        internal Header(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
        {
            KeyUtf8 = key;
            ValueUtf8 = value;
        }

        /// <summary>The key's UTF-8 bytes, aliasing the packed block.</summary>
        public ReadOnlyMemory<byte> KeyUtf8 { get; }

        /// <summary>The value's UTF-8 bytes, aliasing the packed block.</summary>
        public ReadOnlyMemory<byte> ValueUtf8 { get; }

        /// <summary>The key as a string. Allocates.</summary>
        public string Key => Encoding.UTF8.GetString(KeyUtf8.Span);

        /// <summary>The value as a string. Allocates.</summary>
        public string Value => Encoding.UTF8.GetString(ValueUtf8.Span);

        /// <inheritdoc />
        public override string ToString() => $"{Key}={Value}";
    }
}
