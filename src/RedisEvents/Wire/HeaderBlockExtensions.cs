using RedisEvents.Errors;

namespace RedisEvents.Wire;

/// <summary>
/// Convenience over the packed block — none of these are on the hot path.
/// </summary>
/// <remarks>
/// <para>
/// They live off <see cref="HeaderBlock"/> on purpose: the struct's own surface is the part the
/// library reads on every message (<see cref="HeaderBlock.TryGetValueUtf8"/> and the enumerator),
/// and everything that allocates, walks the whole block, or exists only for readability sits here
/// instead. Each is implemented over the public enumerator or lookup, so nothing here has any
/// privileged access to the packing.
/// </para>
/// <para>
/// Note that <see cref="CountHeaders"/> is a method, not a property: it is O(n) in the packed size
/// and it can throw on a malformed block, both of which make it a footgun dressed as a property.
/// </para>
/// </remarks>
public static class HeaderBlockExtensions
{
    /// <summary>The value for <paramref name="key"/>, or <see langword="null"/> when absent.</summary>
    public static string? GetValueOrDefault(this in HeaderBlock headers, string key)
        => headers.TryGetValue(key, out var value) ? value : null;

    /// <summary>Whether a header with this key is present.</summary>
    public static bool ContainsKey(this in HeaderBlock headers, string key)
        => headers.TryGetValue(key, out _);

    /// <summary>
    /// Materialises every header into a dictionary. This is the allocating path — use the
    /// enumerator or <see cref="HeaderBlock.TryGetValueUtf8"/> on anything hot.
    /// </summary>
    public static Dictionary<string, string> ToDictionary(this in HeaderBlock headers)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var header in headers)
        {
            result[header.Key] = header.Value;
        }

        return result;
    }

    /// <summary>
    /// Number of headers. O(n) in the packed size — it walks the block — so hold the result rather
    /// than calling it in a loop condition.
    /// </summary>
    /// <exception cref="StreamTransportException">The packed bytes are malformed.</exception>
    public static int CountHeaders(this in HeaderBlock headers)
    {
        var count = 0;
        foreach (var _ in headers)
        {
            count++;
        }

        return count;
    }
}
