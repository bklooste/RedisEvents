using System.Text.RegularExpressions;

namespace RedisEvents.Diagnostics;

/// <summary>
/// Builders for named Redis client connections and thread names.
/// All components are sanitised to [A-Za-z0-9._:-] and truncated to safe lengths,
/// because Redis CLIENT SETNAME rejects spaces, newlines, and other control characters.
/// </summary>
internal static partial class StreamNames
{
    // Safe max length for Redis CLIENT SETNAME commands and thread names.
    // Redis limits are higher, but this is a conservative bound that accommodates
    // service names, pod names, and index numbers with reasonable margin.
    private const int MaxNameLength = 256;

    /// <summary>
    /// Builds the socket manager thread name for a reader connection.
    /// Format: streams-rd:{consumer}:i{index}
    /// </summary>
    public static string ReaderThreadName(string consumer, int index)
    {
        var consumer_s = Sanitise(consumer);
        var formatted = $"streams-rd:{consumer_s}:i{index}";
        return Truncate(formatted);
    }

    /// <summary>
    /// Builds the ClientName for a dedicated reader multiplexer.
    /// Format: streams-rd:{service}:{consumer}:i{index}:{pod}
    /// </summary>
    public static string ReaderClientName(string service, string consumer, int index, string podName)
    {
        var service_s = Sanitise(service);
        var consumer_s = Sanitise(consumer);
        var pod_s = Sanitise(podName);
        var formatted = $"streams-rd:{service_s}:{consumer_s}:i{index}:{pod_s}";
        return Truncate(formatted);
    }

    /// <summary>
    /// Builds the ClientName for the shared (admin/write) multiplexer.
    /// Format: streams:{service}:{pod}
    /// </summary>
    public static string SharedClientName(string service, string podName)
    {
        var service_s = Sanitise(service);
        var pod_s = Sanitise(podName);
        var formatted = $"streams:{service_s}:{pod_s}";
        return Truncate(formatted);
    }

    /// <summary>
    /// Builds the hosted service task name for a consumer host.
    /// Format: streams-host:{consumer}
    /// </summary>
    public static string ConsumerHostTaskName(string consumer)
    {
        var consumer_s = Sanitise(consumer);
        var formatted = $"streams-host:{consumer_s}";
        return Truncate(formatted);
    }

    /// <summary>
    /// Matches every character that Redis <c>CLIENT SETNAME</c> and thread names must not carry.
    /// </summary>
    /// <remarks>
    /// R-18: source-generated rather than interpreted. <see cref="Regex.Replace(string, string, string)"/>
    /// parses and compiles the pattern on first use behind a cache lookup keyed on the pattern
    /// string; <c>[GeneratedRegex]</c> emits the matcher at build time, which is both faster on this
    /// path (it runs once per connection and once per reader thread at startup) and the only form
    /// that is provably trim/AOT-safe.
    /// </remarks>
    [GeneratedRegex(@"[^A-Za-z0-9._:\-]")]
    private static partial Regex UnsafeNameChars();

    /// <summary>
    /// Sanitises a string by replacing any character not matching [A-Za-z0-9._:-] with '-'.
    /// </summary>
    private static string Sanitise(string input)
    {
        if (string.IsNullOrEmpty(input))
            return "unknown";

        // Replace anything not in [A-Za-z0-9._:-] with '-'
        return UnsafeNameChars().Replace(input, "-");
    }

    /// <summary>
    /// Truncates a name to MaxNameLength bytes (UTF-8 encoded).
    /// </summary>
    private static string Truncate(string name)
    {
        // For ASCII-only inputs (which ours are after sanitisation), length == byte count.
        // But we check byte count to be safe.
        var bytes = System.Text.Encoding.UTF8.GetByteCount(name);
        if (bytes <= MaxNameLength)
            return name;

        // Truncate and re-encode until we fit.
        var truncated = name;
        while (System.Text.Encoding.UTF8.GetByteCount(truncated) > MaxNameLength && truncated.Length > 0)
            truncated = truncated[..^1];

        return truncated;
    }
}
