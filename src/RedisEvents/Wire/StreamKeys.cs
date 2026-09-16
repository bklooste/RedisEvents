using System.Globalization;
using RedisEvents.Config;
using StackExchange.Redis;

namespace RedisEvents.Wire;

/// <summary>
/// Builds every Redis key the library touches.
/// </summary>
/// <remarks>
/// The braces in the key shapes are literal: they are Redis Cluster hash tags around the topic
/// name, so every key belonging to one topic hashes to the same slot and a worker's multi-stream
/// <c>XREAD</c> plus its position write stay single-node operations. Every shape below carries
/// <see cref="KeyNamespace.Prefix"/> ahead of the hash tag, e.g. <c>dev:re:s:{topic}:0</c> — outside
/// the braces, so it does not affect which slot the topic's keys land on.
/// <list type="bullet">
///   <item><description>partition stream: <c>&lt;prefix&gt;s:{topic}:&lt;p&gt;</c></description></item>
///   <item><description>positions: <c>&lt;prefix&gt;p:{topic}:&lt;consumer&gt;</c></description></item>
///   <item><description>positions meta: <c>&lt;prefix&gt;p:{topic}:&lt;consumer&gt;:meta</c></description></item>
///   <item><description>ownership / leases: <c>&lt;prefix&gt;o:{topic}:&lt;consumer&gt;</c></description></item>
///   <item><description>topic meta: <c>&lt;prefix&gt;m:{topic}</c></description></item>
/// </list>
/// All members are allocation-lean: one string per key, built into a stack buffer where it fits.
/// </remarks>
internal static class StreamKeys
{
    /// <summary>Keys at or below this length are assembled in a stack buffer.</summary>
    private const int StackBufferChars = 256;

    /// <summary>Longest <see cref="int"/> rendering, including the sign.</summary>
    private const int MaxIntChars = 11;

    private const string MetaSuffix = ":meta";

    /// <summary>
    /// Stream key for one partition: <c>s:{topic}:&lt;partition&gt;</c>, or <c>s:topic:&lt;partition&gt;</c>
    /// when <paramref name="coLocate"/> is <see langword="false"/> so partitions spread across slots.
    /// </summary>
    internal static RedisKey Stream(string topic, int partition, bool coLocate = true)
        => Build('s', topic, coLocate, partition, suffix: null);

    /// <summary>Positions hash for a consumer: <c>p:{topic}:&lt;consumer&gt;</c>.</summary>
    internal static RedisKey Positions(string topic, string consumer)
        => Build('p', topic, coLocate: true, consumer, suffix: null);

    /// <summary>Positions metadata hash for a consumer: <c>p:{topic}:&lt;consumer&gt;:meta</c>.</summary>
    internal static RedisKey PositionsMeta(string topic, string consumer)
        => Build('p', topic, coLocate: true, consumer, MetaSuffix);

    /// <summary>Ownership / lease hash for a consumer: <c>o:{topic}:&lt;consumer&gt;</c>.</summary>
    internal static RedisKey Ownership(string topic, string consumer)
        => Build('o', topic, coLocate: true, consumer, suffix: null);

    /// <summary>Topic metadata hash: <c>m:{topic}</c>.</summary>
    internal static RedisKey TopicMeta(string topic)
        => Build('m', topic, coLocate: true, tail: null, suffix: null);

    /// <summary>Glob matching every consumer's positions hash for a topic: <c>p:{topic}:*</c>.</summary>
    internal static string PositionsPattern(string topic)
        => Build('p', topic, coLocate: true, "*", suffix: null);

    /// <summary>Glob matching every partition stream of a topic.</summary>
    internal static string StreamPattern(string topic, bool coLocate = true)
        => Build('s', topic, coLocate, "*", suffix: null);

    private static string Build(char prefix, string topic, bool coLocate, int partition, string? suffix)
    {
        Span<char> digits = stackalloc char[MaxIntChars];
        if (!partition.TryFormat(digits, out int written, provider: CultureInfo.InvariantCulture))
        {
            // Unreachable: MaxIntChars holds every int.
            return Build(prefix, topic, coLocate, partition.ToString(CultureInfo.InvariantCulture), suffix);
        }

        return Build(prefix, topic, coLocate, digits[..written], suffix);
    }

    private static string Build(char prefix, string topic, bool coLocate, string? tail, string? suffix)
        => Build(prefix, topic, coLocate, tail.AsSpan(), suffix, hasTail: tail is not null);

    private static string Build(char prefix, string topic, bool coLocate, ReadOnlySpan<char> tail, string? suffix)
        => Build(prefix, topic, coLocate, tail, suffix, hasTail: true);

    private static string Build(
        char prefix,
        string topic,
        bool coLocate,
        ReadOnlySpan<char> tail,
        string? suffix,
        bool hasTail)
    {
        // namespace + prefix + ':' + ('{' topic '}' | topic) + (':' tail)? + suffix?
        var ns = KeyNamespace.Prefix();

        int length = ns.Length + 2 + topic.Length
            + (coLocate ? 2 : 0)
            + (hasTail ? 1 + tail.Length : 0)
            + (suffix?.Length ?? 0);

        char[]? rented = length > StackBufferChars ? new char[length] : null;
        Span<char> buffer = rented ?? stackalloc char[StackBufferChars];

        int at = 0;
        ns.CopyTo(buffer);
        at += ns.Length;
        buffer[at++] = prefix;
        buffer[at++] = ':';
        if (coLocate)
        {
            buffer[at++] = '{';
        }

        topic.CopyTo(buffer[at..]);
        at += topic.Length;
        if (coLocate)
        {
            buffer[at++] = '}';
        }

        if (hasTail)
        {
            buffer[at++] = ':';
            tail.CopyTo(buffer[at..]);
            at += tail.Length;
        }

        if (suffix is not null)
        {
            suffix.CopyTo(buffer[at..]);
            at += suffix.Length;
        }

        return new string(buffer[..at]);
    }
}
