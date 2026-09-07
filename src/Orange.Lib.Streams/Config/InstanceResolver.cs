using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;
using Orange.Lib.Streams.Errors;

namespace Orange.Lib.Streams.Config;

/// <summary>
/// Where a resolved instance <c>Count</c> or <c>Index</c> came from, in precedence order.
/// </summary>
internal enum InstanceSource
{
    /// <summary>Explicit <see cref="InstanceOptions"/> configuration — tests and local runs.</summary>
    Config = 0,

    /// <summary>The <c>STREAMS_INSTANCE_COUNT</c> / <c>STREAMS_INSTANCE_INDEX</c> environment variables.</summary>
    Environment = 1,

    /// <summary>The trailing <c>-&lt;n&gt;</c> ordinal of a StatefulSet <c>POD_NAME</c>.</summary>
    PodOrdinal = 2,

    /// <summary>Nothing was configured — <c>Count=1, Index=0</c>, this instance owns everything.</summary>
    Fallback = 3,
}

/// <summary>
/// The resolved identity of this process within its instance pool.
/// </summary>
/// <param name="Count">Total number of instances in the pool (always &gt;= 1).</param>
/// <param name="Index">This instance's zero-based index in the pool (always &gt;= 0).</param>
/// <param name="CountSource">Where <paramref name="Count"/> came from.</param>
/// <param name="IndexSource">Where <paramref name="Index"/> came from.</param>
/// <param name="PodName">The observed <c>POD_NAME</c>, or <c>null</c> when unset.</param>
internal readonly record struct InstanceIdentity(
    int Count,
    int Index,
    InstanceSource CountSource,
    InstanceSource IndexSource,
    string? PodName);

/// <summary>
/// A contiguous half-open range of partitions <c>[Start, Start + Length)</c> owned by one instance.
/// </summary>
/// <param name="Start">First owned partition. Meaningless when <paramref name="Length"/> is zero.</param>
/// <param name="Length">Number of owned partitions; zero when this instance owns nothing.</param>
internal readonly record struct PartitionRange(int Start, int Length)
{
    /// <summary>Exclusive end of the range.</summary>
    public int End => this.Start + this.Length;

    /// <summary>True when this instance owns no partitions at all.</summary>
    public bool IsEmpty => this.Length <= 0;

    /// <summary>True when <paramref name="partition"/> falls inside this range.</summary>
    public bool Contains(int partition) => partition >= this.Start && partition < this.End;

    /// <summary>Materialises the owned partition ids, in ascending order.</summary>
    public int[] ToArray()
    {
        if (this.IsEmpty)
        {
            return [];
        }

        var owned = new int[this.Length];
        for (var i = 0; i < this.Length; i++)
        {
            owned[i] = this.Start + i;
        }

        return owned;
    }

    /// <summary>Renders the range as <c>[0,1]</c>, or <c>[]</c> when nothing is owned.</summary>
    public override string ToString()
    {
        if (this.IsEmpty)
        {
            return "[]";
        }

        var sb = new StringBuilder("[");
        for (var i = 0; i < this.Length; i++)
        {
            if (i > 0)
            {
                sb.Append(',');
            }

            sb.Append((this.Start + i).ToString(CultureInfo.InvariantCulture));
        }

        return sb.Append(']').ToString();
    }
}

/// <summary>
/// Resolves instance identity (<c>Count</c>, <c>Index</c>) and the contiguous partition range this
/// instance owns.
///
/// Precedence is strict: explicit <see cref="InstanceOptions"/> config, then the
/// <c>STREAMS_INSTANCE_COUNT</c> / <c>STREAMS_INSTANCE_INDEX</c> environment variables, then the
/// StatefulSet pod ordinal (the trailing <c>-&lt;n&gt;</c> of <c>POD_NAME</c>, with the count still
/// coming from <c>STREAMS_INSTANCE_COUNT</c>), and finally <c>Count=1, Index=0</c>.
///
/// Ambient inputs are never fatal: a missing or malformed <c>POD_NAME</c> or environment variable
/// falls through to the next level and logs, rather than throwing. Only explicit configuration —
/// which a human wrote and can fix — throws <see cref="StreamConfigurationException"/>.
///
/// Ownership is a contiguous balanced range rather than a modulo stripe, because "pod 0 has the
/// first half" is how an operator reasons about a stuck partition at 3am. Getting this wrong means
/// partitions consumed twice or not at all, so the arithmetic lives in exactly one place.
/// </summary>
internal static class InstanceResolver
{
    /// <summary>Environment variable naming the total number of instances.</summary>
    public const string CountVariable = "STREAMS_INSTANCE_COUNT";

    /// <summary>Environment variable naming this instance's index.</summary>
    public const string IndexVariable = "STREAMS_INSTANCE_INDEX";

    /// <summary>Environment variable carrying the Kubernetes pod name (via the downward API).</summary>
    public const string PodNameVariable = "POD_NAME";

    /// <summary>
    /// Resolves identity from configuration and the process environment.
    /// </summary>
    /// <param name="options">Explicit instance configuration, or <c>null</c> when none was supplied.</param>
    /// <param name="logger">Optional logger; malformed ambient values and drift are reported here.</param>
    /// <returns>The resolved <see cref="InstanceIdentity"/>.</returns>
    public static InstanceIdentity Resolve(InstanceOptions? options, ILogger? logger = null)
        => Resolve(options, System.Environment.GetEnvironmentVariable, logger);

    /// <summary>
    /// Resolves identity against a caller-supplied environment lookup, so tests need not mutate the
    /// process environment.
    /// </summary>
    /// <param name="options">Explicit instance configuration, or <c>null</c> when none was supplied.</param>
    /// <param name="environment">Environment variable lookup; returns <c>null</c> for unset names.</param>
    /// <param name="logger">Optional logger; malformed ambient values and drift are reported here.</param>
    /// <returns>The resolved <see cref="InstanceIdentity"/>.</returns>
    /// <exception cref="StreamConfigurationException">Explicit configuration is self-contradictory.</exception>
    public static InstanceIdentity Resolve(
        InstanceOptions? options,
        Func<string, string?> environment,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(environment);

        var podName = Trimmed(environment(PodNameVariable));

        var (count, countSource) = ResolveCount(options, environment, logger);
        var (index, indexSource) = ResolveIndex(options, environment, podName, logger);

        // Explicit config is the one place a contradiction is a human error we can name.
        if (countSource == InstanceSource.Config && indexSource == InstanceSource.Config && index >= count)
        {
            throw new StreamConfigurationException(
                $"Streams:Instances is invalid: Index {index} must be less than Count {count}. " +
                "Indexes are zero-based, so a Count of 2 permits indexes 0 and 1.");
        }

        return new InstanceIdentity(count, index, countSource, indexSource, podName);
    }

    /// <summary>
    /// The contiguous balanced range of partitions owned by <paramref name="index"/>.
    /// Remainder partitions go to the earlier instances, so the ranges tile <c>[0, partitions)</c>
    /// with no gap and no overlap.
    /// </summary>
    /// <param name="partitions">Total partitions on the topic.</param>
    /// <param name="count">Total instances in the pool.</param>
    /// <param name="index">This instance's index.</param>
    /// <returns>The owned range; empty when there are fewer partitions than instances, or when
    /// <paramref name="index"/> is beyond <paramref name="count"/> (a drifted pool).</returns>
    public static PartitionRange Owned(int partitions, int count, int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partitions);
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(index);

        // A pod ordinal past the declared count owns nothing — its partitions belong to nobody,
        // which is exactly the scale-up drift the ownership registry is there to surface.
        if (index >= count)
        {
            return new PartitionRange(partitions, 0);
        }

        var per = partitions / count;
        var rem = partitions % count;
        var start = (index * per) + Math.Min(index, rem);
        var mine = per + (index < rem ? 1 : 0);

        return new PartitionRange(start, mine);
    }

    /// <summary>
    /// The contiguous balanced range owned by <paramref name="identity"/>, logging the two states
    /// that are almost always misconfigurations.
    /// </summary>
    /// <param name="partitions">Total partitions on the topic.</param>
    /// <param name="identity">This instance's resolved identity.</param>
    /// <param name="logger">Optional logger for the surplus-instance warning and the drift error.</param>
    /// <returns>The owned range.</returns>
    public static PartitionRange Owned(int partitions, InstanceIdentity identity, ILogger? logger = null)
    {
        var range = Owned(partitions, identity.Count, identity.Index);

        if (identity.Index >= identity.Count)
        {
            logger?.LogError(
                "Streams: instance index {Index} is beyond instance count {Count} (pod {PodName}); this instance owns no partitions and partitions assigned to it are consumed by nobody. Check that {CountVariable} matches spec.replicas.",
                identity.Index,
                identity.Count,
                identity.PodName ?? "(unset)",
                CountVariable);
        }
        else if (partitions < identity.Count)
        {
            logger?.LogWarning(
                "Streams: {Partitions} partitions across {Count} instances leaves instances {FirstIdle}..{LastIdle} idle; surplus instances own nothing. This is almost always a misconfiguration — reduce replicas or raise Partitions.",
                partitions,
                identity.Count,
                partitions,
                identity.Count - 1);
        }

        return range;
    }

    /// <summary>
    /// Parses the StatefulSet ordinal from a pod name — the digits after the final <c>-</c>.
    /// Never throws: a null, empty, or non-ordinal name (a Deployment's <c>svc-7d4f8b-x2k9p</c>,
    /// say) simply returns <c>false</c> so the caller falls through to the next precedence level.
    /// </summary>
    /// <param name="podName">The pod name, typically from <c>POD_NAME</c>.</param>
    /// <param name="ordinal">The parsed ordinal when this returns <c>true</c>; otherwise zero.</param>
    /// <returns><c>true</c> when <paramref name="podName"/> ends in <c>-&lt;n&gt;</c>.</returns>
    public static bool TryParsePodOrdinal(string? podName, out int ordinal)
    {
        ordinal = 0;

        if (string.IsNullOrWhiteSpace(podName))
        {
            return false;
        }

        var name = podName.Trim();
        var dash = name.LastIndexOf('-');

        // Require something before the dash, and something after it.
        if (dash <= 0 || dash == name.Length - 1)
        {
            return false;
        }

        var suffix = name.AsSpan(dash + 1);

        // Only the segment after the FINAL dash is considered, so "svc--3" is ordinal 3.
        // NumberStyles.None rejects signs, separators and whitespace, so "svc- 3" and "svc-+3"
        // are not ordinals. Overflow is a parse failure, not a wrap.
        return int.TryParse(suffix, NumberStyles.None, CultureInfo.InvariantCulture, out ordinal);
    }

    /// <summary>
    /// Formats the startup ownership line, e.g.
    /// <c>instance 0/2 (pod svc-0) owns partitions [0,1] of 4 | full map: 0-&gt;svc-0 1-&gt;svc-0 2-&gt;svc-1 3-&gt;svc-1</c>.
    /// The full map is what answers "which pod runs which partition" without reading any code.
    /// </summary>
    /// <param name="partitions">Total partitions on the topic.</param>
    /// <param name="identity">This instance's resolved identity.</param>
    /// <returns>A single-line, log-safe description of the whole ownership map.</returns>
    public static string DescribeOwnership(int partitions, InstanceIdentity identity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partitions);

        var range = Owned(partitions, Math.Max(identity.Count, 1), Math.Max(identity.Index, 0));

        var sb = new StringBuilder();
        sb.Append("instance ")
          .Append(identity.Index.ToString(CultureInfo.InvariantCulture))
          .Append('/')
          .Append(identity.Count.ToString(CultureInfo.InvariantCulture))
          .Append(" (pod ")
          .Append(identity.PodName ?? "unknown")
          .Append(") owns partitions ")
          .Append(range.ToString())
          .Append(" of ")
          .Append(partitions.ToString(CultureInfo.InvariantCulture))
          .Append(" | full map:");

        for (var p = 0; p < partitions; p++)
        {
            sb.Append(' ')
              .Append(p.ToString(CultureInfo.InvariantCulture))
              .Append("->")
              .Append(PodLabel(identity, OwnerOf(p, partitions, identity.Count)));
        }

        return sb.ToString();
    }

    /// <summary>
    /// The instance index that owns <paramref name="partition"/> under the contiguous balanced rule.
    /// </summary>
    /// <param name="partition">Partition id in <c>[0, partitions)</c>.</param>
    /// <param name="partitions">Total partitions on the topic.</param>
    /// <param name="count">Total instances in the pool.</param>
    /// <returns>The owning instance index.</returns>
    public static int OwnerOf(int partition, int partitions, int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(partition, partitions);
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);

        var per = partitions / count;
        var rem = partitions % count;

        // The first `rem` instances hold `per + 1` partitions each; the rest hold `per`.
        var fat = (per + 1) * rem;
        return partition < fat
            ? partition / (per + 1)
            : rem + ((partition - fat) / per);
    }

    private static (int Count, InstanceSource Source) ResolveCount(
        InstanceOptions? options,
        Func<string, string?> environment,
        ILogger? logger)
    {
        if (options?.Count is int configured)
        {
            if (configured < 1)
            {
                throw new StreamConfigurationException(
                    $"Streams:Instances:Count is {configured}; it must be at least 1.");
            }

            return (configured, InstanceSource.Config);
        }

        var raw = Trimmed(environment(CountVariable));
        if (raw is not null)
        {
            if (int.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) && parsed >= 1)
            {
                return (parsed, InstanceSource.Environment);
            }

            logger?.LogWarning(
                "Streams: {CountVariable} is {Value}, which is not a positive integer; falling back to a single instance owning every partition.",
                CountVariable,
                raw);
        }

        return (1, InstanceSource.Fallback);
    }

    private static (int Index, InstanceSource Source) ResolveIndex(
        InstanceOptions? options,
        Func<string, string?> environment,
        string? podName,
        ILogger? logger)
    {
        if (options?.Index is int configured)
        {
            if (configured < 0)
            {
                throw new StreamConfigurationException(
                    $"Streams:Instances:Index is {configured}; it must be zero or greater.");
            }

            return (configured, InstanceSource.Config);
        }

        var raw = Trimmed(environment(IndexVariable));
        if (raw is not null)
        {
            if (int.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed))
            {
                return (parsed, InstanceSource.Environment);
            }

            logger?.LogWarning(
                "Streams: {IndexVariable} is {Value}, which is not a non-negative integer; falling back to the pod ordinal.",
                IndexVariable,
                raw);
        }

        if (TryParsePodOrdinal(podName, out var ordinal))
        {
            return (ordinal, InstanceSource.PodOrdinal);
        }

        if (podName is not null)
        {
            logger?.LogInformation(
                "Streams: pod name {PodName} has no trailing StatefulSet ordinal, so instance index falls back to 0. Partitioned consumers must run as a StatefulSet to keep ownership stable across restarts.",
                podName);
        }

        return (0, InstanceSource.Fallback);
    }

    /// <summary>
    /// The pod name to print for an instance index: derived from this pod's own name when it carries
    /// an ordinal (<c>svc-0</c> to <c>svc-1</c>), our own name for our own index, else <c>i&lt;n&gt;</c>.
    /// </summary>
    private static string PodLabel(InstanceIdentity identity, int index)
    {
        if (identity.PodName is { Length: > 0 } pod)
        {
            if (index == identity.Index)
            {
                return pod;
            }

            if (TryParsePodOrdinal(pod, out _))
            {
                var dash = pod.LastIndexOf('-');
                return string.Concat(pod.AsSpan(0, dash + 1), index.ToString(CultureInfo.InvariantCulture));
            }
        }

        return "i" + index.ToString(CultureInfo.InvariantCulture);
    }

    private static string? Trimmed(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value.Trim();
}
