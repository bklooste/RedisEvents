using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;
using RedisEvents.Config;
using RedisEvents.Diagnostics;
using RedisEvents.Errors;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Ownership;

/// <summary>
/// One instance's claim on one partition, as recorded in <c>o:{topic}:&lt;consumer&gt;</c>.
/// </summary>
/// <param name="Partition">The claimed partition.</param>
/// <param name="InstanceId">Identity of the claiming instance — stable across a restart of the same
/// pod (see <see cref="OwnershipRegistry.StableInstanceId"/>). Two pods that both believe they are
/// index 0 are distinguishable only by this.</param>
/// <param name="PodName">The claiming pod's name — what an operator actually recognises.</param>
public readonly record struct PartitionOwner(int Partition, Guid InstanceId, string PodName)
{
    /// <summary>Separator between the instance id and the pod name in the stored value.</summary>
    internal const char Separator = '|';

    /// <summary>The stored hash value: <c>&lt;instanceId&gt;|&lt;podName&gt;</c>.</summary>
    public string Format()
        => string.Concat(this.InstanceId.ToString("D", CultureInfo.InvariantCulture), "|", this.PodName);

    /// <summary>
    /// Parses a stored value. A value that is not <c>&lt;guid&gt;|&lt;pod&gt;</c> still yields an
    /// owner — with <see cref="Guid.Empty"/> and the raw text as the pod name — because an
    /// unreadable claim is still somebody else's claim, and hiding it would hide a real conflict.
    /// </summary>
    /// <param name="partition">The partition the value was stored against.</param>
    /// <param name="value">The raw hash value.</param>
    /// <returns>The parsed owner.</returns>
    public static PartitionOwner Parse(int partition, string value)
    {
        ArgumentNullException.ThrowIfNull(value);

        var at = value.IndexOf(Separator, StringComparison.Ordinal);
        if (at <= 0)
        {
            return new PartitionOwner(partition, Guid.Empty, value);
        }

        return Guid.TryParseExact(value.AsSpan(0, at), "D", out var id)
            ? new PartitionOwner(partition, id, value[(at + 1)..])
            : new PartitionOwner(partition, Guid.Empty, value);
    }

    /// <summary>Renders the owner as <c>pod(instance-prefix)</c> for log lines.</summary>
    public override string ToString()
        => this.InstanceId == Guid.Empty
            ? this.PodName
            : $"{this.PodName}({this.InstanceId.ToString("D", CultureInfo.InvariantCulture)[..8]})";
}

/// <summary>
/// A partition this instance believes it owns that is recorded against a different instance id —
/// the signature of a partitioned consumer running on a Deployment instead of a StatefulSet, where
/// every pod falls back to <c>Index=0, Count=1</c> and processes every message.
/// </summary>
/// <param name="Partition">The contested partition.</param>
/// <param name="Expected">The instance id that expected to own it — ours.</param>
/// <param name="Actual">The owner actually recorded in Redis.</param>
public readonly record struct ContestedPartition(int Partition, Guid Expected, PartitionOwner Actual);

/// <summary>
/// A point-in-time view of <c>o:{topic}:&lt;consumer&gt;</c>: who owns what, which partitions nobody
/// owns, and which are claimed twice.
/// </summary>
/// <param name="Topic">Topic the map describes.</param>
/// <param name="Consumer">Consumer name the map describes.</param>
/// <param name="Partitions">Total partitions on the topic; the gap check runs over <c>[0, Partitions)</c>.</param>
/// <param name="Owners">Partition to recorded owner, for every partition currently claimed.</param>
/// <param name="Unowned">Partitions in <c>[0, Partitions)</c> with no live claim, ascending.</param>
/// <param name="Contested">Partitions claimed by an instance other than the observer, ascending.
/// Always empty for a read-only observation, which has no claim of its own to compare against.</param>
/// <param name="ObservedUtc">When the map was read.</param>
public sealed record OwnershipMap(
    string Topic,
    string Consumer,
    int Partitions,
    IReadOnlyDictionary<int, PartitionOwner> Owners,
    IReadOnlyList<int> Unowned,
    IReadOnlyList<ContestedPartition> Contested,
    DateTimeOffset ObservedUtc)
{
    /// <summary>True when a partition is unowned or contested — the health check reports Degraded.</summary>
    public bool IsDegraded => this.Unowned.Count > 0 || this.Contested.Count > 0;

    /// <summary>
    /// The single-line ownership map an operator reads at 3am:
    /// <c>0-&gt;svc-0 1-&gt;svc-0 2-&gt;svc-1 3-&gt;(none)</c>.
    /// </summary>
    /// <returns>A log-safe, single-line rendering of every partition's owner.</returns>
    public string Describe()
    {
        var sb = new StringBuilder("full map:");
        for (var p = 0; p < this.Partitions; p++)
        {
            sb.Append(' ')
              .Append(p.ToString(CultureInfo.InvariantCulture))
              .Append("->")
              .Append(this.Owners.TryGetValue(p, out var owner) ? owner.ToString() : "(none)");
        }

        return sb.ToString();
    }
}

/// <summary>
/// Options for one <see cref="OwnershipRegistry"/> — one topic, one consumer, one instance.
/// </summary>
internal sealed record OwnershipRegistryOptions
{
    /// <summary>Topic whose ownership hash is written.</summary>
    public required string Topic { get; init; }

    /// <summary>Consumer name; ownership is per consumer, so two consumers of one topic do not collide.</summary>
    public required string Consumer { get; init; }

    /// <summary>Total partitions on the topic. The gap check runs over <c>[0, Partitions)</c>.</summary>
    public required int Partitions { get; init; }

    /// <summary>Partitions this instance claims — normally <c>InstanceResolver.Owned(...).ToArray()</c>.</summary>
    public int[] OwnedPartitions { get; init; } = [];

    /// <summary>Pod name for the claim value; <c>POD_NAME</c>, or a local placeholder.</summary>
    public string PodName { get; init; } = "unknown";

    /// <summary>Per-field TTL in seconds. A departed instance's claims expire on their own.</summary>
    public int TtlSeconds { get; init; } = 30;

    /// <summary>Renewal interval in seconds; must be comfortably shorter than <see cref="TtlSeconds"/>.</summary>
    public int RenewSeconds { get; init; } = 10;

    /// <summary>
    /// Instance id written into the claim. Defaults to
    /// <see cref="OwnershipRegistry.StableInstanceId"/> — the identity of this <em>pod</em>, not of
    /// this process — so a restart is recognisably the same writer. Tests override it to simulate a
    /// second pod.
    /// </summary>
    public Guid InstanceId { get; init; } = OwnershipRegistry.StableInstanceId;

    /// <summary>
    /// <see cref="InstanceMode.Static"/> (the default) reports the ownership the configured
    /// arithmetic decided and flags disagreement; <see cref="InstanceMode.Lease"/> makes the
    /// registry authoritative — it claims free partitions itself, and
    /// <see cref="OwnershipRegistryOptions.OwnedPartitions"/> must be empty.
    /// </summary>
    public InstanceMode Mode { get; init; } = InstanceMode.Static;

    /// <summary>
    /// Called after a <see cref="InstanceMode.Lease"/> refresh that changed the held set — a
    /// partition claimed, or one lost because a renewal did not land before its TTL. The argument is
    /// a fresh, ascending array the callee may keep. Never called in
    /// <see cref="InstanceMode.Static"/> mode, where ownership does not move.
    /// </summary>
    /// <remarks>
    /// It runs on the renewal loop, so it must return promptly: anything that takes as long as a
    /// consumer drain belongs on a task of its own. An exception out of it is logged and swallowed —
    /// losing the renewal loop would cost this instance every lease it holds.
    /// </remarks>
    public Action<int[]>? OnLeaseChanged { get; init; }
}

/// <summary>
/// Records which instance owns which partition in <c>o:{topic}:&lt;consumer&gt;</c>, and validates the
/// whole picture on every claim.
/// </summary>
/// <remarks>
/// <para>
/// <c>STREAMS_INSTANCE_COUNT</c> is a hand-maintained copy of <c>spec.replicas</c>, and drift between
/// them fails silently: scale 2 to 3 without updating it and partitions 2 and 3 are consumed by
/// nobody; run a partitioned consumer on a Deployment and every pod falls back to index 0 and
/// consumes everything twice. Neither raises an error anywhere else, so the registry is what makes
/// them visible.
/// </para>
/// <para>
/// Claims carry a per-field TTL (<c>HEXPIRE</c>, Redis 7.4+), so a departed instance's claims simply
/// vanish. There are no heartbeat timestamps to parse and no read-side staleness filter to get wrong.
/// </para>
/// <para>
/// <b>Two modes, one hash.</b> In <see cref="InstanceMode.Static"/> — the default, and what every
/// service runs today — the registry is <em>observational</em>: it reports what the configured
/// ownership arithmetic decided and flags disagreement, and never reassigns a partition. In
/// <see cref="InstanceMode.Lease"/> it is <em>authoritative</em>: the drift above cannot happen,
/// because nothing declares a <c>Count</c> at all — an instance claims free partitions with
/// <c>HSETNX</c>, renews what it holds, drops what it fails to renew, and the live member count is
/// read out of the hash. That is the mode to reach for when something actually scales past one
/// replica on a plain Deployment; see <see cref="LeaseScript"/> for why it claims per partition
/// rather than ranking the members.
/// </para>
/// <para>
/// Redis failures are logged and swallowed: an unreachable ownership hash must not stop a consumer
/// that is otherwise healthy, and the next renew retries.
/// </para>
/// </remarks>
internal sealed class OwnershipRegistry : IAsyncDisposable
{
    private static readonly ConcurrentDictionary<string, OwnershipMap> Observed = new(StringComparer.Ordinal);

    /// <summary>
    /// Prefix of the per-instance presence field, <c>i:&lt;instanceId&gt;</c>.
    /// </summary>
    /// <remarks>
    /// The partition fields answer "who owns partition 3", which is last-writer-wins while two pods
    /// are fighting over it. This answers "is instance X still here at all", which nobody else
    /// overwrites, so the position flusher's liveness cross-check (R-01, option B) has an
    /// unambiguous signal. It carries the same <c>HEXPIRE</c> TTL as a claim, so a departed pod's
    /// presence lapses with its claims. It is not a number, so <see cref="ReadOwners"/> — and the
    /// admin endpoint that uses it — skip it.
    /// </remarks>
    internal const string PresencePrefix = "i:";

    /// <summary>
    /// Prefix of the field used once at startup to prove the server understands <c>HEXPIRE</c>.
    /// </summary>
    /// <remarks>
    /// Suffixed with the instance id so two registries probing the same hash at once cannot delete
    /// each other's probe field and read the resulting <c>NoSuchField</c> as an incapable server.
    /// Not a number, so <see cref="ReadOwners"/> skips it in the window before it is deleted.
    /// </remarks>
    private const string ProbeFieldPrefix = "__hexpire-probe:";

    /// <summary>
    /// Compare-and-delete: drop only the fields this instance still holds.
    /// </summary>
    /// <remarks>
    /// R-15. An unconditional <c>HDEL</c> made a departing pod delete whatever was in its fields —
    /// including a rival's live claims in exactly the contested case the registry exists to report,
    /// which then read as an ownership <em>gap</em> for a whole TTL.
    /// </remarks>
    private const string ReleaseScript = """
        local removed = 0
        for i = 2, #ARGV do
          if redis.call('HGET', KEYS[1], ARGV[i]) == ARGV[1] then
            redis.call('HDEL', KEYS[1], ARGV[i])
            removed = removed + 1
          end
        end
        return removed
        """;

    /// <summary>
    /// One <c>Lease</c>-mode cycle, server-side and atomic: renew what is still ours, give back
    /// anything above this instance's fair share, then claim what is free.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Claim per partition, never rank per member.</b> The alternative — list the live members,
    /// sort them, take your index — reshuffles most partitions every time a member with a random id
    /// joins in the middle of the sort order. Claiming a field at a time has no such churn, for the
    /// same amount of code.
    /// </para>
    /// <para>
    /// <b>The fair share is what makes joining work.</b> Purely greedy claiming would let whichever
    /// pod started first hold every partition and renew it forever, and a pod that joined later
    /// would find nothing free and idle for good. So each cycle counts the live
    /// <c>i:&lt;instanceId&gt;</c> presence fields — that is <c>Count</c> observed rather than
    /// declared — and caps this instance at <c>ceil(partitions / members)</c>, releasing the
    /// surplus. The ceiling means the caps always sum to at least the partition count, so no
    /// partition is left permanently unclaimable.
    /// </para>
    /// <para>
    /// One script, one round trip, and every claim carries its TTL before the script returns: there
    /// is no window in which a field exists without an expiry, and <c>HSETNX</c> is what makes two
    /// instances claiming one partition at once impossible rather than merely unlikely.
    /// </para>
    /// </remarks>
    private const string LeaseScript = """
        local me = ARGV[1]
        local ttl = tonumber(ARGV[2])
        local n = tonumber(ARGV[3])
        local presence = ARGV[4]

        redis.call('HSET', KEYS[1], presence, me)
        redis.call('HEXPIRE', KEYS[1], ttl, 'FIELDS', 1, presence)

        local members = 0
        local names = redis.call('HKEYS', KEYS[1])
        for i = 1, #names do
          if string.sub(names[i], 1, 2) == 'i:' then members = members + 1 end
        end
        if members < 1 then members = 1 end
        local cap = math.ceil(n / members)

        local held = {}
        for p = 0, n - 1 do
          if redis.call('HGET', KEYS[1], tostring(p)) == me then held[#held + 1] = p end
        end

        while #held > cap do
          redis.call('HDEL', KEYS[1], tostring(table.remove(held)))
        end

        for i = 1, #held do
          redis.call('HEXPIRE', KEYS[1], ttl, 'FIELDS', 1, tostring(held[i]))
        end

        if #held < cap then
          for p = 0, n - 1 do
            if #held >= cap then break end
            local f = tostring(p)
            if redis.call('HSETNX', KEYS[1], f, me) == 1 then
              redis.call('HEXPIRE', KEYS[1], ttl, 'FIELDS', 1, f)
              held[#held + 1] = p
            end
          end
        end

        table.sort(held)
        return held
        """;

    private readonly IConnectionMultiplexer redis;
    private readonly OwnershipRegistryOptions options;
    private readonly ILogger? logger;
    private readonly RedisKey key;
    private readonly RedisValue[] fields;
    private readonly RedisValue[] leasedFields;
    private readonly RedisValue[] releaseArgs;
    private readonly RedisValue[] leaseArgs;
    private readonly HashEntry[] claims;
    private readonly HashSet<int> owned;
    private readonly TimeSpan ttl;
    private readonly TimeSpan renewInterval;
    private readonly string metricKey;
    private readonly bool lease;

    private CancellationTokenSource? renewals;
    private Task? renewLoop;
    private bool described;
    private int[] held = [];

    /// <summary>
    /// Creates a registry for one topic/consumer/instance triple.
    /// </summary>
    /// <param name="redis">Shared multiplexer — ownership is a write, so never the reader connection.</param>
    /// <param name="options">Topic, consumer, partition counts and identity.</param>
    /// <param name="logger">Optional logger; gaps and overlaps are reported at Error.</param>
    /// <exception cref="StreamConfigurationException">The options cannot produce working claims.</exception>
    public OwnershipRegistry(IConnectionMultiplexer redis, OwnershipRegistryOptions options, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(redis);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Consumer);

        if (options.Partitions < 1)
        {
            throw new StreamConfigurationException(
                $"Ownership registry for topic '{options.Topic}' was given {options.Partitions} partitions; it must be at least 1.");
        }

        if (options.TtlSeconds < 1)
        {
            throw new StreamConfigurationException(
                $"Streams:Instances:LeaseTtlSeconds is {options.TtlSeconds}; it must be at least 1 second.");
        }

        if (options.RenewSeconds < 1 || options.RenewSeconds >= options.TtlSeconds)
        {
            throw new StreamConfigurationException(
                $"Streams:Instances:LeaseRenewSeconds is {options.RenewSeconds} against a TTL of {options.TtlSeconds}s; " +
                "the renew interval must be at least 1 second and shorter than the TTL, or claims expire between renewals.");
        }

        if (options.Mode == InstanceMode.Lease && options.OwnedPartitions.Length > 0)
        {
            throw new StreamConfigurationException(
                $"Ownership registry for topic '{options.Topic}' is in Lease mode but was handed {options.OwnedPartitions.Length} " +
                "pre-assigned partitions. In Lease mode the registry decides what this instance owns by claiming free fields; " +
                "an assignment computed from Instances:Count / Instances:Index is exactly what the mode exists to replace.");
        }

        this.redis = redis;
        this.options = options;
        this.logger = logger;
        this.key = StreamKeys.Ownership(options.Topic, options.Consumer);
        this.ttl = TimeSpan.FromSeconds(options.TtlSeconds);
        this.renewInterval = TimeSpan.FromSeconds(options.RenewSeconds);
        this.metricKey = $"{options.Topic}:{options.Consumer}";
        this.lease = options.Mode == InstanceMode.Lease;

        this.owned = [.. options.OwnedPartitions];
        var mine = new int[this.owned.Count];
        this.owned.CopyTo(mine);
        Array.Sort(mine);

        var value = new PartitionOwner(0, options.InstanceId, options.PodName).Format();
        this.fields = new RedisValue[mine.Length];
        for (var i = 0; i < mine.Length; i++)
        {
            this.fields[i] = mine[i].ToString(CultureInfo.InvariantCulture);
        }

        // The presence field rides along with the partition claims: same value, same batch, same
        // TTL, so "this instance is alive" cannot drift out of step with what it claims.
        var leased = mine.Length == 0 ? 0 : mine.Length + 1;
        this.leasedFields = new RedisValue[leased];
        this.claims = new HashEntry[leased];
        this.releaseArgs = new RedisValue[leased == 0 ? 0 : leased + 1];

        for (var i = 0; i < mine.Length; i++)
        {
            this.leasedFields[i] = this.fields[i];
            this.claims[i] = new HashEntry(this.fields[i], value);
        }

        if (leased > 0)
        {
            this.leasedFields[mine.Length] = PresenceField(options.InstanceId);
            this.claims[mine.Length] = new HashEntry(this.leasedFields[mine.Length], value);

            this.releaseArgs[0] = value;
            for (var i = 0; i < leased; i++)
            {
                this.releaseArgs[i + 1] = this.leasedFields[i];
            }
        }

        // Lease mode claims nothing up front, so all of the above is empty; what it needs instead is
        // the four arguments its cycle script runs on, built once here rather than per renewal.
        this.leaseArgs = this.lease
            ?
            [
                value,
                options.TtlSeconds,
                options.Partitions,
                PresenceField(options.InstanceId),
            ]
            : [];
    }

    /// <summary>
    /// A GUID minted once per process. This is the fallback identity for a process that nothing in
    /// its environment identifies — a laptop or a test — and is <em>not</em> what claims are stamped
    /// with in a cluster; see <see cref="StableInstanceId"/>.
    /// </summary>
    public static Guid ProcessInstanceId { get; } = Guid.NewGuid();

    /// <summary>
    /// The identity this <em>instance</em> writes into ownership claims and positions — stable
    /// across a restart of the same pod.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>R-01, option A.</b> This used to be <see cref="ProcessInstanceId"/>, which is minted fresh
    /// on every start. A pod that bounced therefore found a stranger's id in the position fields it
    /// owned and stood its own partitions down on the first flush of every new process — finding
    /// P0-1. The identity of a writer is the pod, not the process: two lives of <c>svc-1</c> are one
    /// writer, and nothing is lost by saying so, because the case A gives up ("same ordinal, two
    /// live processes") cannot happen in a StatefulSet and, in the Deployment shape the README warns
    /// about, the two pods have different names and so still differ here.
    /// </para>
    /// <para>
    /// Derived from <c>POD_NAME</c>, else from <c>STREAMS_INSTANCE_INDEX</c> plus the machine name.
    /// When nothing identifies the instance — a laptop, a test — there is no stable identity to be
    /// had and this falls back to <see cref="ProcessInstanceId"/>, which keeps two processes on one
    /// developer machine distinguishable rather than silently merging them.
    /// </para>
    /// </remarks>
    public static Guid StableInstanceId { get; } = ResolveInstanceId(Environment.GetEnvironmentVariable);

    /// <summary>
    /// The instance id a pod of the given name runs under. Deterministic: the same name always
    /// yields the same id, in this process and in any other.
    /// </summary>
    /// <param name="podName">The pod's name — its <c>POD_NAME</c>. <see langword="null"/> or blank
    /// means "nothing identifies this instance" and yields <see cref="ProcessInstanceId"/>.</param>
    /// <returns>The instance id to stamp on claims and positions.</returns>
    public static Guid InstanceIdFor(string? podName)
        => string.IsNullOrWhiteSpace(podName) ? ProcessInstanceId : DeriveInstanceId(podName);

    /// <summary>The most recent map this registry observed, or <c>null</c> before the first claim.</summary>
    public OwnershipMap? Latest { get; private set; }

    /// <summary>Instance id written by this registry.</summary>
    public Guid InstanceId => this.options.InstanceId;

    /// <summary>
    /// The partitions this instance holds, ascending. In <see cref="InstanceMode.Static"/> mode that
    /// is the configured assignment and never changes; in <see cref="InstanceMode.Lease"/> mode it is
    /// whatever the last cycle claimed and renewed, and it changes as instances come and go — read it
    /// again rather than caching it, or react to
    /// <see cref="OwnershipRegistryOptions.OnLeaseChanged"/>.
    /// </summary>
    public IReadOnlyList<int> Held
        => this.lease ? Volatile.Read(ref this.held) : this.options.OwnedPartitions;

    /// <summary>
    /// Every map observed by a registry in this process, for the health check and the admin endpoint.
    /// </summary>
    /// <returns>One map per topic/consumer pair claimed in this process.</returns>
    public static IReadOnlyList<OwnershipMap> Snapshots() => [.. Observed.Values];

    /// <summary>
    /// Whether any registry in this process last saw a gap or an overlap — the health check reports
    /// Degraded when this is true.
    /// </summary>
    /// <returns><c>true</c> when at least one observed map is degraded.</returns>
    public static bool AnyDegraded()
    {
        foreach (var map in Observed.Values)
        {
            if (map.IsDegraded)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Looks up the last observed map for one topic/consumer pair.</summary>
    /// <param name="topic">Topic name.</param>
    /// <param name="consumer">Consumer name.</param>
    /// <param name="map">The observed map when this returns <c>true</c>.</param>
    /// <returns><c>true</c> when this process has claimed that pair at least once.</returns>
    public static bool TryGetSnapshot(string topic, string consumer, [NotNullWhen(true)] out OwnershipMap? map)
        => Observed.TryGetValue($"{topic}:{consumer}", out map);

    /// <summary>
    /// Claims this instance's partitions and starts the renewal loop.
    /// </summary>
    /// <param name="ct">Cancellation for the initial claim.</param>
    /// <returns>A task that completes once the first claim has been attempted.</returns>
    /// <remarks>
    /// A Redis failure is logged and swallowed — the first claim is best effort and the loop
    /// retries. A server that cannot <c>HEXPIRE</c> is not a failure but a misconfiguration, and
    /// that one does throw; see <see cref="ProbeHashFieldExpiryAsync"/>.
    /// </remarks>
    /// <exception cref="StreamConfigurationException">The server cannot expire hash fields.</exception>
    public async Task StartAsync(CancellationToken ct = default)
    {
        if (this.renewals is not null)
        {
            return;
        }

        // Before anything is claimed, and before the renewal source exists, so a server that cannot
        // expire fields leaves nothing running behind it.
        await this.ProbeHashFieldExpiryAsync(ct).ConfigureAwait(false);

        this.renewals = new CancellationTokenSource();

        try
        {
            await this.RefreshAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            this.logger?.LogError(
                ex,
                "Streams: could not claim partition ownership for topic {Topic} consumer {Consumer} at startup; ownership drift will go undetected until the next renew.",
                this.options.Topic,
                this.options.Consumer);
        }

        this.renewLoop = Task.Run(() => this.RenewLoopAsync(this.renewals.Token), CancellationToken.None);
    }

    /// <summary>
    /// Stops renewing and drops this instance's claims, so a restarting pod does not contend with
    /// itself for the remainder of the TTL. Best effort.
    /// </summary>
    /// <param name="ct">Cancellation for the release.</param>
    /// <returns>A task that completes once the loop has stopped and the release was attempted.</returns>
    public async Task StopAsync(CancellationToken ct = default)
    {
        if (this.renewals is null)
        {
            return;
        }

        await this.renewals.CancelAsync().ConfigureAwait(false);

        if (this.renewLoop is not null)
        {
            try
            {
                await this.renewLoop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }

            this.renewLoop = null;
        }

        this.renewals.Dispose();
        this.renewals = null;

        await this.ReleaseAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// One ownership cycle: in <see cref="InstanceMode.Static"/> mode it writes this instance's
    /// claims and checks the hash for gaps and overlaps; in <see cref="InstanceMode.Lease"/> mode it
    /// renews, releases its surplus, and claims whatever is free.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The map as observed after the cycle.</returns>
    public async Task<OwnershipMap> RefreshAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        return this.lease
            ? await this.RefreshLeaseAsync(ct).ConfigureAwait(false)
            : await this.RefreshStaticAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// One <see cref="InstanceMode.Lease"/> cycle: renew, give back the surplus, claim what is free,
    /// then read the whole hash back to report the map.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The map as observed after the cycle.</returns>
    private async Task<OwnershipMap> RefreshLeaseAsync(CancellationToken ct)
    {
        var db = this.redis.GetDatabase();

        var result = await db.ScriptEvaluateAsync(LeaseScript, [this.key], this.leaseArgs).ConfigureAwait(false);
        var claimed = (int[]?)result ?? [];

        ct.ThrowIfCancellationRequested();
        var after = await db.HashGetAllAsync(this.key).ConfigureAwait(false);

        var owners = ReadOwners(after);
        var members = CountMembers(after);

        // A gap here is not the drift Static mode reports: it is a lease that has lapsed and has not
        // been picked up yet, which the next cycle of some instance fixes on its own.
        var unowned = new List<int>();
        for (var p = 0; p < this.options.Partitions; p++)
        {
            if (!owners.ContainsKey(p))
            {
                unowned.Add(p);
            }
        }

        var map = new OwnershipMap(
            this.options.Topic,
            this.options.Consumer,
            this.options.Partitions,
            owners,
            unowned,
            [],
            DateTimeOffset.UtcNow);

        this.Latest = map;
        Observed[this.metricKey] = map;

        StreamsDiagnostics.SetUnownedPartitions(this.metricKey, unowned.Count);

        // Never contested in this mode: a claim is only ever taken with HSETNX on a field nobody
        // holds, so two instances cannot both believe they own one partition.
        StreamsDiagnostics.SetContestedPartitions(this.metricKey, 0);

        var previous = Volatile.Read(ref this.held);
        var changed = !SameSet(previous, claimed);

        if (changed)
        {
            Volatile.Write(ref this.held, claimed);
        }

        this.ReportLease(map, previous, claimed, members, changed);

        if (changed && this.options.OnLeaseChanged is { } notify)
        {
            try
            {
                notify(claimed);
            }
            catch (Exception ex)
            {
                // Swallowed on purpose: this runs on the renewal loop, and losing that loop would
                // cost this instance every lease it holds within one TTL.
                this.logger?.LogError(
                    ex,
                    "Streams: the lease-change callback for topic {Topic} consumer {Consumer} threw; the leases themselves are unaffected, but this instance's workers may now be reading the wrong partitions.",
                    this.options.Topic,
                    this.options.Consumer);
            }
        }

        return map;
    }

    /// <summary>
    /// One <see cref="InstanceMode.Static"/> cycle: write the configured claims, read the hash back,
    /// and check it for gaps and overlaps.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The map as observed after the claim.</returns>
    private async Task<OwnershipMap> RefreshStaticAsync(CancellationToken ct)
    {
        var db = this.redis.GetDatabase();

        // Read first: HSET overwrites, so a competing claim on one of our partitions is only
        // visible before we stamp our own id over it.
        var before = await db.HashGetAllAsync(this.key).ConfigureAwait(false);

        if (this.claims.Length > 0)
        {
            ct.ThrowIfCancellationRequested();

            // HSET then HEXPIRE in one batch, so the window in which a field exists without a TTL
            // is a single round trip rather than two.
            var batch = db.CreateBatch();
            var set = batch.HashSetAsync(this.key, this.claims);
            var expire = batch.HashFieldExpireAsync(this.key, this.leasedFields, this.ttl, ExpireWhen.Always);
            batch.Execute();
            await Task.WhenAll(set, expire).ConfigureAwait(false);
        }

        ct.ThrowIfCancellationRequested();
        var after = await db.HashGetAllAsync(this.key).ConfigureAwait(false);

        var map = this.Evaluate(before, after);
        this.Latest = map;
        Observed[this.metricKey] = map;

        StreamsDiagnostics.SetUnownedPartitions(this.metricKey, map.Unowned.Count);
        StreamsDiagnostics.SetContestedPartitions(this.metricKey, map.Contested.Count);

        this.Report(map);
        return map;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await this.StopAsync(CancellationToken.None).ConfigureAwait(false);
        StreamsDiagnostics.SetUnownedPartitions(this.metricKey, 0);
        StreamsDiagnostics.SetContestedPartitions(this.metricKey, 0);
        Observed.TryRemove(this.metricKey, out _);
    }

    /// <summary>
    /// The compare-and-delete arguments a departing Lease-mode instance releases with: its claim
    /// value first, then every field it currently holds and its presence field.
    /// </summary>
    /// <returns>The script arguments, or an empty array when it holds nothing.</returns>
    /// <remarks>
    /// Built per call rather than cached, because unlike Static mode the held set moves. Handing a
    /// lease back is what lets the next instance pick the partition up immediately instead of after
    /// a whole TTL of silence.
    /// </remarks>
    private RedisValue[] LeaseReleaseArgs()
    {
        var mine = Volatile.Read(ref this.held);
        var args = new RedisValue[mine.Length + 2];

        args[0] = new PartitionOwner(0, this.options.InstanceId, this.options.PodName).Format();

        for (var i = 0; i < mine.Length; i++)
        {
            args[i + 1] = mine[i].ToString(CultureInfo.InvariantCulture);
        }

        args[^1] = PresenceField(this.options.InstanceId);
        return args;
    }

    /// <summary>The presence field one instance writes into the ownership hash.</summary>
    /// <param name="instanceId">The instance.</param>
    /// <returns><c>i:&lt;instanceId&gt;</c>.</returns>
    internal static RedisValue PresenceField(Guid instanceId)
        => string.Concat(PresencePrefix, instanceId.ToString("D", CultureInfo.InvariantCulture));

    /// <summary>
    /// Resolves the instance identity from an environment lookup, the way
    /// <see cref="StableInstanceId"/> does from the real environment.
    /// </summary>
    /// <param name="environment">Environment variable lookup; <c>null</c> for unset names.</param>
    /// <returns>The instance id, or <see cref="ProcessInstanceId"/> when nothing identifies it.</returns>
    internal static Guid ResolveInstanceId(Func<string, string?> environment)
    {
        ArgumentNullException.ThrowIfNull(environment);

        // Same precedence the partition arithmetic uses (InstanceResolver): the pod name is the
        // strongest identity available, and the explicit index is the fallback for anything not
        // running as a StatefulSet.
        if (environment(InstanceResolver.PodNameVariable) is { Length: > 0 } pod)
        {
            return DeriveInstanceId(pod);
        }

        if (environment(InstanceResolver.IndexVariable) is { Length: > 0 } index)
        {
            return DeriveInstanceId(string.Concat(Environment.MachineName, "#", index));
        }

        return ProcessInstanceId;
    }

    /// <summary>
    /// How many instances the hash currently shows as alive — one <c>i:&lt;instanceId&gt;</c>
    /// presence field each, expiring with the same TTL as a claim. This is <c>Count</c> observed
    /// rather than declared, which is the whole point of Lease mode.
    /// </summary>
    /// <param name="entries">The ownership hash as read back.</param>
    /// <returns>The number of live members; at least one, since the caller has just written its own.</returns>
    internal static int CountMembers(HashEntry[] entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var members = 0;
        foreach (var entry in entries)
        {
            if (((string?)entry.Name)?.StartsWith(PresencePrefix, StringComparison.Ordinal) == true)
            {
                members++;
            }
        }

        return members < 1 ? 1 : members;
    }

    /// <summary>Whether two ascending partition arrays hold the same partitions.</summary>
    /// <param name="left">One array, ascending.</param>
    /// <param name="right">The other array, ascending.</param>
    /// <returns><see langword="true"/> when they are element-for-element equal.</returns>
    private static bool SameSet(int[] left, int[] right)
    {
        if (left.Length != right.Length)
        {
            return false;
        }

        for (var i = 0; i < left.Length; i++)
        {
            if (left[i] != right[i])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Renders a partition array for a log line: <c>0,1,2</c>, or <c>[]</c> when empty.</summary>
    /// <param name="partitions">The partitions, ascending.</param>
    /// <returns>A log-safe rendering.</returns>
    private static string Describe(int[] partitions)
        => partitions.Length == 0 ? "[]" : string.Join(',', partitions);

    /// <summary>Reads a claim hash into partition-to-owner form, ignoring non-numeric fields.</summary>
    internal static Dictionary<int, PartitionOwner> ReadOwners(HashEntry[] entries)
    {
        var owners = new Dictionary<int, PartitionOwner>(entries.Length);
        foreach (var entry in entries)
        {
            var name = (string?)entry.Name;
            var value = (string?)entry.Value;
            if (name is null || value is null)
            {
                continue;
            }

            if (int.TryParse(name, NumberStyles.None, CultureInfo.InvariantCulture, out var partition))
            {
                owners[partition] = PartitionOwner.Parse(partition, value);
            }
        }

        return owners;
    }

    /// <summary>
    /// A name-derived, process-independent id. SHA-256 of the name, stamped with the UUIDv8 version
    /// and variant bits so it is a well-formed GUID rather than sixteen arbitrary bytes.
    /// </summary>
    /// <param name="name">The identifying name — a pod name, or machine plus ordinal.</param>
    /// <returns>The derived id.</returns>
    private static Guid DeriveInstanceId(string name)
    {
        Span<byte> utf8 = stackalloc byte[256];
        var bytes = Encoding.UTF8.GetByteCount(name) <= utf8.Length
            ? utf8[..Encoding.UTF8.GetBytes(name, utf8)]
            : Encoding.UTF8.GetBytes(name);

        Span<byte> hash = stackalloc byte[32];
        _ = System.Security.Cryptography.SHA256.HashData(bytes, hash);

        hash[6] = (byte)((hash[6] & 0x0F) | 0x80);
        hash[8] = (byte)((hash[8] & 0x3F) | 0x80);

        return new Guid(hash[..16]);
    }

    /// <summary>
    /// Proves the server can expire hash fields before any claim is written.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes when the probe has passed.</returns>
    /// <exception cref="StreamConfigurationException">The server rejected <c>HEXPIRE</c>.</exception>
    /// <remarks>
    /// Only a server-side rejection is fatal. A connection failure or a timeout is Redis being
    /// briefly unavailable, which the registry is explicitly allowed to shrug off — turning that
    /// into a startup crash would make an ownership blip take down a healthy consumer.
    /// </remarks>
    private async Task ProbeHashFieldExpiryAsync(CancellationToken ct)
    {
        // Lease mode always probes: it writes nothing at construction time, and a server that cannot
        // expire fields would hand it claims that outlive their instance and never come free.
        if (!this.lease && this.claims.Length == 0)
        {
            return;
        }

        var db = this.redis.GetDatabase();
        RedisValue probe = string.Concat(
            ProbeFieldPrefix,
            this.options.InstanceId.ToString("D", CultureInfo.InvariantCulture));

        try
        {
            await db.HashSetAsync(this.key, probe, "1").ConfigureAwait(false);
            var result = await db
                .HashFieldExpireAsync(this.key, [probe], TimeSpan.FromSeconds(1), ExpireWhen.Always)
                .ConfigureAwait(false);

            if (result.Length == 0 || result[0] is not (ExpireResult.Success or ExpireResult.Due))
            {
                throw new StreamConfigurationException(
                    $"Streams: HEXPIRE on {this.key} returned {(result.Length == 0 ? "nothing" : result[0].ToString())}, so partition " +
                    "ownership claims for topic " + this.options.Topic + " would never expire and every later restart would read " +
                    "them as a live rival. Point Streams:ConnectionString at a Redis 7.4 or newer server.");
            }
        }
        catch (RedisServerException ex)
        {
            throw new StreamConfigurationException(
                $"Streams: this Redis server rejected HEXPIRE ({ex.Message}), which the ownership registry needs to let a " +
                $"departed instance's claims lapse. Without it the claims for topic {this.options.Topic} consumer " +
                $"{this.options.Consumer} would live forever and every restart would be reported as contested. Redis 7.4 or " +
                "newer is required.",
                ex);
        }
        catch (Exception ex) when (ex is not (OperationCanceledException or StreamConfigurationException))
        {
            // Redis unreachable rather than incapable: the claim below will fail too and be logged
            // there, and the renew loop retries.
            this.logger?.LogWarning(
                ex,
                "Streams: could not probe HEXPIRE support for topic {Topic} consumer {Consumer}; continuing, and the first claim will report the failure.",
                this.options.Topic,
                this.options.Consumer);
        }
        finally
        {
            try
            {
                await db.HashDeleteAsync(this.key, probe).ConfigureAwait(false);
            }
            catch (RedisException)
            {
                // The probe field expires on its own within a second; failing to tidy it is not worth
                // a log line, let alone a failure.
            }
        }

        ct.ThrowIfCancellationRequested();
    }

    private OwnershipMap Evaluate(HashEntry[] before, HashEntry[] after)
    {
        var previous = ReadOwners(before);
        var owners = ReadOwners(after);

        var unowned = new List<int>();
        for (var p = 0; p < this.options.Partitions; p++)
        {
            if (!owners.ContainsKey(p))
            {
                unowned.Add(p);
            }
        }

        // Contention is visible from either side of our own write: somebody held the field before we
        // stamped it, or somebody stamped it after us.
        var contested = new List<ContestedPartition>();
        foreach (var partition in this.owned)
        {
            if (previous.TryGetValue(partition, out var was) && was.InstanceId != this.options.InstanceId)
            {
                contested.Add(new ContestedPartition(partition, this.options.InstanceId, was));
                continue;
            }

            if (owners.TryGetValue(partition, out var now) && now.InstanceId != this.options.InstanceId)
            {
                contested.Add(new ContestedPartition(partition, this.options.InstanceId, now));
            }
        }

        contested.Sort(static (a, b) => a.Partition.CompareTo(b.Partition));

        return new OwnershipMap(
            this.options.Topic,
            this.options.Consumer,
            this.options.Partitions,
            owners,
            unowned,
            contested,
            DateTimeOffset.UtcNow);
    }

    /// <summary>
    /// The Lease-mode counterpart of <see cref="Report"/>: what this instance holds, what it gained
    /// and lost, and how many members it is sharing the topic with.
    /// </summary>
    /// <param name="map">The map observed after this cycle.</param>
    /// <param name="previous">What this instance held before it.</param>
    /// <param name="current">What it holds now.</param>
    /// <param name="members">Live members observed in the hash, this instance included.</param>
    /// <param name="changed">Whether the held set moved.</param>
    private void ReportLease(OwnershipMap map, int[] previous, int[] current, int members, bool changed)
    {
        if (changed || !this.described)
        {
            this.described = true;
            this.logger?.LogInformation(
                "Streams: consumer={Consumer} topic={Topic} instance {Instance} (pod {PodName}) leases partitions {Held} of {Partitions} across {Members} live instances (was {Previous}) | {Map}",
                map.Consumer,
                map.Topic,
                this.options.InstanceId,
                this.options.PodName,
                Describe(current),
                map.Partitions,
                members,
                Describe(previous),
                map.Describe());
        }

        if (map.Unowned.Count > 0)
        {
            // Warning, not the Error that Static mode logs: in Lease mode an unclaimed partition is
            // a lapsed lease that the next cycle picks up by itself, not a configuration fault that
            // will still be there tomorrow.
            this.logger?.LogWarning(
                "Streams: partitions {Unowned} of topic {Topic} consumer {Consumer} hold no lease; some instance claims them within {TtlSeconds}s. {Map}",
                string.Join(',', map.Unowned),
                map.Topic,
                map.Consumer,
                this.options.TtlSeconds,
                map.Describe());
        }
    }

    private void Report(OwnershipMap map)
    {
        if (map.Unowned.Count > 0)
        {
            this.logger?.LogError(
                "Streams: ownership gap on topic {Topic} consumer {Consumer} — partitions {Unowned} of {Partitions} have no owner, so their messages are consumed by nobody. Check that {CountVariable} matches spec.replicas. {Map}",
                map.Topic,
                map.Consumer,
                string.Join(',', map.Unowned),
                map.Partitions,
                "STREAMS_INSTANCE_COUNT",
                map.Describe());
        }

        foreach (var clash in map.Contested)
        {
            this.logger?.LogError(
                "Streams: ownership overlap on topic {Topic} consumer {Consumer} — partition {Partition} is claimed by {Other} as well as this instance {Instance} (pod {PodName}), so its messages are processed twice. A partitioned consumer must run as a StatefulSet, not a Deployment.",
                map.Topic,
                map.Consumer,
                clash.Partition,
                clash.Actual.ToString(),
                clash.Expected,
                this.options.PodName);
        }

        if (!this.described || map.IsDegraded)
        {
            this.described = true;
            this.logger?.LogInformation(
                "Streams: consumer={Consumer} topic={Topic} instance {Instance} (pod {PodName}) claims partitions {Owned} of {Partitions} | {Map}",
                map.Consumer,
                map.Topic,
                this.options.InstanceId,
                this.options.PodName,
                this.fields.Length == 0 ? "[]" : string.Join(',', this.options.OwnedPartitions),
                map.Partitions,
                map.Describe());
        }
    }

    private async Task RenewLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(this.renewInterval);

        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                try
                {
                    await this.RefreshAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // Ownership is observational: a Redis blip must not take down a healthy consumer.
                    this.logger?.LogWarning(
                        ex,
                        "Streams: ownership renew failed for topic {Topic} consumer {Consumer}; retrying in {RenewSeconds}s. Claims expire after {TtlSeconds}s.",
                        this.options.Topic,
                        this.options.Consumer,
                        this.options.RenewSeconds,
                        this.options.TtlSeconds);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
    }

    private async Task ReleaseAsync(CancellationToken ct)
    {
        if (ct.IsCancellationRequested)
        {
            return;
        }

        var args = this.lease ? this.LeaseReleaseArgs() : this.releaseArgs;

        if (args.Length == 0)
        {
            return;
        }

        try
        {
            // Compare-and-delete, not HDEL: in the contested case the field we are about to drop may
            // already hold the other pod's claim, and deleting that would report a gap where there
            // is a live owner (R-15).
            await this.redis.GetDatabase()
                .ScriptEvaluateAsync(ReleaseScript, [this.key], args)
                .ConfigureAwait(false);

            if (this.lease)
            {
                Volatile.Write(ref this.held, []);
            }
        }
        catch (Exception ex)
        {
            this.logger?.LogDebug(
                ex,
                "Streams: could not release ownership claims for topic {Topic} consumer {Consumer}; they expire in at most {TtlSeconds}s.",
                this.options.Topic,
                this.options.Consumer,
                this.options.TtlSeconds);
        }
    }
}
