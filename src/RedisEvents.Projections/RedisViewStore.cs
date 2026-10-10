using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

using RedisEvents.Config;
using StackExchange.Redis;

namespace RedisEvents.Projections;

/// <summary>
/// A Redis-backed <see cref="IViewStore{TView}"/>: one Redis HASH per view type, field = view id,
/// value = the view serialised to JSON.
/// </summary>
/// <remarks>
/// <para>
/// <b>Storage shape.</b> All views of one type live in a single hash keyed
/// <c>&lt;env&gt;:re:{topic}:view:&lt;owner&gt;:&lt;viewName&gt;</c> — the same <c>{topic}</c>
/// hash-tag brace convention core uses for state keys (see <c>Outbox.StateKey</c>), though a view
/// does not need to share the topic's hash slot; the prefix is namespacing only. The environment
/// segment (<see cref="KeyNamespace.Prefix"/>) keeps two environments sharing one Redis apart, and
/// the owner segment keeps two services that pick the same view name under the same topic apart too.
/// <see cref="GetAsync"/>/<see cref="SetAsync"/>/<see cref="DeleteAsync"/> are <c>HGET</c>/<c>HSET</c>/
/// <c>HDEL</c> on the view's field; <see cref="ListAsync"/> is <c>HSCAN</c> over the whole hash — one
/// key to reason about, and listing comes for free.
/// </para>
/// <para>
/// <b>The owner segment.</b> By default it is <see cref="KeyNamespace.DefaultServiceName"/> — the
/// entry assembly's name — so existing keys do not move. That default is only right when the process
/// that writes the view is the process that reads it. When a view is written by one process (say a
/// headless projection worker) and read by another (a query service), or read from a test host, the
/// entry assemblies differ and so would the keys; the reader would see an empty view. Pass the same
/// explicit <c>owner</c> to the store on both sides, using a stable logical name for the view's
/// owning service rather than either process's assembly name. Two stores with different owners are
/// two different hashes, even for the same topic and view name.
/// </para>
/// <para>
/// <b>Serialisation.</b> Values are UTF-8 JSON bytes produced and consumed entirely through the
/// supplied <see cref="JsonTypeInfo{T}"/> — no reflection, so this type is Native AOT and trimming
/// safe. Pass the source-generated <c>JsonTypeInfo&lt;TView&gt;</c> from your <c>JsonSerializerContext</c>.
/// </para>
/// <para>
/// <b>Scale envelope.</b> A single hash is fine for up to tens of thousands of small views: it is one
/// key, so it is one entry in <c>DBSIZE</c>/<c>SCAN</c>, and normal Redis memory bookkeeping applies
/// per-hash rather than per-field. Beyond that, or for any query beyond "by id" or "all", implement
/// <see cref="IViewStore{TView}"/> over a real database instead — the interface is the seam precisely
/// so that swap needs no change anywhere else. See the package README for an Azure Tables skeleton.
/// </para>
/// <para>
/// Obtain the <see cref="IDatabase"/> from <c>StreamsConnection.GetSharedDatabase</c> so views land on
/// the same, durable Redis the streams themselves use — never a service's own cache connection, which
/// may run with <c>--save "" --appendonly no</c> and lose everything on restart.
/// </para>
/// </remarks>
/// <typeparam name="TView">The view model type.</typeparam>
public sealed class RedisViewStore<TView> : IViewStore<TView>
    where TView : class
{
    private readonly IDatabase db;
    private readonly RedisKey key;
    private readonly JsonTypeInfo<TView> typeInfo;

    /// <summary>
    /// Creates a store for one view type, backed by a single Redis hash.
    /// </summary>
    /// <param name="db">The shared streams database — see <c>StreamsConnection.GetSharedDatabase</c>.</param>
    /// <param name="topic">The topic this view is projected from; used only to namespace the hash key.</param>
    /// <param name="viewName">The view's name, e.g. <c>"detail"</c>; the hash field is the view id.</param>
    /// <param name="typeInfo">The source-generated <see cref="JsonTypeInfo{T}"/> for <typeparamref name="TView"/>.</param>
    /// <remarks>
    /// The owner segment of the key is the entry assembly's name (<see cref="KeyNamespace.DefaultServiceName"/>).
    /// If another process (or a test host) must read what this one writes, use the overload taking an
    /// explicit <c>owner</c> on both sides.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="db"/> or <paramref name="typeInfo"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="topic"/> or <paramref name="viewName"/> is null, empty or whitespace.</exception>
    public RedisViewStore(IDatabase db, string topic, string viewName, JsonTypeInfo<TView> typeInfo)
        : this(db, topic, viewName, typeInfo, KeyNamespace.DefaultServiceName())
    {
    }

    /// <summary>
    /// Creates a store for one view type, backed by a single Redis hash, whose key carries an explicit
    /// owner name instead of the entry assembly's name.
    /// </summary>
    /// <param name="db">The shared streams database — see <c>StreamsConnection.GetSharedDatabase</c>.</param>
    /// <param name="topic">The topic this view is projected from; used only to namespace the hash key.</param>
    /// <param name="viewName">The view's name, e.g. <c>"detail"</c>; the hash field is the view id.</param>
    /// <param name="typeInfo">The source-generated <see cref="JsonTypeInfo{T}"/> for <typeparamref name="TView"/>.</param>
    /// <param name="owner">
    /// The logical owner of the view, folded into the key in place of the entry assembly's name. Pass the
    /// same value from every process that writes or reads this view — a projection worker and the query
    /// service that serves it, or a test host — and choose a stable name for the owning service, not
    /// either process's assembly name. Omit it (use the other overload) only when the writer and reader
    /// are the same entry assembly.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="db"/> or <paramref name="typeInfo"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="topic"/>, <paramref name="viewName"/> or <paramref name="owner"/> is null, empty or whitespace.</exception>
    public RedisViewStore(IDatabase db, string topic, string viewName, JsonTypeInfo<TView> typeInfo, string owner)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        ArgumentNullException.ThrowIfNull(typeInfo);
        ArgumentException.ThrowIfNullOrWhiteSpace(owner);

        this.db = db;
        this.key = $"{KeyNamespace.Prefix()}{{{topic}}}:view:{owner}:{viewName}";
        this.typeInfo = typeInfo;
    }

    /// <inheritdoc />
    public async ValueTask<TView?> GetAsync(string id, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        var value = await this.db.HashGetAsync(this.key, id).ConfigureAwait(false);
        return value.IsNull ? null : Deserialize(value);
    }

    /// <inheritdoc />
    public async ValueTask SetAsync(string id, TView view, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        ArgumentNullException.ThrowIfNull(view);

        var bytes = JsonSerializer.SerializeToUtf8Bytes(view, this.typeInfo);
        await this.db.HashSetAsync(this.key, id, bytes).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DeleteAsync(string id, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);

        await this.db.HashDeleteAsync(this.key, id).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TView> ListAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var entry in this.db.HashScanAsync(this.key).WithCancellation(ct).ConfigureAwait(false))
            yield return Deserialize(entry.Value);
    }

    private TView Deserialize(RedisValue value) =>
        JsonSerializer.Deserialize(((byte[])value)!, this.typeInfo)
            ?? throw new InvalidOperationException($"View hash '{this.key}' held a JSON null.");
}
