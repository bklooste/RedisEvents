using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

using StackExchange.Redis;

namespace RedisEvents.EventSourcing;

/// <summary>
/// A Redis-backed <see cref="IViewStore{TView}"/>: one Redis HASH per view type, field = view id,
/// value = the view serialised to JSON.
/// </summary>
/// <remarks>
/// <para>
/// <b>Storage shape.</b> All views of one type live in a single hash keyed
/// <c>{topic}:view:&lt;viewName&gt;</c> — the same <c>{topic}</c> hash-tag brace convention core uses
/// for state keys (see <c>Outbox.StateKey</c>), though a view does not need to share the topic's hash
/// slot; the prefix is namespacing only, chosen so two services on the same Redis using the same
/// <paramref name="viewName">view name</paramref> under different topics never collide.
/// <see cref="GetAsync"/>/<see cref="SetAsync"/>/<see cref="DeleteAsync"/> are <c>HGET</c>/<c>HSET</c>/
/// <c>HDEL</c> on the view's field; <see cref="ListAsync"/> is <c>HSCAN</c> over the whole hash — one
/// key to reason about, and listing comes for free.
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
    /// <exception cref="ArgumentNullException"><paramref name="db"/> or <paramref name="typeInfo"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="topic"/> or <paramref name="viewName"/> is null, empty or whitespace.</exception>
    public RedisViewStore(IDatabase db, string topic, string viewName, JsonTypeInfo<TView> typeInfo)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        ArgumentNullException.ThrowIfNull(typeInfo);

        this.db = db;
        this.key = $"{{{topic}}}:view:{viewName}";
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
