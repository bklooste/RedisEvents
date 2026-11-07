namespace RedisEvents.Config;

/// <summary>
/// The fixed prefix folded into every key this library, and its
/// <c>RedisEvents.EventSourcing</c>/<c>RedisEvents.Projections</c> siblings, write to Redis:
/// <c>re:</c>, so the library's keys are recognisable and kept apart from an application's own.
/// </summary>
/// <remarks>
/// <para>
/// <b>The library does not know about environments.</b> Topic and consumer names are opaque, and
/// isolation comes from them alone: a stream, its topic meta and any outbox state belong to the
/// topic name; positions, ownership leases, a work-queue group and (by default) projection views
/// belong to the consumer name. An application that runs several environments on one Redis composes
/// those names itself — a topic <c>np.mult_feed_events</c> shared by every environment, consumers
/// <c>dev.my-service</c> and <c>tst.my-service</c> — and two consumers with different names on one
/// topic never share a key. The library does not derive anything from <c>ASPNETCORE_ENVIRONMENT</c>
/// or <c>DOTNET_ENVIRONMENT</c>.
/// </para>
/// <para>
/// <c>STREAMS_KEY_PREFIX</c> replaces the prefix verbatim (it must end in <c>:</c> to read as a
/// segment) for the rare case that needs a hand-chosen one, such as two unrelated applications
/// sharing one Redis. It is the only override. The prefix sits outside any hash-tag brace, so it
/// never affects which cluster slot a key lands in.
/// </para>
/// </remarks>
public static class KeyNamespace
{
    private const string OverrideVariable = "STREAMS_KEY_PREFIX";
    private const string DefaultPrefix = "re:";
    private const string HashTagVariable = "STREAMS_HASH_TAG";

    /// <summary>
    /// Whether keys carry a Redis Cluster hash tag — the <c>{topic}</c> brace that forces one topic's
    /// keys onto one slot. On by default, because the library's co-location guarantees (an outbox
    /// transaction over a stream and its state key, a worker's multi-stream <c>XREAD</c>) hold only
    /// when the keys share a slot.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>STREAMS_HASH_TAG=false</c> (also <c>0</c> / <c>no</c> / <c>off</c>) drops the brace from
    /// every key the library and its siblings write, so keys read as a plain <c>:</c>-delimited
    /// hierarchy. It is for a <b>single-instance</b> deployment, where there are no slots: one
    /// <c>MULTI/EXEC</c> spans everything, a multi-stream <c>XREAD</c> is one node's work regardless,
    /// and the cluster slot guards (<c>Outbox.EnsureSameSlot</c>, <c>StreamStore.RequireCoLocatedPartitions</c>)
    /// have nothing to check, so they are skipped. Turning it off on a real cluster would let a
    /// transaction or a grouped read fail at runtime with <c>CROSSSLOT</c>; leave it on there.
    /// </para>
    /// </remarks>
    /// <returns><see langword="true"/> unless <c>STREAMS_HASH_TAG</c> is set to a falsey value.</returns>
    public static bool HashTagTopics()
    {
        var set = Environment.GetEnvironmentVariable(HashTagVariable);
        if (string.IsNullOrWhiteSpace(set))
        {
            return true;
        }

        return set.Trim() switch
        {
            "false" or "False" or "FALSE" or "0" or "no" or "No" or "NO" or "off" or "Off" or "OFF" => false,
            _ => true,
        };
    }

    /// <summary>
    /// The prefix to fold into every key: <c>"re:"</c>, or <c>STREAMS_KEY_PREFIX</c> verbatim when set.
    /// </summary>
    /// <returns>The literal prefix.</returns>
    public static string Prefix()
    {
        var overridden = Environment.GetEnvironmentVariable(OverrideVariable);
        return string.IsNullOrWhiteSpace(overridden) ? DefaultPrefix : overridden;
    }

    /// <summary>
    /// The last-resort service name: the entry assembly name. It is the consumer name when neither a
    /// consumer entry nor <c>Streams:Consumer</c> sets one, and the projection view owner when no owner
    /// and no <c>Streams:Consumer</c> is given. Two deployments of one assembly that must not share
    /// keys therefore have to set <c>Streams:Consumer</c>.
    /// </summary>
    /// <returns>The entry assembly's simple name, or <c>"unknown"</c> if it cannot be resolved.</returns>
    public static string DefaultServiceName()
    {
        var name = System.Reflection.Assembly.GetEntryAssembly()?.GetName().Name;
        return string.IsNullOrWhiteSpace(name) ? "unknown" : name;
    }
}
