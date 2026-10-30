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
