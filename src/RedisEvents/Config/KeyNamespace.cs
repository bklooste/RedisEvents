namespace RedisEvents.Config;

/// <summary>
/// The environment- and service-scoped prefix folded into every key this library, and its
/// <c>RedisEvents.EventSourcing</c>/<c>RedisEvents.Projections</c> siblings, write to Redis.
/// </summary>
/// <remarks>
/// <para>
/// Every key this library builds — <c>s:{topic}:&lt;p&gt;</c>, <c>o:{topic}:&lt;consumer&gt;</c>,
/// <c>p:{topic}:&lt;consumer&gt;</c>, <c>m:{topic}</c>, <c>{topic}:state:&lt;name&gt;</c>,
/// <c>{topic}:view:&lt;viewName&gt;</c>, <c>dedupe:{scope}:&lt;id&gt;</c> — is scoped only by topic
/// name. Two environments (dev, qa, …) pointed at the same Redis collide on every one of them the
/// moment they share a topic name, which is the common case. <see cref="Prefix"/> is prepended
/// (outside any hash-tag brace, so partition co-location is unaffected) to fix that.
/// </para>
/// <para>
/// Resolved fresh on every call from the ambient environment, the same way
/// <c>StreamConfigBinder</c> resolves the environment name for validation — no configuration
/// section is needed for the common case, and a service already setting
/// <c>ASPNETCORE_ENVIRONMENT</c>/<c>DOTNET_ENVIRONMENT</c> for every other purpose gets a correctly
/// namespaced key store for free. <c>STREAMS_KEY_PREFIX</c> overrides it outright for the rare case
/// that needs a literal, hand-chosen prefix (a migration, a shared sandbox).
/// </para>
/// </remarks>
public static class KeyNamespace
{
    private const string OverrideVariable = "STREAMS_KEY_PREFIX";
    private const string Marker = "re";

    /// <summary>
    /// The prefix to fold into every key, e.g. <c>"dev:re:"</c>.
    /// </summary>
    /// <param name="environmentName">
    /// Overrides the ambient environment name; primarily for tests. <see langword="null"/> reads
    /// <c>ASPNETCORE_ENVIRONMENT</c> / <c>DOTNET_ENVIRONMENT</c>, defaulting to <c>"production"</c>.
    /// </param>
    /// <returns>The literal prefix, always ending in <c>:</c>.</returns>
    public static string Prefix(string? environmentName = null)
    {
        var overridden = Environment.GetEnvironmentVariable(OverrideVariable);
        if (!string.IsNullOrWhiteSpace(overridden))
            return overridden;

        var environment = environmentName
            ?? Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
            ?? Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
            ?? "production";

        return $"{environment.ToLowerInvariant()}:{Marker}:";
    }

    /// <summary>
    /// The service name folded into <c>RedisEvents.Projections</c> view keys, so two services that
    /// happen to choose the same view name on the same topic do not overwrite each other. The entry
    /// assembly name — the same default <c>StreamConfigBinder</c> uses for the consumer group name.
    /// </summary>
    /// <returns>The entry assembly's simple name, or <c>"unknown"</c> if it cannot be resolved.</returns>
    public static string DefaultServiceName()
    {
        var name = System.Reflection.Assembly.GetEntryAssembly()?.GetName().Name;
        return string.IsNullOrWhiteSpace(name) ? "unknown" : name;
    }
}
