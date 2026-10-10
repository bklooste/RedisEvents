namespace RedisEvents.UnitTests;

/// <summary>
/// Serialises the test classes that register monitors in the process-wide <c>StreamLag</c> registry
/// and clear it between tests.
/// </summary>
/// <remarks>
/// xUnit runs test classes in parallel unless they share a collection, and the registry is static:
/// one class's <c>StreamLag.Clear()</c> would otherwise drop the monitor another class is in the
/// middle of asserting on, and the health check — which grades every monitor in the process — would
/// see its neighbours' partitions.
/// </remarks>
[CollectionDefinition(Name)]
public sealed class StreamMonitorCollection
{
    public const string Name = "StreamMonitors";
}
