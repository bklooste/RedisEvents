namespace RedisEvents.UnitTests;

/// <summary>
/// Runs the golden-vector tests alone. They set process-wide environment variables
/// (<c>STREAMS_KEY_PREFIX</c>, <c>ASPNETCORE_ENVIRONMENT</c>, <c>DOTNET_ENVIRONMENT</c>) that every key
/// builder reads, so no other test class may run beside them.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class GoldenVectorCollection
{
    public const string Name = "GoldenVectors";
}
