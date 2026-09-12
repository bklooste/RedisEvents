namespace RedisEvents.Tests;

/// <summary>
/// Registers this assembly's own <c>"RedisStreams"</c> xUnit collection against
/// <see cref="RedisStreamsFixture"/>.
/// </summary>
/// <remarks>
/// xUnit only discovers <c>[CollectionDefinition]</c>s declared in the assembly it is actually
/// running, never in a referenced library — so even though <see cref="RedisStreamsFixture"/> and the
/// <see cref="RedisStreamsCollection"/> name constant live in <c>RedisEvents.TestSupport</c> (shared
/// with <c>RedisEvents.Tests</c>), each consuming test assembly still needs this one-line registration
/// of its own. See the remarks on <see cref="RedisStreamsCollection"/> for why.
/// </remarks>
[CollectionDefinition(RedisStreamsCollection.Name)]
public sealed class RedisStreamsCollectionRegistration : ICollectionFixture<RedisStreamsFixture>;
