namespace RedisEvents.Config;

/// <summary>
/// Root configuration options for all Redis Streams functionality.
/// Can be bound from appsettings.json under "Streams" section.
/// Every key has a sensible default; an empty "Streams" section is legal.
/// </summary>
/// <remarks>
/// Properties are <c>set</c> rather than <c>init</c> because the source-generated configuration
/// binder cannot assign an <c>init</c>-only member and silently bound nothing at all; see the
/// remarks on <see cref="TopicOptions"/> (R-17).
/// </remarks>
public sealed record StreamOptions
{
    /// <summary>
    /// Redis connection string (defaults to redis-db.infra:6379 if not specified).
    /// Must point to redis-db, not redis-cache, since streams require persistence.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Default consumer group name for all consumers (can be overridden per consumer).
    /// Defaults to the service/assembly name if not specified.
    /// </summary>
    public string? Consumer { get; set; }

    /// <summary>
    /// Topic-level configuration options.
    /// Topics referenced by consumers/producers but not configured here are defaulted.
    /// </summary>
    public Dictionary<string, TopicOptions> Topics { get; set; } = [];

    /// <summary>
    /// Consumer configurations (one per topic reader).
    /// </summary>
    public ConsumerOptions[] Consumers { get; set; } = [];

    /// <summary>
    /// Producer configurations (one per topic writer).
    /// </summary>
    public ProducerOptions[] Producers { get; set; } = [];
}
