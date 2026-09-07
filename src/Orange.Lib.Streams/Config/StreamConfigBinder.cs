using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Orange.Lib.Streams.Errors;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Config;

/// <summary>
/// Binds <see cref="StreamOptions"/> from the <c>Streams</c> configuration section and validates it.
///
/// Binding uses the source-generated configuration binder (<c>EnableConfigurationBindingGenerator</c>),
/// so no reflection is used and the library stays AOT-clean.
///
/// Zero configuration is legal: a missing <c>Streams</c> section, or an empty one, yields an options
/// instance where every value is the record default.
///
/// Every validation failure throws <see cref="StreamConfigurationException"/> with a message naming
/// the offending configuration key, so an operator can go straight to it in appsettings.
/// </summary>
internal static class StreamConfigBinder
{
    /// <summary>
    /// The configuration section the options are bound from.
    /// </summary>
    public const string SectionName = "Streams";

    /// <summary>
    /// Connection string used when <see cref="StreamOptions.ConnectionString"/> is not set.
    /// Streams live on <c>redis-db</c> (AOF persistence), never on <c>redis-cache</c>.
    /// </summary>
    public const string DefaultConnectionString = "redis-db.infra:6379,abortConnect=false";

    /// <summary>
    /// The StackExchange.Redis default <c>syncTimeout</c>, used when the connection string does not set one.
    /// </summary>
    public const int DefaultSyncTimeoutMs = 5000;

    private const string DevelopmentEnvironmentName = "Development";

    /// <summary>
    /// Binds <see cref="StreamOptions"/> from the <c>Streams</c> section of the supplied configuration.
    /// A missing or empty section is legal and yields all defaults.
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The bound options; never <c>null</c>.</returns>
    public static StreamOptions Bind(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var section = configuration.GetSection(SectionName);

        // Source-generated binder (no reflection). A missing section binds to null, which is legal.
        return section.Get<StreamOptions>() ?? new StreamOptions();
    }

    /// <summary>
    /// Validates the options using the ambient environment name and the <c>syncTimeout</c> resolved
    /// from the configured connection string.
    /// </summary>
    /// <param name="options">The options to validate.</param>
    public static void Validate(StreamOptions options) => Validate(options, environmentName: null, logger: null);

    /// <summary>
    /// Validates the options, resolving <c>syncTimeout</c> from the configured connection string.
    /// </summary>
    /// <param name="options">The options to validate.</param>
    /// <param name="environmentName">The host environment name; <c>null</c> reads <c>ASPNETCORE_ENVIRONMENT</c> / <c>DOTNET_ENVIRONMENT</c>.</param>
    /// <param name="logger">Optional logger for informational defaulting and warnings.</param>
    public static void Validate(StreamOptions options, string? environmentName, ILogger? logger)
        => Validate(options, environmentName, ResolveSyncTimeoutMs(options?.ConnectionString), logger);

    /// <summary>
    /// Validates the options against every rule in the design plan. Throws on the first violation.
    /// </summary>
    /// <param name="options">The options to validate.</param>
    /// <param name="environmentName">The host environment name; <c>null</c> reads <c>ASPNETCORE_ENVIRONMENT</c> / <c>DOTNET_ENVIRONMENT</c>.</param>
    /// <param name="syncTimeoutMs">The <c>syncTimeout</c> of the resolved connection, in milliseconds.</param>
    /// <param name="logger">Optional logger for informational defaulting and warnings.</param>
    /// <exception cref="StreamConfigurationException">Thrown when the configuration is invalid; the message names the offending key.</exception>
    public static void Validate(StreamOptions options, string? environmentName, int syncTimeoutMs, ILogger? logger)
    {
        ArgumentNullException.ThrowIfNull(options);

        var environment = environmentName ?? AmbientEnvironmentName();
        var isDevelopment = string.Equals(environment, DevelopmentEnvironmentName, StringComparison.OrdinalIgnoreCase);

        ValidateTopics(options, isDevelopment, environment);
        ValidateConsumers(options, syncTimeoutMs, logger);
        ValidateProducers(options, logger);
    }

    /// <summary>
    /// Returns the topic options for <paramref name="topic"/>, or the defaults when the topic is not
    /// configured. A defaulted topic is logged at Information — it is not an error, because
    /// zero-config must work.
    /// </summary>
    /// <param name="options">The root options.</param>
    /// <param name="topic">The topic name.</param>
    /// <param name="logger">Optional logger.</param>
    /// <returns>The configured or defaulted topic options.</returns>
    public static TopicOptions ResolveTopic(StreamOptions options, string topic, ILogger? logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        if (options.Topics.TryGetValue(topic, out var configured))
            return configured;

        var defaulted = new TopicOptions();
        logger?.LogInformation(
            "Streams: topic {Topic} is not present in Streams:Topics; using defaults (Partitions={Partitions}, MaxLen={MaxLen}, Trim={Trim}).",
            topic,
            defaulted.Partitions,
            defaulted.MaxLen,
            defaulted.Trim);

        return defaulted;
    }

    /// <summary>
    /// Resolves the effective consumer name for a consumer entry: its own <c>Consumer</c>,
    /// otherwise the root <c>Streams:Consumer</c>, otherwise the entry assembly name.
    /// </summary>
    /// <param name="options">The root options.</param>
    /// <param name="consumer">The consumer entry.</param>
    /// <returns>The effective consumer name.</returns>
    public static string ResolveConsumerName(StreamOptions options, ConsumerOptions consumer)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(consumer);

        if (!string.IsNullOrWhiteSpace(consumer.Consumer))
            return consumer.Consumer;

        if (!string.IsNullOrWhiteSpace(options.Consumer))
            return options.Consumer;

        return DefaultConsumerName();
    }

    /// <summary>
    /// Resolves the effective connection string: the configured one, otherwise <see cref="DefaultConnectionString"/>.
    /// </summary>
    /// <param name="options">The root options.</param>
    /// <returns>The connection string to use.</returns>
    public static string ResolveConnectionString(StreamOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return string.IsNullOrWhiteSpace(options.ConnectionString) ? DefaultConnectionString : options.ConnectionString;
    }

    /// <summary>
    /// Parses the <c>syncTimeout</c> out of a connection string, falling back to the
    /// StackExchange.Redis default when it is not specified.
    /// </summary>
    /// <param name="connectionString">The connection string; <c>null</c> or empty uses the default connection string.</param>
    /// <returns>The sync timeout in milliseconds.</returns>
    /// <exception cref="StreamConfigurationException">Thrown when the connection string cannot be parsed.</exception>
    public static int ResolveSyncTimeoutMs(string? connectionString)
    {
        var effective = string.IsNullOrWhiteSpace(connectionString) ? DefaultConnectionString : connectionString;

        try
        {
            return ConfigurationOptions.Parse(effective).SyncTimeout;
        }
        catch (Exception ex)
        {
            throw new StreamConfigurationException(
                $"Streams:ConnectionString is not a valid Redis connection string ('{effective}'): {ex.Message}",
                ex);
        }
    }

    private static void ValidateTopics(StreamOptions options, bool isDevelopment, string environment)
    {
        foreach (var (name, topic) in options.Topics)
        {
            var key = $"{SectionName}:Topics:{name}";

            if (topic is null)
                throw new StreamConfigurationException($"{key} is null. Remove the entry or give it an object value.");

            if (topic.Partitions < 1)
                throw new StreamConfigurationException($"{key}:Partitions is {topic.Partitions}; it must be >= 1.");

            if (topic.RetentionSeconds is int retention)
            {
                if (retention < 1)
                    throw new StreamConfigurationException($"{key}:RetentionSeconds is {retention}; it must be >= 1 or omitted.");

                if (topic.MaxLen < 1)
                {
                    throw new StreamConfigurationException(
                        $"{key}:RetentionSeconds is set ({retention}) but {key}:MaxLen is {topic.MaxLen}. " +
                        "Time-based trimming does not bound memory, so a MaxLen ceiling (>= 1) is mandatory.");
                }

                // R-17: the pairing rule the plan asked for. Until MaxLen tracked whether it had been
                // assigned, this could only fire on an explicit MaxLen of 0 — a defaulted ceiling
                // looked identical to a configured one, so "RetentionSeconds without MaxLen" was
                // unreachable. A retention window measured in hours or days against the defaulted
                // 10,000-entry ceiling is the silent misconfiguration: the ceiling, not the window,
                // decides what survives, and the operator never said what it should be.
                if (!topic.MaxLenConfigured)
                {
                    throw new StreamConfigurationException(
                        $"{key}:RetentionSeconds is set ({retention}s) but {key}:MaxLen is not configured. " +
                        $"Time-based trimming does not bound memory, so the ceiling is what actually protects Redis — and the " +
                        $"default of {TopicOptions.DefaultMaxLen} entries per partition is almost certainly not the ceiling a " +
                        $"{retention}s window needs. Set {key}:MaxLen explicitly (entries per partition), or remove " +
                        $"{key}:RetentionSeconds and let the inline MAXLEN window do the trimming.");
                }
            }

            if (topic.Trim == TrimMode.None && !isDevelopment)
            {
                throw new StreamConfigurationException(
                    $"{key}:Trim is None in environment '{environment}'. Trim=None is an OOM footgun and is only allowed in Development.");
            }
        }
    }

    private static void ValidateConsumers(StreamOptions options, int syncTimeoutMs, ILogger? logger)
    {
        var seen = new Dictionary<string, int>(StringComparer.Ordinal);

        for (var i = 0; i < options.Consumers.Length; i++)
        {
            var consumer = options.Consumers[i];
            var key = $"{SectionName}:Consumers[{i}]";

            if (consumer is null)
                throw new StreamConfigurationException($"{key} is null. Remove the entry or give it an object value.");

            if (string.IsNullOrWhiteSpace(consumer.Topic))
                throw new StreamConfigurationException($"{key}:Topic is missing; every consumer must name a topic.");

            if (consumer.BatchSize < 1)
                throw new StreamConfigurationException($"{key}:BatchSize is {consumer.BatchSize}; it must be >= 1.");

            if (consumer.Backpressure is null)
                throw new StreamConfigurationException($"{key}:Backpressure is null. Remove the entry or give it an object value.");

            if (consumer.Backpressure.Capacity < 1)
                throw new StreamConfigurationException($"{key}:Backpressure:Capacity is {consumer.Backpressure.Capacity}; it must be >= 1.");

            if (consumer.StartFrom == StartFrom.Date && consumer.StartFromDate is null)
                throw new StreamConfigurationException($"{key}:StartFrom is Date but {key}:StartFromDate is not set.");

            if (consumer.Persist == PersistMode.None && consumer.StartFrom == StartFrom.Stored)
            {
                throw new StreamConfigurationException(
                    $"{key}:Persist is None but {key}:StartFrom is Stored — a contradiction, because nothing is ever stored. " +
                    "Use StartFrom Now, Beginning or Date.");
            }

            // R-17: bound, documented, and read by nothing in src. The consumer host starts its read
            // loops from the stored position or from StartFrom; nothing anywhere applies StartFromDate
            // as a reset first. Accepting the key would mean a redeploy that was meant to replay
            // quietly not replaying — the exact failure the flag exists to make visible. Refused with
            // the working alternative named, until the host wires it (see 12-remediation.md, R-17).
            if (consumer.ResetOnStart)
            {
                throw new StreamConfigurationException(
                    $"{key}:ResetOnStart is true, but nothing in the consumer host reads it: the positions would NOT be reset and " +
                    "the consumer would resume where it left off, silently. Remove the key and perform the replay explicitly — " +
                    "StreamAdmin.ResetPositionAsync (or the admin reset endpoint) with the consumer scaled to zero, then start it.");
            }

            ValidateInstances(consumer.Instances, key);

            var topic = ResolveTopic(options, consumer.Topic, logger);
            var consumerName = ResolveConsumerName(options, consumer);

            // NUL separates the two halves because it cannot occur in either — so no (topic, consumer)
            // pair can be spelled two ways and collide. R-18: written as the escape \0 rather than as a
            // literal NUL byte in the source, which made this file read as binary to grep and diff.
            var pair = $"{consumer.Topic}\0{consumerName}";
            if (seen.TryGetValue(pair, out var firstIndex))
            {
                throw new StreamConfigurationException(
                    $"{key} duplicates {SectionName}:Consumers[{firstIndex}]: both consume topic '{consumer.Topic}' as consumer " +
                    $"'{consumerName}'. Two consumers on the same (Topic, Consumer) pair in one process would fight over the same position hash. " +
                    $"Set a distinct {key}:Consumer.");
            }

            seen[pair] = i;

            if (consumer.ReadMode != ReadMode.Block)
                continue;

            if (consumer.BlockMs >= syncTimeoutMs)
            {
                throw new StreamConfigurationException(
                    $"{key}:BlockMs is {consumer.BlockMs}ms but the resolved connection's syncTimeout is {syncTimeoutMs}ms. " +
                    "In Block mode BlockMs must stay well under syncTimeout, otherwise StackExchange.Redis times the XREAD out client-side. " +
                    $"Lower {key}:BlockMs or raise syncTimeout in {SectionName}:ConnectionString.");
            }

            if (!topic.CoLocatePartitions && topic.Partitions > 2)
            {
                logger?.LogWarning(
                    "Streams: {Key}:ReadMode is Block and Streams:Topics:{Topic}:CoLocatePartitions is false with {Partitions} partitions — " +
                    "each partition needs its own blocking reader connection, so this consumer will open {ConnectionCount} dedicated Redis connections. " +
                    "Set CoLocatePartitions to true to use a single multi-stream XREAD.",
                    key,
                    consumer.Topic,
                    topic.Partitions,
                    topic.Partitions);
            }
        }
    }

    private static void ValidateInstances(InstanceOptions? instances, string consumerKey)
    {
        if (instances is null)
            return;

        var key = $"{consumerKey}:Instances";

        // P4-20 wired these three keys through: Mode now selects the registry the consumer host
        // builds, and both timings reach OwnershipRegistryOptions.TtlSeconds / RenewSeconds in
        // either mode. What is left to validate is that they can produce working claims — a renew
        // interval at or past the TTL means every lease expires between renewals.
        if (instances.LeaseTtlSeconds < 1)
            throw new StreamConfigurationException($"{key}:LeaseTtlSeconds is {instances.LeaseTtlSeconds}; it must be at least 1 second.");

        if (instances.LeaseRenewSeconds < 1)
            throw new StreamConfigurationException($"{key}:LeaseRenewSeconds is {instances.LeaseRenewSeconds}; it must be at least 1 second.");

        if (instances.LeaseRenewSeconds >= instances.LeaseTtlSeconds)
        {
            throw new StreamConfigurationException(
                $"{key}:LeaseRenewSeconds is {instances.LeaseRenewSeconds}s against {key}:LeaseTtlSeconds of {instances.LeaseTtlSeconds}s. " +
                "The renew interval must be shorter than the TTL — comfortably shorter, since one missed renewal must not cost the " +
                $"claim — or every ownership claim expires between renewals. The defaults are {InstanceOptions.DefaultLeaseTtlSeconds}s " +
                $"and {InstanceOptions.DefaultLeaseRenewSeconds}s.");
        }

        // R-17 again, from the other side: in Lease mode nothing reads Count or Index — ownership is
        // claimed, and the live instance count is observed in the registry hash. Accepting them would
        // let a service declare a pool size that has no effect at all.
        if (instances.Mode == InstanceMode.Lease && (instances.Count is not null || instances.Index is not null))
        {
            throw new StreamConfigurationException(
                $"{key}:Mode is Lease, but {key}:Count / {key}:Index are set and Lease mode reads neither: it claims free partitions " +
                "and counts the live instances out of the ownership hash, which is the drift these two keys cause in Static mode. " +
                "Remove them, or set Mode to Static (the default) to keep the fixed assignment.");
        }

        if (instances.Count is int count && count < 1)
            throw new StreamConfigurationException($"{key}:Count is {count}; it must be >= 1.");

        if (instances.Index is int index && index < 0)
            throw new StreamConfigurationException($"{key}:Index is {index}; it must be >= 0.");

        if (instances.Count is int c && instances.Index is int idx && idx >= c)
        {
            throw new StreamConfigurationException(
                $"{key}:Index is {idx} but {key}:Count is {c}; Index must be less than Count (indexes are zero-based).");
        }
    }

    private static void ValidateProducers(StreamOptions options, ILogger? logger)
    {
        for (var i = 0; i < options.Producers.Length; i++)
        {
            var producer = options.Producers[i];
            var key = $"{SectionName}:Producers[{i}]";

            if (producer is null)
                throw new StreamConfigurationException($"{key} is null. Remove the entry or give it an object value.");

            if (string.IsNullOrWhiteSpace(producer.Topic))
                throw new StreamConfigurationException($"{key}:Topic is missing; every producer must name a topic.");

            if (producer.MaxBatch < 1)
                throw new StreamConfigurationException($"{key}:MaxBatch is {producer.MaxBatch}; it must be >= 1.");

            if (producer.MaxQueue < 1)
                throw new StreamConfigurationException($"{key}:MaxQueue is {producer.MaxQueue}; it must be >= 1.");

            // Referencing an unconfigured topic is legal — it is defaulted and logged at Information.
            _ = ResolveTopic(options, producer.Topic, logger);
        }
    }

    private static string AmbientEnvironmentName()
        => Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
            ?? Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
            ?? "Production";

    private static string DefaultConsumerName()
    {
        var name = System.Reflection.Assembly.GetEntryAssembly()?.GetName().Name;
        return string.IsNullOrWhiteSpace(name) ? "unknown" : name;
    }
}
