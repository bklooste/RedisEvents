using System.Net;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RedisEvents.Config;
using RedisEvents.Diagnostics;
using RedisEvents.Errors;
using RedisEvents.Tracing;
using StackExchange.Redis;

namespace RedisEvents.Extensions;

/// <summary>
/// The shared streams connection, for code that has to speak to the same Redis the library does.
/// </summary>
/// <remarks>
/// <para>
/// The resolver itself (<see cref="StreamsConnectionProvider"/>) is internal, and core deliberately
/// registers no <see cref="IConnectionMultiplexer"/> of its own — a service's cache connection must
/// never be mistaken for the streams one. This is the three-line facade over it, so a sibling
/// package or a service can put its own keys (a view store's hashes, say) on the connection the
/// library already holds instead of opening a second one to the same server.
/// </para>
/// <para>
/// It is the <b>shared</b> connection: the one writes, positions and admin work go through, never a
/// consumer's dedicated reader multiplexer, which is read-only and may be parked in a blocking
/// <c>XREAD</c>. It is resolved once per process and cached, and the endpoint-match guard has
/// already run on it — which is the point of going through here rather than connecting by hand.
/// </para>
/// </remarks>
public static class StreamsConnection
{
    /// <summary>
    /// The shared streams <see cref="IDatabase"/>, connecting on first use.
    /// </summary>
    /// <param name="services">A built container that has had an <c>AddStream…</c> call on its builder.</param>
    /// <returns>A database handle on the shared streams multiplexer.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is null.</exception>
    /// <exception cref="StreamConfigurationException">Nothing in the container registered the streams connection — no <c>AddStream…</c> call was made.</exception>
    /// <exception cref="StreamTransportException">The library's own connect attempt failed.</exception>
    public static IDatabase GetSharedDatabase(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);

        var provider = services.GetService<StreamsConnectionProvider>()
            ?? throw new StreamConfigurationException(
                "Streams: the shared connection is not registered, so there is no streams database to hand out. " +
                "It is registered by the first AddStream/AddStreamPublisher/AddStreamStore call on the host builder; " +
                "make one before asking for the connection.");

        return provider.Connection.GetDatabase();
    }
}

/// <summary>
/// Where the shared streams multiplexer came from.
/// </summary>
internal enum StreamsConnectionSource
{
    /// <summary>An <see cref="IConnectionMultiplexer"/> already in the container was reused because its endpoint matched.</summary>
    Reused = 0,

    /// <summary>The library connected its own multiplexer from <c>Streams:ConnectionString</c>.</summary>
    Created = 1,
}

/// <summary>
/// Resolves the shared multiplexer used for writes, positions and admin work.
///
/// A multiplexer already registered in the container is reused <b>only if its endpoint matches the
/// configured one</b>. That check is the whole point of this type: a service wired to
/// <c>redis-cache</c> (which runs with <c>--save "" --appendonly no</c>) must never have that
/// connection reused for streams, because a pod restart would lose every stream, every position and
/// every unprocessed message. When the endpoints differ the library connects its own multiplexer to
/// <c>Streams:ConnectionString</c> — defaulting to <c>redis-db.infra:6379</c> — and logs both
/// endpoints so the mismatch is visible rather than silent.
///
/// Whichever path is taken is logged at Information, together with the resolved endpoint, in the
/// first few lines of startup.
///
/// Note this covers the <i>shared</i> connection only. A consumer host running in
/// <see cref="ReadMode.Block"/> gets its own dedicated, unregistered reader multiplexer (P1).
/// </summary>
internal sealed class StreamsConnectionProvider : IAsyncDisposable, IDisposable
{
    private readonly StreamOptions options;
    private readonly IServiceProvider? services;
    private readonly ILogger? logger;
    private readonly IRedisTracingApplier? tracingApplier;
    private readonly Lock gate = new();

    private IConnectionMultiplexer? connection;
    private bool owned;
    private bool disposed;

    /// <summary>
    /// Creates a provider that resolves lazily on first access to <see cref="Connection"/>.
    /// </summary>
    /// <param name="options">The bound stream options.</param>
    /// <param name="services">The container to look for an existing multiplexer in; may be <see langword="null"/>.</param>
    /// <param name="logger">Optional logger; the resolution path is logged at Information.</param>
    public StreamsConnectionProvider(StreamOptions options, IServiceProvider? services, ILogger? logger)
        : this(options, services, logger, null)
    {
    }
    
    /// <summary>
    /// Creates a provider that resolves lazily on first access to <see cref="Connection"/> with tracing support.
    /// </summary>
    /// <param name="options">The bound stream options.</param>
    /// <param name="services">The container to look for an existing multiplexer in; may be <see langword="null"/>.</param>
    /// <param name="logger">Optional logger; the resolution path is logged at Information.</param>
    /// <param name="tracingApplier">Optional tracing applier for Redis operation tracing.</param>
    public StreamsConnectionProvider(StreamOptions options, IServiceProvider? services, ILogger? logger, IRedisTracingApplier? tracingApplier)
    {
        ArgumentNullException.ThrowIfNull(options);

        this.options = options;
        this.services = services;
        this.logger = logger;
        this.tracingApplier = tracingApplier;
    }

    /// <summary>
    /// The configured connection string, after defaulting.
    /// </summary>
    public string ConnectionString => StreamConfigBinder.ResolveConnectionString(this.options);

    /// <summary>
    /// The tracing applier for Redis operation tracing, if configured.
    /// </summary>
    public IRedisTracingApplier? TracingApplier => this.tracingApplier;

    /// <summary>
    /// Whether the multiplexer was reused from the container or created by the library.
    /// Resolves the connection if it has not been resolved yet.
    /// </summary>
    public StreamsConnectionSource Source
    {
        get
        {
            _ = this.Connection;
            return this.owned ? StreamsConnectionSource.Created : StreamsConnectionSource.Reused;
        }
    }

    /// <summary>
    /// The shared multiplexer, resolved on first access and cached for the life of the provider.
    /// </summary>
    /// <exception cref="StreamTransportException">Thrown when the library's own connect attempt fails.</exception>
    public IConnectionMultiplexer Connection
    {
        get
        {
            ObjectDisposedException.ThrowIf(this.disposed, this);

            var existing = Volatile.Read(ref this.connection);
            if (existing is not null)
                return existing;

            lock (this.gate)
            {
                if (this.connection is not null)
                    return this.connection;

                this.connection = Resolve(this.services, this.options, this.logger, out var created);
        this.owned = created;
        
        // Apply Redis tracing if a tracing applier is available
        this.tracingApplier?.ApplyTracing(this.connection);
        
        return this.connection;
            }
        }
    }

    /// <summary>
    /// Reuse-or-connect. Reuses an <see cref="IConnectionMultiplexer"/> from <paramref name="services"/>
    /// only when its endpoint matches the configured one; otherwise connects a new multiplexer.
    /// </summary>
    /// <param name="services">The container to look in; may be <see langword="null"/>.</param>
    /// <param name="options">The bound stream options.</param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="created"><see langword="true"/> when a new multiplexer was created and must be disposed by the caller.</param>
    /// <returns>The multiplexer to use for writes, positions and admin.</returns>
    /// <exception cref="StreamTransportException">Thrown when connecting fails.</exception>
    public static IConnectionMultiplexer Resolve(
        IServiceProvider? services,
        StreamOptions options,
        ILogger? logger,
        out bool created)
    {
        ArgumentNullException.ThrowIfNull(options);

        var connectionString = StreamConfigBinder.ResolveConnectionString(options);
        var configured = ParseConfiguration(connectionString);
        var configuredEndpoint = DescribeEndpoints(configured.EndPoints);

        var candidate = services?.GetService<IConnectionMultiplexer>();
        if (candidate is not null)
        {
            var candidateEndpoint = DescribeEndpoints(candidate.GetEndPoints());

            if (EndpointsMatch(candidate, connectionString))
            {
                logger?.LogInformation(
                    "Streams: reusing the IConnectionMultiplexer already registered in the container; endpoint={Endpoint} clientName={ClientName}.",
                    candidateEndpoint,
                    candidate.ClientName);

                created = false;
                return candidate;
            }

            logger?.LogInformation(
                "Streams: NOT reusing the registered IConnectionMultiplexer — its endpoint ({ExistingEndpoint}) does not match the configured Streams endpoint ({Endpoint}). " +
                "Connecting a dedicated streams multiplexer. Streams must live on redis-db (AOF persistence); redis-cache would lose every stream and position on restart.",
                candidateEndpoint,
                configuredEndpoint);
        }

        var clientName = StreamNames.SharedClientName(ServiceName(), PodName());
        configured.ClientName = clientName;

        logger?.LogInformation(
            "Streams: connecting a dedicated streams multiplexer; endpoint={Endpoint} clientName={ClientName} source={Source}.",
            configuredEndpoint,
            clientName,
            string.IsNullOrWhiteSpace(options.ConnectionString) ? "default" : $"{StreamConfigBinder.SectionName}:ConnectionString");

        try
        {
            created = true;
            return ConnectionMultiplexer.Connect(configured);
        }
        catch (Exception ex)
        {
            throw new StreamTransportException(
                $"Streams: failed to connect to Redis at '{configuredEndpoint}'. " +
                $"Check {StreamConfigBinder.SectionName}:ConnectionString (default '{StreamConfigBinder.DefaultConnectionString}').",
                ex);
        }
    }

    /// <summary>
    /// True when <paramref name="existing"/> is connected to at least one of the endpoints named by
    /// <paramref name="connectionString"/>. Comparison is on host (case-insensitive) and port, so a
    /// resolved <see cref="IPEndPoint"/> only matches an equally literal configured address.
    /// </summary>
    /// <param name="existing">The multiplexer already in the container.</param>
    /// <param name="connectionString">The configured streams connection string.</param>
    /// <returns><see langword="true"/> when the endpoints match and the multiplexer may be reused.</returns>
    public static bool EndpointsMatch(IConnectionMultiplexer existing, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(existing);

        var wanted = ParseConfiguration(connectionString).EndPoints;
        if (wanted.Count == 0)
            return false;

        var have = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var endpoint in existing.GetEndPoints())
            have.Add(Describe(endpoint));

        // The multiplexer's own configuration string is also consulted: once connected, GetEndPoints()
        // can report a resolved address where the configuration still carries the DNS name.
        if (!string.IsNullOrWhiteSpace(existing.Configuration))
        {
            try
            {
                foreach (var endpoint in ConfigurationOptions.Parse(existing.Configuration).EndPoints)
                    have.Add(Describe(endpoint));
            }
            catch (ArgumentException)
            {
                // An unparseable configuration string simply contributes nothing.
            }
        }

        foreach (EndPoint endpoint in wanted)
        {
            if (have.Contains(Describe(endpoint)))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Renders an endpoint collection as a comma-separated <c>host:port</c> list, for logging.
    /// </summary>
    /// <param name="endpoints">The endpoints.</param>
    /// <returns>A human-readable endpoint list.</returns>
    public static string DescribeEndpoints(IEnumerable<EndPoint> endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        return string.Join(",", endpoints.Select(Describe));
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (this.disposed)
            return;

        this.disposed = true;

        // Only a multiplexer this provider created is ours to close; a reused one belongs to the container.
        if (this.owned)
            this.connection?.Dispose();

        this.connection = null;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (this.disposed)
            return;

        this.disposed = true;

        if (this.owned && this.connection is not null)
            await this.connection.DisposeAsync().ConfigureAwait(false);

        this.connection = null;
    }

    private static ConfigurationOptions ParseConfiguration(string connectionString)
    {
        var effective = string.IsNullOrWhiteSpace(connectionString)
            ? StreamConfigBinder.DefaultConnectionString
            : connectionString;

        try
        {
            return ConfigurationOptions.Parse(effective);
        }
        catch (Exception ex)
        {
            throw new StreamConfigurationException(
                $"{StreamConfigBinder.SectionName}:ConnectionString is not a valid Redis connection string ('{effective}'): {ex.Message}",
                ex);
        }
    }

    private static string Describe(EndPoint endpoint) => endpoint switch
    {
        DnsEndPoint dns => $"{dns.Host}:{dns.Port}",
        IPEndPoint ip => $"{ip.Address}:{ip.Port}",
        _ => endpoint.ToString() ?? "unknown",
    };

    private static string ServiceName() =>
        Assembly.GetEntryAssembly()?.GetName().Name ?? "unknown";

    private static string PodName() =>
        Environment.GetEnvironmentVariable(InstanceResolver.PodNameVariable) ?? Environment.MachineName;
}
