using System.Net;
using System.Reflection;
using Microsoft.Extensions.Logging;
using RedisEvents.Config;
using RedisEvents.Diagnostics;
using RedisEvents.Errors;
using RedisEvents.Tracing;
using StackExchange.Redis;

namespace RedisEvents.Consumer;

/// <summary>
/// A dedicated, <b>read-only</b> Redis connection used by a consumer running in
/// <see cref="ReadMode.Block"/>. It owns one <see cref="ConnectionMultiplexer"/> and one
/// <see cref="SocketManager"/> sized at <see cref="ConsumerOptions.ReaderThreads"/> workers
/// (default 1), so the cost of a blocking reader is exactly one connection and one named thread —
/// independent of <see cref="Environment.ProcessorCount"/>.
///
/// <para><b>Why a dedicated connection.</b> StackExchange.Redis multiplexes every command in the
/// process over one connection per endpoint. A blocking <c>XREAD</c> parks that connection
/// server-side for up to <c>BLOCK</c> milliseconds, so on the shared multiplexer it would stall
/// every command queued behind it — cache reads, position flushes, other services' calls. That is
/// head-of-line blocking at the connection, which no amount of task or channel separation in front
/// of it fixes; it is why <see cref="IDatabase"/> exposes no blocking commands at all.</para>
///
/// <para><b>Why one dedicated thread rather than the ThreadPool.</b> The default
/// <see cref="SocketManager"/> sizes its worker pool off <see cref="Environment.ProcessorCount"/>,
/// which on a 32-core node is tens of threads per multiplexer. Explicit
/// <c>workerCount: 1</c> removes that without giving up a dedicated thread — and the dedicated
/// thread matters, because socket continuations queued behind application work on a saturated
/// ThreadPool would delay this connection's heartbeat handling and trip StackExchange.Redis's
/// unhealthy-connection detection, causing reconnect churn on a connection that is perfectly fine.
/// The thread is named (see <see cref="StreamNames.ReaderThreadName"/>) so it is identifiable in a
/// dump or profile.</para>
///
/// <para><b>Read-only invariant, enforced structurally.</b> This type exposes no write operation and
/// no way to reach the underlying <see cref="IDatabase"/> or <see cref="IConnectionMultiplexer"/>.
/// <see cref="ExecuteXReadAsync"/> is the entire surface, and the command name it issues is a
/// private constant, so no other command can be sent through it. Two reasons this is an invariant
/// rather than a convention:</para>
/// <list type="number">
///   <item><description><b>Head-of-line blocking against our own publishes.</b> Any write moved
///   onto this connection would sit behind an <c>XREAD</c> parked for up to <c>BlockMs</c>,
///   reintroducing exactly the stall the dedicated connection exists to avoid.</description></item>
///   <item><description><b>It would break the outbox transaction.</b> A <c>MULTI</c>/<c>EXEC</c> is
///   bound to a single connection and cannot span two multiplexers. If publishes ever moved onto
///   the reader connection, the outbox could no longer put the state write and the <c>XADD</c> in
///   one transaction.</description></item>
/// </list>
///
/// <para><b>Never register this in DI as <see cref="IConnectionMultiplexer"/>.</b> It deliberately
/// does not implement that interface and does not hand out the multiplexer it owns, so nothing else
/// in the process can resolve it and reuse it for anything. Writes, position flushes and admin work
/// all stay on the shared multiplexer resolved by
/// <c>RedisEvents.Extensions.StreamsConnectionProvider</c>.</para>
/// </summary>
internal sealed class StreamReaderConnection : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// The only command this connection ever issues. A private constant, not a parameter: that is
    /// what makes the read-only invariant structural rather than a naming convention.
    /// </summary>
    private const string XReadCommand = "XREAD";

    /// <summary>
    /// Headroom added to <see cref="ConsumerOptions.BlockMs"/> when raising the connection's
    /// <c>syncTimeout</c> / <c>asyncTimeout</c>. The timeouts must sit above <c>BlockMs</c>,
    /// otherwise StackExchange.Redis times the blocking <c>XREAD</c> out client-side while Redis is
    /// still legitimately parked waiting for an entry.
    /// </summary>
    private const int TimeoutMarginMs = 5_000;

    private readonly IConnectionMultiplexer multiplexer;
    private readonly IDatabase db;
    private readonly SocketManager? socketManager;
    private readonly bool ownsMultiplexer;
    private bool disposed;

    /// <summary>
    /// Wraps an already-connected multiplexer. Internal so unit tests can supply a recording fake
    /// and assert that no command other than <c>XREAD</c> is ever issued.
    /// </summary>
    /// <param name="multiplexer">The multiplexer this reader will use exclusively.</param>
    /// <param name="socketManager">The socket manager to dispose with this instance; may be <see langword="null"/>.</param>
    /// <param name="ownsMultiplexer"><see langword="true"/> when disposal should close the multiplexer.</param>
    /// <param name="clientName">The resolved Redis client name.</param>
    /// <param name="threadName">The resolved socket-manager thread name.</param>
    /// <param name="blockMs">The <c>BLOCK</c> timeout the reader loop will use.</param>
    /// <param name="timeoutMs">The connection's effective sync/async timeout.</param>
    /// <param name="readerThreads">The socket manager worker count.</param>
    internal StreamReaderConnection(
        IConnectionMultiplexer multiplexer,
        SocketManager? socketManager,
        bool ownsMultiplexer,
        string clientName,
        string threadName,
        int blockMs,
        int timeoutMs,
        int readerThreads)
    {
        ArgumentNullException.ThrowIfNull(multiplexer);

        this.multiplexer = multiplexer;
        this.db = multiplexer.GetDatabase();
        this.socketManager = socketManager;
        this.ownsMultiplexer = ownsMultiplexer;
        this.ClientName = clientName;
        this.ThreadName = threadName;
        this.BlockMs = blockMs;
        this.TimeoutMs = timeoutMs;
        this.ReaderThreads = readerThreads;
    }

    /// <summary>The Redis <c>CLIENT SETNAME</c> value for this connection.</summary>
    public string ClientName { get; }

    /// <summary>The name given to this reader's <see cref="SocketManager"/> threads.</summary>
    public string ThreadName { get; }

    /// <summary>The <c>BLOCK</c> timeout, in milliseconds, the reader loop should use.</summary>
    public int BlockMs { get; }

    /// <summary>
    /// The effective <c>syncTimeout</c> / <c>asyncTimeout</c> of this connection, in milliseconds.
    /// Always above <see cref="BlockMs"/>.
    /// </summary>
    public int TimeoutMs { get; }

    /// <summary>The number of dedicated socket worker threads (default 1).</summary>
    public int ReaderThreads { get; }

    /// <summary>Whether the underlying connection is currently established.</summary>
    public bool IsConnected => !this.disposed && this.multiplexer.IsConnected;

    /// <summary>
    /// Opens a dedicated reader connection for one consumer.
    /// </summary>
    /// <param name="options">The root stream options; supplies the connection string.</param>
    /// <param name="consumer">The consumer entry; supplies <c>BlockMs</c> and <c>ReaderThreads</c>.</param>
    /// <param name="consumerName">The resolved consumer name, used in the client and thread names.</param>
    /// <param name="index">Reader index within the consumer, for when a consumer needs more than one.</param>
    /// <param name="logger">Optional logger; the resolved endpoint, client name and timeouts are logged at Information.</param>
    /// <param name="tracingApplier">Optional tracing applier for Redis operation tracing.</param>
    /// <param name="concurrentReads">
    /// How many blocking reads this connection may have outstanding at once — one per read loop that
    /// will use it. See <see cref="CreateAsync"/> for why the timeout depends on it.
    /// </param>
    /// <returns>A connected, read-only reader connection. Dispose it with the host.</returns>
    /// <exception cref="StreamTransportException">Thrown when the connect attempt fails.</exception>
    public static StreamReaderConnection Create(
        StreamOptions options,
        ConsumerOptions consumer,
        string consumerName,
        int index,
        ILogger? logger,
        IRedisTracingApplier? tracingApplier = null,
        int concurrentReads = 1)
    {
        var plan = Plan(options, consumer, consumerName, index, concurrentReads);

        try
        {
            var connected = ConnectionMultiplexer.Connect(plan.Configuration);
            
            // Apply Redis tracing if a tracing applier is available
            tracingApplier?.ApplyTracing(connected);
            
            Log(logger, plan, connected);
            return Wrap(plan, connected);
        }
        catch (Exception ex) when (ex is not StreamConfigurationException)
        {
            plan.SocketManager.Dispose();
            throw Failure(plan, ex);
        }
    }

    /// <summary>
    /// Opens a dedicated reader connection for one consumer, asynchronously.
    /// </summary>
    /// <param name="options">The root stream options; supplies the connection string.</param>
    /// <param name="consumer">The consumer entry; supplies <c>BlockMs</c> and <c>ReaderThreads</c>.</param>
    /// <param name="consumerName">The resolved consumer name, used in the client and thread names.</param>
    /// <param name="index">Reader index within the consumer, for when a consumer needs more than one.</param>
    /// <param name="logger">Optional logger; the resolved endpoint, client name and timeouts are logged at Information.</param>
    /// <param name="tracingApplier">Optional tracing applier for Redis operation tracing.</param>
    /// <param name="concurrentReads">
    /// How many blocking reads this connection may have outstanding at once — one per read loop that
    /// will use it. A co-located consumer issues one multi-stream <c>XREAD</c> for all its
    /// partitions and so leaves this at 1; a consumer with <c>CoLocatePartitions = false</c> runs one
    /// read loop per partition, and they queue on this one connection, so the last of N waits about
    /// (N-1) x <c>BlockMs</c> before its own block even starts. The client-side timeout has to cover
    /// that wait or StackExchange.Redis abandons a perfectly healthy read.
    /// </param>
    /// <returns>A connected, read-only reader connection. Dispose it with the host.</returns>
    /// <exception cref="StreamTransportException">Thrown when the connect attempt fails.</exception>
    public static async Task<StreamReaderConnection> CreateAsync(
        StreamOptions options,
        ConsumerOptions consumer,
        string consumerName,
        int index,
        ILogger? logger,
        IRedisTracingApplier? tracingApplier = null,
        int concurrentReads = 1)
    {
        var plan = Plan(options, consumer, consumerName, index, concurrentReads);

        try
        {
            var connected = await ConnectionMultiplexer.ConnectAsync(plan.Configuration).ConfigureAwait(false);
            
            // Apply Redis tracing if a tracing applier is available
            tracingApplier?.ApplyTracing(connected);
            
            Log(logger, plan, connected);
            return Wrap(plan, connected);
        }
        catch (Exception ex) when (ex is not StreamConfigurationException)
        {
            plan.SocketManager.Dispose();
            throw Failure(plan, ex);
        }
    }

    /// <summary>
    /// Issues <c>XREAD</c> — the only command this connection can send.
    /// </summary>
    /// <param name="args">
    /// The arguments <i>after</i> the command name, for example
    /// <c>BLOCK</c>, <c>1000</c>, <c>COUNT</c>, <c>100</c>, <c>STREAMS</c>, keys…, ids….
    /// The reader loop builds this array once and mutates the id slots between reads, so a steady
    /// read costs no allocation.
    /// </param>
    /// <param name="ct">
    /// Observed on entry only. StackExchange.Redis cannot abort a command already on the wire, and
    /// wrapping the task to do so would leak the in-flight read; a bounded <c>BlockMs</c> is what
    /// makes that acceptable — shutdown waits at most one block interval.
    /// </param>
    /// <returns>The raw <c>XREAD</c> reply, or a nil result when the block expired with no entries.</returns>
    /// <exception cref="OperationCanceledException">Thrown when <paramref name="ct"/> is already cancelled.</exception>
    public Task<RedisResult> ExecuteXReadAsync(object[] args, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);
        ArgumentNullException.ThrowIfNull(args);
        ct.ThrowIfCancellationRequested();

        // DemandMaster: streams must be read from the primary — a replica can lag arbitrarily and
        // would silently hand the consumer a stale tail.
        return this.db.ExecuteAsync(XReadCommand, args, CommandFlags.DemandMaster);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (this.disposed)
            return;

        this.disposed = true;

        // Multiplexer first: its shutdown still needs the socket manager's thread to close cleanly.
        if (this.ownsMultiplexer)
            this.multiplexer.Dispose();

        // A SocketManager handed to ConnectionMultiplexer is not owned by it, so it is ours to close.
        this.socketManager?.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (this.disposed)
            return;

        this.disposed = true;

        if (this.ownsMultiplexer)
            await this.multiplexer.DisposeAsync().ConfigureAwait(false);

        this.socketManager?.Dispose();
    }

    private static ReaderPlan Plan(
        StreamOptions options,
        ConsumerOptions consumer,
        string consumerName,
        int index,
        int concurrentReads)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(consumer);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerName);
        ArgumentOutOfRangeException.ThrowIfNegative(index);

        var connectionString = StreamConfigBinder.ResolveConnectionString(options);

        ConfigurationOptions configuration;
        try
        {
            configuration = ConfigurationOptions.Parse(connectionString);
        }
        catch (Exception ex)
        {
            throw new StreamConfigurationException(
                $"{StreamConfigBinder.SectionName}:ConnectionString is not a valid Redis connection string ('{connectionString}'): {ex.Message}",
                ex);
        }

        var blockMs = consumer.BlockMs;
        var readerThreads = consumer.ReaderThreads < 1 ? 1 : consumer.ReaderThreads;

        // syncTimeout and asyncTimeout must sit above BlockMs, or StackExchange.Redis abandons the
        // blocking XREAD client-side while Redis is still legitimately parked waiting for an entry.
        //
        // And above N x BlockMs when N read loops share this connection. A blocking XREAD parks the
        // connection server-side, so the reads queue: the last of N does not even start until the
        // other N-1 have each blocked out their interval, and the client's timer for it is running
        // the whole time. At N=8 and the default BlockMs=1000 a flat BlockMs + margin is 6 s against
        // an 8 s wait — a RedisTimeoutException on a connection that is perfectly healthy. Co-located
        // consumers pass 1 because they issue one multi-stream read for every partition they own.
        var reads = concurrentReads < 1 ? 1 : concurrentReads;
        var required = (int)Math.Min(int.MaxValue, ((long)blockMs * reads) + TimeoutMarginMs);
        if (configuration.SyncTimeout < required)
            configuration.SyncTimeout = required;
        if (configuration.AsyncTimeout < required)
            configuration.AsyncTimeout = required;

        var threadName = StreamNames.ReaderThreadName(consumerName, index);
        var clientName = StreamNames.ReaderClientName(ServiceName(), consumerName, index, PodName());

        // workerCount is the whole point: the default SocketManager sizes its pool off
        // ProcessorCount, giving tens of threads per multiplexer on a big node.
        var manager = new SocketManager(threadName, readerThreads, useHighPrioritySocketThreads: false);

        configuration.SocketManager = manager;
        configuration.ClientName = clientName;

        return new ReaderPlan(configuration, manager, clientName, threadName, blockMs, configuration.SyncTimeout, readerThreads);
    }

    private static StreamReaderConnection Wrap(in ReaderPlan plan, IConnectionMultiplexer connected) =>
        new(
            connected,
            plan.SocketManager,
            ownsMultiplexer: true,
            plan.ClientName,
            plan.ThreadName,
            plan.BlockMs,
            plan.TimeoutMs,
            plan.ReaderThreads);

    private static void Log(ILogger? logger, in ReaderPlan plan, IConnectionMultiplexer connected)
    {
        if (logger is null)
            return;

        logger.LogInformation(
            "Streams: opened a dedicated read-only reader connection; endpoint={Endpoint} clientName={ClientName} thread={ThreadName} " +
            "workerCount={ReaderThreads} blockMs={BlockMs} timeoutMs={TimeoutMs}. This multiplexer issues XREAD only and is never registered in DI.",
            Describe(connected.GetEndPoints()),
            plan.ClientName,
            plan.ThreadName,
            plan.ReaderThreads,
            plan.BlockMs,
            plan.TimeoutMs);
    }

    private static StreamTransportException Failure(in ReaderPlan plan, Exception ex) =>
        new($"Streams: failed to open the dedicated reader connection '{plan.ClientName}' to Redis at " +
            $"'{Describe(plan.Configuration.EndPoints)}'. Check {StreamConfigBinder.SectionName}:ConnectionString.",
            ex);

    private static string Describe(IEnumerable<EndPoint> endpoints)
    {
        var text = string.Join(",", endpoints.Select(static endpoint => endpoint switch
        {
            DnsEndPoint dns => $"{dns.Host}:{dns.Port}",
            IPEndPoint ip => $"{ip.Address}:{ip.Port}",
            _ => endpoint.ToString() ?? "unknown",
        }));

        return string.IsNullOrEmpty(text) ? "unknown" : text;
    }

    private static string ServiceName() =>
        Assembly.GetEntryAssembly()?.GetName().Name ?? "unknown";

    private static string PodName() =>
        Environment.GetEnvironmentVariable(InstanceResolver.PodNameVariable) ?? Environment.MachineName;

    /// <summary>Everything resolved before the connect attempt, so failure can clean up and report precisely.</summary>
    private readonly record struct ReaderPlan(
        ConfigurationOptions Configuration,
        SocketManager SocketManager,
        string ClientName,
        string ThreadName,
        int BlockMs,
        int TimeoutMs,
        int ReaderThreads);
}
