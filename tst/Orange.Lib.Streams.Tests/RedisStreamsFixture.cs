using System.Globalization;
using System.Runtime.CompilerServices;

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

using Orange.Lib.Streams.Ownership;

using StackExchange.Redis;

namespace Orange.Lib.Streams.Tests;

/// <summary>
/// The collection every Orange.Lib.Streams service test belongs to.
/// </summary>
/// <remarks>
/// <para>
/// Put <c>[Collection(RedisStreamsCollection.Name)]</c> on the test class and take
/// <see cref="RedisStreamsFixture"/> in its constructor. Membership of one collection is what keeps
/// the suite to a <em>single</em> Redis container: a fixture per class would start (and wait for)
/// a container per test class, which turns a two-minute suite into a twenty-minute one.
/// </para>
/// <para>
/// Classes in one collection do not run in parallel with each other, which is also what makes
/// <see cref="RedisStreamsFixture.FlushAllAsync"/> safe to call between tests.
/// </para>
/// </remarks>
[CollectionDefinition(RedisStreamsCollection.Name)]
public sealed class RedisStreamsCollection : ICollectionFixture<RedisStreamsFixture>
{
    /// <summary>The collection name. Use the constant rather than retyping the string.</summary>
    public const string Name = "RedisStreams";
}

/// <summary>
/// One throwaway Redis container, shared by every service test in
/// <see cref="RedisStreamsCollection"/>, plus the small amount of plumbing every test needs:
/// a connected multiplexer, a <c>FLUSHALL</c>, and unique topic names.
/// </summary>
/// <remarks>
/// <para>
/// Modelled on <c>Orange.Lib.Kalshi.Trading.Tests.RedisFixture</c> — the generic
/// <see cref="ContainerBuilder"/>, no extra Testcontainers module dependency, a random host port,
/// and nothing that reaches for the shared <c>lode-test-network</c> (the CI network-isolation rule:
/// library tests are self-contained).
/// </para>
/// <para>
/// <b>Why <c>redis:8-alpine</c> and not the <c>redis:7-alpine</c> the plan text names.</b> The
/// ownership registry leases fields with <c>HEXPIRE</c>, which only exists from Redis 7.4. The
/// <c>7</c> tag is a floating major: it happens to point at 7.4.x today, but it covered 7.0 and 7.2
/// — where <c>HEXPIRE</c> is an unknown command — and nothing stops a cached or older layer on a CI
/// box from being one of those. Every ownership test would then fail for a reason that has nothing
/// to do with the code under test. <c>8-alpine</c> is unambiguously above the floor and matches the
/// redis-stack version the cluster runs.
/// </para>
/// <para>
/// The container is held statically and reference-counted, so it starts once and stays up for the
/// whole run even if a class is wired with <c>IClassFixture</c> by mistake.
/// </para>
/// </remarks>
public sealed class RedisStreamsFixture : IAsyncLifetime
{
    /// <summary>Redis >= 7.4 is required: the ownership registry leases fields with <c>HEXPIRE</c>.</summary>
    public const string Image = "redis:8-alpine";

    private const int RedisPort = 6379;

    private static readonly SemaphoreSlim Gate = new(1, 1);

    private static IContainer? container;
    private static IConnectionMultiplexer? redis;
    private static int users;

    private static int topicCounter;

    /// <summary>The shared multiplexer, connected with <c>allowAdmin</c> so tests can flush.</summary>
    public IConnectionMultiplexer Redis => redis
        ?? throw new InvalidOperationException(
            "The Redis container has not started. Is the test class tagged " +
            $"[Collection(RedisStreamsCollection.Name)] and taking {nameof(RedisStreamsFixture)} in its constructor?");

    /// <summary>The container's <c>host:port</c>, for building a library connection string.</summary>
    public string Endpoint { get; private set; } = string.Empty;

    /// <summary>
    /// A connection string for code under test — no <c>allowAdmin</c>, so a library bug that issues
    /// an admin command shows up here rather than in production.
    /// </summary>
    public string ConnectionString => $"{this.Endpoint},abortConnect=false";

    /// <summary>The fixture's own connection string, which does allow admin commands.</summary>
    public string AdminConnectionString => $"{this.Endpoint},abortConnect=false,allowAdmin=true";

    /// <summary>A database handle on the shared multiplexer.</summary>
    public IDatabase Db => this.Redis.GetDatabase();

    /// <summary>A database handle, optionally on a non-default database index.</summary>
    public IDatabase GetDatabase(int db = -1) => this.Redis.GetDatabase(db);

    /// <summary>
    /// Opens a second, independent multiplexer — for tests that need to prove two connections do not
    /// interfere (blocking reads, outbox transactions). The caller disposes it.
    /// </summary>
    public async Task<IConnectionMultiplexer> ConnectAsync(bool allowAdmin = false)
        => await ConnectionMultiplexer
            .ConnectAsync(allowAdmin ? this.AdminConnectionString : this.ConnectionString)
            .ConfigureAwait(false);

    /// <summary>
    /// One simulated pod: a pod name, the instance id that pod's process would stamp on to
    /// everything it writes, and a multiplexer of its own.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why this exists.</b> Every restart and reset test in this suite used to run the consumer
    /// and the admin call in one process, so both sides shared
    /// <see cref="OwnershipRegistry.ProcessInstanceId"/> and any bug that depends on the two
    /// disagreeing was invisible (remediation finding P0-1). A test that takes one of these acts
    /// under a <em>second</em> identity.
    /// </para>
    /// <para>
    /// <b>Why not a real second OS process.</b> Nothing in the library can observe a process
    /// boundary. What it observes is (a) the instance id stamped into <c>p:{topic}:&lt;consumer&gt;</c>
    /// and <c>o:{topic}:&lt;consumer&gt;</c> and (b) which connection issued a command. Both are
    /// reproduced here — the identity is injected exactly as a real pod injects it (from
    /// <c>POD_NAME</c>, through the same
    /// <see cref="OwnershipRegistry.InstanceIdFor(string?)"/> the host uses), and
    /// <see cref="ConnectInstanceAsync"/> gives the pod its own multiplexer. Spawning a child
    /// <c>dotnet</c> process would add container plumbing and minutes of wall time to prove a
    /// property neither the flusher nor the registry can see.
    /// </para>
    /// <para>
    /// <b>Modelling a restart.</b> Ask for two instances with the <em>same</em> pod name: that is a
    /// StatefulSet pod bouncing back into its own ordinal, and it is exactly the case P0-1 mistook
    /// for a rival. Two <em>different</em> pod names are two live pods — a genuine overlap, which
    /// must still be detected.
    /// </para>
    /// </remarks>
    public sealed class StreamsInstance : IAsyncDisposable
    {
        private readonly bool ownsConnection;

        internal StreamsInstance(string podName, IConnectionMultiplexer redis, bool ownsConnection)
        {
            this.PodName = podName;
            this.InstanceId = OwnershipRegistry.InstanceIdFor(podName);
            this.Redis = redis;
            this.ownsConnection = ownsConnection;
        }

        /// <summary>The pod name this instance runs under — its <c>POD_NAME</c>.</summary>
        public string PodName { get; }

        /// <summary>The instance id this pod's process stamps into ownership claims and positions.</summary>
        public Guid InstanceId { get; }

        /// <summary>The multiplexer this instance issues its commands on.</summary>
        public IConnectionMultiplexer Redis { get; }

        /// <summary>A database handle on this instance's connection.</summary>
        public IDatabase Db => this.Redis.GetDatabase();

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            if (this.ownsConnection)
            {
                await this.Redis.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// A second identity sharing the fixture's multiplexer — enough for anything that only cares
    /// about who stamped a value, not which socket it arrived on.
    /// </summary>
    /// <param name="podName">The pod name the identity is derived from. Reuse a name to model a
    /// restart of that pod; use a new one to model a second, concurrently live pod.</param>
    /// <returns>The simulated instance. Disposing it is a no-op.</returns>
    public StreamsInstance AsInstance(string podName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(podName);
        return new StreamsInstance(podName, this.Redis, ownsConnection: false);
    }

    /// <summary>
    /// A second identity with a connection of its own, for tests where the two sides must not share
    /// a multiplexer (an admin CLI, or a pod whose connection is meant to drop independently).
    /// </summary>
    /// <param name="podName">The pod name the identity is derived from.</param>
    /// <returns>The simulated instance; the caller disposes it, which closes its connection.</returns>
    public async Task<StreamsInstance> ConnectInstanceAsync(string podName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(podName);
        var connection = await this.ConnectAsync().ConfigureAwait(false);
        return new StreamsInstance(podName, connection, ownsConnection: true);
    }

    /// <summary>
    /// Wipes every key on the server. Call it at the start of a test, not the end: a test that fails
    /// leaves its keys behind for inspection, and the next test still starts clean.
    /// </summary>
    public async Task FlushAllAsync()
    {
        foreach (var endpoint in this.Redis.GetEndPoints())
        {
            var server = this.Redis.GetServer(endpoint);
            if (server.IsConnected && !server.IsReplica)
            {
                await server.FlushAllDatabasesAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// A topic name no other test can collide with: the calling test method, a process-wide counter
    /// and a short random suffix.
    /// </summary>
    /// <remarks>
    /// Topics become Redis key components (<c>s:{topic}:0</c>), so the name is reduced to
    /// <c>[a-z0-9-]</c>. Unique names are what make it safe for a test to assert on <c>XLEN</c> or on
    /// a key glob while other tests share the same server.
    /// </remarks>
    /// <param name="hint">Defaults to the calling member's name.</param>
    public string NewTopic([CallerMemberName] string hint = "topic")
    {
        var n = Interlocked.Increment(ref topicCounter);
        var suffix = Guid.NewGuid().ToString("N")[..6];

        return string.Create(
            CultureInfo.InvariantCulture,
            $"{Sanitise(hint)}-{n.ToString(CultureInfo.InvariantCulture)}-{suffix}");
    }

    /// <summary>
    /// A consumer name unique to the calling test, for the same reason topics are.
    /// </summary>
    public string NewConsumer([CallerMemberName] string hint = "consumer")
        => $"{Sanitise(hint)}-{Guid.NewGuid().ToString("N")[..6]}";

    /// <summary>
    /// Polls <paramref name="condition"/> until it holds or <paramref name="timeout"/> elapses.
    /// </summary>
    /// <remarks>
    /// The supported way to wait for something in this suite. A bare <c>Task.Delay</c> as a
    /// synchronisation device is a flaky test waiting to happen — it is either too short on a loaded
    /// CI box or wasted time on a fast one. This is neither: it returns as soon as the condition
    /// holds and fails loudly with <paramref name="because"/> when it never does.
    /// </remarks>
    /// <exception cref="TimeoutException">The condition did not hold within the timeout.</exception>
    public static async Task WaitUntilAsync(
        Func<bool> condition,
        TimeSpan? timeout = null,
        string? because = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(condition);

        var limit = timeout ?? TimeSpan.FromSeconds(30);
        var deadline = DateTime.UtcNow + limit;
        var wait = TimeSpan.FromMilliseconds(2);

        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"Timed out after {limit.TotalSeconds:0.##}s waiting for {because ?? "a condition"}.");
            }

            await Task.Delay(wait, ct).ConfigureAwait(false);

            // Tight at first so a fast condition returns fast, then backed off so a long wait is cheap.
            if (wait < TimeSpan.FromMilliseconds(25))
            {
                wait *= 2;
            }
        }
    }

    /// <summary>
    /// The async form of <see cref="WaitUntilAsync(Func{bool}, TimeSpan?, string?, CancellationToken)"/>,
    /// for conditions that have to ask Redis.
    /// </summary>
    /// <exception cref="TimeoutException">The condition did not hold within the timeout.</exception>
    public static async Task WaitUntilAsync(
        Func<Task<bool>> condition,
        TimeSpan? timeout = null,
        string? because = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(condition);

        var limit = timeout ?? TimeSpan.FromSeconds(30);
        var deadline = DateTime.UtcNow + limit;
        var wait = TimeSpan.FromMilliseconds(2);

        while (!await condition().ConfigureAwait(false))
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"Timed out after {limit.TotalSeconds:0.##}s waiting for {because ?? "a condition"}.");
            }

            await Task.Delay(wait, ct).ConfigureAwait(false);

            if (wait < TimeSpan.FromMilliseconds(25))
            {
                wait *= 2;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        await Gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (container is null)
            {
                var started = new ContainerBuilder()
                    .WithImage(Image)
                    .WithPortBinding(RedisPort, assignRandomHostPort: true)
                    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(RedisPort))
                    .Build();

                await started.StartAsync().ConfigureAwait(false);

                var endpoint = $"{started.Hostname}:{started.GetMappedPublicPort(RedisPort)}";
                redis = await ConnectionMultiplexer
                    .ConnectAsync($"{endpoint},abortConnect=false,allowAdmin=true")
                    .ConfigureAwait(false);

                container = started;
                this.Endpoint = endpoint;
            }
            else
            {
                this.Endpoint = $"{container.Hostname}:{container.GetMappedPublicPort(RedisPort)}";
            }

            users++;
        }
        finally
        {
            _ = Gate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await Gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (--users > 0)
            {
                return;
            }

            if (redis is not null)
            {
                await redis.DisposeAsync().ConfigureAwait(false);
                redis = null;
            }

            if (container is not null)
            {
                await container.DisposeAsync().ConfigureAwait(false);
                container = null;
            }
        }
        finally
        {
            _ = Gate.Release();
        }
    }

    /// <summary>Reduces a test method name to something safe to embed in a Redis key.</summary>
    private static string Sanitise(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return "topic";
        }

        var chars = text.ToLowerInvariant().ToCharArray();
        for (var i = 0; i < chars.Length; i++)
        {
            if (!char.IsAsciiLetterOrDigit(chars[i]))
            {
                chars[i] = '-';
            }
        }

        var cleaned = new string(chars).Trim('-');
        return cleaned.Length == 0 ? "topic" : cleaned.Length <= 40 ? cleaned : cleaned[..40];
    }
}
