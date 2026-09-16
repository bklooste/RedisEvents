using System.Collections.ObjectModel;
using System.Globalization;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RedisEvents.Admin;
using RedisEvents.Config;
using RedisEvents.Diagnostics;
using RedisEvents.Errors;
using RedisEvents.Extensions;
using RedisEvents.Ownership;
using RedisEvents.Positions;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Consumer;

/// <summary>
/// The <see cref="IHostedService"/> behind one <c>AddStream</c> call: it owns the partition workers
/// for one <c>(topic, consumer)</c> pair, the position flusher they record into, the ownership claim
/// that makes "which pod runs which partition" answerable, and — in <see cref="ReadMode.Block"/> —
/// the dedicated read-only reader connection.
/// </summary>
/// <remarks>
/// <para>
/// <b>Startup order is fixed and each step earns its place.</b> Ensure the topic exists (which also
/// refuses a partition <em>decrease</em>, the one topology change that silently orphans
/// unprocessed entries), resolve this instance's partition range and log the full ownership map,
/// claim the range in the ownership registry, load stored positions and any reset marker, then
/// start one worker per owned partition. Everything that can legitimately fail a service's startup —
/// an unreachable Redis, a shrunken topic, a circular <c>StartFrom</c> — fails here rather than at 3am.
/// </para>
/// <para>
/// <b>The handler is resolved once</b>, when the host is constructed: never per batch and never per
/// message. That is what lets the hot path carry exactly one indirect call.
/// </para>
/// <para>
/// <b>Shutdown is a drain, not a kill.</b> <see cref="StopAsync"/> cancels the linked token,
/// completes each partition's channel writer so the processors finish what is already queued, waits
/// for them for at most <see cref="ConsumerOptions.ShutdownTimeoutSeconds"/> (default 10), and then
/// flushes positions synchronously. On timeout it logs a Warning and still flushes whatever
/// <em>was</em> recorded; the un-drained tail is redelivered on the next start, which is the
/// at-least-once contract working as designed rather than a failure.
/// </para>
/// <para>
/// <b>The handler's token is the linked host token.</b> A handler that ignores cancellation is
/// eventually killed by the generic host's own shutdown timeout, once this host's drain budget has
/// elapsed — so a handler that must finish should observe the token it is given.
/// </para>
/// </remarks>
internal sealed class StreamConsumerHost : IHostedService, IAsyncDisposable
{
    private readonly StreamOptions options;
    private readonly ConsumerOptions consumer;
    private readonly string consumerName;
    private readonly Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> handler;
    private readonly StreamsConnectionProvider connection;
    private readonly IHostApplicationLifetime? lifetime;
    private readonly IPositionStore? positionStoreOverride;
    private readonly ILogger log;
    private readonly List<PartitionRunner> runners = [];
    private readonly List<StreamPartitionMonitor> monitors = [];
    private readonly List<Task> groupReads = [];

    /// <summary>Guards <see cref="SharedSampler"/> and <see cref="SamplerReferences"/>.</summary>
    private static readonly Lock SamplerGate = new();

    /// <summary>The one lag sampler this process runs, shared by every consumer host in it.</summary>
    private static StreamLagSampler? SharedSampler;

    /// <summary>How many started hosts hold <see cref="SharedSampler"/>.</summary>
    private static int SamplerReferences;

    /// <summary>
    /// Serialises the start, a lease rebalance and the stop against each other. Lease mode is the
    /// only thing that takes it more than once, but taking it in every mode is what keeps
    /// <see cref="StopAsync"/> from tearing the read side out from under a rebalance mid-flight.
    /// </summary>
    private readonly SemaphoreSlim rebalanceGate = new(1, 1);

    private CancellationTokenSource? cts;

    /// <summary>
    /// The source this generation of workers hangs off, linked to <see cref="cts"/>. A lease
    /// rebalance cancels and replaces it; the flusher, the ownership renewals and the reader
    /// connection are deliberately not attached to it, because they outlive one set of partitions.
    /// </summary>
    private CancellationTokenSource? workerCts;

    private PositionFlusher? flusher;
    private OwnershipRegistry? ownership;
    private StreamReaderConnection? reader;
    private Task[]? workers;
    private bool disposed;
    private bool holdsSampler;
    private bool stopping;

    /// <summary>
    /// Creates a host for one consumer. Nothing connects, claims or reads until
    /// <see cref="StartAsync"/>.
    /// </summary>
    /// <param name="options">The bound root options; supplies the connection string and topic defaults.</param>
    /// <param name="consumer">The resolved options for this consumer.</param>
    /// <param name="consumerName">The effective consumer name — the owner of the position hash.</param>
    /// <param name="handler">
    /// The batch handler, already resolved. Resolving it here rather than per message is deliberate;
    /// see the remarks on the class.
    /// </param>
    /// <param name="connection">
    /// The shared multiplexer provider. Writes, positions, admin and <see cref="ReadMode.Poll"/>
    /// reads all run on it; a blocking reader gets its own connection instead.
    /// </param>
    /// <param name="logger">Logger for the ownership map, the drain and every failure.</param>
    /// <param name="lifetime">
    /// The generic host's lifetime, used by <see cref="ErrorPolicy.Fail"/> to stop the application so
    /// the orchestrator restarts the pod. Optional only so that a test — or a service composing the
    /// host by hand — can construct one without a running host; when it is absent a
    /// <see cref="ErrorPolicy.Fail"/> fault can do no more than log at Critical, and the host says so.
    /// </param>
    /// <param name="positionStore">
    /// Overrides the position store this host uses instead of the default <see cref="RedisPositionStore"/>
    /// — <see cref="Testing.MemoryPositionStore"/> in tests, or any other <see cref="IPositionStore"/>.
    /// Only consulted when <see cref="ConsumerOptions.Persist"/> is not <see cref="PersistMode.None"/>
    /// and <see cref="ConsumerOptions.UseConsumerGroup"/> is <see langword="false"/> — the cases where
    /// a position store is used at all.
    /// </param>
    /// <exception cref="StreamConfigurationException">The consumer names no topic.</exception>
    public StreamConsumerHost(
        StreamOptions options,
        ConsumerOptions consumer,
        string consumerName,
        Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> handler,
        StreamsConnectionProvider connection,
        ILogger? logger = null,
        IHostApplicationLifetime? lifetime = null,
        IPositionStore? positionStore = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(consumer);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerName);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(connection);

        if (string.IsNullOrWhiteSpace(consumer.Topic))
        {
            throw new StreamConfigurationException(
                $"Streams: consumer '{consumerName}' has no Topic. Set it in {StreamConfigBinder.SectionName}:Consumers " +
                "or pass it to AddStream.");
        }

        this.options = options;
        this.consumer = consumer;
        this.consumerName = consumerName;
        this.handler = handler;
        this.connection = connection;
        this.log = logger ?? NullLogger.Instance;
        this.lifetime = lifetime;
        this.positionStoreOverride = positionStore;
    }

    /// <summary>The topic this host consumes.</summary>
    public string Topic => this.consumer.Topic;

    /// <summary>The effective consumer name.</summary>
    public string Consumer => this.consumerName;

    /// <summary>
    /// The partitions this instance owns, ascending. Empty before <see cref="StartAsync"/>, and also
    /// when there are more instances than partitions — a surplus instance owns nothing.
    /// </summary>
    public IReadOnlyList<int> OwnedPartitions { get; private set; } = [];

    /// <summary>Whether the host has been started and not yet stopped.</summary>
    public bool IsRunning => this.cts is not null;

    /// <summary>
    /// The position store this host was built with, if any — asserted directly by
    /// <c>Create</c>'s DI-resolution test rather than through <see cref="StartAsync"/>, which would
    /// need a real Redis connection to exercise.
    /// </summary>
    internal IPositionStore? PositionStoreOverride => this.positionStoreOverride;

    /// <summary>
    /// Builds a host from a registration, resolving the handler <b>once</b> out of the container.
    /// This is the shape <c>AddStream</c> registers as an <see cref="IHostedService"/>.
    /// </summary>
    /// <param name="options">The bound root options.</param>
    /// <param name="registration">The consumer declared by an <c>AddStream</c> call.</param>
    /// <param name="services">The built container; used here, once, for the handler and the shared connection.</param>
    /// <returns>A host ready for the generic host to start.</returns>
    /// <exception cref="StreamConfigurationException">
    /// The registration carries no handler, or the registered handler type implements neither
    /// <see cref="IBatchHandler"/> nor <see cref="IMessageHandler"/>.
    /// </exception>
    public static StreamConsumerHost Create(
        StreamOptions options,
        StreamConsumerRegistration registration,
        IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(registration);
        ArgumentNullException.ThrowIfNull(services);

        var logger = services.GetService<ILoggerFactory>()?.CreateLogger("RedisEvents.Consumer");

        return new StreamConsumerHost(
            options,
            registration.Options,
            registration.Consumer,
            ResolveHandler(registration, services),
            services.GetRequiredService<StreamsConnectionProvider>(),
            logger,

            // GetService, not GetRequiredService: the lifetime is always there under the generic
            // host, and its absence must not stop a hand-composed host from being built.
            services.GetService<IHostApplicationLifetime>(),

            // GetService, not GetRequiredService: IPositionStore is deliberately not registered by
            // default (see StreamsBuilderExtensions.Core), so a service that never registered one
            // gets the default RedisPositionStore, unchanged from before this override point existed.
            services.GetService<IPositionStore>());
    }

    /// <summary>
    /// Turns a registration into the single batch delegate the pipeline calls, adapting an
    /// <see cref="IMessageHandler"/> through <see cref="HandlerAdapter"/> so both handler shapes
    /// share one internal path.
    /// </summary>
    /// <param name="registration">The consumer registration.</param>
    /// <param name="services">The container the handler class was registered in.</param>
    /// <returns>The batch handler delegate.</returns>
    /// <exception cref="StreamConfigurationException">The registration names no usable handler.</exception>
    public static Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> ResolveHandler(
        StreamConsumerRegistration registration,
        IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(registration);
        ArgumentNullException.ThrowIfNull(services);

        if (registration.Handler is { } configured)
        {
            return configured;
        }

        if (registration.HandlerType is not { } handlerType)
        {
            throw new StreamConfigurationException(
                $"Streams: consumer '{registration.Consumer}' on topic '{registration.Options.Topic}' was registered " +
                "with neither a handler class nor a handler delegate.");
        }

        // Resolved once, here — not per batch, and certainly not per message.
        var instance = services.GetRequiredService(handlerType);

        // Set only by the typed AddStream<THandler, TMessage> overloads: turns the resolved instance
        // into a plain IBatchHandler/IMessageHandler wrapper before the switch below ever runs, so
        // the switch itself stays exactly as it was before typed handlers existed.
        if (registration.WrapAsTypedHandler is { } wrap)
        {
            instance = wrap(instance);
        }

        return instance switch
        {
            IBatchHandler batch => batch.HandleAsync,
            IMessageHandler message => HandlerAdapter.AdaptMessage(message),
            _ => throw new StreamConfigurationException(
                $"Streams: handler '{handlerType.FullName}' registered for topic '{registration.Options.Topic}' implements " +
                $"neither {nameof(IBatchHandler)} nor {nameof(IMessageHandler)}, so there is nothing to call. " +
                $"Implement {nameof(IBatchHandler)} for the batch path, or {nameof(IMessageHandler)} for one message at a time."),
        };
    }

    /// <inheritdoc />
    /// <remarks>
    /// Failures here are startup failures on purpose: a consumer that cannot claim its partitions,
    /// cannot read its stored positions, or is pointed at a topic whose partition count has been
    /// reduced must not come up half-working.
    /// </remarks>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        // Held for the whole start, from before the host's source exists: a stop arriving mid-start
        // then waits for a fully built host and tears it down, rather than disposing the source this
        // start is about to link its workers to. A lease change landing mid-start waits too, and the
        // rebalance it runs afterwards compares against what this start left running.
        await this.rebalanceGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            await this.StartCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.rebalanceGate.Release();
        }
    }

    /// <summary>The body of <see cref="StartAsync"/>; the caller holds <see cref="rebalanceGate"/>.</summary>
    private async Task StartCoreAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        if (this.cts is not null)
        {
            return;
        }

        var topic = this.consumer.Topic;
        var topicOptions = StreamConfigBinder.ResolveTopic(this.options, topic, this.log);
        var redis = this.connection.Connection;
        var db = redis.GetDatabase();

        // Creates the partition streams when they are new, and refuses a partition decrease.
        await StreamAdmin.EnsureTopicAsync(redis, topic, topicOptions, this.log, cancellationToken).ConfigureAwait(false);

        var instances = this.consumer.Instances;
        var leased = (instances?.Mode ?? InstanceMode.Static) == InstanceMode.Lease;
        var identity = InstanceResolver.Resolve(instances, this.log);
        var podName = identity.PodName ?? Environment.MachineName;

        // P4-20. In Lease mode the registry is authoritative and nothing is assigned here: this
        // instance owns whatever it claims below, which is why the mode needs no Count and no
        // StatefulSet ordinal. In Static mode the arithmetic decides and the registry only reports.
        var owned = leased
            ? []
            : InstanceResolver.Owned(topicOptions.Partitions, identity, this.log).ToArray();

        this.OwnedPartitions = owned;

        // The line an operator greps for: the whole map, not just this instance's share of it.
        this.log.LogInformation(
            "Streams: consumer {Consumer} on topic {Topic} — {Ownership}; readMode={ReadMode} persist={Persist} " +
            "batchSize={BatchSize} backpressure={Backpressure} group={UseConsumerGroup} task={Task}.",
            this.consumerName,
            topic,
            leased
                ? string.Create(
                    CultureInfo.InvariantCulture,
                    $"Instances:Mode=Lease (pod {podName}) — partitions are claimed from the ownership registry, not assigned")
                : InstanceResolver.DescribeOwnership(topicOptions.Partitions, identity),
            this.consumer.ReadMode,
            this.consumer.Persist,
            this.consumer.BatchSize,
            this.consumer.Backpressure.Enabled
                ? string.Create(CultureInfo.InvariantCulture, $"on(capacity={this.consumer.Backpressure.Capacity})")
                : "off(inline)",
            this.consumer.UseConsumerGroup,
            StreamNames.ConsumerHostTaskName(this.consumerName));

        // The linked source every worker hangs off; StopAsync cancels it.
        this.cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        this.stopping = false;

        if (!leased && owned.Length == 0)
        {
            this.log.LogWarning(
                "Streams: consumer {Consumer} on topic {Topic} owns no partitions (instance {Index} of {Count}), so it starts no workers. " +
                "That is a misconfiguration unless the instance pool was deliberately over-provisioned.",
                this.consumerName,
                topic,
                identity.Index,
                identity.Count);
            return;
        }

        this.ownership = new OwnershipRegistry(
            redis,
            new OwnershipRegistryOptions
            {
                Topic = topic,
                Consumer = this.consumerName,
                Partitions = topicOptions.Partitions,
                OwnedPartitions = owned,
                PodName = podName,
                Mode = leased ? InstanceMode.Lease : InstanceMode.Static,
                TtlSeconds = instances?.LeaseTtlSeconds ?? InstanceOptions.DefaultLeaseTtlSeconds,
                RenewSeconds = instances?.LeaseRenewSeconds ?? InstanceOptions.DefaultLeaseRenewSeconds,
                OnLeaseChanged = leased ? this.OnLeaseChanged : null,
            },
            this.log);

        await this.ownership.StartAsync(cancellationToken).ConfigureAwait(false);

        if (leased)
        {
            owned = [.. this.ownership.Held];
            this.OwnedPartitions = owned;

            if (owned.Length == 0)
            {
                // Every partition is leased by somebody else — more instances than partitions, or
                // a rollout that has not rebalanced yet. Nothing to start; the renewal loop calls
                // back the moment a lease lapses.
                this.log.LogInformation(
                    "Streams: consumer {Consumer} on topic {Topic} holds no lease yet — its {Partitions} partitions are all " +
                    "claimed by other instances. It starts no workers and picks partitions up as leases lapse.",
                    this.consumerName,
                    topic,
                    topicOptions.Partitions);
                return;
            }
        }

        await this.StartReadSideAsync(owned, topicOptions, db, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Starts everything below the ownership claim for one set of partitions: stored positions, the
    /// reset markers, the position flusher, the dedicated reader connection in
    /// <see cref="ReadMode.Block"/>, and one worker per partition.
    /// </summary>
    /// <param name="owned">The partitions to run, ascending. Never empty.</param>
    /// <param name="topicOptions">The resolved topic options.</param>
    /// <param name="db">The shared database handle.</param>
    /// <param name="cancellationToken">Cancellation for the start itself.</param>
    /// <returns>A task that completes once every worker is running.</returns>
    /// <remarks>
    /// Separated from <see cref="StartAsync"/> because <see cref="InstanceMode.Lease"/> runs it more
    /// than once: a lease that moves takes the read side down and brings it back up on the new set
    /// of partitions, and everything here — cursors, channels, monitors, the flusher — belongs to
    /// that set rather than to the host. In <see cref="InstanceMode.Static"/> mode it runs exactly
    /// once, which is why nothing about the static path changes.
    /// </remarks>
    private async Task StartReadSideAsync(
        int[] owned,
        TopicOptions topicOptions,
        IDatabase db,
        CancellationToken cancellationToken)
    {
        var topic = this.consumer.Topic;
        var redis = this.connection.Connection;

        // Every worker of this generation hangs off this source, so a lease change can take the read
        // side down without touching the host's own token — the flusher, the ownership renewals and
        // the reader connection all outlive it.
        this.workerCts = CancellationTokenSource.CreateLinkedTokenSource(this.cts!.Token);

        var useGroup = this.consumer.UseConsumerGroup;

        // Group mode: Redis owns the read cursor through the PEL, so the position hash is bypassed
        // entirely and an XACK per successful batch takes its place.
        var usePositions = !useGroup && this.consumer.Persist != PersistMode.None;

        IReadOnlyDictionary<int, StreamId> stored = ReadOnlyDictionary<int, StreamId>.Empty;
        IReadOnlyDictionary<int, ResetMarker>? markers = null;
        ResetSignal? resets = null;

        if (usePositions)
        {
            var store = this.positionStoreOverride ?? new RedisPositionStore(redis);

            stored = await store.LoadAsync(topic, this.consumerName, cancellationToken).ConfigureAwait(false);

            // Taken before any read loop starts, so a cold reset has no flusher to race and no
            // in-flight batch to reconcile.
            markers = await ResetMarkers
                .TakeAllAsync(db, topic, this.consumerName, this.log, cancellationToken)
                .ConfigureAwait(false);

            resets = new ResetSignal(owned);

            this.flusher = new PositionFlusher(
                store,
                topic,
                this.consumerName,
                topicOptions.Partitions,
                TimeSpan.FromMilliseconds(Math.Max(1, this.consumer.PersistIntervalMs)),
                this.log,
                redis,
                instanceId: default,
                onContested: this.OnPartitionContested,
                resets: resets);

            this.flusher.Start();
        }

        // D2: co-located partitions share a hash tag, so they share a slot and can share ONE
        // multi-stream XREAD. Taking it is not an optimisation — N blocking reads on the one
        // dedicated reader connection park it in turn, so a partition with a backlog gets a read
        // turn only every N x BlockMs. That is the head-of-line blocking the dedicated connection
        // exists to prevent, reintroduced inside it.
        //
        // Group mode is excluded because Redis owns the cursor there (XREADGROUP per partition, with
        // its own PEL); inline mode because it has no channel to split a reply into; and a single
        // owned partition because one read is one read either way.
        var coLocated = !useGroup
            && topicOptions.CoLocatePartitions
            && owned.Length > 1;

        if (this.consumer.ReadMode == ReadMode.Block)
        {
            if (useGroup)
            {
                this.log.LogInformation(
                    "Streams: consumer {Consumer} on topic {Topic} has ReadMode=Block and UseConsumerGroup=true; group reads " +
                    "run on the shared multiplexer with the idle backoff, so no dedicated reader connection is opened.",
                    this.consumerName,
                    topic);
            }
            else if (this.reader is null)
            {
                // One dedicated, read-only connection per consumer — never registered in DI, never
                // handed a write, disposed with this host.
                // Without co-location every partition runs its own read loop on this one
                // connection and they queue behind each other, so the client-side timeout has to
                // cover the whole queue rather than one block.
                //
                // Opened once and kept across a lease rebalance: it carries no per-partition state,
                // and reconnecting on every lease change would trade a socket for nothing. Sized for
                // the worst case in Lease mode, where the partitions this instance holds — and so
                // the number of read loops queued on this connection — moves with the pool.
                var concurrentReads = (this.consumer.Instances?.Mode ?? InstanceMode.Static) == InstanceMode.Lease
                    ? topicOptions.Partitions
                    : coLocated ? 1 : owned.Length;

                this.reader = await StreamReaderConnection
                    .CreateAsync(
                        this.options,
                        this.consumer,
                        this.consumerName,
                        index: 0,
                        this.log,
                        concurrentReads)
                    .ConfigureAwait(false);
            }

            if (!this.consumer.Backpressure.Enabled)
            {
                this.log.LogInformation(
                    "Streams: consumer {Consumer} on topic {Topic} combines ReadMode=Block with Backpressure.Enabled=false, so a " +
                    "ThreadPool hop is forced before every handler call. Without it the handler would run on the dedicated reader " +
                    "thread and stall this consumer's reads. Enable backpressure to remove the hop.",
                    this.consumerName,
                    topic);
            }
        }

        if (coLocated)
        {
            if (this.consumer.Backpressure.Enabled)
            {
                await this.StartCoLocatedGroupAsync(owned, db, topicOptions, stored, markers, resets, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                await this
                    .StartCoLocatedInlineGroupAsync(owned, db, topicOptions, stored, markers, resets, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        else
        {
            foreach (var partition in owned)
            {
                await this.StartPartitionAsync(partition, db, topicOptions, stored, markers, resets, useGroup, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        var started = new Task[this.runners.Count + this.groupReads.Count];

        for (var i = 0; i < this.runners.Count; i++)
        {
            started[i] = this.runners[i].Task;
        }

        for (var i = 0; i < this.groupReads.Count; i++)
        {
            started[this.runners.Count + i] = this.groupReads[i];
        }

        this.workers = started;

        // R-05. Nothing else starts the sampler, so without this streams.lag.ms, streams.lag.entries
        // and streams.stream.length are never emitted in a real service and the health check's
        // LagEntries stays at -1 forever. It is process-wide (it walks StreamLag.All), so the hosts
        // share one and the last one out stops it.
        // Once per host, not once per generation: a lease rebalance restarts the read side, and
        // taking a second reference to the process-wide sampler would leave the last one out unable
        // to stop it.
        if (!this.holdsSampler)
        {
            this.holdsSampler = AcquireLagSampler(redis, this.log);
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Cancel, complete the writers, await the drain, flush. The drain is bounded by
    /// <see cref="ConsumerOptions.ShutdownTimeoutSeconds"/> and by the caller's token, whichever
    /// comes first; the final flush then runs regardless, because a lost position costs a
    /// redelivery on the next start.
    /// </remarks>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Read by the fault handler: a worker that faults because the host is winding down must not
        // be reported as an ErrorPolicy.Fail and must not stop an application that is already stopping.
        // Set before the gate, so a lease rebalance queued behind it gives up rather than rebuilding
        // a read side this stop is about to tear down.
        this.stopping = true;

        var budget = TimeSpan.FromSeconds(this.consumer.ShutdownTimeoutSeconds > 0
            ? this.consumer.ShutdownTimeoutSeconds
            : 10);

        // Never the caller's token: a stop that skipped the gate could dispose the sources a
        // rebalance is mid-way through using.
        await this.rebalanceGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            // Read under the gate, never before it. Stops overlap in practice — minimal hosting's
            // app.Run() and the host's own StopAsync both stop every hosted service — and a second
            // stop that captured the source before queueing here would cancel it after the first
            // stop had disposed it.
            if (this.cts is not { } source)
            {
                return;
            }

            // Again, because a start that held the gate when the flag was first set resets it.
            this.stopping = true;

            this.log.LogInformation(
                "Streams: stopping consumer {Consumer} on topic {Topic}; draining {Partitions} partitions with a {TimeoutSeconds}s budget.",
                this.consumerName,
                this.consumer.Topic,
                this.runners.Count,
                budget.TotalSeconds);

            await source.CancelAsync().ConfigureAwait(false);

            await this.StopReadSideAsync(budget, cancellationToken).ConfigureAwait(false);

            if (this.ownership is { } claims)
            {
                await claims.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }

            await this.ReleaseAsync().ConfigureAwait(false);

            this.log.LogInformation(
                "Streams: stopped consumer {Consumer} on topic {Topic}.",
                this.consumerName,
                this.consumer.Topic);
        }
        finally
        {
            this.rebalanceGate.Release();
        }
    }

    /// <summary>
    /// Takes one generation of workers down: cancel, complete the writers, drain within the budget,
    /// then flush and drop the position flusher and every per-partition monitor.
    /// </summary>
    /// <param name="budget">The drain budget, from <see cref="ConsumerOptions.ShutdownTimeoutSeconds"/>.</param>
    /// <param name="cancellationToken">The caller's token; it shortens the drain, never lengthens it.</param>
    /// <returns>A task that completes once the read side is down.</returns>
    /// <remarks>
    /// Everything here belongs to one set of partitions, which is why a lease rebalance runs it and
    /// then starts a fresh read side rather than trying to add and remove partitions in place. The
    /// flusher goes with it deliberately: its final flush is what stops the instance that picks a
    /// partition up from replaying the last flush interval.
    /// </remarks>
    private async Task StopReadSideAsync(TimeSpan budget, CancellationToken cancellationToken)
    {
        if (this.workerCts is not { } generation)
        {
            return;
        }

        await generation.CancelAsync().ConfigureAwait(false);

        // Completing the writers is what lets each processor finish the batches already queued and
        // then exit on its own. The read loops complete them too; doing it here as well covers a
        // reader still parked in an in-flight BLOCK that outlives its own cancellation check.
        foreach (var runner in this.runners)
        {
            runner.CompleteWriter();
        }

        var drained = await this.DrainAsync(budget, cancellationToken).ConfigureAwait(false);

        if (!drained)
        {
            this.log.LogWarning(
                "Streams: consumer {Consumer} on topic {Topic} did not drain within {TimeoutSeconds}s. Positions recorded so far " +
                "are still flushed; whatever was in flight is redelivered on the next start, which is the at-least-once contract " +
                "rather than data loss.",
                this.consumerName,
                this.consumer.Topic,
                budget.TotalSeconds);
        }

        // The final flush is synchronous and deliberately does not observe the caller's token: it is
        // the difference between resuming where we stopped and replaying the last flush interval.
        if (this.flusher is { } pending)
        {
            using var flushTimeout = new CancellationTokenSource(budget);

            try
            {
                await pending.StopAsync(flushTimeout.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                this.log.LogWarning(
                    ex,
                    "Streams: the final position flush for consumer {Consumer} on topic {Topic} failed; the last interval's " +
                    "messages will be redelivered on the next start.",
                    this.consumerName,
                    this.consumer.Topic);
            }

            await pending.DisposeAsync().ConfigureAwait(false);
            this.flusher = null;
        }

        foreach (var runner in this.runners)
        {
            runner.Dispose();
        }

        this.runners.Clear();
        this.groupReads.Clear();
        this.workers = null;

        // A pod that no longer owns a partition must stop reporting its lag rather than pinning the
        // gauge — and must stop counting towards this process's health.
        foreach (var monitor in this.monitors)
        {
            StreamLag.Forget(monitor);
        }

        this.monitors.Clear();

        generation.Dispose();
        this.workerCts = null;
    }

    /// <summary>
    /// The <see cref="InstanceMode.Lease"/> callback: the set of partitions this instance holds has
    /// moved, so the read side has to move with it.
    /// </summary>
    /// <param name="leased">The partitions now held, ascending.</param>
    /// <remarks>
    /// Deliberately fire-and-forget. This runs on the ownership renewal loop, and a rebalance costs a
    /// whole drain budget; blocking the loop for that long would cost this instance the very leases
    /// the rebalance is about. <see cref="RebalanceAsync"/> serialises on the gate and re-reads the
    /// held set when it gets there, so two notifications in quick succession collapse into one
    /// rebalance rather than fighting.
    /// </remarks>
    private void OnLeaseChanged(int[] leased)
    {
        if (this.stopping || this.disposed || this.cts is null)
        {
            return;
        }

        this.log.LogDebug(
            "Streams: consumer {Consumer} on topic {Topic} now leases partitions {Held}; queueing a read-side rebalance.",
            this.consumerName,
            this.consumer.Topic,
            leased.Length == 0 ? "[]" : string.Join(',', leased));

        _ = Task.Run(this.RebalanceAsync, CancellationToken.None);
    }

    /// <summary>
    /// Restarts the read side on the partitions this instance currently leases.
    /// </summary>
    /// <returns>A task that completes when the read side matches the lease again.</returns>
    /// <remarks>
    /// <para>
    /// Stop-then-start rather than add-and-remove: a co-located group is one multi-stream
    /// <c>XREAD</c> over a fixed set of keys, and rebuilding it is both simpler and less
    /// error-prone than mutating it under a running read loop. Rebalances happen when a pod comes or
    /// goes, not per message.
    /// </para>
    /// <para>
    /// A partition given up is drained first, so its position is flushed before the instance that
    /// claims it starts reading — but the two do overlap for the length of the drain, which is the
    /// at-least-once contract lease mode inherits rather than a defect.
    /// </para>
    /// <para>
    /// If the read side cannot be brought back up — Redis unreachable, a group that cannot be
    /// created — this instance would hold leases it is not reading, and no further notification is
    /// coming to fix it, because the held set has not changed. So it retries, and if that fails it
    /// logs at Critical and stops the application: a pod restart hands the leases to someone else
    /// within the TTL, which is strictly better than a pod that quietly consumes nothing.
    /// </para>
    /// </remarks>
    private async Task RebalanceAsync()
    {
        await this.rebalanceGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            if (this.stopping || this.disposed || this.cts is not { } source || this.ownership is not { } registry)
            {
                return;
            }

            int[] owned = [.. registry.Held];

            if (SameSet(this.OwnedPartitions, owned))
            {
                // Two notifications collapsed into one rebalance, or the start already ran on this
                // set. Nothing to do, and nothing to log about.
                return;
            }

            var budget = TimeSpan.FromSeconds(this.consumer.ShutdownTimeoutSeconds > 0
                ? this.consumer.ShutdownTimeoutSeconds
                : 10);

            this.log.LogInformation(
                "Streams: consumer {Consumer} on topic {Topic} rebalancing from partitions {Before} to {After} — its lease moved.",
                this.consumerName,
                this.consumer.Topic,
                this.OwnedPartitions.Count == 0 ? "[]" : string.Join(',', this.OwnedPartitions),
                owned.Length == 0 ? "[]" : string.Join(',', owned));

            await this.StopReadSideAsync(budget, CancellationToken.None).ConfigureAwait(false);

            this.OwnedPartitions = owned;

            if (owned.Length == 0)
            {
                this.log.LogInformation(
                    "Streams: consumer {Consumer} on topic {Topic} holds no lease any more and reads nothing; it claims partitions " +
                    "again as they lapse.",
                    this.consumerName,
                    this.consumer.Topic);
                return;
            }

            var topicOptions = StreamConfigBinder.ResolveTopic(this.options, this.consumer.Topic, this.log);
            var redis = this.connection.Connection;

            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    await this.StartReadSideAsync(owned, topicOptions, redis.GetDatabase(), source.Token)
                        .ConfigureAwait(false);
                    return;
                }
                catch (Exception ex) when (ex is not OperationCanceledException && attempt < 3)
                {
                    this.log.LogWarning(
                        ex,
                        "Streams: consumer {Consumer} on topic {Topic} could not restart its read side on partitions {Owned} " +
                        "(attempt {Attempt}); retrying.",
                        this.consumerName,
                        this.consumer.Topic,
                        string.Join(',', owned),
                        attempt);

                    await Task.Delay(TimeSpan.FromSeconds(2), source.Token).ConfigureAwait(false);
                    await this.StopReadSideAsync(budget, CancellationToken.None).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // The host is stopping; there is nothing left to rebalance onto.
        }
        catch (Exception ex)
        {
            this.log.LogCritical(
                ex,
                "Streams: consumer {Consumer} on topic {Topic} could not restart its read side after a lease change, so it holds " +
                "leases it is not reading and nothing will notify it again. Stopping the application: a restart hands the leases " +
                "on within {TtlSeconds}s.",
                this.consumerName,
                this.consumer.Topic,
                this.consumer.Instances?.LeaseTtlSeconds ?? InstanceOptions.DefaultLeaseTtlSeconds);

            this.lifetime?.StopApplication();
        }
        finally
        {
            this.rebalanceGate.Release();
        }
    }

    /// <summary>Whether a held set and a leased set name the same partitions, both ascending.</summary>
    /// <param name="running">What the read side is running on.</param>
    /// <param name="leased">What the registry says this instance holds.</param>
    /// <returns><see langword="true"/> when they match element for element.</returns>
    private static bool SameSet(IReadOnlyList<int> running, int[] leased)
    {
        if (running.Count != leased.Length)
        {
            return false;
        }

        for (var i = 0; i < leased.Length; i++)
        {
            if (running[i] != leased[i])
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (this.disposed)
        {
            return;
        }

        this.stopping = true;

        await this.StopAsync(CancellationToken.None).ConfigureAwait(false);

        await this.rebalanceGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            await this.ReleaseAsync().ConfigureAwait(false);
            this.disposed = true;
        }
        finally
        {
            this.rebalanceGate.Release();
        }
    }

    /// <summary>
    /// Starts every owned partition as one co-located group: a <b>single</b> multi-stream
    /// <c>XREAD</c> feeding one channel and one processor per partition.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is D2 made real. The alternative — one fetch and one read loop per partition — issues N
    /// blocking <c>XREAD</c>s on the single dedicated reader connection, where each one parks the
    /// connection server-side and the others queue behind it: a partition with a backlog then gets a
    /// read turn roughly every N x <c>BlockMs</c>, which at eight partitions and the default 1 s
    /// block turned a 400-message drain into half a minute. One command covering every key returns
    /// the moment <em>any</em> of them has an entry.
    /// </para>
    /// <para>
    /// <b>Everything per-partition stays per-partition</b>: its own <see cref="PartitionContext"/>,
    /// channel, processor loop, lag monitor and recorded position, and ordering within a partition
    /// is untouched — one reply slot is decoded and written to one channel in reply order.
    /// </para>
    /// <para>
    /// <b>The group shares one cancellation source</b>, deliberately. The partitions share a read, so
    /// standing one of them down (a contested position, or a processor faulted under
    /// <c>ErrorPolicy.Fail</c>) has to stand the read down too: a reader left filling a channel
    /// nobody drains would wedge on the first full channel and take its siblings with it. The
    /// per-partition path keeps its per-partition sources, which is one more reason
    /// <c>CoLocatePartitions = false</c> remains a real choice.
    /// </para>
    /// </remarks>
    /// <param name="owned">The partitions this instance owns, ascending.</param>
    /// <param name="db">The shared multiplexer's database.</param>
    /// <param name="topicOptions">The resolved topic options.</param>
    /// <param name="stored">Positions loaded from the position hash.</param>
    /// <param name="markers">Reset markers taken at startup, or <see langword="null"/>.</param>
    /// <param name="resets">The live reset hand-off, or <see langword="null"/>.</param>
    /// <param name="startToken">The startup token; it bounds the <c>StartFrom.Now</c> tail lookups.</param>
    private async Task StartCoLocatedGroupAsync(
        int[] owned,
        IDatabase db,
        TopicOptions topicOptions,
        IReadOnlyDictionary<int, StreamId> stored,
        IReadOnlyDictionary<int, ResetMarker>? markers,
        ResetSignal? resets,
        CancellationToken startToken)
    {
        var topic = this.consumer.Topic;
        var batchSize = Math.Max(1, this.consumer.BatchSize);
        var capacity = Math.Max(1, this.consumer.Backpressure.Capacity);
        var count = owned.Length;

        var contexts = new PartitionContext[count];
        var from = new StreamId[count];
        var keys = new RedisKey[count];
        var readers = new ChannelReader<StreamBatch>[count];

        // One source for the whole group — see the remarks. Linked to the host's, so StopAsync still
        // winds the group down the ordinary way.
        var groupCts = CancellationTokenSource.CreateLinkedTokenSource(this.workerCts!.Token);

        for (var i = 0; i < count; i++)
        {
            var partition = owned[i];
            var key = StreamKeys.Stream(topic, partition, topicOptions.CoLocatePartitions);

            var start = ResetMarkers.ApplyToStart(
                StartPosition.Resolve(this.consumer, stored.TryGetValue(partition, out var id) ? id : null),
                markers,
                partition);

            // StartFrom.Now would otherwise arrive as 0-0; pin it to a concrete id here, once, from
            // the SERVER's tail rather than this client's clock. See ResolveNowAsync.
            from[i] = start.FromNow
                ? await ResolveNowAsync(db, key, this.log, startToken).ConfigureAwait(false)
                : start.After;

            keys[i] = key;

            var channel = PartitionWorker.CreateChannel(capacity);
            readers[i] = channel.Reader;

            // One monitor per partition, registered before the loops start, so the health check and
            // the streams.* gauges see it from the moment it exists.
            var monitor = StreamLag.Track(
                topic,
                this.consumerName,
                partition,
                key,
                this.consumer.UnhealthyLagMs,
                this.consumer.UnhealthyBlockSeconds);

            this.monitors.Add(monitor);

            contexts[i] = new PartitionContext(
                db,
                key,
                partition,
                topic,
                this.consumerName,
                batchSize,
                this.consumer.Filter,
                this.consumer.OnError,
                channel.Writer,
                this.log)
            {
                Monitor = monitor,
            };

            this.runners.Add(new PartitionRunner(partition, groupCts, channel.Writer));
        }

        PositionRecorder recorder;
        PositionFlush? flush;

        if (this.flusher is { } positions)
        {
            recorder = positions.Recorder;
            flush = positions.FlushAsync;
        }
        else
        {
            recorder = NoPositions;
            flush = null;
        }

        MultiStreamFetch fetch;

        if (this.consumer.ReadMode == ReadMode.Block)
        {
            fetch = new BlockFetch(
                this.reader ?? throw new StreamConfigurationException(
                    $"Streams: consumer '{this.consumerName}' is in ReadMode.Block but no reader connection was opened."),
                keys,
                batchSize).FetchManyAsync;
        }
        else
        {
            fetch = new PollFetch(db, batchSize, this.consumer.MaxIdleDelayMs).FetchManyAsync;
        }

        this.log.LogInformation(
            "Streams: consumer {Consumer} on topic {Topic} reads its {PartitionCount} owned partitions with one co-located " +
            "{ReadMode} XREAD per round; each partition keeps its own channel, processor and position.",
            this.consumerName,
            topic,
            count,
            this.consumer.ReadMode);

        var reading = PartitionWorker.ReadGroupLoopAsync(contexts, from, fetch, groupCts.Token, resets, this.RewindNotifier());

        _ = reading.ContinueWith(
            faulted => this.OnLoopFaulted(faulted, partition: null, "the co-located read loop"),
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);

        this.groupReads.Add(reading);

        for (var i = 0; i < count; i++)
        {
            var ctx = contexts[i];
            var channelReader = readers[i];
            var runner = this.runners[this.runners.Count - count + i];

            // Task.Run: handler code starts on the standard ThreadPool, never on a reader thread.
            var processing = Task.Run(
                () => PartitionWorker.ProcessLoopAsync(
                    ctx, this.handler, recorder, groupCts.Token, channelReader, this.consumer.Persist, flush),
                CancellationToken.None);

            // A faulted processor stands the whole group down: the read loop is parked in a fetch
            // shared by every partition and would go on filling a channel nobody drains.
            _ = processing.ContinueWith(
                _ => runner.Stop(),
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted,
                TaskScheduler.Default);

            runner.Task = processing;

            // Any end of the processor loop — ErrorPolicy.StopPartition, a contested stand-down, or
            // a fault — completes this partition's channel. That is how the shared read loop learns
            // to retire the slot: without it the reader would keep filling a channel nobody drains
            // and wedge the whole group at the first full one.
            _ = processing.ContinueWith(
                _ => runner.CompleteWriter(),
                CancellationToken.None,
                TaskContinuationOptions.None,
                TaskScheduler.Default);

            _ = processing.ContinueWith(
                faulted => this.OnLoopFaulted(faulted, ctx.Partition, "the worker"),
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted,
                TaskScheduler.Default);
        }
    }

    /// <summary>
    /// Starts every owned partition as one co-located group with <c>Backpressure.Enabled = false</c>:
    /// the same single multi-stream <c>XREAD</c> as <see cref="StartCoLocatedGroup"/>, but with no
    /// channel and no per-partition processor — <see cref="PartitionWorker.ReadGroupInlineLoopAsync"/>
    /// decodes each reply slot and calls the handler in place, one loop for the whole group.
    /// </summary>
    /// <remarks>
    /// Without this, co-located + inline fell back to <see cref="StartPartitionAsync"/>'s per-partition
    /// path: N single-stream blocking <c>XREAD</c>s sharing the one dedicated reader connection,
    /// each parking it in turn — exactly the head-of-line blocking co-location exists to avoid,
    /// reintroduced by the one combination that skipped it. See the remarks on
    /// <see cref="StartCoLocatedGroup"/> for the underlying problem.
    /// </remarks>
    /// <param name="owned">The partitions this instance owns, ascending.</param>
    /// <param name="db">The shared multiplexer's database.</param>
    /// <param name="topicOptions">The resolved topic options.</param>
    /// <param name="stored">Positions loaded from the position hash.</param>
    /// <param name="markers">Reset markers taken at startup, or <see langword="null"/>.</param>
    /// <param name="resets">The live reset hand-off, or <see langword="null"/>.</param>
    /// <param name="startToken">The startup token; it bounds the <c>StartFrom.Now</c> tail lookups.</param>
    private async Task StartCoLocatedInlineGroupAsync(
        int[] owned,
        IDatabase db,
        TopicOptions topicOptions,
        IReadOnlyDictionary<int, StreamId> stored,
        IReadOnlyDictionary<int, ResetMarker>? markers,
        ResetSignal? resets,
        CancellationToken startToken)
    {
        var topic = this.consumer.Topic;
        var batchSize = Math.Max(1, this.consumer.BatchSize);
        var count = owned.Length;

        var contexts = new PartitionContext[count];
        var from = new StreamId[count];

        // One source for the whole group, exactly as StartCoLocatedGroup: the partitions share a
        // read, so standing one of them down has to stand the read down too.
        var groupCts = CancellationTokenSource.CreateLinkedTokenSource(this.workerCts!.Token);

        for (var i = 0; i < count; i++)
        {
            var partition = owned[i];
            var key = StreamKeys.Stream(topic, partition, topicOptions.CoLocatePartitions);

            var start = ResetMarkers.ApplyToStart(
                StartPosition.Resolve(this.consumer, stored.TryGetValue(partition, out var id) ? id : null),
                markers,
                partition);

            // The server's tail, not this client's clock — see ResolveNowAsync.
            from[i] = start.FromNow
                ? await ResolveNowAsync(db, key, this.log, startToken).ConfigureAwait(false)
                : start.After;

            var monitor = StreamLag.Track(
                topic,
                this.consumerName,
                partition,
                key,
                this.consumer.UnhealthyLagMs,
                this.consumer.UnhealthyBlockSeconds);

            this.monitors.Add(monitor);

            // Writer: null — inline mode has no channel; ReadGroupInlineLoopAsync calls the handler
            // itself for each reply slot.
            contexts[i] = new PartitionContext(
                db,
                key,
                partition,
                topic,
                this.consumerName,
                batchSize,
                this.consumer.Filter,
                this.consumer.OnError,
                Writer: null,
                this.log)
            {
                Monitor = monitor,
            };

            this.runners.Add(new PartitionRunner(partition, groupCts, writer: null));
        }

        PositionRecorder recorder;
        PositionFlush? flush;

        if (this.flusher is { } positions)
        {
            recorder = positions.Recorder;
            flush = positions.FlushAsync;
        }
        else
        {
            recorder = NoPositions;
            flush = null;
        }

        MultiStreamFetch fetch;

        if (this.consumer.ReadMode == ReadMode.Block)
        {
            var keys = new RedisKey[count];
            for (var i = 0; i < count; i++)
            {
                keys[i] = contexts[i].StreamKey;
            }

            fetch = new BlockFetch(
                this.reader ?? throw new StreamConfigurationException(
                    $"Streams: consumer '{this.consumerName}' is in ReadMode.Block but no reader connection was opened."),
                keys,
                batchSize).FetchManyAsync;
        }
        else
        {
            fetch = new PollFetch(db, batchSize, this.consumer.MaxIdleDelayMs).FetchManyAsync;
        }

        this.log.LogInformation(
            "Streams: consumer {Consumer} on topic {Topic} reads its {PartitionCount} owned partitions with one co-located " +
            "{ReadMode} XREAD per round (inline, Backpressure.Enabled=false); the handler runs in the read loop for each reply slot.",
            this.consumerName,
            topic,
            count,
            this.consumer.ReadMode);

        var reading = PartitionWorker.ReadGroupInlineLoopAsync(
            contexts,
            from,
            fetch,
            this.handler,
            recorder,
            groupCts.Token,
            hopToThreadPool: this.consumer.ReadMode == ReadMode.Block,
            this.consumer.Persist,
            flush,
            resets,
            this.RewindNotifier());

        _ = reading.ContinueWith(
            faulted => this.OnLoopFaulted(faulted, partition: null, "the co-located inline read loop"),
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);

        this.groupReads.Add(reading);

        for (var i = 0; i < count; i++)
        {
            this.runners[this.runners.Count - count + i].Task = reading;
        }
    }

    /// <summary>
    /// Builds and starts one partition's worker: start position, fetch delegate, channel, and the
    /// read and processing loops.
    /// </summary>
    private async Task StartPartitionAsync(
        int partition,
        IDatabase db,
        TopicOptions topicOptions,
        IReadOnlyDictionary<int, StreamId> stored,
        IReadOnlyDictionary<int, ResetMarker>? markers,
        ResetSignal? resets,
        bool useGroup,
        CancellationToken startToken)
    {
        var topic = this.consumer.Topic;
        var key = StreamKeys.Stream(topic, partition, topicOptions.CoLocatePartitions);

        var start = ResetMarkers.ApplyToStart(
            StartPosition.Resolve(this.consumer, stored.TryGetValue(partition, out var id) ? id : null),
            markers,
            partition);

        // StartFrom.Now would otherwise arrive as 0-0: the fetches own a concrete cursor so that a
        // reconnect resumes rather than skips, so "now" is pinned to an id here, once, from the
        // server's tail rather than this client's clock. See ResolveNowAsync.
        // Group mode is excluded: there the cursor belongs to the group, XGROUP CREATE resolves `$`
        // server-side itself, and StartPosition.ReadFrom carries it — so a tail lookup here would be
        // a round trip whose answer nothing reads.
        var from = start.FromNow && !useGroup
            ? await ResolveNowAsync(db, key, this.log, startToken).ConfigureAwait(false)
            : start.After;
        var batchSize = Math.Max(1, this.consumer.BatchSize);
        var persist = this.consumer.Persist;

        Func<CancellationToken, ValueTask<StreamEntryBatch>> fetch;
        ResetSeek? seek;
        PositionRecorder recorder;
        PositionFlush? flush;

        if (useGroup)
        {
            var group = new ConsumerGroupFetch(
                db,
                key,
                this.consumerName,
                this.InstanceName(),
                start,
                batchSize,
                this.consumer.MaxIdleDelayMs,
                this.log);

            // Created at the resolved start, tolerating a group that already exists.
            await group.EnsureGroupAsync(startToken).ConfigureAwait(false);

            var acknowledger = new GroupAcknowledger(group);

            fetch = group.FetchAsync;
            seek = null;
            recorder = acknowledger.Record;
            flush = acknowledger.AcknowledgeAsync;

            // XACK is this mode's position write and it belongs after a successful batch — exactly
            // what SyncBatch means to the processing loop.
            persist = PersistMode.SyncBatch;
        }
        else
        {
            if (this.consumer.ReadMode == ReadMode.Block)
            {
                var block = new BlockFetch(
                    this.reader ?? throw new StreamConfigurationException(
                        $"Streams: consumer '{this.consumerName}' is in ReadMode.Block but no reader connection was opened."),
                    key,
                    from,
                    batchSize);

                fetch = block.FetchAsync;
                seek = block.SeekTo;
            }
            else
            {
                var poll = new PollFetch(db, key, from, batchSize, this.consumer.MaxIdleDelayMs);

                fetch = poll.FetchAsync;
                seek = poll.SeekTo;
            }

            if (this.flusher is { } positions)
            {
                recorder = positions.Recorder;
                flush = positions.FlushAsync;
            }
            else
            {
                // Persist = None: the processing loop never records, so this is only here to keep
                // the parameter non-null.
                recorder = NoPositions;
                flush = null;
            }
        }

        var channel = this.consumer.Backpressure.Enabled
            ? PartitionWorker.CreateChannel(Math.Max(1, this.consumer.Backpressure.Capacity))
            : null;

        // Registered before the loops start, so the health check and the streams.* gauges can see
        // this partition from the moment it exists rather than from its first successful batch.
        var monitor = StreamLag.Track(
            topic,
            this.consumerName,
            partition,
            key,
            this.consumer.UnhealthyLagMs,
            this.consumer.UnhealthyBlockSeconds);

        this.monitors.Add(monitor);

        var ctx = new PartitionContext(
            db,
            key,
            partition,
            topic,
            this.consumerName,
            batchSize,
            this.consumer.Filter,
            this.consumer.OnError,
            channel?.Writer,
            this.log)
        {
            Monitor = monitor,
        };

        // One source per partition, so the flusher's second-writer detection can stand a single
        // partition down without touching its siblings.
        var partitionCts = CancellationTokenSource.CreateLinkedTokenSource(this.workerCts!.Token);
        var runner = new PartitionRunner(partition, partitionCts, channel?.Writer);
        this.runners.Add(runner);

        Task worker;

        if (channel is null)
        {
            // Inline (no-backpressure) mode: one loop, the reader calls the handler itself. The hop
            // is forced under ReadMode.Block so application code never lands on a reader thread.
            worker = PartitionWorker.RunInlineAsync(
                in ctx,
                from,
                fetch,
                this.handler,
                recorder,
                partitionCts.Token,
                hopToThreadPool: this.consumer.ReadMode == ReadMode.Block,
                persist,
                flush,
                resets,
                seek,
                this.RewindNotifier());
        }
        else
        {
            var reading = PartitionWorker.ReadLoopAsync(ctx, from, fetch, partitionCts.Token, resets, seek, this.RewindNotifier());
            var channelReader = channel.Reader;

            // Task.Run: handler code starts on the standard ThreadPool, never on a reader thread.
            var processing = Task.Run(
                () => PartitionWorker.ProcessLoopAsync(
                    ctx, this.handler, recorder, partitionCts.Token, channelReader, persist, flush),
                CancellationToken.None);

            // R-08. ANY end of the processor loop stands the partition down — not just a faulted
            // one. ErrorPolicy.StopPartition and a contested stand-down both END the loop normally,
            // and the OnlyOnFaulted form left the reader parked in WriteAsync on a channel nobody
            // drains: the WhenAll below never completed, shutdown burned the whole
            // ShutdownTimeoutSeconds, and every batch the reader had already queued kept its pooled
            // array. Cancelling stops the fetch; completing the writer releases a reader already
            // parked in WriteAsync (which returns its rented array on the way out). The co-located
            // path has always done the second half unconditionally — see StartCoLocatedGroupAsync.
            _ = processing.ContinueWith(
                _ => this.OnPartitionEnded(runner, partition),
                CancellationToken.None,
                TaskContinuationOptions.None,
                TaskScheduler.Default);

            worker = Task.WhenAll(reading, processing);
        }

        runner.Task = worker;

        // A worker only faults on ErrorPolicy.Fail or a transport error the loops chose not to
        // swallow; either way it must be visible immediately rather than at shutdown.
        _ = worker.ContinueWith(
            faulted => this.OnLoopFaulted(faulted, partition, "the worker"),
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);
    }

    /// <summary>
    /// Resolves <see cref="StartFrom.Now"/> against the <b>server's</b> notion of the tail.
    /// </summary>
    /// <param name="db">The shared multiplexer's database.</param>
    /// <param name="key">The partition's stream key.</param>
    /// <param name="log">Logger for the one line this writes when the stream does not exist yet.</param>
    /// <param name="ct">The startup token.</param>
    /// <returns>The last id Redis has generated for the stream — read strictly after it.</returns>
    /// <remarks>
    /// <para>
    /// <b>Decision (R-10).</b> The plan says <c>StartFrom.Now</c> should send the literal <c>$</c>;
    /// the code sent <c>StreamId.FromDate(DateTimeOffset.UtcNow)</c>, i.e. <em>this pod's</em> clock,
    /// so a client running a second ahead of Redis skipped a second of entries and one running behind
    /// replayed them. Neither is acceptable, but <c>$</c> is not the fix either: the fetch seams own a
    /// concrete cursor precisely so that a reconnect or a block timeout resumes rather than skips, and
    /// a re-sent <c>$</c> re-resolves to whatever the tail is <em>then</em> — silently dropping
    /// everything published in between (the hazard <see cref="StartPosition.ReadFrom"/> documents).
    /// </para>
    /// <para>
    /// So the tail is resolved <em>once</em>, here, by the server, and the concrete id it returns is
    /// the cursor from then on. That is exactly what <c>$</c> means on the first read, with none of
    /// the re-resolution risk and no dependency on the client clock. <c>$</c> itself survives where it
    /// is safe: consumer-group mode, where <c>XGROUP CREATE</c> resolves it server-side and the group
    /// owns the cursor afterwards.
    /// </para>
    /// <para>
    /// <c>XINFO STREAM</c> rather than <c>XREVRANGE</c>: <c>last-generated-id</c> survives trimming and
    /// deletion, so a stream whose tail entries have been trimmed away still yields the right cursor.
    /// </para>
    /// </remarks>
    private static async Task<StreamId> ResolveNowAsync(IDatabase db, RedisKey key, ILogger log, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        try
        {
            var info = await db.StreamInfoAsync(key).ConfigureAwait(false);

            if (StreamId.TryParse(((string?)info.LastGeneratedId).AsSpan(), out var last))
            {
                return last;
            }
        }
        catch (RedisServerException ex)
        {
            // ERR no such key: nothing has ever been written to this partition, so "now" and "the
            // beginning" are the same position and StreamId.Min is exact rather than approximate.
            log.LogDebug(
                ex,
                "Streams: StartFrom.Now found no stream at {Key} yet, so it starts from the beginning of a stream that has no entries.",
                key.ToString());

            return StreamId.Min;
        }

        return StreamId.Min;
    }

    /// <summary>
    /// Winds one partition down when its processing loop ends, whatever ended it.
    /// </summary>
    /// <param name="runner">The partition's runner.</param>
    /// <param name="partition">The partition index, for the log line.</param>
    /// <remarks>
    /// See the R-08 note at the call site: this runs on <em>every</em> end of the loop, because
    /// <see cref="ErrorPolicy.StopPartition"/> and a contested stand-down end it normally and used to
    /// leave the reader filling a channel nobody drains. Cancelling stops the fetch; completing the
    /// writer releases a reader already parked in <c>WriteAsync</c>.
    /// </remarks>
    private void OnPartitionEnded(PartitionRunner runner, int partition)
    {
        runner.Stop();
        runner.CompleteWriter();

        if (this.stopping)
        {
            // The ordinary drain. Every partition ends here on the way out and saying so once per
            // partition would bury the shutdown line the operator actually reads.
            return;
        }

        this.log.LogInformation(
            "Streams: partition {Partition} of topic {Topic} consumer {Consumer} has stood down while the host keeps running — " +
            "its read loop is cancelled and its channel completed, so nothing reads that partition until this consumer restarts. " +
            "Its stored position is unchanged, so the backlog is replayed then.",
            partition,
            this.consumer.Topic,
            this.consumerName);
    }

    /// <summary>
    /// Reports a loop that ended with an exception and, when the failure is one the pod cannot work
    /// through, stops the application so the orchestrator restarts it.
    /// </summary>
    /// <param name="faulted">The faulted task; only its exception is read.</param>
    /// <param name="partition">The partition, or <see langword="null"/> for a whole-group read loop.</param>
    /// <param name="what">What faulted, for the log line: "the worker", "the co-located read loop".</param>
    /// <remarks>
    /// <para>
    /// <b>R-03.</b> <see cref="ErrorPolicy.Fail"/> is documented as "fault the host so Kubernetes
    /// restarts the pod", and until this existed it was nothing of the kind: the processing loop
    /// rethrew, the worker task faulted, a <c>ContinueWith</c> logged it, and the pod carried on
    /// serving traffic with one partition — or, on a single-partition consumer, its entire consumer —
    /// silently dead. Nothing awaits a worker task, so a faulted one is invisible to the generic host.
    /// <see cref="IHostApplicationLifetime.StopApplication"/> is the only thing that turns it into a
    /// restart, and it is deliberately a graceful stop: <see cref="StopAsync"/> still runs, so
    /// positions are flushed and the healthy partitions drain what they had.
    /// </para>
    /// <para>
    /// A <see cref="StreamConfigurationException"/> is fatal under every policy, not just
    /// <see cref="ErrorPolicy.Fail"/>: it comes from the startup validation, no amount of running
    /// fixes it, and a pod that stays Ready while consuming nothing is the worst of the outcomes.
    /// </para>
    /// <para>
    /// Faults raised while the host is stopping are logged and go no further — a shutdown race must
    /// not be reported as a policy failure, and stopping an application that is already stopping is
    /// noise at best.
    /// </para>
    /// </remarks>
    private void OnLoopFaulted(Task faulted, int? partition, string what)
    {
        var error = faulted.Exception?.GetBaseException();

        this.log.LogError(
            faulted.Exception,
            "Streams: {What} for topic {Topic} partition {Partition} consumer {Consumer} stopped with an error.",
            what,
            this.consumer.Topic,
            partition,
            this.consumerName);

        if (this.stopping || error is null or OperationCanceledException)
        {
            return;
        }

        var fatal = this.consumer.OnError == ErrorPolicy.Fail || error is StreamConfigurationException;

        if (!fatal)
        {
            return;
        }

        if (this.lifetime is not { } host)
        {
            this.log.LogCritical(
                error,
                "Streams: {What} for topic {Topic} partition {Partition} consumer {Consumer} failed under ErrorPolicy.{Policy}, " +
                "but this host was built without an IHostApplicationLifetime, so the application cannot be stopped from here. " +
                "The partition is dead and the process will NOT restart itself. Register the consumer through AddStream so the " +
                "lifetime is injected, or pass one to the StreamConsumerHost constructor.",
                what,
                this.consumer.Topic,
                partition,
                this.consumerName,
                this.consumer.OnError);

            return;
        }

        this.log.LogCritical(
            error,
            "Streams: {What} for topic {Topic} partition {Partition} consumer {Consumer} failed under ErrorPolicy.{Policy}. " +
            "Stopping the application so the orchestrator restarts this pod — positions are flushed on the way out and the " +
            "un-drained tail is redelivered on the next start.",
            what,
            this.consumer.Topic,
            partition,
            this.consumerName,
            this.consumer.OnError);

        host.StopApplication();
    }

    /// <summary>
    /// Starts the process-wide <see cref="StreamLagSampler"/> if it is not already running, and takes
    /// a reference to it.
    /// </summary>
    /// <param name="redis">The shared multiplexer — never a dedicated reader connection, which may be
    /// parked in a blocking <c>XREAD</c>.</param>
    /// <param name="log">Logger for the sampler.</param>
    /// <returns><see langword="true"/> when the caller now holds a reference it must release.</returns>
    /// <remarks>
    /// One sampler per process, not per host: it walks <see cref="StreamLag.All"/>, which is every
    /// monitor in the process, so a second one would double the <c>XINFO STREAM</c> traffic and push
    /// the same gauge values twice. Reference counted so that a service hosting several consumers
    /// keeps sampling until the last of them stops. The first host in wins the multiplexer, which is
    /// exactly right while every consumer resolves its connection from the one
    /// <see cref="StreamsConnectionProvider"/> in the container — the shape <c>AddStream</c> builds.
    /// </remarks>
    private static bool AcquireLagSampler(IConnectionMultiplexer redis, ILogger log)
    {
        lock (SamplerGate)
        {
            if (SharedSampler is null)
            {
                var started = new StreamLagSampler(redis, log);
                started.Start();
                SharedSampler = started;
            }

            SamplerReferences++;

            return true;
        }
    }

    /// <summary>Releases this host's reference to the shared sampler, stopping it when it was the last.</summary>
    private static async ValueTask ReleaseLagSamplerAsync()
    {
        StreamLagSampler? stopping;

        lock (SamplerGate)
        {
            if (SamplerReferences > 0)
            {
                SamplerReferences--;
            }

            if (SamplerReferences > 0 || SharedSampler is null)
            {
                return;
            }

            stopping = SharedSampler;
            SharedSampler = null;
        }

        await stopping.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Waits for every worker, bounded by <paramref name="budget"/> and by the caller's token.
    /// </summary>
    /// <param name="budget">The drain budget, from <see cref="ConsumerOptions.ShutdownTimeoutSeconds"/>.</param>
    /// <param name="cancellationToken">The caller's stopping token; it shortens the drain, never lengthens it.</param>
    /// <returns><see langword="true"/> when every worker finished inside the budget.</returns>
    private async Task<bool> DrainAsync(TimeSpan budget, CancellationToken cancellationToken)
    {
        if (this.workers is not { Length: > 0 } running)
        {
            return true;
        }

        var all = Task.WhenAll(running);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(budget);

        var winner = await Task.WhenAny(all, Task.Delay(Timeout.Infinite, timeout.Token)).ConfigureAwait(false);

        if (!ReferenceEquals(winner, all))
        {
            return false;
        }

        try
        {
            await all.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Cancellation is how a drain ends; it is not a failure.
        }
        catch (Exception ex)
        {
            this.log.LogWarning(
                ex,
                "Streams: consumer {Consumer} on topic {Topic} had a worker fail during shutdown.",
                this.consumerName,
                this.consumer.Topic);
        }

        return true;
    }

    /// <summary>
    /// Disposes everything this host owns, in the order that keeps the loops' ground under them:
    /// partition sources, flusher, ownership claims, then the reader connection the read loops were
    /// parked on. Safe to call twice — <see cref="StopAsync"/> and <see cref="DisposeAsync"/> both
    /// route through it.
    /// </summary>
    private async ValueTask ReleaseAsync()
    {
        foreach (var runner in this.runners)
        {
            runner.Dispose();
        }

        this.runners.Clear();
        this.groupReads.Clear();
        this.workers = null;

        // A pod that no longer owns a partition must stop reporting its lag rather than pinning the
        // gauge — and must stop counting towards this process's health.
        foreach (var monitor in this.monitors)
        {
            StreamLag.Forget(monitor);
        }

        this.monitors.Clear();

        // After the monitors are forgotten, so the last sample cannot resurrect a gauge for a
        // partition this pod no longer owns.
        if (this.holdsSampler)
        {
            this.holdsSampler = false;
            await ReleaseLagSamplerAsync().ConfigureAwait(false);
        }

        if (this.flusher is { } positions)
        {
            await positions.DisposeAsync().ConfigureAwait(false);
            this.flusher = null;
        }

        if (this.ownership is { } claims)
        {
            await claims.DisposeAsync().ConfigureAwait(false);
            this.ownership = null;
        }

        if (this.reader is { } connected)
        {
            await connected.DisposeAsync().ConfigureAwait(false);
            this.reader = null;
        }

        if (this.workerCts is { } generation)
        {
            generation.Dispose();
            this.workerCts = null;
        }

        if (this.cts is { } source)
        {
            source.Dispose();
            this.cts = null;
        }
    }

    /// <summary>
    /// Stands one partition down after the flusher found another instance writing its position.
    /// Fighting over a position hash only moves it backwards; one owner is the better outcome.
    /// </summary>
    /// <param name="partition">The contested partition.</param>
    /// <param name="mine">This instance's id.</param>
    /// <param name="theirs">The instance id found in the hash.</param>
    private void OnPartitionContested(int partition, Guid mine, Guid theirs)
    {
        foreach (var runner in this.runners)
        {
            if (runner.Partition != partition)
            {
                continue;
            }

            this.log.LogWarning(
                "Streams: standing partition {Partition} of topic {Topic} down for consumer {Consumer} — instance {Theirs} is " +
                "writing the position this instance ({Mine}) owns. Check STREAMS_INSTANCE_COUNT against spec.replicas.",
                partition,
                this.consumer.Topic,
                this.consumerName,
                theirs,
                mine);

            runner.Stop();
            return;
        }
    }

    /// <summary>
    /// This instance's name inside a consumer group. Must be stable across a restart, or a bounced
    /// pod's pending entries wait for the claim sweep instead of resuming directly.
    /// </summary>
    private string InstanceName() =>
        Environment.GetEnvironmentVariable(InstanceResolver.PodNameVariable) is { Length: > 0 } pod
            ? pod
            : Environment.MachineName;

    /// <summary>
    /// The recorder used when there is no position store (<see cref="PersistMode.None"/>). The
    /// processing loop skips the call entirely in that mode; this exists so the parameter is never
    /// null.
    /// </summary>
    /// <param name="partition">Ignored.</param>
    /// <param name="id">Ignored.</param>
    private static void NoPositions(int partition, StreamId id)
    {
        _ = partition;
        _ = id;
    }

    /// <summary>
    /// The read loop's hook back into the position flusher for a live reset, or <see langword="null"/>
    /// when there is no flusher (<see cref="PersistMode.None"/>) for it to notify.
    /// </summary>
    /// <returns><see cref="PositionFlusher.Rewind"/>, bound to this host's flusher.</returns>
    private Action<int>? RewindNotifier() => this.flusher is { } positions ? positions.Rewind : null;

    /// <summary>
    /// Group mode's stand-in for the position flusher: remember the batch's last id when the handler
    /// succeeds, then <c>XACK</c> it. Both calls happen on one partition's processing loop, so
    /// nothing here needs synchronisation.
    /// </summary>
    /// <param name="fetch">The group fetch whose pending entries are acknowledged.</param>
    private sealed class GroupAcknowledger(ConsumerGroupFetch fetch)
    {
        private StreamId last;

        /// <summary>Records the last successfully handled id. Allocation-free, never awaits.</summary>
        /// <param name="partition">Ignored — one acknowledger per partition already.</param>
        internal void Record(int partition, StreamId id)
        {
            _ = partition;
            this.last = id;
        }

        /// <summary>Acknowledges every fetched batch up to the last recorded id.</summary>
        /// <returns>A task that completes when the <c>XACK</c> has landed.</returns>
        internal ValueTask AcknowledgeAsync(CancellationToken ct) => fetch.AckAsync(this.last, ct);
    }

    /// <summary>One partition's worker, its own cancellation source, and its channel writer.</summary>
    /// <param name="writer">The channel writer, or <see langword="null"/> in inline mode.</param>
    private sealed class PartitionRunner(int partition, CancellationTokenSource cts, ChannelWriter<StreamBatch>? writer)
    {
        internal int Partition { get; } = partition;

        /// <summary>The read and processing loops, as one task.</summary>
        internal Task Task { get; set; } = Task.CompletedTask;

        /// <summary>Completes the channel so the processor drains what is queued and then exits.</summary>
        internal void CompleteWriter() => writer?.TryComplete();

        /// <summary>Stops just this partition, leaving its siblings running.</summary>
        internal void Stop()
        {
            try
            {
                cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // The host is already gone; there is nothing left to stop.
            }
        }

        internal void Dispose() => cts.Dispose();
    }
}
