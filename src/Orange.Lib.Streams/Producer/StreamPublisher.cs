using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Text;
using Microsoft.Extensions.Logging;
using Orange.Lib.Streams.Admin;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Diagnostics;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Producer;

/// <summary>
/// The non-buffered publisher: one <c>XADD</c>, awaited to completion.
/// </summary>
/// <remarks>
/// <para>
/// The command it issues is
/// <code>XADD s:{topic}:&lt;p&gt; MAXLEN ~ &lt;maxLen&gt; * b &lt;body&gt; t &lt;type&gt; k &lt;key&gt; [c &lt;corr&gt;] [p &lt;traceparent&gt;] [h &lt;headers&gt;]</code>
/// built through <see cref="EntryCodec"/> and <see cref="TrimArgs"/> so producer and background
/// trimmer cannot drift apart on the wire format or on what trimming means.
/// </para>
/// <para>
/// <b>Trimming is inline.</b> The <c>MAXLEN</c> clause rides along with every publish, which is
/// free at the Redis end for <see cref="TrimMode.Approx"/> — it drops a whole macro node or does
/// nothing. Time-based retention cannot be expressed on <c>XADD</c> and is the background trimmer's
/// job.
/// </para>
/// <para>
/// <b>No retries.</b> See <see cref="IStreamPublisher"/>: a failure throws.
/// </para>
/// <para>
/// <b>It reconciles the topic.</b> The first publish runs
/// <see cref="StreamAdmin.EnsureTopicAsync(IConnectionMultiplexer, string, TopicOptions, ILogger?, CancellationToken)"/>
/// once, exactly as the consumer host does at startup. Without it a publish-only service never
/// writes <c>m:{topic}</c>, never creates the partition streams (so <c>XLEN</c> and every lag read
/// error on a partition nothing has been routed to yet), and — the one that costs data — gets no
/// partition-decrease guard: a producer misconfigured downwards would happily write to a subset of
/// the partitions while consumers still read the recorded count, and nobody would find out. The
/// reconcile is idempotent and guarded process-wide, so it costs a handful of round trips once.
/// Call <see cref="EnsureTopicAsync"/> from startup to pay that cost before the first message
/// rather than on it.
/// </para>
/// <para>
/// <b>Allocation.</b> Stream keys, trim arguments and the boxed partition tags are built once in
/// the constructor. A publish then costs the codec's field array, the argument array, and the boxes
/// for the fields themselves — no key formatting, no hashing allocation, no LINQ.
/// The body is never copied.
/// </para>
/// <para>Instances are thread-safe and are intended to be registered as singletons.</para>
/// </remarks>
internal sealed class StreamPublisher : IStreamPublisher
{
    /// <summary>Partition keys up to this UTF-8 length are hashed from a stack buffer.</summary>
    private const int KeyStackBytes = 256;

    private const string XAddCommand = "XADD";

    /// <summary>The auto-id placeholder, boxed once — it is the same object on every command.</summary>
    private static readonly object AutoId = (RedisValue)"*";

    private readonly IDatabase database;

    /// <summary>
    /// The multiplexer behind <see cref="database"/>, for the topic reconcile. Null only when the
    /// publisher was handed an <see cref="IDatabase"/> stand-in that cannot produce one, in which
    /// case the reconcile is skipped — see <see cref="MultiplexerOf"/>.
    /// </summary>
    private readonly IConnectionMultiplexer? multiplexer;

    private readonly ILogger? log;
    private readonly Lock ensureGate = new();
    private readonly RedisKey[] streamKeys;

    /// <summary>Pre-boxed <c>MAXLEN ~ n</c> arguments; empty for <see cref="TrimMode.None"/>.</summary>
    private readonly object[] trimArgs;

    /// <summary>Pre-boxed partition indexes, so a metric tag costs no boxing per publish.</summary>
    private readonly object[] partitionTags;

    private uint roundRobin;

    /// <summary>The in-flight or completed reconcile; null until the first publish asks for one.</summary>
    private Task? ensure;

    /// <summary>Set once the topic is reconciled, so the steady-state publish path is one volatile read.</summary>
    private volatile bool topicEnsured;

    /// <summary>
    /// Creates a publisher bound to one topic.
    /// </summary>
    /// <param name="database">The database on the <b>shared</b> multiplexer. Writes must never go through a consumer's dedicated reader connection, which is read-only and may be parked in a blocking <c>XREAD</c>.</param>
    /// <param name="topic">The topic name.</param>
    /// <param name="topicOptions">The topic's options; <see langword="null"/> uses record defaults.</param>
    /// <param name="logger">Optional logger, for the topic reconcile's partition-increase Warning.</param>
    /// <exception cref="ArgumentException"><paramref name="topic"/> is null, empty or whitespace.</exception>
    public StreamPublisher(IDatabase database, string topic, TopicOptions? topicOptions = null, ILogger? logger = null)
        : this(database, MultiplexerOf(database), topic, topicOptions, logger)
    {
    }

    /// <summary>
    /// Creates a publisher bound to one topic, over the multiplexer rather than a database handle.
    /// This is the form a service should use: it is the one that can reconcile the topic.
    /// </summary>
    /// <param name="redis">The <b>shared</b> multiplexer. Never a consumer's dedicated reader connection.</param>
    /// <param name="topic">The topic name.</param>
    /// <param name="topicOptions">The topic's options; <see langword="null"/> uses record defaults.</param>
    /// <param name="logger">Optional logger, for the topic reconcile's partition-increase Warning.</param>
    /// <exception cref="ArgumentException"><paramref name="topic"/> is null, empty or whitespace.</exception>
    public StreamPublisher(IConnectionMultiplexer redis, string topic, TopicOptions? topicOptions = null, ILogger? logger = null)
        : this(Database(redis), redis, topic, topicOptions, logger)
    {
    }

    private StreamPublisher(
        IDatabase database,
        IConnectionMultiplexer? redis,
        string topic,
        TopicOptions? topicOptions,
        ILogger? logger)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        var options = topicOptions ?? new TopicOptions();
        if (options.Partitions < 1)
        {
            throw new StreamConfigurationException(
                $"Streams:Topics:{topic}:Partitions is {options.Partitions}; it must be >= 1.");
        }

        this.database = database;
        this.multiplexer = redis;
        this.log = logger;
        this.Topic = topic;
        this.Options = options;

        // Nothing to reconcile against: publishing works, the guard does not. Say so once here
        // rather than leaving the first publish to discover it.
        this.topicEnsured = redis is null;

        this.streamKeys = new RedisKey[options.Partitions];
        this.partitionTags = new object[options.Partitions];
        for (var p = 0; p < options.Partitions; p++)
        {
            this.streamKeys[p] = StreamKeys.Stream(topic, p, options.CoLocatePartitions);
            this.partitionTags[p] = p;
        }

        var trim = TrimArgs.GetMaxLenArgs(options.Trim, options.MaxLen);
        this.trimArgs = new object[trim.Length];
        for (var i = 0; i < trim.Length; i++)
        {
            this.trimArgs[i] = trim[i];
        }
    }

    /// <summary>The topic this publisher writes to.</summary>
    public string Topic { get; }

    /// <summary>The topic's resolved options — partition count, trim mode and length ceiling.</summary>
    public TopicOptions Options { get; }

    /// <summary>The topic's partition count.</summary>
    public int Partitions => this.streamKeys.Length;

    /// <summary>
    /// Reconciles the topic's metadata and partition streams, once per publisher.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The first publish calls this itself, so it is never required — but calling it from startup
    /// moves the round trips, and more importantly the partition-decrease refusal, off the first
    /// message and onto the point where a service can still fail to start. That is the difference
    /// between a misconfigured producer being caught by the pod and being caught by whoever notices
    /// the missing messages.
    /// </para>
    /// <para>
    /// A publisher constructed over an <see cref="IDatabase"/> that cannot produce its multiplexer —
    /// only test stand-ins — completes immediately without reconciling anything. A failed reconcile
    /// is not cached: the next publish tries again.
    /// </para>
    /// </remarks>
    /// <param name="ct">Cancellation for the wait. It does not cancel a reconcile another caller started.</param>
    /// <returns>A task that completes when the topic is reconciled.</returns>
    /// <exception cref="StreamConfigurationException">
    /// The configured partition count is below the count recorded for the topic, or the topic was
    /// created under a codec version this build cannot read.
    /// </exception>
    public Task EnsureTopicAsync(CancellationToken ct = default)
    {
        if (this.topicEnsured)
        {
            return Task.CompletedTask;
        }

        Task pending;
        lock (this.ensureGate)
        {
            pending = this.ensure ??= this.ReconcileAsync();
        }

        return ct.CanBeCanceled ? pending.WaitAsync(ct) : pending;
    }

    /// <inheritdoc />
    public ValueTask<StreamId> PublishAsync(
        string partitionKey,
        ReadOnlyMemory<byte> body,
        string type,
        PublishOptions options = default,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(type);
        ct.ThrowIfCancellationRequested();

        if (!this.topicEnsured)
        {
            return this.PublishAfterEnsureAsync(partitionKey, body, type, options, ct);
        }

        // R-16. HasListeners() is a bool field read on the ActivitySource, so a process with no
        // tracing pays one branch and keeps the untraced path exactly as it was: no extra state
        // machine, no span object, no tags. When something IS listening the traced path takes over,
        // because the span has to be started INSIDE an async method — see PublishTracedAsync.
        if (StreamsDiagnostics.Source.HasListeners())
        {
            return this.PublishTracedAsync(partitionKey, body, type, options);
        }

        var partition = this.Route(partitionKey, options.Partition);
        var args = this.BuildArgs(partition, body, type, partitionKey, options, CurrentTraceParent());

        return this.SendAsync(args, partition);
    }

    /// <inheritdoc />
    public ValueTask PublishBatchAsync(
        string partitionKey,
        ReadOnlyMemory<ReadOnlyMemory<byte>> bodies,
        string type,
        PublishOptions options = default,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(type);
        ct.ThrowIfCancellationRequested();

        if (bodies.IsEmpty)
        {
            return ValueTask.CompletedTask;
        }

        if (!this.topicEnsured)
        {
            return this.PublishBatchAfterEnsureAsync(partitionKey, bodies, type, options, ct);
        }

        if (StreamsDiagnostics.Source.HasListeners())
        {
            return this.PublishBatchTracedAsync(partitionKey, bodies, type, options);
        }

        var partition = this.Route(partitionKey, options.Partition);
        var span = bodies.Span;

        // Every command is issued before any is awaited: StackExchange.Redis pipelines them onto the
        // one connection, so the batch costs a single round trip's latency rather than one each.
        // CommandFlags.FireAndForget is deliberately NOT used — that would discard the assigned ids
        // and, more importantly, report success for writes that never landed.
        var traceParent = CurrentTraceParent();

        var pending = new Task<RedisResult>[span.Length];
        for (var i = 0; i < span.Length; i++)
        {
            var args = this.BuildArgs(partition, span[i], type, partitionKey, options, traceParent);
            pending[i] = this.database.ExecuteAsync(XAddCommand, args, CommandFlags.DemandMaster);
        }

        return this.AwaitBatchAsync(pending, partition);
    }

    /// <summary>
    /// The traced path of <see cref="PublishAsync"/>: one <c>streams.publish {topic}</c> span
    /// around the route, the encode and the <c>XADD</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>R-16 — why this exists at all.</b> <see cref="StreamSpans.StartPublish"/>,
    /// <see cref="StreamSpans.Published(Activity?, StreamId)"/> and
    /// <see cref="StreamSpans.Failed"/> had no call site anywhere in the library: the producer only
    /// copied <see cref="Activity.Current"/> into the entry's <c>p</c> field, so a trace showed the
    /// consumer's <c>streams.process</c> span hanging off whatever the caller happened to be doing
    /// and no publish span at all. The producer half of every hop was invisible in Jaeger.
    /// </para>
    /// <para>
    /// <b>Why it is a separate <c>async</c> method rather than a span opened in
    /// <see cref="PublishAsync"/>.</b> <see cref="ActivitySource.StartActivity(string, ActivityKind)"/>
    /// assigns <see cref="Activity.Current"/>, which is an <c>AsyncLocal</c>. Started in a
    /// non-<c>async</c> method it mutates the <em>caller's</em> execution context, and the matching
    /// <c>Dispose</c> — which happens after an <c>await</c>, in a context the caller does not
    /// share — cannot undo it: the caller would be left with the publish span as its ambient
    /// activity forever. An <c>async</c> method's builder restores the caller's execution context
    /// after the initial synchronous run, so starting and stopping the span inside one is the only
    /// shape that leaks nothing.
    /// </para>
    /// <para>
    /// <b>The entry still carries the caller's <c>traceparent</c>, not this span's.</b> That is
    /// deliberate and is captured before the span starts. Adding a span must not move the context on
    /// the wire: the consumer's link-versus-parent rule (06-errors-and-observability.md, "Spans") is
    /// written against the producer's own context, and <c>TraceContextTests</c> asserts message by
    /// message which trace each entry was published under. The publish span is a child of that same
    /// caller, so the whole hop still lands in one trace — the consumer span is a sibling of the
    /// publish rather than its child.
    /// </para>
    /// </remarks>
    private async ValueTask<StreamId> PublishTracedAsync(
        string partitionKey,
        ReadOnlyMemory<byte> body,
        string type,
        PublishOptions options)
    {
        var partition = this.Route(partitionKey, options.Partition);

        // Captured BEFORE the span starts: the entry carries the caller's context, not the publish
        // span's. See the remarks on BuildArgs and on this method.
        var traceParent = CurrentTraceParent();

        using var activity = StreamSpans.StartPublish(this.Topic, partition, type);

        try
        {
            var args = this.BuildArgs(partition, body, type, partitionKey, options, traceParent);
            var id = await this.SendAsync(args, partition).ConfigureAwait(false);

            StreamSpans.Published(activity, id);
            return id;
        }
        catch (Exception ex)
        {
            StreamSpans.Failed(activity, ex);
            throw;
        }
    }

    /// <summary>
    /// The traced path of <see cref="PublishBatchAsync"/>: one span over the whole pipelined batch,
    /// carrying <c>messaging.batch.message_count</c>. See <see cref="PublishTracedAsync"/> for why
    /// it is an <c>async</c> method.
    /// </summary>
    /// <remarks>
    /// One span per batch, not per message: the batch is one round trip, every entry shares the
    /// partition and the type, and a span per message would multiply trace volume by the batch size
    /// for no extra information. The per-entry ids are not stamped for the same reason — the
    /// attribute is single-valued and the batch's entries are contiguous.
    /// </remarks>
    private async ValueTask PublishBatchTracedAsync(
        string partitionKey,
        ReadOnlyMemory<ReadOnlyMemory<byte>> bodies,
        string type,
        PublishOptions options)
    {
        var partition = this.Route(partitionKey, options.Partition);
        var traceParent = CurrentTraceParent();

        using var activity = StreamSpans.StartPublish(this.Topic, partition, type, bodies.Length);

        try
        {
            var span = bodies.Span;

            var pending = new Task<RedisResult>[span.Length];
            for (var i = 0; i < span.Length; i++)
            {
                var args = this.BuildArgs(partition, span[i], type, partitionKey, options, traceParent);
                pending[i] = this.database.ExecuteAsync(XAddCommand, args, CommandFlags.DemandMaster);
            }

            await this.AwaitBatchAsync(pending, partition).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            StreamSpans.Failed(activity, ex);
            throw;
        }
    }

    /// <summary>
    /// The cold path of <see cref="PublishAsync"/>: reconcile the topic, then publish. Split out so
    /// the steady-state path stays a synchronous route-and-send behind one volatile read.
    /// </summary>
    private async ValueTask<StreamId> PublishAfterEnsureAsync(
        string partitionKey,
        ReadOnlyMemory<byte> body,
        string type,
        PublishOptions options,
        CancellationToken ct)
    {
        await this.EnsureTopicAsync(ct).ConfigureAwait(false);

        // Back through the front door, exactly as PublishBatchAfterEnsureAsync does, so the very
        // first publish of a publisher's life gets the same treatment as every one after it — the
        // traced path included. Duplicating route/encode/send here is what left the first publish
        // spanless. The recursion terminates: EnsureTopicAsync either sets topicEnsured or throws.
        return await this.PublishAsync(partitionKey, body, type, options, ct).ConfigureAwait(false);
    }

    /// <summary>The cold path of <see cref="PublishBatchAsync"/>. See <see cref="PublishAfterEnsureAsync"/>.</summary>
    private async ValueTask PublishBatchAfterEnsureAsync(
        string partitionKey,
        ReadOnlyMemory<ReadOnlyMemory<byte>> bodies,
        string type,
        PublishOptions options,
        CancellationToken ct)
    {
        await this.EnsureTopicAsync(ct).ConfigureAwait(false);
        await this.PublishBatchAsync(partitionKey, bodies, type, options, ct).ConfigureAwait(false);
    }

    private async Task ReconcileAsync()
    {
        try
        {
            await StreamAdmin
                .EnsureTopicAsync(this.multiplexer!, this.Topic, this.Options, this.log, CancellationToken.None)
                .ConfigureAwait(false);

            this.topicEnsured = true;
        }
        catch
        {
            // A transport blip must not condemn the publisher for the life of the process: clear the
            // cached task so the next publish retries. A StreamConfigurationException will simply
            // fail again, which is the point of it.
            lock (this.ensureGate)
            {
                this.ensure = null;
            }

            throw;
        }
    }

    private async ValueTask<StreamId> SendAsync(object[] args, int partition)
    {
        RedisResult result;
        try
        {
            result = await this.database
                .ExecuteAsync(XAddCommand, args, CommandFlags.DemandMaster)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (IsPublishFailure(ex))
        {
            throw this.Failure(partition, 1, ex);
        }

        this.Count(partition, 1);
        return ParseId(result);
    }

    /// <summary>
    /// Awaits a pipelined batch, counting what landed before reporting what did not.
    /// </summary>
    /// <remarks>
    /// Each task is awaited in turn rather than through <see cref="Task.WhenAll(Task[])"/>, for two
    /// reasons. Every task is observed even after one fails, so a second failure is not left as an
    /// unobserved exception; and the successes are countable, so a batch that half-landed increments
    /// <c>streams.published</c> by what actually landed instead of by nothing. The commands were all
    /// issued before the first await, so this still costs one round trip.
    /// </remarks>
    private async ValueTask AwaitBatchAsync(Task<RedisResult>[] pending, int partition)
    {
        Exception? failure = null;
        var written = 0;

        for (var i = 0; i < pending.Length; i++)
        {
            try
            {
                _ = await pending[i].ConfigureAwait(false);
                written++;
            }
            catch (Exception ex)
            {
                failure ??= ex;
            }
        }

        if (written > 0)
        {
            this.Count(partition, written);
        }

        if (failure is not null)
        {
            if (!IsPublishFailure(failure))
            {
                // Cancellation is control flow, not a transport failure — same rule as SendAsync.
                ExceptionDispatchInfo.Throw(failure);
            }

            throw this.Failure(partition, pending.Length - written, failure);
        }
    }

    /// <summary>
    /// Whether a publish failure should be wrapped as a <see cref="StreamTransportException"/>.
    /// </summary>
    /// <remarks>
    /// Catching only <see cref="RedisException"/> let everything else out unwrapped, and the common
    /// "everything else" is real: a multiplexer disposed during shutdown throws
    /// <see cref="ObjectDisposedException"/>, and a socket that dies mid-command surfaces as an
    /// <see cref="IOException"/>. Callers that catch <see cref="StreamTransportException"/> — which
    /// is what the contract tells them to catch — were missing both. Cancellation is deliberately
    /// left alone: it is control flow, not a transport failure.
    /// </remarks>
    /// <param name="ex">The exception the command threw.</param>
    /// <returns>Whether to wrap it.</returns>
    private static bool IsPublishFailure(Exception ex) => ex is not OperationCanceledException;

    /// <summary>
    /// The multiplexer behind an <see cref="IDatabase"/>, or <see langword="null"/> when the handle
    /// cannot produce one.
    /// </summary>
    /// <remarks>
    /// Every <see cref="IDatabase"/> a service can actually build answers this — it is how the topic
    /// reconcile reaches Redis from the database-shaped constructor, which is what made the publisher
    /// structurally unable to reconcile before. The only handles that do not are test stand-ins that
    /// implement the one method under test, and those get a publisher that skips the reconcile rather
    /// than one that throws on the first publish.
    /// </remarks>
    /// <param name="database">The database handle.</param>
    /// <returns>Its multiplexer, or null.</returns>
    private static IConnectionMultiplexer? MultiplexerOf(IDatabase database)
    {
        ArgumentNullException.ThrowIfNull(database);

        try
        {
            return database.Multiplexer;
        }
        catch (Exception ex) when (ex is NotSupportedException or NotImplementedException)
        {
            return null;
        }
    }

    private static IDatabase Database(IConnectionMultiplexer redis)
    {
        ArgumentNullException.ThrowIfNull(redis);
        return redis.GetDatabase();
    }

    private static StreamId ParseId(RedisResult result)
    {
        var id = (string?)result;
        if (string.IsNullOrEmpty(id))
        {
            throw new StreamTransportException("XADD returned no entry id. The command was accepted but the reply was empty or nil.");
        }

        return StreamId.Parse(id);
    }

    /// <summary>
    /// Assembles the full <c>XADD</c> argument list: key, trim clause, auto-id, then the codec's
    /// field/value pairs flattened.
    /// </summary>
    /// <remarks>
    /// <paramref name="traceParent"/> is passed in rather than read from
    /// <see cref="Activity.Current"/> here, because on the traced path <c>Current</c> is the publish
    /// span by the time this runs. The entry must carry the <em>caller's</em> context — that is the
    /// contract the consumer's link-versus-parent rule is written against, and what
    /// <c>TraceContextTests</c> pins — so the traced path captures it before the span starts.
    /// </remarks>
    private object[] BuildArgs(
        int partition,
        ReadOnlyMemory<byte> body,
        string type,
        string partitionKey,
        in PublishOptions options,
        string? traceParent)
    {
        var fields = EntryCodec.Encode(
            body,
            type,
            partitionKey,
            options.CorrelationId,
            traceParent,
            options.Headers);

        var trim = this.trimArgs;
        var args = new object[1 + trim.Length + 1 + (fields.Length * 2)];

        var at = 0;
        args[at++] = this.streamKeys[partition];

        for (var i = 0; i < trim.Length; i++)
        {
            args[at++] = trim[i];
        }

        args[at++] = AutoId;

        for (var i = 0; i < fields.Length; i++)
        {
            ref readonly var field = ref fields[i];
            args[at++] = field.Name;
            args[at++] = field.Value;
        }

        return args;
    }

    /// <summary>
    /// The ambient W3C <c>traceparent</c>, so the consumer can continue this trace across the hop.
    /// Non-W3C activity ids are skipped rather than written as a header the consumer cannot parse.
    /// </summary>
    private static string? CurrentTraceParent()
    {
        var activity = Activity.Current;
        return activity is { IdFormat: ActivityIdFormat.W3C } ? activity.Id : null;
    }

    /// <summary>
    /// Picks the partition: an explicit <see cref="PublishOptions.Partition"/> wins outright,
    /// otherwise a non-empty key hashes stably and an empty key round-robins.
    /// </summary>
    private int Route(string? partitionKey, int? explicitPartition)
    {
        var partitions = this.streamKeys.Length;

        if (explicitPartition is int chosen)
        {
            if ((uint)chosen >= (uint)partitions)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(explicitPartition),
                    chosen,
                    $"Topic '{this.Topic}' has {partitions} partition(s), so an explicit partition must be in [0, {partitions - 1}].");
            }

            return chosen;
        }

        if (string.IsNullOrEmpty(partitionKey))
        {
            return PartitionRouter.RoundRobin(ref this.roundRobin, partitions);
        }

        if (Encoding.UTF8.GetMaxByteCount(partitionKey.Length) <= KeyStackBytes)
        {
            Span<byte> scratch = stackalloc byte[KeyStackBytes];
            var written = Encoding.UTF8.GetBytes(partitionKey.AsSpan(), scratch);
            return PartitionRouter.ForKey(scratch[..written], partitions);
        }

        return PartitionRouter.ForKey(Encoding.UTF8.GetBytes(partitionKey), partitions);
    }

    private void Count(int partition, int count)
    {
        var tags = new TagList
        {
            { "topic", this.Topic },
            { "partition", this.partitionTags[partition] },
        };

        StreamsDiagnostics.StreamsPublished.Add(count, in tags);
    }

    private StreamTransportException Failure(int partition, int count, Exception inner)
        => new(
            $"Failed to publish {count} message(s) to topic '{this.Topic}' partition {partition} ({this.streamKeys[partition]}). " +
            "The publisher does not retry — a silent retry would hide the outage and reorder messages within the key — so this is the caller's to handle.",
            inner);
}
