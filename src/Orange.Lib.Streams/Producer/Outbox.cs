using System.Diagnostics;
using System.Text;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Diagnostics;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Producer;

/// <summary>
/// One publish inside an outbox transaction, for <see cref="Outbox.WriteAndPublishManyAsync"/>.
/// </summary>
/// <param name="Topic">The topic to publish to. Every publish in one transaction must land in one hash slot — see <see cref="Outbox"/>.</param>
/// <param name="PartitionKey">The routing key; empty round-robins.</param>
/// <param name="Body">The message body. Passed to Redis by reference — not copied — so it must not be mutated until the returned task completes.</param>
/// <param name="Type">The message type string; consumers filter on it, so it is required.</param>
/// <param name="Options">Correlation id, headers and an optional explicit partition.</param>
/// <param name="TopicOptions">
/// The topic's configured options. <see langword="null"/> takes record defaults, which is only safe
/// when the topic is genuinely unconfigured: a wrong partition count mis-routes the key, and a wrong
/// <see cref="Config.TopicOptions.MaxLen"/> trims the stream to a ceiling that is not the topic's.
/// </param>
public readonly record struct OutboxPublish(
    string Topic,
    string PartitionKey,
    ReadOnlyMemory<byte> Body,
    string Type,
    PublishOptions Options = default,
    TopicOptions? TopicOptions = null);

/// <summary>
/// Writes state and publishes the event announcing it as one Redis <c>MULTI</c>/<c>EXEC</c>, so a
/// crash cannot leave the state and the stream disagreeing.
/// </summary>
/// <remarks>
/// <para>
/// The classic outbox needs a table and a relay process because the state store and the broker are
/// different systems. Here they are the same Redis, so the whole thing collapses into one
/// transaction: the state writes and the <c>XADD</c> are queued on one connection and run by the
/// server back to back, with no other client interleaved.
/// </para>
/// <para>
/// <b>MULTI/EXEC is isolation, not rollback.</b> This is the single most misread property of Redis
/// transactions, and it is not a limitation of this helper — Lua behaves the same way. A command
/// that fails at <i>queue</i> time (unknown command, wrong arity) aborts the whole batch. A command
/// that fails at <i>run</i> time — <c>WRONGTYPE</c>, because the state key is a hash and the delegate
/// treated it as a string — does <b>not</b> stop the other queued commands applying, and there is no
/// rollback. So the guarantee bought here is precisely:
/// </para>
/// <list type="bullet">
///   <item><description>no other client ever observes a half-applied state (isolation), and</description></item>
///   <item><description>neither the state write nor the publish can be lost to a process crash between them.</description></item>
/// </list>
/// <para>
/// It is <b>not</b> "either both succeed or both fail". Handlers must still be idempotent —
/// <see cref="Idempotency.TryBeginAsync"/> is there for the ones that are not naturally so.
/// </para>
/// <para>
/// <b>Always non-buffered, always on the shared multiplexer.</b> A buffered publish would defeat the
/// atomicity, so buffering cannot be passed in. A <c>MULTI</c>/<c>EXEC</c> is bound to a single
/// connection and cannot span two multiplexers, so the database must come from the shared streams
/// multiplexer: a consumer's dedicated reader connection is read-only and may be parked in a
/// blocking <c>XREAD</c>. See <see cref="Extensions.StreamsConnectionProvider"/>.
/// </para>
/// <para>
/// <b>Same slot.</b> In a Redis Cluster every key in a transaction must hash to one slot. Stream keys
/// are <c>s:{topic}:&lt;p&gt;</c> — hash-tagged on the topic — so a state key must carry the same
/// tag. <see cref="StateKey(string, string)"/> builds one; declared keys are validated before
/// anything is sent and throw <see cref="StreamConfigurationException"/> rather than letting a
/// cluster fail obscurely later. Where state genuinely cannot share a slot, take the escape hatch:
/// write the state, then publish, and make the consumer idempotent.
/// </para>
/// <para>
/// <b>Optimistic concurrency.</b> Conditions map straight onto <c>WATCH</c> via
/// <see cref="ITransaction.AddCondition"/>, so "publish this only if the version is still N" needs no
/// Lua script:
/// <code>
/// var id = await Outbox.WriteAndPublishAsync(
///     db,
///     tran =&gt; tran.HashSetAsync(stateKey, [new HashEntry("version", expected + 1)]),   // NO await in here
///     topic: "bets", partitionKey: betId, body: bytes, type: "BetPlaced",
///     conditions: [Condition.HashEqual(stateKey, "version", expected)],
///     stateKeys: [stateKey],
///     topicOptions: topicOptions);
///
/// if (id is null) { /* someone else moved the version on; nothing was applied */ }
/// </code>
/// </para>
/// </remarks>
public static class Outbox
{
    /// <summary>Partition keys up to this UTF-8 length are hashed from a stack buffer.</summary>
    private const int KeyStackBytes = 256;

    /// <summary>
    /// Client-name prefix of a consumer's dedicated reader multiplexer, from
    /// <c>StreamNames.ReaderClientName</c>. Writes must never go there.
    /// </summary>
    private const string ReaderClientPrefix = "streams-rd:";

    /// <summary>Record defaults, allocated once for the unconfigured-topic case.</summary>
    private static readonly TopicOptions DefaultTopicOptions = new();

    /// <summary>Round-robin cursor for publishes with no partition key; shared across topics, which only costs spread.</summary>
    private static uint roundRobin;

    /// <summary>
    /// Writes state and publishes one event in a single <c>MULTI</c>/<c>EXEC</c>.
    /// </summary>
    /// <param name="db">
    /// A database on the <b>shared</b> multiplexer — never a consumer's dedicated reader connection.
    /// </param>
    /// <param name="stateWrites">
    /// Queues the state commands onto the transaction, e.g. <c>tran =&gt; tran.HashSetAsync(key, fields)</c>.
    /// <para>
    /// <b>MUST NOT await anything.</b> A command issued on an <see cref="ITransaction"/> returns a
    /// task that does not complete until <see cref="ITransaction.ExecuteAsync"/> runs, and
    /// <see cref="ITransaction.ExecuteAsync"/> cannot run until this delegate returns — so awaiting
    /// inside it deadlocks forever. That is exactly why this is an <see cref="Action{T}"/> and not a
    /// <c>Func&lt;IDatabaseAsync, Task&gt;</c>, which would invite it.
    /// </para>
    /// <para>
    /// The type system still cannot stop you writing <c>async db =&gt; await db.StringSetAsync(...)</c>:
    /// that binds as an <c>async void</c> lambda, so this method returns at your first <c>await</c>,
    /// the state write ends up <b>outside</b> the transaction, and the task you awaited never
    /// completes. Queue commands, discard the tasks they return, return synchronously.
    /// </para>
    /// </param>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="partitionKey">The routing key; empty round-robins.</param>
    /// <param name="body">The message body. Not copied — do not mutate it until the returned task completes.</param>
    /// <param name="type">The message type string; consumers filter on it, so it is required.</param>
    /// <param name="options">Correlation id, headers and an optional explicit partition.</param>
    /// <param name="conditions">
    /// Optional <c>WATCH</c> conditions. All must hold at <c>EXEC</c> time; if any fails the
    /// transaction is abandoned, nothing is applied, and this returns <see langword="null"/>.
    /// </param>
    /// <param name="stateKeys">
    /// Optional: the keys <paramref name="stateWrites"/> touches, declared so their hash tag can be
    /// checked against the stream's before anything is sent. Nothing else can know them — the
    /// delegate writes them itself — so omitting this skips the check and leaves a cluster deployment
    /// to fail later with <c>CROSSSLOT</c>. A service that would rather not repeat the array on every
    /// call should assert its key shapes once at startup with
    /// <see cref="EnsureSameSlot(string, IReadOnlyList{RedisKey}, bool, int)"/>, which fails the pod
    /// instead of the publish.
    /// </param>
    /// <param name="topicOptions">
    /// The topic's configured options. <see langword="null"/> takes record defaults, which is only
    /// safe for a genuinely unconfigured topic: a wrong partition count mis-routes the key, and a
    /// wrong <see cref="TopicOptions.MaxLen"/> trims the stream to a ceiling that is not the topic's.
    /// </param>
    /// <param name="ct">
    /// Checked before the transaction is built. Once <c>EXEC</c> is on the wire there is nothing left
    /// to cancel, so it is not observed after that point.
    /// </param>
    /// <returns>
    /// The assigned <see cref="StreamId"/>, or <see langword="null"/> when a condition in
    /// <paramref name="conditions"/> failed — in which case <b>nothing was applied</b>, neither the
    /// state writes nor the publish. <see langword="null"/> is not a general failure signal: with no
    /// conditions nothing can produce it, and every other failure throws.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="topic"/> is null, empty or whitespace.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="PublishOptions.Partition"/> is outside the topic's partition range.</exception>
    /// <exception cref="StreamConfigurationException">A declared state key is in another hash slot, the database is a reader connection, or the topic's options are unusable.</exception>
    /// <exception cref="StreamTransportException">Redis rejected or failed the transaction.</exception>
    public static Task<StreamId?> WriteAndPublishAsync(
        IDatabase db,
        Action<IDatabaseAsync> stateWrites,
        string topic,
        string partitionKey,
        ReadOnlyMemory<byte> body,
        string type,
        PublishOptions options = default,
        IReadOnlyList<Condition>? conditions = null,
        IReadOnlyList<RedisKey>? stateKeys = null,
        TopicOptions? topicOptions = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(stateWrites);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(type);
        ct.ThrowIfCancellationRequested();

        RequireSharedMultiplexer(db);
        var effective = Validate(topic, topicOptions);

        var partition = Route(topic, partitionKey, options.Partition, effective.Partitions);
        var streamKey = StreamKeys.Stream(topic, partition, effective.CoLocatePartitions);

        RequireStateKeysInSlot(streamKey, topic, effective.CoLocatePartitions, stateKeys);

        var tran = db.CreateTransaction();
        AddConditions(tran, conditions);

        // Queue-only. Anything awaited in here can never complete: the tasks it would await are
        // released by ExecuteAsync, which is below.
        stateWrites(tran);

        var idTask = Queue(tran, streamKey, partitionKey, body, type, options, effective);

        return CommitAsync(tran, idTask, topic, partition, type);
    }

    /// <summary>
    /// Writes state and publishes several events in a single <c>MULTI</c>/<c>EXEC</c>.
    /// </summary>
    /// <param name="db">A database on the <b>shared</b> multiplexer.</param>
    /// <param name="stateWrites">
    /// Queues the state commands. <b>MUST NOT await anything</b> — see
    /// <see cref="WriteAndPublishAsync"/> for why, and for the <c>async void</c> trap.
    /// </param>
    /// <param name="publishes">
    /// The events, published in the order given. Every one must land in the same hash slot as the
    /// first, which in practice means the same topic — stream keys are tagged per topic.
    /// </param>
    /// <param name="conditions">Optional <c>WATCH</c> conditions; all must hold or nothing is applied.</param>
    /// <param name="stateKeys">
    /// Optional declared state keys, hash-tag checked up front. The stream keys are always checked
    /// against each other; the state keys can only be checked when they are declared here, or once
    /// at startup via <see cref="EnsureSameSlot(string, IReadOnlyList{RedisKey}, bool, int)"/>.
    /// </param>
    /// <param name="ct">Checked before the transaction is built.</param>
    /// <returns>
    /// The assigned ids, in the order of <paramref name="publishes"/>, or <see langword="null"/> when
    /// a condition failed and nothing was applied.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="publishes"/> is empty, or an entry names no topic or type.</exception>
    /// <exception cref="StreamConfigurationException">The publishes do not share one hash slot, or a declared state key does not share it.</exception>
    /// <exception cref="StreamTransportException">Redis rejected or failed the transaction.</exception>
    public static Task<StreamId[]?> WriteAndPublishManyAsync(
        IDatabase db,
        Action<IDatabaseAsync> stateWrites,
        ReadOnlyMemory<OutboxPublish> publishes,
        IReadOnlyList<Condition>? conditions = null,
        IReadOnlyList<RedisKey>? stateKeys = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(stateWrites);
        ct.ThrowIfCancellationRequested();

        var span = publishes.Span;
        if (span.Length == 0)
        {
            throw new ArgumentException(
                "WriteAndPublishManyAsync was given no publishes. A transaction that only writes state is just a write — issue it on the database directly.",
                nameof(publishes));
        }

        RequireSharedMultiplexer(db);

        var keys = new RedisKey[span.Length];
        var partitions = new int[span.Length];
        var topicOptions = new TopicOptions[span.Length];

        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var publish = ref span[i];
            if (string.IsNullOrWhiteSpace(publish.Topic))
            {
                throw new ArgumentException($"Outbox: publish {i} names no topic.", nameof(publishes));
            }

            if (publish.Type is null)
            {
                throw new ArgumentException($"Outbox: publish {i} to topic '{publish.Topic}' names no type; consumers filter on it.", nameof(publishes));
            }

            var effective = Validate(publish.Topic, publish.TopicOptions);
            topicOptions[i] = effective;
            partitions[i] = Route(publish.Topic, publish.PartitionKey, publish.Options.Partition, effective.Partitions);
            keys[i] = StreamKeys.Stream(publish.Topic, partitions[i], effective.CoLocatePartitions);
        }

        // One MULTI/EXEC covers one slot, so every stream key must share the first one's hash tag,
        // and so must the declared state keys.
        RequireStreamKeysInOneSlot(keys, span);
        RequireStateKeysInSlot(keys[0], span[0].Topic, topicOptions[0].CoLocatePartitions, stateKeys);

        var tran = db.CreateTransaction();
        AddConditions(tran, conditions);

        // Queue-only: see WriteAndPublishAsync.
        stateWrites(tran);

        var pending = new Task<RedisValue>[span.Length];
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var publish = ref span[i];
            pending[i] = Queue(tran, keys[i], publish.PartitionKey, publish.Body, publish.Type, publish.Options, topicOptions[i]);
        }

        return CommitManyAsync(tran, pending, publishes, partitions);
    }

    /// <summary>
    /// Builds a state key that shares a topic's hash slot: <c>{topic}:state:&lt;name&gt;</c>.
    /// </summary>
    /// <param name="topic">The topic whose slot the key must share.</param>
    /// <param name="name">The rest of the key, e.g. an entity id.</param>
    /// <returns>The tagged key.</returns>
    /// <exception cref="ArgumentException"><paramref name="topic"/> or <paramref name="name"/> is null, empty or whitespace.</exception>
    /// <exception cref="StreamConfigurationException"><paramref name="topic"/> itself contains a brace, which would corrupt the hash tag.</exception>
    public static RedisKey StateKey(string topic, string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        RequirePlainTopic(topic);

        return $"{{{topic}}}:state:{name}";
    }

    /// <summary>
    /// Checks that every state key hashes to the same Redis Cluster slot as a topic's partition
    /// stream, so an outbox transaction over them is legal. Call it at startup to fail a bad key
    /// shape early; <see cref="WriteAndPublishAsync"/> applies the same check to the keys it is told
    /// about.
    /// </summary>
    /// <remarks>
    /// The check compares hash tags rather than computed slots, so it gives the same answer on a
    /// single node as it would on a cluster — the point is to catch the mistake in development, where
    /// there are no slots to fail on. That makes it very slightly stricter than Redis: two different
    /// tags can collide onto one slot, and this still rejects them. Depending on a hash collision is
    /// not a design.
    /// </remarks>
    /// <param name="topic">The topic being published to.</param>
    /// <param name="stateKeys">The keys the state-write delegate touches; <see langword="null"/> or empty checks only the topic name.</param>
    /// <param name="coLocatePartitions">The topic's <see cref="TopicOptions.CoLocatePartitions"/>.</param>
    /// <param name="partition">
    /// The partition that will be published to. It only matters when
    /// <paramref name="coLocatePartitions"/> is <see langword="false"/>, where each partition is a
    /// slot of its own and no state key can join it.
    /// </param>
    /// <exception cref="StreamConfigurationException">A key would land in another slot, or the topic name contains a brace.</exception>
    public static void EnsureSameSlot(
        string topic,
        IReadOnlyList<RedisKey>? stateKeys,
        bool coLocatePartitions = true,
        int partition = 0)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        RequirePlainTopic(topic);

        RequireStateKeysInSlot(
            StreamKeys.Stream(topic, partition, coLocatePartitions),
            topic,
            coLocatePartitions,
            stateKeys);
    }

    /// <summary>Queues one <c>XADD</c> onto the transaction and hands back its (not yet completed) task.</summary>
    private static Task<RedisValue> Queue(
        ITransaction tran,
        RedisKey streamKey,
        string partitionKey,
        ReadOnlyMemory<byte> body,
        string type,
        in PublishOptions options,
        TopicOptions topicOptions)
    {
        var entry = EntryCodec.Encode(
            body,
            type,
            partitionKey,
            options.CorrelationId,
            CurrentTraceParent(),
            options.Headers);

        var trim = topicOptions.Trim;

        // MAXLEN rides along with the XADD exactly as the non-transactional publisher does, so the
        // two paths cannot drift on what a topic's ceiling means. Time retention is the background
        // trimmer's job; XADD cannot express it.
        return tran.StreamAddAsync(
            streamKey,
            entry,
            messageId: null,
            maxLength: trim == TrimMode.None ? null : topicOptions.MaxLen,
            useApproximateMaxLength: trim == TrimMode.Approx,
            limit: null,
            trimMode: StreamTrimMode.KeepReferences,
            flags: CommandFlags.None);
    }

    /// <summary>
    /// Runs <c>EXEC</c>, then — and only then — reads the queued command's result.
    /// </summary>
    /// <remarks>
    /// <para>
    /// R-16. The <c>streams.publish</c> span is opened here rather than in
    /// <see cref="WriteAndPublishAsync"/> because this is the first <c>async</c> frame on the path:
    /// <see cref="ActivitySource.StartActivity(string, ActivityKind)"/> assigns the
    /// <c>AsyncLocal</c> <see cref="Activity.Current"/>, and starting it in a non-<c>async</c> method
    /// would mutate the caller's execution context with no way to restore it after the
    /// <c>await</c>. The cost is that the entry's <c>p</c> traceparent still names the caller's
    /// ambient activity rather than this span — the same trace, one level up — which is the
    /// deliberate trade for not building the transaction inside an async frame and turning every
    /// synchronous argument validation into a faulted task.
    /// </para>
    /// </remarks>
    private static async Task<StreamId?> CommitAsync(
        ITransaction tran,
        Task<RedisValue> idTask,
        string topic,
        int partition,
        string type)
    {
        using var activity = StreamSpans.StartPublish(topic, partition, type);

        bool committed;
        try
        {
            committed = await tran.ExecuteAsync(CommandFlags.DemandMaster).ConfigureAwait(false);
        }
        catch (RedisException ex)
        {
            Observe(idTask);
            var failure = Failure(topic, partition, 1, ex);
            StreamSpans.Failed(activity, failure);
            throw failure;
        }

        if (!committed)
        {
            // Abandoned: SE.Redis cancels the queued command tasks, so awaiting idTask here would
            // throw a TaskCanceledException that means nothing. A failed condition is a normal
            // outcome, reported as a null id.
            Observe(idTask);
            return null;
        }

        var id = ParseId(await idTask.ConfigureAwait(false), topic, partition);
        Count(topic, partition, 1);
        StreamSpans.Published(activity, id);
        return id;
    }

    /// <inheritdoc cref="CommitAsync"/>
    private static async Task<StreamId[]?> CommitManyAsync(
        ITransaction tran,
        Task<RedisValue>[] pending,
        ReadOnlyMemory<OutboxPublish> publishes,
        int[] partitions)
    {
        // One span for the transaction, named after the first publish's topic and partition. A
        // MULTI/EXEC is one round trip covering one hash slot, so splitting it into a span per entry
        // would report N spans for one Redis operation; the count rides on
        // messaging.batch.message_count instead. The type is left off because the batch may mix them.
        using var activity = StreamSpans.StartPublish(
            publishes.Span[0].Topic, partitions[0], messageType: null, pending.Length);

        bool committed;
        try
        {
            committed = await tran.ExecuteAsync(CommandFlags.DemandMaster).ConfigureAwait(false);
        }
        catch (RedisException ex)
        {
            Observe(pending);
            var failure = Failure(publishes.Span[0].Topic, partitions[0], pending.Length, ex);
            StreamSpans.Failed(activity, failure);
            throw failure;
        }

        if (!committed)
        {
            Observe(pending);
            return null;
        }

        var ids = new StreamId[pending.Length];
        for (var i = 0; i < pending.Length; i++)
        {
            var topic = publishes.Span[i].Topic;
            var value = await pending[i].ConfigureAwait(false);

            ids[i] = ParseId(value, topic, partitions[i]);
            Count(topic, partitions[i], 1);
        }

        StreamSpans.Published(activity, ids[0]);
        return ids;
    }

    private static void AddConditions(ITransaction tran, IReadOnlyList<Condition>? conditions)
    {
        if (conditions is null)
        {
            return;
        }

        for (var i = 0; i < conditions.Count; i++)
        {
            var condition = conditions[i];
            if (condition is null)
            {
                throw new ArgumentException($"Outbox: condition {i} is null.", nameof(conditions));
            }

            // The ConditionResult is only readable after EXEC and says nothing this API does not:
            // a failed condition surfaces as a null id, since in that case nothing was applied.
            _ = tran.AddCondition(condition);
        }
    }

    /// <summary>
    /// Picks the partition: an explicit <see cref="PublishOptions.Partition"/> wins outright,
    /// otherwise a non-empty key hashes stably and an empty key round-robins. Identical to
    /// <see cref="StreamPublisher"/>'s routing, so an outbox publish and a plain publish of the same
    /// key land in the same partition and stay ordered.
    /// </summary>
    private static int Route(string topic, string? partitionKey, int? explicitPartition, int partitions)
    {
        if (explicitPartition is int chosen)
        {
            if ((uint)chosen >= (uint)partitions)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(explicitPartition),
                    chosen,
                    $"Topic '{topic}' has {partitions} partition(s), so an explicit partition must be in [0, {partitions - 1}].");
            }

            return chosen;
        }

        if (string.IsNullOrEmpty(partitionKey))
        {
            return PartitionRouter.RoundRobin(ref roundRobin, partitions);
        }

        if (Encoding.UTF8.GetMaxByteCount(partitionKey.Length) <= KeyStackBytes)
        {
            Span<byte> scratch = stackalloc byte[KeyStackBytes];
            var written = Encoding.UTF8.GetBytes(partitionKey.AsSpan(), scratch);
            return PartitionRouter.ForKey(scratch[..written], partitions);
        }

        return PartitionRouter.ForKey(Encoding.UTF8.GetBytes(partitionKey), partitions);
    }

    /// <summary>Resolves and sanity-checks a topic's options before anything is queued.</summary>
    private static TopicOptions Validate(string topic, TopicOptions? topicOptions)
    {
        RequirePlainTopic(topic);

        var effective = topicOptions ?? DefaultTopicOptions;

        if (effective.Partitions < 1)
        {
            throw new StreamConfigurationException(
                $"Streams:Topics:{topic}:Partitions is {effective.Partitions}; it must be >= 1.");
        }

        if (effective.Trim != TrimMode.None && effective.MaxLen < 1)
        {
            throw new StreamConfigurationException(
                $"Streams:Topics:{topic}:MaxLen is {effective.MaxLen} with Trim={effective.Trim}; " +
                "XADD MAXLEN 0 would empty the stream on every publish. Set MaxLen >= 1, or Trim=None.");
        }

        return effective;
    }

    /// <summary>
    /// Rejects a database on a consumer's dedicated reader multiplexer. That connection is read-only
    /// by design and may be parked in a blocking <c>XREAD</c>; a transaction issued on it would queue
    /// behind the block, and it is not where the rest of the service's writes live.
    /// </summary>
    private static void RequireSharedMultiplexer(IDatabase db)
    {
        var client = db.Multiplexer.ClientName;
        if (client is not null && client.StartsWith(ReaderClientPrefix, StringComparison.Ordinal))
        {
            throw new StreamConfigurationException(
                $"Outbox: this database belongs to a consumer's dedicated reader multiplexer (client name '{client}'), which is read-only. " +
                "A MULTI/EXEC is bound to one connection, so the outbox must run on the shared streams multiplexer — " +
                "take it from StreamsConnectionProvider.Connection.");
        }
    }

    /// <summary>Rejects a topic name whose braces would corrupt the hash tag in <c>s:{topic}:&lt;p&gt;</c>.</summary>
    private static void RequirePlainTopic(string topic)
    {
        if (topic.AsSpan().IndexOfAny('{', '}') >= 0)
        {
            throw new StreamConfigurationException(
                $"Outbox: topic '{topic}' contains a brace. Stream keys are hash-tagged as s:{{topic}}:<p>, so a brace in the name " +
                "yields a hash tag nobody intended and silently splits the topic across cluster slots. Rename the topic.");
        }
    }

    /// <summary>Requires every publish in one transaction to hash to the first one's slot.</summary>
    private static void RequireStreamKeysInOneSlot(RedisKey[] keys, ReadOnlySpan<OutboxPublish> publishes)
    {
        var reference = Text(keys[0]);
        var tag = HashTag(reference);

        for (var i = 1; i < keys.Length; i++)
        {
            var key = Text(keys[i]);
            if (HashTag(key).SequenceEqual(tag))
            {
                continue;
            }

            var sameTopic = string.Equals(publishes[0].Topic, publishes[i].Topic, StringComparison.Ordinal);

            throw new StreamConfigurationException(
                $"Outbox: publish 0 ('{publishes[0].Topic}', key '{reference}') and publish {i} ('{publishes[i].Topic}', key '{key}') " +
                "hash to different Redis Cluster slots, and one MULTI/EXEC covers one slot. " +
                (sameTopic
                    ? $"Topic '{publishes[i].Topic}' has CoLocatePartitions=false, so its partitions are deliberately spread across slots: " +
                      "publish one partition per transaction, or co-locate the topic's partitions."
                    : "Stream keys are hash-tagged per topic, so a transaction cannot span topics: publish each topic in its own transaction, " +
                      "or publish one event and let a consumer fan out."));
        }
    }

    /// <summary>Requires every declared state key to share the stream key's hash tag.</summary>
    private static void RequireStateKeysInSlot(
        RedisKey streamKey,
        string topic,
        bool coLocatePartitions,
        IReadOnlyList<RedisKey>? stateKeys)
    {
        // R-19 decision: this check is opt-in and cannot be anything else.
        //
        // The review read "declared keys are validated before anything is sent" as "the same-slot
        // guard runs on every call", and noted that both public entry points default stateKeys to
        // null. The default is right, and the guard is not the one that was missed: the guard that
        // always runs is RequireStreamKeysInOneSlot, which covers every *stream* key in a
        // WriteAndPublishManyAsync, plus RequirePlainTopic via Validate on both entry points. What
        // is opt-in is the *state* key check — and it has to be, because nothing here can know which
        // keys the delegate touches. stateWrites is an Action<IDatabaseAsync>; the keys exist only
        // inside its body, SE.Redis's ITransaction exposes no record of what was queued on it, and
        // the only way to capture them would be to hand the delegate a recording proxy implementing
        // the whole of IDatabaseAsync — several hundred members, for a check the caller can opt into
        // with one array literal.
        //
        // So the shape stands: declare the keys and get the check before anything is sent, or omit
        // them and let a cluster deployment fail with CROSSSLOT. What a service should do instead of
        // relying on the per-call default is call EnsureSameSlot once at startup with its key shapes,
        // which fails the pod rather than the publish.
        if (stateKeys is null || stateKeys.Count == 0)
        {
            return;
        }

        var reference = Text(streamKey);
        var tag = HashTag(reference);

        for (var i = 0; i < stateKeys.Count; i++)
        {
            var key = Text(stateKeys[i]);
            if (key.Length == 0)
            {
                throw new StreamConfigurationException(
                    $"Outbox: state key {i} is empty. Declare the keys the state-write delegate actually touches, or declare none at all.");
            }

            if (HashTag(key).SequenceEqual(tag))
            {
                continue;
            }

            throw new StreamConfigurationException(
                $"Outbox: state key '{key}' (hash tag '{HashTag(key)}') is not in the same Redis Cluster slot as topic '{topic}' " +
                $"(stream key '{reference}', hash tag '{tag}'), and every key in one MULTI/EXEC must hash to one slot. " +
                $"Tag the state key on the topic — Outbox.StateKey(\"{topic}\", \"…\") gives '{{{topic}}}:state:…'. " +
                (coLocatePartitions
                    ? string.Empty
                    : $"Note that topic '{topic}' has CoLocatePartitions=false, so its partition streams are spread across slots and no state key can join them. ") +
                "Where the state genuinely cannot share a slot, drop the transaction: write the state, then publish, and make the consumer idempotent.");
        }
    }

    /// <summary>
    /// The effective hash tag of a key: the text between the first <c>{</c> and the first <c>}</c>
    /// after it, or the whole key when there is no non-empty tag — Redis's own rule.
    /// </summary>
    private static ReadOnlySpan<char> HashTag(string key)
    {
        var span = key.AsSpan();

        var open = span.IndexOf('{');
        if (open < 0)
        {
            return span;
        }

        var rest = span[(open + 1)..];
        var close = rest.IndexOf('}');
        return close <= 0 ? span : rest[..close];
    }

    /// <summary>A key's text, never null, for tag comparison and messages.</summary>
    private static string Text(RedisKey key) => key.ToString() ?? string.Empty;

    /// <summary>
    /// The ambient W3C <c>traceparent</c>, so the consumer can continue this trace across the hop.
    /// Non-W3C activity ids are skipped rather than written as a header the consumer cannot parse.
    /// </summary>
    private static string? CurrentTraceParent()
    {
        var activity = Activity.Current;
        return activity is { IdFormat: ActivityIdFormat.W3C } ? activity.Id : null;
    }

    private static StreamId ParseId(RedisValue value, string topic, int partition)
    {
        var id = (string?)value;
        if (string.IsNullOrEmpty(id))
        {
            throw new StreamTransportException(
                $"The outbox transaction on topic '{topic}' partition {partition} committed, but XADD returned no entry id. " +
                "The state writes in that transaction did apply.");
        }

        return StreamId.Parse(id);
    }

    private static void Count(string topic, int partition, int count)
    {
        var tags = new TagList
        {
            { "topic", topic },
            { "partition", partition },
        };

        StreamsDiagnostics.StreamsPublished.Add(count, in tags);
    }

    private static StreamTransportException Failure(string topic, int partition, int count, Exception inner)
        => new(
            $"The outbox transaction failed: {count} message(s) to topic '{topic}' partition {partition}, plus its state writes. " +
            "MULTI/EXEC hands the whole batch to the server as one unit, so it either never ran or ran completely; retrying the call " +
            "is safe as long as the handler is idempotent. The outbox does not retry for you — a silent retry would hide the outage.",
            inner);

    /// <summary>
    /// Marks an abandoned queued task observed. When a transaction does not execute, SE.Redis
    /// completes its command tasks as cancelled or faulted; nobody awaits them on that path, and an
    /// unobserved fault would resurface later as a process-level unhandled task exception.
    /// </summary>
    private static void Observe(Task task)
    {
        if (task.IsCompleted)
        {
            _ = task.Exception;
            return;
        }

        _ = task.ContinueWith(
            static t => _ = t.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private static void Observe(Task[] tasks)
    {
        for (var i = 0; i < tasks.Length; i++)
        {
            Observe(tasks[i]);
        }
    }
}
