using System.Diagnostics;
using RedisEvents.Config;
using RedisEvents.Errors;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Producer;

/// <summary>
/// The state store: <see cref="Outbox"/> narrowed to "the state is another stream in this topic's
/// slot", with the length check that makes it an event store.
/// </summary>
/// <remarks>
/// <para>
/// The transaction it issues for a two-event append is
/// <code>
/// WATCH {topic}:state:&lt;name&gt;        (from Condition.StreamLengthEqual)
/// MULTI
///   XADD {topic}:state:&lt;name&gt; * b .. t ..          ← the state stream, no MAXLEN, ever
///   XADD {topic}:state:&lt;name&gt; * b .. t ..
///   XADD s:{topic}:&lt;p&gt; MAXLEN ~ n * b .. t .. k ..  ← the topic, trimmed like any publish
///   XADD s:{topic}:&lt;p&gt; MAXLEN ~ n * b .. t .. k ..
/// EXEC
/// </code>
/// built through <see cref="Outbox.WriteAndPublishManyAsync"/> so the publish half cannot drift from
/// the plain publisher's on routing, trimming or the wire format. Both halves carry the same body and
/// type, so the aggregate's history and the projection feed can never disagree about what an event
/// was.
/// </para>
/// <para>
/// <b>The state half is leaner than the topic half.</b> A state entry is written by
/// <see cref="EntryCodec.EncodeState"/>: no <c>k</c> — the state stream's name already is the
/// aggregate — and no correlation id, <c>traceparent</c> or headers unless
/// <see cref="TopicOptions.StateMetadata"/> is set. Loading an aggregate reads only body and type, and
/// a state stream is never trimmed, so every per-entry byte it does not need is paid for forever. The
/// topic half keeps everything: that is what projections and consumers read.
/// </para>
/// <para>
/// <b>The state appends are queued inside the outbox's <c>stateWrites</c> callback</b>, which is
/// also the only place they can be: <see cref="Outbox.WriteAndPublishManyAsync"/> returns the ids of
/// the topic publishes, and this type has to hand back the ids of the state entries — the version
/// numbers of an aggregate's own history. So it keeps the queued tasks itself and reads them after
/// the commit, exactly as the outbox reads its own.
/// </para>
/// <para>
/// <b>No topic reconcile.</b> Unlike <see cref="StreamPublisher"/>, an append does not run
/// <c>StreamAdmin.EnsureTopicAsync</c>: the outbox path never has, and a reconcile is several round
/// trips that would land inside nobody's transaction. A service whose only writer is a store should
/// call <c>StreamAdmin.EnsureTopicAsync</c> at startup to get <c>m:{topic}</c> and the
/// partition-decrease guard; a consumer host on the same topic already does.
/// </para>
/// <para>Instances are thread-safe and are intended to be registered as singletons.</para>
/// </remarks>
internal sealed class StreamStore : IStreamStore
{
    /// <summary>
    /// The partition stamped on a decoded state entry. A state stream is one key with no partitions
    /// — the concept belongs to a topic — and <see cref="StreamMsg"/> has nowhere to say "none", so
    /// it reads as zero.
    /// </summary>
    private const int StatePartition = 0;

    private readonly IDatabase database;

    /// <summary>
    /// Creates a store bound to one topic.
    /// </summary>
    /// <param name="database">
    /// The database on the <b>shared</b> multiplexer. A <c>MULTI</c>/<c>EXEC</c> is bound to one
    /// connection, and a consumer's dedicated reader connection is read-only and may be parked in a
    /// blocking <c>XREAD</c>; <see cref="Outbox"/> refuses one.
    /// </param>
    /// <param name="topic">The topic every append also publishes to.</param>
    /// <param name="topicOptions">The topic's options; <see langword="null"/> uses record defaults.</param>
    /// <exception cref="ArgumentException"><paramref name="topic"/> is null, empty or whitespace.</exception>
    /// <exception cref="StreamConfigurationException">The topic has fewer than one partition, or its partitions are not co-located.</exception>
    public StreamStore(IDatabase database, string topic, TopicOptions? topicOptions = null)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        var options = topicOptions ?? new TopicOptions();
        if (options.Partitions < 1)
        {
            throw new StreamConfigurationException(
                $"Streams:Topics:{topic}:Partitions is {options.Partitions}; it must be >= 1.");
        }

        RequireCoLocatedPartitions(topic, options);

        this.database = database;
        this.Topic = topic;
        this.Options = options;
    }

    /// <summary>The topic this store publishes to.</summary>
    public string Topic { get; }

    /// <summary>The topic's effective options, as the publishes are routed and trimmed by.</summary>
    public TopicOptions Options { get; }

    /// <summary>
    /// Refuses a topic whose partition streams are deliberately spread across cluster slots: no
    /// state key can join a transaction with them, and a state store is nothing but that
    /// transaction.
    /// </summary>
    /// <param name="topic">The topic being registered.</param>
    /// <param name="options">The topic's effective options.</param>
    /// <exception cref="StreamConfigurationException"><see cref="TopicOptions.CoLocatePartitions"/> is <see langword="false"/>.</exception>
    /// <remarks>
    /// It fails here — at registration, and again at construction — rather than at the first append,
    /// because there is no non-transactional fallback to fall back to. One would be an
    /// at-least-once-with-a-hole mode: the aggregate's history and the projection feed could
    /// disagree after a crash between the two writes, which is the single thing an event store must
    /// not do.
    /// </remarks>
    public static void RequireCoLocatedPartitions(string topic, TopicOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(options);

        if (options.CoLocatePartitions)
        {
            return;
        }

        throw new StreamConfigurationException(
            $"Streams:Topics:{topic}:CoLocatePartitions is false, so the topic's partition streams are spread across Redis Cluster " +
            $"slots and no state key can share one with them — but a state store's whole guarantee is that the append to " +
            $"'{{{topic}}}:state:<name>' and the publish to 's:{topic}:<p>' are one MULTI/EXEC. Co-locate the topic's partitions, " +
            "or use a topic that does.");
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<StreamMsg>> ReadAsync(
        string name,
        StreamId after,
        int max,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(max);
        ct.ThrowIfCancellationRequested();

        var key = Outbox.StateKey(this.Topic, name);

        // XRANGE key (<after> + COUNT max. The '(' is Redis 6.2's exclusive-range syntax, which is
        // what makes "everything after the last id I saw" one command with no id arithmetic; this
        // library already requires 7.4 for the ownership registry's HEXPIRE.
        var entries = await this.database
            .StreamRangeAsync(key, Exclusive(after), maxId: null, max, Order.Ascending)
            .ConfigureAwait(false);

        if (entries.Length == 0)
        {
            return [];
        }

        var messages = new StreamMsg[entries.Length];
        for (var i = 0; i < entries.Length; i++)
        {
            // Decode guards the codec version for us. State entries carry no version of their own —
            // it is a property of the topic's layout, recorded once in m:{topic} — so this is the
            // same "what this build writes" assumption the consumer's read loop decodes under, and
            // StreamAdmin's startup reconcile is what turns a mismatch into a startup failure.
            messages[i] = EntryCodec.Decode(entries[i], StatePartition);
        }

        return messages;
    }

    /// <inheritdoc />
    public async ValueTask<StreamId[]?> AppendAndPublishAsync(
        string name,
        long expectedLength,
        string partitionKey,
        IReadOnlyList<StateEvent> events,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentOutOfRangeException.ThrowIfNegative(expectedLength);
        ArgumentNullException.ThrowIfNull(events);

        if (events.Count == 0)
        {
            throw new ArgumentException(
                $"AppendAndPublishAsync was given no events for '{name}' on topic '{this.Topic}'. An append of nothing is not an " +
                "append: there is no state change to record and nothing to announce.",
                nameof(events));
        }

        var key = Outbox.StateKey(this.Topic, name);
        var traceParent = CurrentTraceParent();

        var publishes = new OutboxPublish[events.Count];
        for (var i = 0; i < events.Count; i++)
        {
            var e = events[i];
            if (e.Type is null)
            {
                throw new ArgumentException(
                    $"AppendAndPublishAsync: event {i} for '{name}' on topic '{this.Topic}' names no type; consumers filter on it.",
                    nameof(events));
            }

            publishes[i] = new OutboxPublish(this.Topic, partitionKey, e.Body, e.Type, e.Options, this.Options);
        }

        // Filled by the queue-only callback below, which the outbox invokes before EXEC. Entries stay
        // null if the outbox rejects the call before it gets that far.
        var appended = new Task<RedisValue>?[events.Count];

        StreamId[]? published;
        try
        {
            published = await Outbox.WriteAndPublishManyAsync(
                this.database,
                tran =>
                {
                    for (var i = 0; i < events.Count; i++)
                    {
                        var e = events[i];

                        appended[i] = tran.StreamAddAsync(
                            key,
                            this.EncodeState(e, traceParent),
                            messageId: null,

                            // No MAXLEN, and no way to ask for one: a trimmed state stream's length
                            // stops being its version and every later concurrency check silently
                            // compares against the wrong number.
                            maxLength: null,
                            useApproximateMaxLength: false,
                            limit: null,
                            trimMode: StreamTrimMode.KeepReferences,
                            flags: CommandFlags.None);
                    }
                },
                publishes,
                conditions: [Condition.StreamLengthEqual(key, expectedLength)],
                stateKeys: [key],
                ct).ConfigureAwait(false);
        }
        catch
        {
            Observe(appended);
            throw;
        }

        if (published is null)
        {
            // The length check failed: nothing was applied, and SE.Redis cancelled the queued tasks,
            // so there is nothing here to read — only to observe.
            Observe(appended);
            return null;
        }

        var ids = new StreamId[appended.Length];
        for (var i = 0; i < appended.Length; i++)
        {
            var value = await appended[i]!.ConfigureAwait(false);
            ids[i] = ParseId(value, name, i);
        }

        return ids;
    }

    /// <inheritdoc />
    public async ValueTask<StreamId[]> AppendAndPublishAsync(
        string name,
        string partitionKey,
        IReadOnlyList<StateEvent> events,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(events);

        if (events.Count == 0)
        {
            throw new ArgumentException(
                $"AppendAndPublishAsync was given no events for '{name}' on topic '{this.Topic}'. An append of nothing is not an " +
                "append: there is no state change to record and nothing to announce.",
                nameof(events));
        }

        var key = Outbox.StateKey(this.Topic, name);
        var traceParent = CurrentTraceParent();

        var publishes = new OutboxPublish[events.Count];
        for (var i = 0; i < events.Count; i++)
        {
            var e = events[i];
            if (e.Type is null)
            {
                throw new ArgumentException(
                    $"AppendAndPublishAsync: event {i} for '{name}' on topic '{this.Topic}' names no type; consumers filter on it.",
                    nameof(events));
            }

            publishes[i] = new OutboxPublish(this.Topic, partitionKey, e.Body, e.Type, e.Options, this.Options);
        }

        // Filled by the queue-only callback below, which the outbox invokes before EXEC.
        var appended = new Task<RedisValue>?[events.Count];

        // No conditions: no WATCH is issued, so this MULTI/EXEC cannot be abandoned by a lost
        // version check — WriteAndPublishManyAsync only ever returns null when a condition fails,
        // and there are none here, so the result is never null and there is no conflict outcome to
        // observe for.
        _ = await Outbox.WriteAndPublishManyAsync(
            this.database,
            tran =>
            {
                for (var i = 0; i < events.Count; i++)
                {
                    var e = events[i];

                    appended[i] = tran.StreamAddAsync(
                        key,
                        this.EncodeState(e, traceParent),
                        messageId: null,
                        maxLength: null,
                        useApproximateMaxLength: false,
                        limit: null,
                        trimMode: StreamTrimMode.KeepReferences,
                        flags: CommandFlags.None);
                }
            },
            publishes,
            conditions: null,
            stateKeys: [key],
            ct).ConfigureAwait(false);

        var ids = new StreamId[appended.Length];
        for (var i = 0; i < appended.Length; i++)
        {
            var value = await appended[i]!.ConfigureAwait(false);
            ids[i] = ParseId(value, name, i);
        }

        return ids;
    }

    /// <summary>
    /// The state-stream copy of one event: body and type, plus the event's metadata only when
    /// <see cref="TopicOptions.StateMetadata"/> asks for it. Never the partition key.
    /// </summary>
    private NameValueEntry[] EncodeState(in StateEvent e, string? traceParent)
        => this.Options.StateMetadata
            ? EntryCodec.EncodeState(e.Body, e.Type, e.Options.CorrelationId, traceParent, e.Options.Headers)
            : EntryCodec.EncodeState(e.Body, e.Type);

    /// <summary>The exclusive-range form of an id: <c>(&lt;ms&gt;-&lt;seq&gt;</c>.</summary>
    private static RedisValue Exclusive(StreamId after) => "(" + after.Format();

    /// <summary>
    /// The ambient W3C <c>traceparent</c>, stamped on the topic copy of every event and on the state
    /// copy when <see cref="TopicOptions.StateMetadata"/> is set. Non-W3C activity ids are skipped rather than written
    /// as a header nobody can parse — the same rule <see cref="Outbox"/> applies to the publish half.
    /// </summary>
    private static string? CurrentTraceParent()
    {
        var activity = Activity.Current;
        return activity is { IdFormat: ActivityIdFormat.W3C } ? activity.Id : null;
    }

    private static StreamId ParseId(RedisValue value, string name, int index)
    {
        var id = (string?)value;
        if (string.IsNullOrEmpty(id))
        {
            throw new StreamTransportException(
                $"The state append for '{name}' committed, but XADD {index} returned no entry id. The transaction did apply: the " +
                "events are on the state stream and on the topic, and only their ids are lost.");
        }

        return StreamId.Parse(id);
    }

    /// <summary>
    /// Marks the abandoned queued appends observed. When a transaction does not execute, SE.Redis
    /// completes its command tasks as cancelled or faulted; nobody awaits them on that path, and an
    /// unobserved fault would resurface later as a process-level unhandled task exception.
    /// </summary>
    private static void Observe(Task<RedisValue>?[] tasks)
    {
        for (var i = 0; i < tasks.Length; i++)
        {
            if (tasks[i] is not Task task)
            {
                continue;
            }

            if (task.IsCompleted)
            {
                _ = task.Exception;
                continue;
            }

            _ = task.ContinueWith(
                static t => _ = t.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }
}
