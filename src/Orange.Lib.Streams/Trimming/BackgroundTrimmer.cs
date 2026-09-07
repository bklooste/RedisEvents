using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Diagnostics;
using Orange.Lib.Streams.Errors;
using Orange.Lib.Streams.Extensions;
using Orange.Lib.Streams.Positions;
using Orange.Lib.Streams.Wire;
using StackExchange.Redis;

namespace Orange.Lib.Streams.Trimming;

/// <summary>
/// What one sweep did to one partition.
/// </summary>
/// <param name="Partition">The partition.</param>
/// <param name="Length">The partition's <c>XLEN</c> at the moment the clamp was evaluated, or
/// <c>-1</c> when no clamp was in force and the length was therefore never read.</param>
/// <param name="Cutoff">The id retention alone asked for — <c>now - RetentionSeconds</c>.</param>
/// <param name="Clamp">The lowest position any consumer has recorded for this partition, or
/// <see cref="StreamId.Min"/> when a registered consumer has never recorded one.</param>
/// <param name="MinId">
/// The <c>MINID</c> actually sent, after clamping. When <see cref="PartitionTrimResult.ClampReleased"/>
/// is <see langword="true"/> the release is a <em>length</em>-derived trim rather than a <c>MINID</c>
/// one (see <see cref="BackgroundTrimmer"/>), so this reports the id of the oldest entry that
/// survived it — the floor now in force — not the retention cutoff.
/// </param>
/// <param name="Removed">Entries <c>XTRIM</c> reported deleted. Approximate trimming deletes whole
/// macro nodes, so this is not the count below <see cref="MinId"/>.</param>
/// <param name="HeldBy">The consumer whose stored position held the clamp back, or
/// <see langword="null"/> when retention was applied in full.</param>
/// <param name="ClampReleased">
/// <see langword="true"/> when the clamp was overridden to protect memory and
/// <see cref="HeldBy"/>'s unprocessed backlog on this partition was deliberately deleted.
/// </param>
internal readonly record struct PartitionTrimResult(
    int Partition,
    long Length,
    StreamId Cutoff,
    StreamId Clamp,
    StreamId MinId,
    long Removed,
    string? HeldBy,
    bool ClampReleased);

/// <summary>
/// What one sweep did to one topic.
/// </summary>
/// <param name="Topic">The topic swept.</param>
/// <param name="CutoffUtc">The retention cutoff the sweep was computed against.</param>
/// <param name="Partitions">One result per partition, ascending.</param>
internal sealed record TopicTrimResult(
    string Topic,
    DateTimeOffset CutoffUtc,
    IReadOnlyList<PartitionTrimResult> Partitions)
{
    /// <summary>Total entries <c>XTRIM</c> reported deleted across every partition.</summary>
    public long Removed
    {
        get
        {
            var total = 0L;
            for (var i = 0; i < this.Partitions.Count; i++)
            {
                total += this.Partitions[i].Removed;
            }

            return total;
        }
    }

    /// <summary>Whether any partition's clamp was released, sacrificing a consumer's backlog.</summary>
    public bool AnyClampReleased
    {
        get
        {
            for (var i = 0; i < this.Partitions.Count; i++)
            {
                if (this.Partitions[i].ClampReleased)
                {
                    return true;
                }
            }

            return false;
        }
    }
}

/// <summary>
/// The optional <see cref="IHostedService"/> that applies <b>time</b>-based retention:
/// <c>XTRIM s:{topic}:&lt;p&gt; MINID ~ &lt;now - RetentionSeconds&gt;</c>, on every topic whose
/// <see cref="TopicOptions.BackgroundTrimIntervalSeconds"/> is greater than zero.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why it exists at all.</b> <c>XADD … MAXLEN</c> — the inline trim every publish carries — can
/// only express "keep N entries". "Keep six hours" cannot be said on <c>XADD</c>, so it is said
/// here, on a timer. The two mechanisms coexist: <c>MAXLEN</c> is the memory bound and is always in
/// force, this is the retention policy on top of it.
/// </para>
/// <para>
/// <b>The safety interlock.</b> Trimming past a consumer's stored position deletes messages it has
/// not processed, silently. So each sweep reads every <c>p:{topic}:*</c> positions hash and clamps
/// the <c>MINID</c> it sends to the lowest position recorded for that partition, logging a Warning
/// while it is being held back. Inline <c>MAXLEN</c> trimming <em>cannot</em> do this — it is a
/// blind rolling window, which is exactly why <see cref="TopicOptions.MaxLen"/> must exceed
/// worst-case consumer lag.
/// </para>
/// <para>
/// <b>The clamp yields, and this is the part not to "fix".</b> The clamp is itself an OOM path: a
/// blocked, crashed or simply forgotten consumer holds it forever while producers keep appending,
/// and the <c>DontIgnoreException</c> blocking-retry policy makes that <em>more</em> likely by
/// design — a partition retrying a dead downstream holds its position indefinitely, on purpose. So
/// when honouring the clamp would keep a partition at or above
/// <see cref="TopicOptions.ClampReleaseThreshold"/> of <see cref="TopicOptions.MaxLen"/> (default
/// 0.8), this trims <b>anyway</b>, logs at Error naming the consumer and partition whose backlog is
/// being sacrificed, and increments <c>streams.clamp.released</c>.
/// </para>
/// <para>
/// <b>A released clamp trims by length, not by the retention cutoff.</b> This is the part that makes
/// the valve actually free memory, and it is not obvious. The volume case the valve exists for is a
/// consumer stuck since yesterday on a topic doing thousands of entries a minute: its stored position
/// is hours old — behind the cutoff, so the clamp engages — while every entry still <em>in</em> the
/// stream was written in the last few minutes and is therefore <b>newer</b> than the cutoff. Trimming
/// to <c>MINID &lt;cutoff&gt;</c> there deletes nothing at all, and the pre-remediation code did
/// exactly that while reporting a release and logging an Error on every single sweep. So the release
/// falls back to a length-derived floor: it keeps
/// <c>min(length - MaxLen x (1 - ClampReleaseThreshold), ClampReleaseThreshold x MaxLen)</c> entries,
/// which drops at least the configured slice on the first sweep and converges straight to the
/// threshold when the partition is further over. The <c>MINID</c> retention trim still runs first —
/// it is the retention policy, and it removes more whenever genuinely old entries are present.
/// </para>
/// <para>
/// <b>The Error and the metric fire once per transition</b> into the released state, not once per
/// sweep. A partition that stays released for a day is one alertable event, not 2 880 of them; the
/// return to a honoured clamp is logged at Information.
/// </para>
/// <para>
/// Losing one stuck consumer's backlog is strictly better than OOM-killing Redis and losing
/// everyone's — every stream, every position and every other service sharing the instance. That
/// trade is deliberate. An implementation that "hardens" this into an unconditional clamp turns a
/// stuck consumer into an unbounded memory leak; if the release is firing, fix the consumer or raise
/// <c>MaxLen</c>, do not remove the valve.
/// </para>
/// <para>
/// <b>What the clamp is computed from.</b> Only recorded positions. A topic with no
/// <c>p:{topic}:*</c> hash at all has nothing claiming to read it, so retention applies in full. A
/// consumer whose hash exists but carries no field for a partition has processed <em>nothing</em>
/// there, so it clamps that partition to <see cref="StreamId.Min"/> — held completely, until the
/// release threshold says otherwise.
/// </para>
/// <para>
/// A sweep never throws: transport failures are logged and the next tick retries. Trimming is
/// housekeeping, and a failed sweep costs memory, not correctness.
/// </para>
/// </remarks>
internal sealed class BackgroundTrimmer : IHostedService, IAsyncDisposable
{
    private const string XTrimCommand = "XTRIM";

    /// <summary>Positions keys are scanned in pages of this size, so a big keyspace does not block Redis.</summary>
    private const int ScanPageSize = 100;

    /// <summary>Length sentinel meaning "no clamp was in force, so XLEN was never read".</summary>
    private const long LengthNotRead = -1;

    private readonly StreamOptions options;
    private readonly Func<IConnectionMultiplexer> resolve;
    private readonly ILogger log;
    private readonly string[] topics;

    /// <summary>
    /// The partitions currently in the released state, so the Error log and
    /// <c>streams.clamp.released</c> fire once per transition into it rather than once per sweep.
    /// Used as a set; the value is a filler. Concurrent because one loop runs per topic and
    /// <see cref="TrimTopicOnceAsync"/> can be called alongside them.
    /// </summary>
    private readonly ConcurrentDictionary<(string Topic, int Partition), byte> releasedPartitions = new();

    private CancellationTokenSource? cts;
    private Task[]? loops;
    private bool disposed;

    /// <summary>
    /// Creates a trimmer over the shared multiplexer provider, resolving the connection lazily on
    /// <see cref="StartAsync"/> rather than at container-build time.
    /// </summary>
    /// <param name="options">The bound root options; every topic with a background trim interval is swept.</param>
    /// <param name="connection">The shared multiplexer provider. Trimming is admin work and never runs on a reader connection.</param>
    /// <param name="logger">Logger for the held-back Warning and the release Error.</param>
    public BackgroundTrimmer(StreamOptions options, StreamsConnectionProvider connection, ILogger? logger = null)
        : this(options, ResolverFor(connection), logger)
    {
    }

    /// <summary>
    /// Creates a trimmer over an existing multiplexer.
    /// </summary>
    /// <param name="options">The bound root options; every topic with a background trim interval is swept.</param>
    /// <param name="redis">The shared multiplexer.</param>
    /// <param name="logger">Logger for the held-back Warning and the release Error.</param>
    public BackgroundTrimmer(StreamOptions options, IConnectionMultiplexer redis, ILogger? logger = null)
        : this(options, ResolverFor(redis), logger)
    {
    }

    private BackgroundTrimmer(StreamOptions options, Func<IConnectionMultiplexer> resolve, ILogger? logger)
    {
        ArgumentNullException.ThrowIfNull(options);

        this.options = options;
        this.resolve = resolve;
        this.log = logger ?? NullLogger.Instance;
        this.topics = EnabledTopics(options);
    }

    /// <summary>
    /// The topics this trimmer sweeps, ascending by configuration order. Empty when no topic opts in,
    /// in which case <see cref="StartAsync"/> starts nothing.
    /// </summary>
    public IReadOnlyList<string> Topics => this.topics;

    /// <summary>Whether the trimmer has been started and not yet stopped.</summary>
    public bool IsRunning => this.cts is not null;

    /// <summary>
    /// Whether a topic opts into background trimming: an interval above zero, a retention window to
    /// apply, and a trim mode that permits deletion.
    /// </summary>
    /// <param name="topic">The topic's options.</param>
    /// <returns><see langword="true"/> when a sweep would do something.</returns>
    public static bool IsEnabled(TopicOptions topic)
    {
        ArgumentNullException.ThrowIfNull(topic);

        return topic.BackgroundTrimIntervalSeconds > 0
            && topic.RetentionSeconds is > 0
            && topic.Trim != TrimMode.None;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);

        if (this.cts is not null)
        {
            return Task.CompletedTask;
        }

        this.WarnAboutIneffectiveConfiguration();

        if (this.topics.Length == 0)
        {
            return Task.CompletedTask;
        }

        this.cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var started = new Task[this.topics.Length];
        for (var i = 0; i < this.topics.Length; i++)
        {
            var topic = this.topics[i];
            var topicOptions = StreamConfigBinder.ResolveTopic(this.options, topic, this.log);

            this.log.LogInformation(
                "Streams: background trimmer on topic {Topic} — retention={RetentionSeconds}s interval={IntervalSeconds}s " +
                "maxLen={MaxLen} trim={Trim} clampRelease={ClampReleaseThreshold} (releases at {ReleaseAt} entries). " +
                "It applies time-based retention that XADD MAXLEN cannot express, clamped to the slowest stored consumer position.",
                topic,
                topicOptions.RetentionSeconds,
                topicOptions.BackgroundTrimIntervalSeconds,
                topicOptions.MaxLen,
                topicOptions.Trim,
                topicOptions.ClampReleaseThreshold,
                ReleaseAt(topicOptions));

            started[i] = this.LoopAsync(topic, topicOptions, this.cts.Token);
        }

        this.loops = started;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        var source = this.cts;
        if (source is null)
        {
            return;
        }

        await source.CancelAsync().ConfigureAwait(false);

        var running = this.loops;
        if (running is not null)
        {
            // The loops swallow cancellation, so this completes rather than faults. A sweep already
            // in flight finishes its round trip; there is nothing to drain beyond that.
            await Task.WhenAll(running).WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        this.loops = null;
        this.cts = null;
        source.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (this.disposed)
        {
            return;
        }

        this.disposed = true;

        try
        {
            await this.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Nothing left to do on the way out.
        }
    }

    /// <summary>
    /// Runs one sweep of one topic immediately, on the caller's thread of control, and reports what
    /// it did. This is the same code path the timer runs — it exists so trimming can be driven from a
    /// test or an admin call rather than only by the clock.
    /// </summary>
    /// <param name="topic">The topic to sweep. It need not be one of <see cref="Topics"/>.</param>
    /// <param name="ct">Cancellation token, observed between round trips.</param>
    /// <returns>The per-partition outcome.</returns>
    /// <exception cref="StreamConfigurationException">The topic has no <see cref="TopicOptions.RetentionSeconds"/>, so there is no cutoff to trim to.</exception>
    /// <exception cref="RedisException">A round trip failed. The timer loop swallows these; a direct caller does not.</exception>
    public Task<TopicTrimResult> TrimTopicOnceAsync(string topic, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        var topicOptions = StreamConfigBinder.ResolveTopic(this.options, topic, this.log);

        if (topicOptions.RetentionSeconds is not > 0)
        {
            // Without a retention window the cutoff would be "now", which trims the entire stream.
            // Refusing is the only safe reading of a request this ambiguous.
            throw new StreamConfigurationException(
                $"{StreamConfigBinder.SectionName}:Topics:{topic}:RetentionSeconds is not set, so there is no time window to trim to. " +
                "The background trimmer applies time-based retention only; length-based trimming rides on every XADD.");
        }

        return this.TrimAsync(topic, topicOptions, ct);
    }

    private static Func<IConnectionMultiplexer> ResolverFor(StreamsConnectionProvider connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        return () => connection.Connection;
    }

    private static Func<IConnectionMultiplexer> ResolverFor(IConnectionMultiplexer redis)
    {
        ArgumentNullException.ThrowIfNull(redis);
        return () => redis;
    }

    private static string[] EnabledTopics(StreamOptions options)
    {
        var enabled = new List<string>();
        foreach (var (name, topic) in options.Topics)
        {
            if (topic is not null && IsEnabled(topic))
            {
                enabled.Add(name);
            }
        }

        return [.. enabled];
    }

    /// <summary>
    /// The length at or above which the clamp is released. A threshold at or below zero releases
    /// unconditionally; one at or above one releases only at the <c>MaxLen</c> ceiling itself.
    /// </summary>
    /// <param name="topic">The topic's options.</param>
    /// <returns>The stream length that triggers a release.</returns>
    private static long ReleaseAt(TopicOptions topic)
    {
        var threshold = topic.ClampReleaseThreshold;
        if (double.IsNaN(threshold) || threshold <= 0)
        {
            return 0;
        }

        var at = (long)(topic.MaxLen * Math.Min(threshold, 1d));
        return at < 1 ? 1 : at;
    }

    /// <summary>The retention cutoff: entries older than this are candidates for deletion.</summary>
    /// <param name="topic">The topic's options.</param>
    /// <param name="now">The current time.</param>
    /// <returns>The cutoff instant, floored at the unix epoch.</returns>
    private static DateTimeOffset CutoffFor(TopicOptions topic, DateTimeOffset now)
    {
        var retention = topic.RetentionSeconds ?? 0;
        var cutoff = now.AddSeconds(-retention);
        return cutoff.ToUnixTimeMilliseconds() < 0 ? DateTimeOffset.UnixEpoch : cutoff;
    }

    private async Task LoopAsync(string topic, TopicOptions topicOptions, CancellationToken ct)
    {
        // Yield first so StartAsync returns to the generic host without waiting on a round trip.
        await Task.Yield();

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(topicOptions.BackgroundTrimIntervalSeconds));

        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            {
                try
                {
                    _ = await this.TrimAsync(topic, topicOptions, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    // Housekeeping: a failed sweep costs memory, never correctness, so the loop must
                    // outlive it. The next tick retries; the sustained failure shows up as a stream
                    // that stops shrinking, which the Redis memory alert catches.
                    this.log.LogError(
                        ex,
                        "Streams: background trim sweep of topic {Topic} failed; retrying in {IntervalSeconds}s. " +
                        "Time-based retention is not being applied to this topic until it succeeds, so the stream is held by MaxLen alone.",
                        topic,
                        topicOptions.BackgroundTrimIntervalSeconds);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
    }

    private async Task<TopicTrimResult> TrimAsync(string topic, TopicOptions topicOptions, CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        var cutoffUtc = CutoffFor(topicOptions, now);
        var cutoff = StreamId.FromDate(cutoffUtc);

        var redis = this.resolve();
        var db = redis.GetDatabase();

        // One scan for the whole topic: every consumer's positions, keyed by consumer name.
        var positions = await this.ReadPositionsAsync(redis, db, topic, ct).ConfigureAwait(false);

        var results = new PartitionTrimResult[topicOptions.Partitions];
        for (var partition = 0; partition < topicOptions.Partitions; partition++)
        {
            ct.ThrowIfCancellationRequested();
            results[partition] = await this
                .TrimPartitionAsync(db, topic, topicOptions, partition, cutoff, positions)
                .ConfigureAwait(false);
        }

        return new TopicTrimResult(topic, cutoffUtc, results);
    }

    private async Task<PartitionTrimResult> TrimPartitionAsync(
        IDatabase db,
        string topic,
        TopicOptions topicOptions,
        int partition,
        StreamId cutoff,
        List<ConsumerPositions> positions)
    {
        var key = StreamKeys.Stream(topic, partition, topicOptions.CoLocatePartitions);
        var (clamp, holder) = Clamp(positions, partition);

        // The clamp only matters when it is *behind* what retention asked for. A healthy consumer
        // sits near the tail, far ahead of the cutoff, and costs this sweep nothing at all.
        var held = holder is not null && clamp < cutoff;
        var minId = held ? clamp : cutoff;
        var length = LengthNotRead;
        var released = false;
        var releaseTo = 0L;

        if (held)
        {
            length = await db.StreamLengthAsync(key).ConfigureAwait(false);
            var releaseAt = ReleaseAt(topicOptions);

            if (length >= releaseAt)
            {
                // THE VALVE. Honouring the clamp here would let one stuck consumer hold the stream
                // above the memory ceiling indefinitely, taking down every other stream, position and
                // service on this Redis with it. So the clamp yields: this deletes the sacrificed
                // consumer's unprocessed backlog on purpose. Do not "fix" it into an unconditional
                // clamp — that converts a stuck consumer into an unbounded memory leak.
                //
                // The MINID below is still the retention cutoff, because retention is still policy and
                // still removes whatever is genuinely older than it. But it is no longer what frees the
                // memory: in the volume case the valve exists for, every surviving entry is NEWER than
                // the cutoff and MINID removes nothing. ReleaseTarget is the length-derived floor that
                // does the freeing, applied after the MINID trim below.
                minId = cutoff;
                released = true;
                releaseTo = ReleaseTarget(topicOptions, releaseAt, length);

                this.ReportRelease(topic, topicOptions, partition, holder!, clamp, cutoff, length, releaseAt, releaseTo);
            }
            else
            {
                this.ReportReleaseEnded(topic, partition);

                if (length > 0)
                {
                    this.log.LogWarning(
                        "Streams: trim of topic {Topic} partition {Partition} is held back by consumer {Consumer} at {Position} " +
                        "(retention alone would trim to {Cutoff}). The partition holds {Length} entries; the clamp is released at {ReleaseAt}, " +
                        "above which that backlog is dropped to protect memory.",
                        topic,
                        partition,
                        holder,
                        clamp.Format(),
                        cutoff.Format(),
                        length,
                        releaseAt);
                }
            }
        }
        else
        {
            this.ReportReleaseEnded(topic, partition);
        }

        var removed = minId > StreamId.Min
            ? await this.ExecuteTrimAsync(db, key, topicOptions.Trim, minId).ConfigureAwait(false)
            : 0L;

        if (released)
        {
            // The half that actually frees memory. See the class remarks: MINID cutoff above can and
            // routinely does remove nothing, because a stuck consumer's stored position is old while
            // the entries still in the stream are new.
            removed += await this.ExecuteMaxLenTrimAsync(db, key, topicOptions.Trim, releaseTo).ConfigureAwait(false);

            // Report the floor that is now in force rather than the cutoff, which under a release is
            // not what the trim was driven by. One entry, read only on this path.
            var head = await HeadIdAsync(db, key).ConfigureAwait(false);
            if (head is { } surviving)
            {
                minId = surviving;
            }
        }

        return new PartitionTrimResult(partition, length, cutoff, clamp, minId, removed, holder, released);
    }

    /// <summary>
    /// The number of entries a released partition is trimmed down to: at least
    /// <c>MaxLen x (1 - ClampReleaseThreshold)</c> entries are dropped, and a partition that is
    /// further over the line goes straight back to the release threshold rather than shedding one
    /// slice per sweep.
    /// </summary>
    /// <param name="topic">The topic's options.</param>
    /// <param name="releaseAt">The length at which the clamp releases, from <see cref="ReleaseAt"/>.</param>
    /// <param name="length">The partition's current length.</param>
    /// <returns>The target length, never below one.</returns>
    private static long ReleaseTarget(TopicOptions topic, long releaseAt, long length)
    {
        var slice = topic.MaxLen - releaseAt;
        if (slice < 1)
        {
            // A threshold at or above 1 releases only at the ceiling itself; still free something,
            // or a "release" that keeps every entry would report a sacrifice it never made.
            slice = 1;
        }

        var target = Math.Min(length - slice, releaseAt);
        return target < 1 ? 1 : target;
    }

    /// <summary>
    /// The id of the oldest entry still in a partition, via <c>XRANGE key - + COUNT 1</c>.
    /// </summary>
    /// <param name="db">The database.</param>
    /// <param name="key">The partition's stream key.</param>
    /// <returns>The head id, or <see langword="null"/> when the partition is empty.</returns>
    private static async Task<StreamId?> HeadIdAsync(IDatabase db, RedisKey key)
    {
        var head = await db.StreamRangeAsync(key, null, null, 1, Order.Ascending).ConfigureAwait(false);
        if (head.Length == 0)
        {
            return null;
        }

        var id = head[0].Id.ToString();
        return string.IsNullOrEmpty(id) ? null : StreamId.Parse(id);
    }

    /// <summary>
    /// Reports a clamp release, but only on the transition into it. A partition that stays released
    /// for a day must not raise 2 880 identical Errors and 2 880 increments of a counter an alert is
    /// hung off; the alert wants "this started", not "this is still true".
    /// </summary>
    private void ReportRelease(
        string topic,
        TopicOptions topicOptions,
        int partition,
        string holder,
        StreamId clamp,
        StreamId cutoff,
        long length,
        long releaseAt,
        long releaseTo)
    {
        if (!this.releasedPartitions.TryAdd((topic, partition), 0))
        {
            this.log.LogDebug(
                "Streams: trim clamp on topic {Topic} partition {Partition} is still released; consumer {Consumer} is still stuck at {Position}. " +
                "Trimming to {ReleaseTo} entries.",
                topic,
                partition,
                holder,
                clamp.Format(),
                releaseTo);
            return;
        }

        var tags = new TagList
        {
            { "topic", topic },
            { "partition", partition },
            { "consumer", holder },
        };

        StreamsDiagnostics.StreamsClampReleased.Add(1, in tags);

        this.log.LogError(
            "Streams: trim clamp RELEASED on topic {Topic} partition {Partition} — consumer {Consumer} is stuck at {Position} " +
            "and the partition holds {Length} entries, at or above {ReleaseAt} ({ClampReleaseThreshold:P0} of MaxLen {MaxLen}). " +
            "Trimming to the retention cutoff {MinId} and then down to {ReleaseTo} entries by length: that consumer's unprocessed " +
            "backlog on this partition is being deleted deliberately, because holding it would run Redis out of memory and lose " +
            "every stream, position and service on this instance. Fix or restart the consumer, or raise MaxLen for this topic. " +
            "This is logged once per release, not once per sweep.",
            topic,
            partition,
            holder,
            clamp.Format(),
            length,
            releaseAt,
            topicOptions.ClampReleaseThreshold,
            topicOptions.MaxLen,
            cutoff.Format(),
            releaseTo);
    }

    /// <summary>
    /// Clears the released state for a partition, logging the recovery once. Called on every sweep
    /// that did not release, so the next release is reported as a fresh transition.
    /// </summary>
    private void ReportReleaseEnded(string topic, int partition)
    {
        if (this.releasedPartitions.TryRemove((topic, partition), out _))
        {
            this.log.LogInformation(
                "Streams: trim clamp release on topic {Topic} partition {Partition} has ended; the clamp is being honoured again.",
                topic,
                partition);
        }
    }

    private async Task<long> ExecuteTrimAsync(IDatabase db, RedisKey key, TrimMode trim, StreamId minId)
    {
        // Shared with the producer's inline MAXLEN clause so the two trim paths cannot drift on the
        // wire. TrimMode.None yields no arguments at all, which is the "never delete" contract.
        var trimArgs = TrimArgs.GetMinIdArgs(trim, minId);
        if (trimArgs.Length == 0)
        {
            return 0;
        }

        var args = new object[1 + trimArgs.Length];
        args[0] = key;
        for (var i = 0; i < trimArgs.Length; i++)
        {
            args[i + 1] = trimArgs[i];
        }

        var result = await db.ExecuteAsync(XTrimCommand, args, CommandFlags.DemandMaster).ConfigureAwait(false);
        return result.IsNull ? 0L : (long)result;
    }

    /// <summary>
    /// The length-derived half of a clamp release: <c>XTRIM key MAXLEN [~] target</c>. It is the same
    /// clause the producer rides inline, issued here with a lower ceiling so a released partition
    /// actually shrinks — a <c>MINID</c> at the retention cutoff cannot, once every surviving entry
    /// is newer than that cutoff.
    /// </summary>
    /// <param name="db">The database.</param>
    /// <param name="key">The partition's stream key.</param>
    /// <param name="trim">The trim mode; <see cref="TrimMode.None"/> deletes nothing.</param>
    /// <param name="target">The number of entries to keep.</param>
    /// <returns>Entries <c>XTRIM</c> reported deleted.</returns>
    private async Task<long> ExecuteMaxLenTrimAsync(IDatabase db, RedisKey key, TrimMode trim, long target)
    {
        var trimArgs = TrimArgs.GetMaxLenArgs(trim, target);
        if (trimArgs.Length == 0)
        {
            return 0;
        }

        var args = new object[1 + trimArgs.Length];
        args[0] = key;
        for (var i = 0; i < trimArgs.Length; i++)
        {
            args[i + 1] = trimArgs[i];
        }

        var result = await db.ExecuteAsync(XTrimCommand, args, CommandFlags.DemandMaster).ConfigureAwait(false);
        return result.IsNull ? 0L : (long)result;
    }

    /// <summary>
    /// The lowest position any consumer has recorded for one partition, and who recorded it.
    /// </summary>
    /// <param name="positions">Every consumer's positions hash for the topic.</param>
    /// <param name="partition">The partition.</param>
    /// <returns>The clamp and the consumer holding it, or <see cref="StreamId.Min"/> and
    /// <see langword="null"/> when nothing claims to read the topic.</returns>
    private static (StreamId Clamp, string? Holder) Clamp(List<ConsumerPositions> positions, int partition)
    {
        var clamp = StreamId.Max;
        string? holder = null;

        for (var i = 0; i < positions.Count; i++)
        {
            var consumer = positions[i];

            // A consumer with a hash but no field for this partition has processed nothing here, so
            // everything on the partition is unprocessed: it clamps to the floor. That is the strict
            // reading, and the release valve above is what stops it holding forever.
            var recorded = consumer.Positions.TryGetValue(partition, out var record) ? record.Id : StreamId.Min;

            if (recorded < clamp)
            {
                clamp = recorded;
                holder = consumer.Consumer;
            }
        }

        return holder is null ? (StreamId.Min, null) : (clamp, holder);
    }

    /// <summary>
    /// Scans <c>p:{topic}:*</c> and reads every consumer's positions hash.
    /// </summary>
    /// <param name="redis">The multiplexer, for the servers to scan.</param>
    /// <param name="db">The database the hashes live in.</param>
    /// <param name="topic">The topic.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>One entry per consumer that has a positions hash; empty when nothing reads the topic.</returns>
    private async Task<List<ConsumerPositions>> ReadPositionsAsync(
        IConnectionMultiplexer redis,
        IDatabase db,
        string topic,
        CancellationToken ct)
    {
        var pattern = StreamKeys.PositionsPattern(topic);
        var prefix = pattern[..^1];
        const string metaSuffix = ":meta";

        var names = new HashSet<string>(StringComparer.Ordinal);
        var scanned = false;

        foreach (var endpoint in redis.GetEndPoints())
        {
            ct.ThrowIfCancellationRequested();

            var server = redis.GetServer(endpoint);
            if (!server.IsConnected || server.IsReplica)
            {
                continue;
            }

            scanned = true;

            // Every key of a topic carries the same hash tag, so in a cluster they all live on one
            // node — but which node is not known here, so every master is scanned and the names are
            // unioned.
            await foreach (var key in server
                .KeysAsync(db.Database, pattern, ScanPageSize)
                .WithCancellation(ct)
                .ConfigureAwait(false))
            {
                var text = key.ToString();
                if (text.Length <= prefix.Length || text.EndsWith(metaSuffix, StringComparison.Ordinal))
                {
                    continue;
                }

                names.Add(text[prefix.Length..]);
            }
        }

        if (!scanned)
        {
            // No master to scan means the positions are unknown, and an unknown position must not be
            // read as "nothing to protect". Reporting one unnamed holder clamps every partition to the
            // floor, which the release valve still overrides once memory is at stake.
            this.log.LogWarning(
                "Streams: background trim of topic {Topic} found no connected Redis master to scan for consumer positions, " +
                "so it is clamping to the floor rather than trimming blind. Trimming resumes when the connection recovers.",
                topic);

            return [new ConsumerPositions("unknown", ReadOnlyDictionary<int, PositionRecord>.Empty)];
        }

        var store = new RedisPositionStore(redis);
        var positions = new List<ConsumerPositions>(names.Count);
        foreach (var consumer in names)
        {
            ct.ThrowIfCancellationRequested();
            var records = await store.LoadRecordsAsync(topic, consumer, ct).ConfigureAwait(false);
            positions.Add(new ConsumerPositions(consumer, records));
        }

        return positions;
    }

    /// <summary>
    /// Logs the configurations where an interval was set but a sweep would do nothing — a silently
    /// inert trimmer is worse than a noisy one.
    /// </summary>
    private void WarnAboutIneffectiveConfiguration()
    {
        foreach (var (name, topic) in this.options.Topics)
        {
            if (topic is null || topic.BackgroundTrimIntervalSeconds <= 0 || IsEnabled(topic))
            {
                continue;
            }

            if (topic.RetentionSeconds is not > 0)
            {
                this.log.LogWarning(
                    "Streams: {Section}:Topics:{Topic}:BackgroundTrimIntervalSeconds is {IntervalSeconds} but RetentionSeconds is not set, " +
                    "so the background trimmer has no time window to apply and does nothing for this topic. " +
                    "Set RetentionSeconds, or remove the interval and let the inline MAXLEN {MaxLen} window do the trimming.",
                    StreamConfigBinder.SectionName,
                    name,
                    topic.BackgroundTrimIntervalSeconds,
                    topic.MaxLen);
                continue;
            }

            this.log.LogWarning(
                "Streams: {Section}:Topics:{Topic}:BackgroundTrimIntervalSeconds is {IntervalSeconds} but Trim is None, " +
                "so nothing is ever deleted and the stream grows without bound. Trim=None is a Development-only setting.",
                StreamConfigBinder.SectionName,
                name,
                topic.BackgroundTrimIntervalSeconds);
        }
    }

    /// <summary>One consumer's recorded positions for a topic.</summary>
    /// <param name="Consumer">The consumer name, as it appears in <c>p:{topic}:&lt;consumer&gt;</c>.</param>
    /// <param name="Positions">Partition to recorded position; a partition it has never processed is absent.</param>
    private readonly record struct ConsumerPositions(
        string Consumer,
        IReadOnlyDictionary<int, PositionRecord> Positions);
}
