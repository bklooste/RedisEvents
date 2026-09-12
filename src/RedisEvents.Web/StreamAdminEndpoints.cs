using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;
using RedisEvents.Admin;
using RedisEvents.Config;
using RedisEvents.Errors;
using RedisEvents.Wire;
using StackExchange.Redis;

namespace RedisEvents.Web;

/// <summary>
/// How <see cref="StreamAdminEndpoints.MapRoutes(IEndpointRouteBuilder, IConnectionMultiplexer, StreamAdminEndpointOptions)"/>
/// maps its routes.
/// </summary>
/// <remarks>
/// The defaults are the safe ones: every route requires an authenticated caller, and applying a reset
/// while a consumer still holds partitions is refused. Both can be relaxed, deliberately and in one
/// visible place.
/// </remarks>
public sealed record StreamAdminEndpointOptions
{
    /// <summary>
    /// Whether the mapped routes carry authorization metadata (default <see langword="true"/>).
    /// </summary>
    /// <remarks>
    /// These endpoints rewind and fast-forward production consumers; a reset to the end of the stream
    /// is unrecoverable data loss for the consumer it is aimed at. Group-level
    /// <c>RequireAuthorization</c> in the caller's <c>Program.cs</c> is easy to forget and invisible
    /// once forgotten, so the routes require it themselves and the caller's group policy stacks on
    /// top. Set this to <see langword="false"/> only for a test host or a network-isolated dev tool.
    /// </remarks>
    public bool RequireAuthorization { get; init; } = true;

    /// <summary>
    /// The authorization policy name to require, or <see langword="null"/> for the application's
    /// default policy (any authenticated user).
    /// </summary>
    /// <remarks>
    /// Naming a policy that is not registered fails the request rather than the mapping, which is the
    /// fail-closed direction.
    /// </remarks>
    public string? PolicyName { get; init; }

    /// <summary>
    /// Whether a reset may be applied while the consumer still holds partition claims
    /// (default <see langword="false"/>).
    /// </summary>
    /// <remarks>
    /// The documented procedure is scale to zero, reset, scale up. A reset against a live consumer
    /// does take effect — the reset marker exists for exactly that — but the worker rewinds mid-batch
    /// on its next flush tick, which is a much harder thing to reason about than a cold start. Left
    /// <see langword="false"/>, an apply against a consumer with live claims answers 409 and names
    /// the owners; a caller who means it passes <c>?force=true</c>.
    /// </remarks>
    public bool AllowResetWhileRunning { get; init; }
}

/// <summary>
/// Minimal API route group builder for stream administration endpoints: preview and apply position
/// resets, and read the ownership map.
/// </summary>
/// <remarks>
/// <para>
/// These endpoints require dynamic code and reflection because they use ASP.NET Core's minimal APIs,
/// which use parameter binding via reflection. Admin endpoints are explicitly opt-in and not called
/// by core stream processing logic, so this does not affect AOT compatibility of services that do not
/// enable them.
/// </para>
/// <para>
/// Usage in a service's Program.cs:
/// <code>
/// var adminGroup = app.MapGroup("/admin/streams")
///     .RequireAuthorization(policy =&gt; policy.RequireClaim("scope", "admin"));
///
/// StreamAdminEndpoints.MapRoutes(adminGroup, app.Services.GetRequiredService&lt;IConnectionMultiplexer&gt;());
/// </code>
/// </para>
/// <para>
/// All routes return plain text. A reset — preview or apply — takes exactly one target:
/// <c>from=2026-09-01T00:00:00Z</c> (by date), <c>id=1757000000000-0</c> (exact position),
/// <c>to=start</c>, or <c>to=end</c>. Supplying none or more than one is 400 Bad Request.
/// </para>
/// </remarks>
[RequiresUnreferencedCode("Admin endpoints use reflection for parameter binding and response serialization.")]
[RequiresDynamicCode("Admin endpoints use reflection for parameter binding and response serialization.")]
public static class StreamAdminEndpoints
{
    /// <summary>Longest topic or consumer name accepted in a route.</summary>
    internal const int MaxNameLength = 128;

    /// <summary>Logger category for the Warning line every applied reset writes.</summary>
    private const string LoggerCategory = "RedisEvents.Admin.Reset";

    private static readonly StreamAdminEndpointOptions Defaults = new();

    /// <summary>What a reset was asked to move the position to.</summary>
    internal enum ResetTargetKind
    {
        /// <summary>Nothing usable was supplied.</summary>
        None = 0,

        /// <summary>An instant: the first entry written at or after it is delivered.</summary>
        Date = 1,

        /// <summary>An exact id: reading resumes at the entry immediately after it.</summary>
        Exact = 2,

        /// <summary>The oldest surviving entry of each partition.</summary>
        Start = 3,

        /// <summary>Each partition's own newest entry — the backlog is abandoned.</summary>
        End = 4,
    }

    /// <summary>
    /// Maps the stream administration routes, taking the multiplexer directly.
    /// </summary>
    /// <param name="group">The minimal API route group (<c>/admin/streams</c> or similar).</param>
    /// <param name="redis">The shared Redis multiplexer.</param>
    /// <param name="options">Mapping options; the defaults require authorization.</param>
    /// <remarks>
    /// Mapped routes:
    /// <list type="bullet">
    ///     <item><c>GET  {topic}/{consumer}/reset/preview?from=…|id=…|to=start|end[&amp;partition=N][&amp;maxCountScan=N][&amp;whenMissing=beginning|now]</c></item>
    ///     <item><c>POST {topic}/{consumer}/reset?from=…|id=…|to=start|end[&amp;partition=N][&amp;force=true]</c></item>
    ///     <item><c>GET  {topic}/ownership</c></item>
    /// </list>
    /// </remarks>
    public static void MapRoutes(
        IEndpointRouteBuilder group,
        IConnectionMultiplexer redis,
        StreamAdminEndpointOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(group);
        ArgumentNullException.ThrowIfNull(redis);

        Map(group, _ => redis, options ?? Defaults);
    }

    /// <summary>
    /// Maps the stream administration routes, resolving <see cref="IConnectionMultiplexer"/> from the
    /// request's services instead of capturing one at startup.
    /// </summary>
    /// <param name="group">The minimal API route group (<c>/admin/streams</c> or similar).</param>
    /// <param name="options">Mapping options; the defaults require authorization.</param>
    /// <remarks>
    /// Use this when the multiplexer is registered in DI: routes can then be mapped before it is
    /// resolvable, and a service that never receives an admin request never forces the connection.
    /// A request that arrives with no registration answers 500 saying exactly that.
    /// </remarks>
    public static void MapRoutes(IEndpointRouteBuilder group, StreamAdminEndpointOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(group);

        Map(group, static ctx => ctx.RequestServices?.GetService(typeof(IConnectionMultiplexer)) as IConnectionMultiplexer, options ?? Defaults);
    }

    private static void Map(
        IEndpointRouteBuilder group,
        Func<HttpContext, IConnectionMultiplexer?> resolve,
        StreamAdminEndpointOptions options)
    {
        var routes = new IEndpointConventionBuilder[3];

        routes[0] = group.MapGet(
            "{topic}/{consumer}/reset/preview",
            (string topic,
             string consumer,
             string? from,
             string? id,
             string? to,
             int? partition,
             long? maxCountScan,
             string? whenMissing,
             HttpContext ctx,
             CancellationToken ct) =>
                ResetHandler(
                    resolve,
                    topic,
                    consumer,
                    new ResetRequest(from, id, to, partition, maxCountScan, whenMissing, Force: false),
                    options,
                    apply: false,
                    ctx,
                    ct))
            .WithName("PreviewStreamReset");

        routes[1] = group.MapPost(
            "{topic}/{consumer}/reset",
            (string topic,
             string consumer,
             string? from,
             string? id,
             string? to,
             int? partition,
             bool? force,
             HttpContext ctx,
             CancellationToken ct) =>
                ResetHandler(
                    resolve,
                    topic,
                    consumer,
                    new ResetRequest(from, id, to, partition, MaxCountScan: null, WhenMissing: null, Force: force ?? false),
                    options,
                    apply: true,
                    ctx,
                    ct))
            .WithName("ApplyStreamReset");

        routes[2] = group.MapGet(
            "{topic}/ownership",
            (string topic, HttpContext ctx, CancellationToken ct) =>
                GetOwnershipHandler(resolve, topic, ctx, ct))
            .WithName("GetStreamOwnership");

        if (!options.RequireAuthorization)
        {
            return;
        }

        foreach (var route in routes)
        {
            if (options.PolicyName is { Length: > 0 } policy)
            {
                route.RequireAuthorization(policy);
            }
            else
            {
                route.RequireAuthorization();
            }
        }
    }

    /// <summary>The query a reset — preview or apply — was asked for.</summary>
    internal sealed record ResetRequest(
        string? From,
        string? Id,
        string? To,
        int? Partition,
        long? MaxCountScan,
        string? WhenMissing,
        bool Force);

    /// <summary>The resolved target of a reset, once the query has been validated.</summary>
    internal readonly record struct ResetTarget(ResetTargetKind Kind, DateTimeOffset Date, StreamId Id);

    internal static async Task ResetHandler(
        Func<HttpContext, IConnectionMultiplexer?> resolve,
        string topic,
        string consumer,
        ResetRequest request,
        StreamAdminEndpointOptions options,
        bool apply,
        HttpContext ctx,
        CancellationToken ct)
    {
        try
        {
            if (NameError(topic, "topic") is { } topicError)
            {
                await WriteAsync(ctx, StatusCodes.Status400BadRequest, topicError).ConfigureAwait(false);
                return;
            }

            if (NameError(consumer, "consumer") is { } consumerError)
            {
                await WriteAsync(ctx, StatusCodes.Status400BadRequest, consumerError).ConfigureAwait(false);
                return;
            }

            if (!TryResolveTarget(request, out var target, out var targetError))
            {
                await WriteAsync(ctx, StatusCodes.Status400BadRequest, targetError!).ConfigureAwait(false);
                return;
            }

            if (!TryParseWhenMissing(request.WhenMissing, out var whenMissing, out var whenMissingError))
            {
                await WriteAsync(ctx, StatusCodes.Status400BadRequest, whenMissingError!).ConfigureAwait(false);
                return;
            }

            if (request.MaxCountScan is <= 0)
            {
                await WriteAsync(ctx, StatusCodes.Status400BadRequest, "Error: maxCountScan must be greater than zero\n").ConfigureAwait(false);
                return;
            }

            var redis = resolve(ctx);
            if (redis is null)
            {
                await WriteAsync(
                    ctx,
                    StatusCodes.Status500InternalServerError,
                    "Error: no IConnectionMultiplexer is registered in the request services; map the admin routes with the overload that takes one\n")
                    .ConfigureAwait(false);
                return;
            }

            // The layout is read back from Redis rather than guessed. Guessing co-location against a
            // spread topic reads keys that do not exist: the preview then reports Length 0 and the
            // reset to the end writes 0-0, both of which look like success.
            var layout = await StreamAdmin.ReadTopicLayoutAsync(redis, topic, ct).ConfigureAwait(false);
            if (layout.Partitions < 1)
            {
                await WriteAsync(
                    ctx,
                    StatusCodes.Status404NotFound,
                    $"Error: topic '{topic}' has no recorded partition count in m:{{{topic}}}; it has never been created by a publisher or consumer\n")
                    .ConfigureAwait(false);
                return;
            }

            if (request.Partition is { } one && (one < 0 || one >= layout.Partitions))
            {
                await WriteAsync(
                    ctx,
                    StatusCodes.Status400BadRequest,
                    $"Error: topic '{topic}' has {layout.Partitions.ToString(CultureInfo.InvariantCulture)} partition(s), numbered 0..{(layout.Partitions - 1).ToString(CultureInfo.InvariantCulture)}; partition {one.ToString(CultureInfo.InvariantCulture)} does not exist\n")
                    .ConfigureAwait(false);
                return;
            }

            var ownership = await StreamAdmin.GetOwnershipAsync(redis, topic, consumer, layout.Partitions, ct).ConfigureAwait(false);
            var running = ownership.Owners.Count;

            if (apply && running > 0 && !request.Force && !options.AllowResetWhileRunning)
            {
                await WriteAsync(ctx, StatusCodes.Status409Conflict, RunningConsumerText(ownership.Describe(), running, blocked: true)).ConfigureAwait(false);
                return;
            }

            var preview = await PreviewAsync(redis, topic, consumer, target, request, layout, whenMissing, ct).ConfigureAwait(false);

            var body = new StringBuilder(512);
            body.Append(preview.Describe());

            if (running > 0)
            {
                body.Append(RunningConsumerText(ownership.Describe(), running, blocked: false));
            }

            if (apply)
            {
                var logger = (ctx.RequestServices?.GetService(typeof(ILoggerFactory)) as ILoggerFactory)?.CreateLogger(LoggerCategory);
                var results = await ApplyAsync(redis, topic, consumer, target, request, layout, logger, ct).ConfigureAwait(false);

                body.Append("\nApplied reset to ")
                    .Append(results.Count.ToString(CultureInfo.InvariantCulture))
                    .AppendLine(" partition(s).");
            }

            await WriteAsync(ctx, StatusCodes.Status200OK, body.ToString()).ConfigureAwait(false);
        }
        catch (StreamConfigurationException ex)
        {
            await WriteAsync(ctx, StatusCodes.Status400BadRequest, $"Error: {ex.Message}\n").ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ctx.RequestAborted.IsCancellationRequested)
        {
            await WriteAsync(ctx, StatusCodes.Status408RequestTimeout, "Error: Request was cancelled\n").ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // The caller hung up. There is nobody to write a status line to.
        }
        catch (RedisConnectionException ex)
        {
            await WriteAsync(ctx, StatusCodes.Status503ServiceUnavailable, $"Error: Redis unavailable: {ex.Message}\n").ConfigureAwait(false);
        }
        catch (RedisException ex)
        {
            await WriteAsync(ctx, StatusCodes.Status502BadGateway, $"Error: Redis rejected the command: {ex.Message}\n").ConfigureAwait(false);
        }
    }

    internal static async Task GetOwnershipHandler(
        Func<HttpContext, IConnectionMultiplexer?> resolve,
        string topic,
        HttpContext ctx,
        CancellationToken ct)
    {
        try
        {
            if (NameError(topic, "topic") is { } topicError)
            {
                await WriteAsync(ctx, StatusCodes.Status400BadRequest, topicError).ConfigureAwait(false);
                return;
            }

            var redis = resolve(ctx);
            if (redis is null)
            {
                await WriteAsync(
                    ctx,
                    StatusCodes.Status500InternalServerError,
                    "Error: no IConnectionMultiplexer is registered in the request services; map the admin routes with the overload that takes one\n")
                    .ConfigureAwait(false);
                return;
            }

            var db = redis.GetDatabase();
            var partitionCountValue = await db.HashGetAsync(StreamKeys.TopicMeta(topic), StreamAdmin.MetaPartitionsField).ConfigureAwait(false);

            var partitions = 0;
            if (partitionCountValue.TryParse(out int count) && count > 0)
            {
                partitions = count;
            }

            var text = new StringBuilder(512);
            text.Append("Ownership map for topic ").AppendLine(topic);
            text.Append("Recorded partitions: ").AppendLine(partitions.ToString(CultureInfo.InvariantCulture));

            var consumers = await ScanConsumersAsync(redis, db, topic, ct).ConfigureAwait(false);

            foreach (var consumerName in consumers)
            {
                ct.ThrowIfCancellationRequested();

                var ownership = await StreamAdmin.GetOwnershipAsync(redis, topic, consumerName, partitions, ct).ConfigureAwait(false);

                text.AppendLine();
                text.Append("Consumer: ").AppendLine(consumerName);
                text.Append("  Total partitions: ").AppendLine(ownership.Partitions.ToString(CultureInfo.InvariantCulture));
                text.Append("  Owned: ").AppendLine(ownership.Owners.Count.ToString(CultureInfo.InvariantCulture));
                text.Append("  Unowned: ").AppendLine(ownership.Unowned.Count.ToString(CultureInfo.InvariantCulture));
                text.Append("  Contested: ").AppendLine(ownership.Contested.Count.ToString(CultureInfo.InvariantCulture));

                for (var partition = 0; partition < ownership.Partitions; partition++)
                {
                    if (ownership.Owners.TryGetValue(partition, out var owner))
                    {
                        text.Append("    ").Append(partition.ToString(CultureInfo.InvariantCulture))
                            .Append(" -> ").AppendLine(owner.ToString());
                    }
                }

                if (ownership.Unowned.Count > 0)
                {
                    text.Append("  Unowned partitions: ").AppendLine(string.Join(", ", ownership.Unowned));
                }
            }

            if (consumers.Count == 0)
            {
                text.AppendLine("No consumers found for this topic.");
            }

            await WriteAsync(ctx, StatusCodes.Status200OK, text.ToString()).ConfigureAwait(false);
        }
        catch (StreamConfigurationException ex)
        {
            await WriteAsync(ctx, StatusCodes.Status400BadRequest, $"Error: {ex.Message}\n").ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ctx.RequestAborted.IsCancellationRequested)
        {
            await WriteAsync(ctx, StatusCodes.Status408RequestTimeout, "Error: Request was cancelled\n").ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // The caller hung up.
        }
        catch (RedisConnectionException ex)
        {
            await WriteAsync(ctx, StatusCodes.Status503ServiceUnavailable, $"Error: Redis unavailable: {ex.Message}\n").ConfigureAwait(false);
        }
        catch (RedisException ex)
        {
            await WriteAsync(ctx, StatusCodes.Status502BadGateway, $"Error: Redis rejected the command: {ex.Message}\n").ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Every consumer of a topic that has an ownership hash, found by <c>SCAN</c> on every connected
    /// master.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The pattern comes from <see cref="StreamKeys"/> rather than being retyped here. Building it by
    /// hand is how this endpoint used to answer "No consumers found" for every topic that had
    /// consumers: the braces in <c>o:{topic}:…</c> are literal — Redis Cluster hash tags — and a C#
    /// interpolated <c>$"o:{topic}:*"</c> substitutes the topic name for them instead.
    /// </para>
    /// <para>
    /// One arbitrary endpoint is not enough either. In a cluster the ownership hashes of different
    /// topics live on different masters, and against a primary/replica pair the arbitrary pick may be
    /// the replica. Every connected master is scanned and the names de-duplicated.
    /// </para>
    /// </remarks>
    private static async Task<List<string>> ScanConsumersAsync(IConnectionMultiplexer redis, IDatabase db, string topic, CancellationToken ct)
    {
        var pattern = OwnershipKeyPattern(topic);
        var prefix = OwnershipKeyPrefix(topic);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var names = new List<string>();

        foreach (var endpoint in redis.GetEndPoints())
        {
            ct.ThrowIfCancellationRequested();

            var server = redis.GetServer(endpoint);
            if (!server.IsConnected || server.IsReplica)
            {
                continue;
            }

            await foreach (var key in server.KeysAsync(database: db.Database, pattern: pattern, pageSize: 100).WithCancellation(ct).ConfigureAwait(false))
            {
                var keyText = key.ToString();
                if (keyText.Length > prefix.Length &&
                    keyText.StartsWith(prefix, StringComparison.Ordinal) &&
                    seen.Add(keyText))
                {
                    names.Add(keyText[prefix.Length..]);
                }
            }
        }

        names.Sort(StringComparer.Ordinal);
        return names;
    }

    private static Task<ResetPreview> PreviewAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        ResetTarget target,
        ResetRequest request,
        TopicOptions layout,
        StartFrom whenMissing,
        CancellationToken ct)
    {
        var maxCountScan = request.MaxCountScan ?? StreamAdmin.DefaultMaxCountScan;

        return target.Kind switch
        {
            ResetTargetKind.Date => StreamAdmin.PreviewResetAsync(
                redis, topic, consumer, target.Date, whenMissing, request.Partition, layout, maxCountScan, ct),
            ResetTargetKind.End => StreamAdmin.PreviewResetToEndAsync(
                redis, topic, consumer, request.Partition, layout, maxCountScan, whenMissing, ct),
            _ => StreamAdmin.PreviewResetAsync(
                redis, topic, consumer, target.Id, whenMissing, request.Partition, layout, maxCountScan, ct),
        };
    }

    private static Task<IReadOnlyList<(int Partition, StreamId Target)>> ApplyAsync(
        IConnectionMultiplexer redis,
        string topic,
        string consumer,
        ResetTarget target,
        ResetRequest request,
        TopicOptions layout,
        ILogger? logger,
        CancellationToken ct)
        => target.Kind switch
        {
            ResetTargetKind.Date => StreamAdmin.ResetPositionAsync(
                redis, topic, consumer, target.Date, request.Partition, layout, logger, ct),
            ResetTargetKind.Start => StreamAdmin.ResetPositionToStartAsync(
                redis, topic, consumer, request.Partition, layout, logger, ct),
            ResetTargetKind.End => StreamAdmin.ResetPositionToEndAsync(
                redis, topic, consumer, request.Partition, layout, logger, ct),
            _ => StreamAdmin.ResetPositionAsync(
                redis, topic, consumer, target.Id, request.Partition, layout, logger, ct),
        };

    /// <summary>The <c>SCAN</c> glob for every consumer's ownership hash: <c>o:{topic}:*</c>, braces and all.</summary>
    internal static string OwnershipKeyPattern(string topic)
        => StreamKeys.Ownership(topic, "*").ToString()!;

    /// <summary>The literal prefix an ownership key starts with: <c>o:{topic}:</c>.</summary>
    internal static string OwnershipKeyPrefix(string topic)
        => StreamKeys.Ownership(topic, string.Empty).ToString()!;

    /// <summary>
    /// Validates a topic or consumer route segment, returning the 400 body or <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// Names become Redis key components, and the braces around the topic are cluster hash tags — a
    /// name containing <c>{</c> or <c>}</c> re-tags the key and silently addresses a different slot
    /// and a different hash. A name containing <c>*</c>, <c>?</c> or <c>[</c> is a glob against the
    /// SCAN in the ownership route. Neither is worth supporting, so the accepted alphabet is the one
    /// the library's own topic names use.
    /// </remarks>
    internal static string? NameError(string? value, string what)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return $"Error: {what} is required\n";
        }

        if (value.Length > MaxNameLength)
        {
            return $"Error: {what} is longer than {MaxNameLength.ToString(CultureInfo.InvariantCulture)} characters\n";
        }

        foreach (var c in value)
        {
            if (!IsNameChar(c))
            {
                return $"Error: {what} may only contain letters, digits, '_', '-', '.' and ':'\n";
            }
        }

        return null;
    }

    private static bool IsNameChar(char c)
        => c is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') or '_' or '-' or '.' or ':';

    /// <summary>
    /// Resolves exactly one of <c>from</c>, <c>id</c> and <c>to</c> into a target, or explains why not.
    /// </summary>
    internal static bool TryResolveTarget(ResetRequest request, out ResetTarget target, out string? error)
    {
        ArgumentNullException.ThrowIfNull(request);

        target = default;

        var supplied = 0;
        if (!string.IsNullOrWhiteSpace(request.From))
        {
            supplied++;
        }

        if (!string.IsNullOrWhiteSpace(request.Id))
        {
            supplied++;
        }

        if (!string.IsNullOrWhiteSpace(request.To))
        {
            supplied++;
        }

        if (supplied != 1)
        {
            error = supplied == 0
                ? "Error: exactly one target is required: from=<ISO-8601 instant>, id=<ms-seq>, to=start or to=end\n"
                : "Error: from, id and to are mutually exclusive; supply exactly one\n";
            return false;
        }

        if (!string.IsNullOrWhiteSpace(request.From))
        {
            if (ParseFromParameter(request.From) is not { } date)
            {
                error = "Error: from must be a valid ISO-8601 instant, e.g. 2026-09-01T00:00:00Z\n";
                return false;
            }

            target = new ResetTarget(ResetTargetKind.Date, date, default);
            error = null;
            return true;
        }

        if (!string.IsNullOrWhiteSpace(request.Id))
        {
            if (!StreamId.TryParse(request.Id, out var id))
            {
                error = "Error: id must be a Redis stream id, e.g. 1757000000000-0\n";
                return false;
            }

            target = new ResetTarget(ResetTargetKind.Exact, default, id);
            error = null;
            return true;
        }

        if (string.Equals(request.To, "start", StringComparison.OrdinalIgnoreCase))
        {
            target = new ResetTarget(ResetTargetKind.Start, default, StreamId.Min);
            error = null;
            return true;
        }

        if (string.Equals(request.To, "end", StringComparison.OrdinalIgnoreCase))
        {
            target = new ResetTarget(ResetTargetKind.End, default, default);
            error = null;
            return true;
        }

        error = "Error: to must be either start or end\n";
        return false;
    }

    /// <summary>
    /// Parses the <c>whenMissing</c> query parameter — what a partition with no stored position would
    /// otherwise have started from, which is the only thing that makes its entry count mean anything.
    /// </summary>
    internal static bool TryParseWhenMissing(string? value, out StartFrom whenMissing, out string? error)
    {
        whenMissing = StartFrom.Beginning;
        error = null;

        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        if (string.Equals(value, "beginning", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (string.Equals(value, "now", StringComparison.OrdinalIgnoreCase))
        {
            whenMissing = StartFrom.Now;
            return true;
        }

        error = "Error: whenMissing must be either beginning or now (the consumer's StartFromWhenMissing)\n";
        return false;
    }

    /// <summary>Parses the from query parameter as a DateTimeOffset.</summary>
    private static DateTimeOffset? ParseFromParameter(string? from)
    {
        if (string.IsNullOrWhiteSpace(from))
        {
            return null;
        }

        return DateTimeOffset.TryParse(
                from,
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind,
                out var parsed)
            ? parsed
            : null;
    }

    private static string RunningConsumerText(string map, int owned, bool blocked)
    {
        var text = new StringBuilder(256);

        text.AppendLine();
        text.Append(blocked ? "Error: " : "Warning: ")
            .Append("this consumer still holds ")
            .Append(owned.ToString(CultureInfo.InvariantCulture))
            .AppendLine(" live partition claim(s), so at least one worker is running.");
        text.Append("  ").AppendLine(map);
        text.AppendLine("  The documented procedure is scale to zero, reset, scale up: a running worker picks the reset up on its");
        text.AppendLine("  next flush tick and rewinds mid-batch, which redelivers whatever it had in flight.");

        if (blocked)
        {
            text.AppendLine("  Nothing was written. Re-issue with ?force=true if that is what you meant.");
        }

        return text.ToString();
    }

    private static Task WriteAsync(HttpContext ctx, int status, string body)
    {
        ctx.Response.StatusCode = status;
        ctx.Response.ContentType = "text/plain; charset=utf-8";
        return ctx.Response.WriteAsync(body);
    }
}
