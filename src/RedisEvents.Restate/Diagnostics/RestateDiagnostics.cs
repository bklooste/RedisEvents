using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace RedisEvents.Restate.Diagnostics;

/// <summary>
/// The <see cref="ActivitySource"/> and <see cref="Meter"/> for RedisEvents.Restate, both named
/// <c>RedisEvents.Restate</c>, separate from core's <c>RedisEvents</c> (core's diagnostics are internal
/// and this package takes no <c>InternalsVisibleTo</c> into it, like the other satellite packages).
/// </summary>
/// <remarks>
/// Every counter carries a <c>handler</c> tag (<see cref="RestateStartOptions.Name"/>, default the
/// handler's type name); the per-start counters also carry <c>workflow</c>. Both are low cardinality.
/// Keys are never tags.
/// </remarks>
internal static class RestateDiagnostics
{
    /// <summary>Source and meter name. Register it with <c>AddSource</c>/<c>AddMeter</c>, or use <c>AddRedisEventsRestate()</c>.</summary>
    internal const string SourceName = "RedisEvents.Restate";

    internal const string StartSpanName = "restate.start";

    private const string Version = "1.0.0";

    internal static readonly ActivitySource Source = new(SourceName, Version);

    internal static readonly Meter Meter = new(SourceName, Version);

    /// <summary>Messages <c>Keep</c> returned <see langword="true"/> for.</summary>
    internal static readonly Counter<long> Kept =
        Meter.CreateCounter<long>("restate.start.kept", "messages", "Messages kept by the filter, one start each");

    /// <summary>Messages dropped by <c>Keep</c> in memory, before any I/O.</summary>
    internal static readonly Counter<long> Filtered =
        Meter.CreateCounter<long>("restate.start.filtered", "messages", "Messages dropped by the filter before any I/O");

    /// <summary>Starts answered with any 2xx, already-started ones included.</summary>
    internal static readonly Counter<long> Accepted =
        Meter.CreateCounter<long>("restate.start.accepted", "starts", "Workflow starts the ingress accepted (any 2xx)");

    /// <summary>The subset of <see cref="Accepted"/> answered <c>202</c> with <c>"status":"PreviouslyAccepted"</c>.</summary>
    internal static readonly Counter<long> AlreadyStarted =
        Meter.CreateCounter<long>("restate.start.already_started", "starts", "Accepted starts whose workflow key had already been submitted (PreviouslyAccepted)");

    /// <summary>Attempts after the first, per item.</summary>
    internal static readonly Counter<long> Retried =
        Meter.CreateCounter<long>("restate.start.retried", "attempts", "Per-item submit retries inside one handler call");

    /// <summary>Batches handed back to the partition as a <see cref="RestateStartBlockedException"/>.</summary>
    internal static readonly Counter<long> Blocked =
        Meter.CreateCounter<long>("restate.start.blocked", "batches", "Batches that threw RestateStartBlockedException, so the partition blocks and retries them");

    internal static void EnsureInitialized() => _ = Meter.Name;
}
