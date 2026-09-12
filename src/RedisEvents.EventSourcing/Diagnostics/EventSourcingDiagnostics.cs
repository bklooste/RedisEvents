using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace RedisEvents.EventSourcing.Diagnostics;

/// <summary>
/// The single <see cref="ActivitySource"/> and <see cref="Meter"/> for RedisEvents.EventSourcing.
/// </summary>
/// <remarks>
/// <para>
/// A separate name from core's own <c>"RedisEvents"</c> source — <c>"RedisEvents.EventSourcing"</c> —
/// and a separate pair of static instances, because <c>RedisEvents.Diagnostics</c> is entirely
/// <see langword="internal"/> to core and this package has no <c>InternalsVisibleTo</c> grant into it
/// (nor should one be added: that would couple two packages meant to stay independently
/// versionable, exactly like <c>RedisEvents.MessagePack</c> uses none of core's internals either).
/// A save still gets a <c>streams.publish</c> child span for free from core's own
/// <c>Outbox.AppendAndPublishAsync</c> — this source exists to add the outer, package-specific spans
/// that core cannot know about: "was this aggregate load/save slow, and did it succeed", and
/// per-event tracing on the projection side.
/// </para>
/// <para>
/// Unlike core, there is no lag, backpressure or ownership state to back-fill with observable
/// gauges — a save or a load is a simple request/response call — so the metrics surface here is
/// deliberately small: three counters, recorded inline where the outcome is decided.
/// </para>
/// </remarks>
internal static class EventSourcingDiagnostics
{
    /// <summary>
    /// Source name for OpenTelemetry traces and metrics. Register it with
    /// <c>AddSource</c>/<c>AddMeter</c>, or use <c>AddRedisEventSourcing()</c>.
    /// </summary>
    internal const string SourceName = "RedisEvents.EventSourcing";

    /// <summary>
    /// Assembly version for diagnostics.
    /// </summary>
    private const string Version = "1.0.0";

    /// <summary>
    /// ActivitySource for OpenTelemetry tracing.
    /// </summary>
    internal static readonly ActivitySource Source = new(SourceName, Version);

    /// <summary>
    /// Meter for OpenTelemetry metrics.
    /// </summary>
    internal static readonly Meter Meter = new(SourceName, Version);

    /// <summary>
    /// Counter: number of aggregate saves that committed successfully.
    /// </summary>
    internal static readonly Counter<long> Saves =
        Meter.CreateCounter<long>("eventsourcing.saves", "saves", "Number of successful aggregate saves");

    /// <summary>
    /// Counter: number of saves that lost their optimistic concurrency check. Every increment is a
    /// <see cref="ConcurrencyException"/> the caller has to reload-and-retry through.
    /// </summary>
    internal static readonly Counter<long> SaveConflicts =
        Meter.CreateCounter<long>("eventsourcing.save.conflicts", "conflicts", "Number of optimistic concurrency conflicts on save");

    /// <summary>
    /// Counter: number of aggregate loads, whether or not the aggregate existed.
    /// </summary>
    internal static readonly Counter<long> Loads =
        Meter.CreateCounter<long>("eventsourcing.loads", "loads", "Number of aggregate loads");

    /// <summary>
    /// Touches the static instruments so the meter exists before the first measurement. Calling it
    /// is optional — every instrument is created by the static constructor — but it makes the intent
    /// explicit at startup, matching core's own <c>StreamsDiagnostics.EnsureInitialized</c>.
    /// </summary>
    internal static void EnsureInitialized() => _ = Meter.Name;
}
