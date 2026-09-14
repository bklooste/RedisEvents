using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using RedisEvents.Errors;
using RedisEvents.Extensions;
using RedisEvents.Producer;
using RedisEvents.Projections;

namespace RedisEvents.EventSourcing;

/// <summary>
/// Every topic an <see cref="EventSourcingBuilderExtensions.AddEventStore"/> call has declared, kept
/// on the builder (via <c>Properties</c>), not in DI: "is there exactly one event store?" must be
/// answerable while the container is still being configured, before <c>Build()</c> runs.
/// </summary>
internal sealed class EventStoreTopics
{
    private readonly List<string> topics = [];

    /// <summary>Every topic an <see cref="EventSourcingBuilderExtensions.AddEventStore"/> call has declared, in order.</summary>
    public IReadOnlyList<string> Topics => this.topics;

    /// <summary>Records a topic. A repeat is a no-op, like a repeat <c>AddStreamStore</c> call.</summary>
    /// <returns><see langword="true"/> if this was the first time <paramref name="topic"/> was added.</returns>
    public bool Add(string topic)
    {
        if (this.topics.Contains(topic, StringComparer.Ordinal))
        {
            return false;
        }

        this.topics.Add(topic);
        return true;
    }
}

/// <summary>
/// Registration API for the write side of <c>RedisEvents.EventSourcing</c>: event-sourced aggregates
/// backed by their own Redis stream. The read side — <c>AddEventProjector</c>, <c>AddProjection</c>,
/// <c>AddRedisViewStore</c> — lives in the sibling <c>RedisEvents.Projections</c> package, which this
/// one depends on (for <see cref="EventTypeRegistry"/>) but which does not depend back: a service that
/// only wants typed projections over an ordinary topic needs nothing from this package at all.
/// </summary>
public static class EventSourcingBuilderExtensions
{
    private const string EventStoreTopicsKey = "RedisEvents.EventSourcing:EventStoreTopics";
    private const string EventStoreDefaultsKey = "RedisEvents.EventSourcing:EventStoreDefaults";

    /// <summary>
    /// Registers the command side for <paramref name="topic"/>: a state store (via
    /// <see cref="StreamsBuilderExtensions.AddStreamStore"/>) and an <see cref="IEventRepository"/>
    /// backed by it, keyed by <paramref name="topic"/>, plus an unkeyed resolution when this is the
    /// only event store on the builder.
    /// </summary>
    /// <param name="builder">The host application builder.</param>
    /// <param name="topic">The topic aggregates on this store are saved to and loaded from.</param>
    /// <param name="events">
    /// Registers every event type this store can save and replay. Run once, the first time
    /// <paramref name="topic"/> is seen by this call or by the sibling package's
    /// <c>AddEventProjector</c> — a second call for the same topic reuses the registry built
    /// by the first and ignores this argument, exactly as a second <c>AddStreamStore</c> call for the
    /// same topic is a no-op.
    /// </param>
    /// <returns>The builder, for chaining.</returns>
    /// <exception cref="StreamConfigurationException">
    /// <paramref name="topic"/>'s partitions are not co-located — see <see cref="IStreamStore"/>.
    /// </exception>
    public static IHostApplicationBuilder AddEventStore(
        this IHostApplicationBuilder builder,
        string topic,
        Action<EventTypeRegistry> events)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(events);

        builder.AddStreamStore(topic);
        EventTypeRegistries.Ensure(builder, topic, events, nameof(AddEventStore));

        var topics = EventStoreTopicsRegistry(builder);
        if (topics.Add(topic))
        {
            builder.Services.AddKeyedSingleton<IEventRepository>(
                topic,
                (sp, _) => new RedisEventRepository(
                    sp.GetRequiredKeyedService<IStreamStore>(topic),
                    sp.GetRequiredKeyedService<EventTypeRegistry>(topic),
                    sp.GetService<ILoggerFactory>()?.CreateLogger("RedisEvents.EventSourcing")));
        }

        RegisterEventStoreDefault(builder);
        return builder;
    }

    private static EventStoreTopics EventStoreTopicsRegistry(IHostApplicationBuilder builder)
    {
        if (builder.Properties.TryGetValue(EventStoreTopicsKey, out var existing) && existing is EventStoreTopics registry)
        {
            return registry;
        }

        registry = new EventStoreTopics();
        builder.Properties[EventStoreTopicsKey] = registry;
        builder.Services.AddSingleton(registry);
        return registry;
    }

    /// <summary>
    /// Registers the unkeyed <see cref="IEventRepository"/> resolution once per builder, resolved
    /// through <see cref="EventStoreTopics"/> at container-build time so "is there exactly one event
    /// store?" is answered after every <see cref="AddEventStore"/> call has run, not after the first —
    /// the same reasoning as core's <c>AddStreamStore</c>.
    /// </summary>
    private static void RegisterEventStoreDefault(IHostApplicationBuilder builder)
    {
        if (builder.Properties.ContainsKey(EventStoreDefaultsKey))
        {
            return;
        }

        builder.Properties[EventStoreDefaultsKey] = true;
        builder.Services.AddSingleton(sp => sp.GetRequiredKeyedService<IEventRepository>(SoleEventStoreTopic(sp)));
    }

    /// <summary>
    /// The topic of the only registered event store. A service with more than one must ask for the
    /// one it means — <c>[FromKeyedServices("topic")]</c> — because an unkeyed resolution could
    /// otherwise silently load or save the wrong aggregate family.
    /// </summary>
    private static string SoleEventStoreTopic(IServiceProvider services)
    {
        var topics = services.GetRequiredService<EventStoreTopics>().Topics;

        if (topics.Count == 1)
        {
            return topics[0];
        }

        if (topics.Count == 0)
        {
            throw new StreamConfigurationException(
                $"RedisEvents.EventSourcing: no event store is registered, so {nameof(IEventRepository)} cannot be resolved. " +
                $"Call {nameof(AddEventStore)}(topic, events) first.");
        }

        var joined = string.Join(", ", topics);
        throw new StreamConfigurationException(
            $"RedisEvents.EventSourcing: {topics.Count} event stores are registered ({joined}), so an unkeyed " +
            $"{nameof(IEventRepository)} is ambiguous. Inject [FromKeyedServices(\"<topic>\")] {nameof(IEventRepository)} instead.");
    }
}
