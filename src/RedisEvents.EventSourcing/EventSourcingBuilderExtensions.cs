using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization.Metadata;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using RedisEvents.Errors;
using RedisEvents.Extensions;
using RedisEvents.Producer;
using StackExchange.Redis;

namespace RedisEvents.EventSourcing;

/// <summary>
/// Every event type registered so far for one topic, and every projection type registered so far on
/// one builder. Kept on the builder (via <c>Properties</c>), not in DI: both questions — "what is the
/// registry for topic X" and "which projections exist" — must be answered while the container is
/// still being configured, before <c>Build()</c> runs.
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

/// <summary>Every projection type registered on one builder via <see cref="EventSourcingBuilderExtensions.AddProjection{TProjection}"/>.</summary>
internal sealed class ProjectionTypes
{
    private readonly List<Type> types = [];

    /// <summary>Every projection type registered so far, in registration order.</summary>
    public IReadOnlyList<Type> Types => this.types;

    /// <summary>Records a projection type. A repeat is a no-op.</summary>
    public void Add(Type type)
    {
        if (!this.types.Contains(type))
        {
            this.types.Add(type);
        }
    }
}

/// <summary>
/// Registration API for <c>RedisEvents.EventSourcing</c>, in the same style as core's
/// <see cref="StreamsBuilderExtensions"/>: additive, idempotent per topic, and resolved through DI at
/// container-build time rather than at call time — so the order <c>AddEventStore</c>,
/// <c>AddEventProjector</c> and <c>AddProjection</c> calls appear in never matters.
/// </summary>
public static class EventSourcingBuilderExtensions
{
    private const string EventTypeRegistriesKey = "RedisEvents.EventSourcing:EventTypeRegistries";
    private const string EventStoreTopicsKey = "RedisEvents.EventSourcing:EventStoreTopics";
    private const string EventStoreDefaultsKey = "RedisEvents.EventSourcing:EventStoreDefaults";
    private const string ProjectionTypesKey = "RedisEvents.EventSourcing:ProjectionTypes";

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
    /// <paramref name="topic"/> is seen by <see cref="AddEventStore"/> or
    /// <see cref="AddEventProjector"/> — a second call for the same topic reuses the registry built
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
        EnsureEventTypeRegistry(builder, topic, events);

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

    /// <summary>
    /// Registers the read side for <paramref name="topic"/>: an <see cref="EventProjector"/>
    /// dispatching to every <see cref="AddProjection{TProjection}"/> instance on this builder, riding
    /// core's ordinary consumer (<see cref="StreamsBuilderExtensions.AddStream(IHostApplicationBuilder, string, Func{ReadOnlyMemory{RedisEvents.Wire.StreamMsg}, CancellationToken, ValueTask})"/>)
    /// — so <c>Streams:Consumers</c> configuration, positions and the error contract are exactly
    /// core's, with nothing added on top.
    /// </summary>
    /// <param name="builder">The host application builder.</param>
    /// <param name="topic">The topic to project.</param>
    /// <param name="events">
    /// Registers every event type this projector can decode. Omit it when a prior
    /// <see cref="AddEventStore"/> or <see cref="AddEventProjector"/> call already registered one for
    /// <paramref name="topic"/> on this builder — the existing registry is reused. Required the first
    /// time <paramref name="topic"/> is seen.
    /// </param>
    /// <returns>The builder, for chaining.</returns>
    /// <exception cref="StreamConfigurationException">
    /// No registry exists yet for <paramref name="topic"/> and <paramref name="events"/> is
    /// <see langword="null"/>.
    /// </exception>
    /// <remarks>
    /// <para>
    /// <b>How the projector gets built without a live <see cref="IServiceProvider"/> to hand it.</b>
    /// <c>AddStream&lt;EventProjector&gt;(topic)</c> is called first — it bare-registers
    /// <c>AddSingleton&lt;EventProjector&gt;()</c> (which cannot actually construct one; nothing here
    /// asks it to) and wires the consumer host to resolve <c>typeof(EventProjector)</c> from the
    /// container once it is built. A second, real registration is added immediately after with the
    /// constructor arguments this projector actually needs; because the last registration for a
    /// service type is the one <see cref="IServiceProvider"/> returns, that is the one the consumer
    /// host gets. This uses only <c>Microsoft.Extensions.DependencyInjection</c>'s documented
    /// last-registration-wins behaviour and core's existing public <c>AddStream&lt;THandler&gt;</c>
    /// overload — no internals of either are touched.
    /// </para>
    /// <para>
    /// Every <see cref="AddProjection{TProjection}"/> call on the builder is available to every
    /// <see cref="AddEventProjector"/> topic, regardless of which was registered first or which
    /// topic a projection is "for": a projection only ever receives events whose wire type is both
    /// registered on that topic and one it implements <see cref="IProjection{TEvent}"/> for, so one
    /// meant for a different topic simply never sees anything on this one.
    /// </para>
    /// </remarks>
    public static IHostApplicationBuilder AddEventProjector(
        this IHostApplicationBuilder builder,
        string topic,
        Action<EventTypeRegistry>? events = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        EnsureEventTypeRegistry(builder, topic, events);

        var projections = ProjectionTypesRegistry(builder);

        // The bare, non-functional registration — see the <remarks> above.
        builder.AddStream<EventProjector>(topic);

        // The real one. Registered after, so it is the one actually resolved.
        builder.Services.AddSingleton(sp => new EventProjector(
            sp.GetRequiredKeyedService<EventTypeRegistry>(topic),
            projections.Types.Select(sp.GetRequiredService).ToList(),
            sp.GetService<ILoggerFactory>()?.CreateLogger("RedisEvents.EventSourcing")));

        return builder;
    }

    /// <summary>
    /// Registers <typeparamref name="TProjection"/> as a singleton available to every
    /// <see cref="AddEventProjector"/> on this builder. Its constructor is resolved from DI
    /// normally — a projection that needs an <see cref="IViewStore{TView}"/> just asks for one.
    /// </summary>
    /// <typeparam name="TProjection">
    /// A class implementing <see cref="IProjection{TEvent}"/> for one or more event types. It needs
    /// no registration beyond this call — see <see cref="IProjection{TEvent}"/>.
    /// </typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <returns>The builder, for chaining.</returns>
    public static IHostApplicationBuilder AddProjection<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TProjection>(
        this IHostApplicationBuilder builder)
        where TProjection : class
    {
        ArgumentNullException.ThrowIfNull(builder);

        ProjectionTypesRegistry(builder).Add(typeof(TProjection));
        builder.Services.AddSingleton<TProjection>();
        return builder;
    }

    /// <summary>
    /// Registers a Redis-backed <see cref="IViewStore{TView}"/> for <typeparamref name="TView"/>,
    /// stored as one hash per view type on the shared streams connection
    /// (<see cref="StreamsConnection.GetSharedDatabase"/>).
    /// </summary>
    /// <typeparam name="TView">The view type.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="topic">
    /// A topic whose configuration has already provisioned the shared connection (any prior
    /// <c>AddStream</c>/<c>AddStreamPublisher</c>/<c>AddStreamStore</c>/<c>AddEventStore</c> call on
    /// this builder does that) — the view store namespaces its key under it but does not need its
    /// hash slot; a view is looked up by id, never joined into an aggregate's transaction.
    /// </param>
    /// <param name="viewName">Distinguishes this view from others stored under the same topic.</param>
    /// <param name="json">Source-generated metadata for <typeparamref name="TView"/>.</param>
    /// <returns>The builder, for chaining.</returns>
    public static IHostApplicationBuilder AddRedisViewStore<TView>(
        this IHostApplicationBuilder builder,
        string topic,
        string viewName,
        JsonTypeInfo<TView> json)
        where TView : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);
        ArgumentNullException.ThrowIfNull(json);

        builder.Services.AddSingleton<IViewStore<TView>>(sp => new RedisViewStore<TView>(
            StreamsConnection.GetSharedDatabase(sp),
            topic,
            viewName,
            json));

        return builder;
    }

    /// <summary>
    /// Returns the <see cref="EventTypeRegistry"/> for <paramref name="topic"/> on this builder,
    /// building and registering one the first time it is asked for.
    /// </summary>
    private static EventTypeRegistry EnsureEventTypeRegistry(
        IHostApplicationBuilder builder,
        string topic,
        Action<EventTypeRegistry>? events)
    {
        var map = EventTypeRegistryMap(builder);
        if (map.TryGetValue(topic, out var existing))
        {
            return existing;
        }

        if (events is null)
        {
            throw new StreamConfigurationException(
                $"RedisEvents.EventSourcing: no event types are registered for topic '{topic}' yet, so " +
                $"'{nameof(AddEventProjector)}(\"{topic}\")' has nothing to decode with. Pass 'events' here, " +
                $"or call '{nameof(AddEventStore)}(\"{topic}\", events)' first.");
        }

        var registry = new EventTypeRegistry();
        events(registry);

        map[topic] = registry;
        builder.Services.AddKeyedSingleton(topic, registry);
        return registry;
    }

    private static Dictionary<string, EventTypeRegistry> EventTypeRegistryMap(IHostApplicationBuilder builder)
    {
        if (builder.Properties.TryGetValue(EventTypeRegistriesKey, out var existing)
            && existing is Dictionary<string, EventTypeRegistry> map)
        {
            return map;
        }

        map = new Dictionary<string, EventTypeRegistry>(StringComparer.Ordinal);
        builder.Properties[EventTypeRegistriesKey] = map;
        return map;
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

    private static ProjectionTypes ProjectionTypesRegistry(IHostApplicationBuilder builder)
    {
        if (builder.Properties.TryGetValue(ProjectionTypesKey, out var existing) && existing is ProjectionTypes registry)
        {
            return registry;
        }

        registry = new ProjectionTypes();
        builder.Properties[ProjectionTypesKey] = registry;
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
