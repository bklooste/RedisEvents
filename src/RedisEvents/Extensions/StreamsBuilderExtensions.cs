using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Errors;
using RedisEvents.Producer;
using RedisEvents.Trimming;
using RedisEvents.Wire;

namespace RedisEvents.Extensions;

/// <summary>
/// One consumer declared by an <c>AddStream</c> call. The consumer host (P1) is built from these.
/// </summary>
/// <param name="Options">The fully resolved consumer options, after config and any override.</param>
/// <param name="Consumer">The effective consumer name.</param>
/// <param name="HandlerType">The handler class registered as a singleton, or <see langword="null"/> for a delegate handler.</param>
/// <param name="Handler">The delegate handler, or <see langword="null"/> when <paramref name="HandlerType"/> is set.</param>
/// <param name="WrapAsTypedHandler">
/// Set only by the typed <c>AddStream&lt;THandler, TMessage&gt;</c> overloads. Turns the resolved
/// <paramref name="HandlerType"/> instance into a plain <see cref="IBatchHandler"/> or
/// <see cref="IMessageHandler"/> — see <see cref="TypedMessageHandlerAdapter{T}"/> /
/// <see cref="TypedBatchHandlerAdapter{T}"/> — before <c>StreamConsumerHost.ResolveHandler</c>'s
/// existing kind switch ever runs, so that switch never needs to know typed handlers exist.
/// </param>
internal sealed record StreamConsumerRegistration(
    ConsumerOptions Options,
    string Consumer,
    Type? HandlerType,
    Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask>? Handler,
    Func<object, object>? WrapAsTypedHandler = null);

/// <summary>
/// One publisher declared by an <c>AddStreamPublisher</c> call.
/// </summary>
/// <param name="Options">The fully resolved producer options.</param>
internal sealed record StreamPublisherRegistration(ProducerOptions Options);

/// <summary>
/// The set of consumers and publishers declared during registration. Registered as a singleton so
/// the consumer host and publisher factory (P1/P2) can enumerate what the service asked for, and so
/// duplicate <c>(Topic, Consumer)</c> pairs are caught at startup rather than at 3am.
/// </summary>
internal sealed class StreamRegistry
{
    private readonly List<StreamConsumerRegistration> consumers = [];
    private readonly List<StreamPublisherRegistration> publishers = [];

    /// <summary>Every consumer declared by an <c>AddStream</c> overload, in registration order.</summary>
    public IReadOnlyList<StreamConsumerRegistration> Consumers => this.consumers;

    /// <summary>Every publisher declared by <c>AddStreamPublisher</c>, in registration order.</summary>
    public IReadOnlyList<StreamPublisherRegistration> Publishers => this.publishers;

    /// <summary>
    /// Records a consumer. Two consumers on the same <c>(Topic, Consumer)</c> pair in one process
    /// would fight over the same position hash, so the second one throws.
    /// </summary>
    /// <param name="registration">The consumer to add.</param>
    /// <exception cref="StreamConfigurationException">Thrown on a duplicate <c>(Topic, Consumer)</c> pair.</exception>
    public void Add(StreamConsumerRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);

        foreach (var existing in this.consumers)
        {
            if (string.Equals(existing.Options.Topic, registration.Options.Topic, StringComparison.Ordinal)
                && string.Equals(existing.Consumer, registration.Consumer, StringComparison.Ordinal))
            {
                throw new StreamConfigurationException(
                    $"Streams: topic '{registration.Options.Topic}' is already registered for consumer '{registration.Consumer}' in this process. " +
                    "Two consumers on the same (Topic, Consumer) pair would fight over the same stored position; " +
                    "give one of them a distinct Consumer name.");
            }
        }

        this.consumers.Add(registration);
    }

    /// <summary>
    /// Records a publisher. A second publisher on the same topic is ignored — publishing twice to one
    /// topic from one process is normal and needs only one producer.
    /// </summary>
    /// <param name="registration">The publisher to add.</param>
    public void Add(StreamPublisherRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);

        foreach (var existing in this.publishers)
        {
            if (string.Equals(existing.Options.Topic, registration.Options.Topic, StringComparison.Ordinal))
                return;
        }

        this.publishers.Add(registration);
    }
}

/// <summary>
/// Registration API for RedisEvents. Every overload is additive and zero-config-safe: a
/// missing <c>Streams</c> section is legal, and <c>AddStream&lt;THandler&gt;("topic")</c> fills the
/// rest in from record defaults.
///
/// The first call binds and validates <c>Streams</c>, registers the shared multiplexer resolver, and
/// registers the <see cref="StreamRegistry"/>. Subsequent calls reuse all of that.
/// </summary>
public static class StreamsBuilderExtensions
{
    private const string OptionsKey = "RedisEvents:StreamOptions";
    private const string RegistryKey = "RedisEvents:StreamRegistry";
    private const string PublisherDefaultsKey = "RedisEvents:PublisherDefaults";

    /// <summary>
    /// Zero-config registration: consume <paramref name="topic"/> with <typeparamref name="THandler"/>,
    /// using the matching <c>Streams:Consumers</c> entry when there is one and record defaults otherwise.
    /// </summary>
    /// <typeparam name="THandler">The handler class; registered as a singleton.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="topic">The topic to consume.</param>
    /// <returns>The builder, for chaining.</returns>
    public static IHostApplicationBuilder AddStream<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        this IHostApplicationBuilder builder,
        string topic)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        var options = Core(builder);
        var consumer = FindByTopic(options, topic) ?? new ConsumerOptions { Topic = topic };

        builder.Services.AddSingleton<THandler>();
        return Register(builder, options, consumer, typeof(THandler), handler: null);
    }

    /// <summary>
    /// Config-driven registration for a service with exactly one <c>Streams:Consumers</c> entry;
    /// the topic comes from that entry.
    /// </summary>
    /// <typeparam name="THandler">The handler class; registered as a singleton.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <returns>The builder, for chaining.</returns>
    /// <exception cref="StreamConfigurationException">Thrown when there is not exactly one configured consumer.</exception>
    public static IHostApplicationBuilder AddStream<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        this IHostApplicationBuilder builder)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = Core(builder);

        if (options.Consumers.Length != 1)
        {
            throw new StreamConfigurationException(
                $"AddStream<{typeof(THandler).Name}>() takes the topic from the single {StreamConfigBinder.SectionName}:Consumers entry, " +
                $"but {options.Consumers.Length} are configured. Use AddStream<{typeof(THandler).Name}>(topic) or AddStream<{typeof(THandler).Name}>(index).");
        }

        builder.Services.AddSingleton<THandler>();
        return Register(builder, options, options.Consumers[0], typeof(THandler), handler: null);
    }

    /// <summary>
    /// Config-driven registration by position in <c>Streams:Consumers</c>. Mirrors the shape of the
    /// Kafka-era <c>AddBatchConsumerHostedServiceV2&lt;T&gt;(index)</c> so migration is a rename.
    /// </summary>
    /// <typeparam name="THandler">The handler class; registered as a singleton.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="index">Zero-based index into <c>Streams:Consumers</c>.</param>
    /// <returns>The builder, for chaining.</returns>
    /// <exception cref="StreamConfigurationException">Thrown when <paramref name="index"/> is out of range.</exception>
    public static IHostApplicationBuilder AddStream<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        this IHostApplicationBuilder builder,
        int index)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = Core(builder);

        if (index < 0 || index >= options.Consumers.Length)
        {
            throw new StreamConfigurationException(
                $"AddStream<{typeof(THandler).Name}>({index}) is out of range: {StreamConfigBinder.SectionName}:Consumers has " +
                $"{options.Consumers.Length} entr{(options.Consumers.Length == 1 ? "y" : "ies")}.");
        }

        builder.Services.AddSingleton<THandler>();
        return Register(builder, options, options.Consumers[index], typeof(THandler), handler: null);
    }

    /// <summary>
    /// Fully explicit registration: the configured entry (when there is exactly one) is passed to
    /// <paramref name="configure"/> for <c>with</c>-modification, otherwise a defaulted entry is.
    /// </summary>
    /// <typeparam name="THandler">The handler class; registered as a singleton.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="configure">Produces the effective consumer options.</param>
    /// <returns>The builder, for chaining.</returns>
    /// <exception cref="StreamConfigurationException">Thrown when the produced options name no topic.</exception>
    public static IHostApplicationBuilder AddStream<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        this IHostApplicationBuilder builder,
        Func<ConsumerOptions, ConsumerOptions> configure)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var options = Core(builder);
        var seed = options.Consumers.Length == 1 ? options.Consumers[0] : new ConsumerOptions();

        var consumer = configure(seed)
            ?? throw new StreamConfigurationException($"AddStream<{typeof(THandler).Name}>(configure) returned null consumer options.");

        if (string.IsNullOrWhiteSpace(consumer.Topic))
        {
            throw new StreamConfigurationException(
                $"AddStream<{typeof(THandler).Name}>(configure) produced options with no Topic. Set Topic in the configure callback " +
                $"or in {StreamConfigBinder.SectionName}:Consumers.");
        }

        builder.Services.AddSingleton<THandler>();
        return Register(builder, options, consumer, typeof(THandler), handler: null);
    }

    /// <summary>
    /// Zero-config typed registration: consume <paramref name="topic"/> with
    /// <typeparamref name="THandler"/>, which implements <see cref="IBatchHandler{T}"/> or
    /// <see cref="IMessageHandler{T}"/> instead of the untyped interfaces — every body is
    /// deserialised for you before the handler is called.
    /// </summary>
    /// <typeparam name="THandler">
    /// The handler class; registered as a singleton. Must implement
    /// <see cref="IBatchHandler{TMessage}"/> or <see cref="IMessageHandler{TMessage}"/>.
    /// </typeparam>
    /// <typeparam name="TMessage">The deserialised message type.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="topic">The topic to consume.</param>
    /// <param name="typeInfo">Source-generated metadata for <typeparamref name="TMessage"/>.</param>
    /// <returns>The builder, for chaining.</returns>
    public static IHostApplicationBuilder AddStream<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler,
        TMessage>(
        this IHostApplicationBuilder builder,
        string topic,
        JsonTypeInfo<TMessage> typeInfo)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(typeInfo);

        var options = Core(builder);
        var consumer = FindByTopic(options, topic) ?? new ConsumerOptions { Topic = topic };

        builder.Services.AddSingleton<THandler>();
        return Register(builder, options, consumer, typeof(THandler), handler: null, BuildTypedWrapper<TMessage>(typeInfo));
    }

    /// <summary>
    /// Config-driven typed registration for a service with exactly one <c>Streams:Consumers</c>
    /// entry; the topic comes from that entry. See the topic-taking overload for what
    /// <typeparamref name="THandler"/> must implement.
    /// </summary>
    /// <typeparam name="THandler">The handler class; registered as a singleton.</typeparam>
    /// <typeparam name="TMessage">The deserialised message type.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="typeInfo">Source-generated metadata for <typeparamref name="TMessage"/>.</param>
    /// <returns>The builder, for chaining.</returns>
    /// <exception cref="StreamConfigurationException">Thrown when there is not exactly one configured consumer.</exception>
    public static IHostApplicationBuilder AddStream<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler,
        TMessage>(
        this IHostApplicationBuilder builder,
        JsonTypeInfo<TMessage> typeInfo)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(typeInfo);

        var options = Core(builder);

        if (options.Consumers.Length != 1)
        {
            throw new StreamConfigurationException(
                $"AddStream<{typeof(THandler).Name}, {typeof(TMessage).Name}>() takes the topic from the single " +
                $"{StreamConfigBinder.SectionName}:Consumers entry, but {options.Consumers.Length} are configured. " +
                $"Use AddStream<{typeof(THandler).Name}, {typeof(TMessage).Name}>(topic, typeInfo) or the index overload.");
        }

        builder.Services.AddSingleton<THandler>();
        return Register(builder, options, options.Consumers[0], typeof(THandler), handler: null, BuildTypedWrapper<TMessage>(typeInfo));
    }

    /// <summary>
    /// Config-driven typed registration by position in <c>Streams:Consumers</c>. See the
    /// topic-taking overload for what <typeparamref name="THandler"/> must implement.
    /// </summary>
    /// <typeparam name="THandler">The handler class; registered as a singleton.</typeparam>
    /// <typeparam name="TMessage">The deserialised message type.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="index">Zero-based index into <c>Streams:Consumers</c>.</param>
    /// <param name="typeInfo">Source-generated metadata for <typeparamref name="TMessage"/>.</param>
    /// <returns>The builder, for chaining.</returns>
    /// <exception cref="StreamConfigurationException">Thrown when <paramref name="index"/> is out of range.</exception>
    public static IHostApplicationBuilder AddStream<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler,
        TMessage>(
        this IHostApplicationBuilder builder,
        int index,
        JsonTypeInfo<TMessage> typeInfo)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(typeInfo);

        var options = Core(builder);

        if (index < 0 || index >= options.Consumers.Length)
        {
            throw new StreamConfigurationException(
                $"AddStream<{typeof(THandler).Name}, {typeof(TMessage).Name}>({index}) is out of range: " +
                $"{StreamConfigBinder.SectionName}:Consumers has " +
                $"{options.Consumers.Length} entr{(options.Consumers.Length == 1 ? "y" : "ies")}.");
        }

        builder.Services.AddSingleton<THandler>();
        return Register(builder, options, options.Consumers[index], typeof(THandler), handler: null, BuildTypedWrapper<TMessage>(typeInfo));
    }

    /// <summary>
    /// Fully explicit typed registration: the configured entry (when there is exactly one) is passed
    /// to <paramref name="configure"/> for <c>with</c>-modification, otherwise a defaulted entry is.
    /// See the topic-taking overload for what <typeparamref name="THandler"/> must implement.
    /// </summary>
    /// <typeparam name="THandler">The handler class; registered as a singleton.</typeparam>
    /// <typeparam name="TMessage">The deserialised message type.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="configure">Produces the effective consumer options.</param>
    /// <param name="typeInfo">Source-generated metadata for <typeparamref name="TMessage"/>.</param>
    /// <returns>The builder, for chaining.</returns>
    /// <exception cref="StreamConfigurationException">Thrown when the produced options name no topic.</exception>
    public static IHostApplicationBuilder AddStream<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler,
        TMessage>(
        this IHostApplicationBuilder builder,
        Func<ConsumerOptions, ConsumerOptions> configure,
        JsonTypeInfo<TMessage> typeInfo)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        ArgumentNullException.ThrowIfNull(typeInfo);

        var options = Core(builder);
        var seed = options.Consumers.Length == 1 ? options.Consumers[0] : new ConsumerOptions();

        var consumer = configure(seed)
            ?? throw new StreamConfigurationException(
                $"AddStream<{typeof(THandler).Name}, {typeof(TMessage).Name}>(configure) returned null consumer options.");

        if (string.IsNullOrWhiteSpace(consumer.Topic))
        {
            throw new StreamConfigurationException(
                $"AddStream<{typeof(THandler).Name}, {typeof(TMessage).Name}>(configure) produced options with no Topic. " +
                $"Set Topic in the configure callback or in {StreamConfigBinder.SectionName}:Consumers.");
        }

        builder.Services.AddSingleton<THandler>();
        return Register(builder, options, consumer, typeof(THandler), handler: null, BuildTypedWrapper<TMessage>(typeInfo));
    }

    /// <summary>
    /// Builds the closure that turns a resolved handler instance into a plain
    /// <see cref="IBatchHandler"/>/<see cref="IMessageHandler"/> wrapper — see
    /// <see cref="StreamConsumerRegistration.WrapAsTypedHandler"/>. Generic only in
    /// <typeparamref name="TMessage"/>: the instance's own runtime type is what the <c>switch</c>
    /// pattern-matches against, so it works for any handler class implementing
    /// <see cref="IBatchHandler{TMessage}"/> or <see cref="IMessageHandler{TMessage}"/>.
    /// </summary>
    private static Func<object, object> BuildTypedWrapper<TMessage>(JsonTypeInfo<TMessage> typeInfo) =>
        instance => instance switch
        {
            IBatchHandler<TMessage> batch => new TypedBatchHandlerAdapter<TMessage>(batch, typeInfo),
            IMessageHandler<TMessage> message => new TypedMessageHandlerAdapter<TMessage>(message, typeInfo),
            _ => throw new StreamConfigurationException(
                $"{instance.GetType().Name} implements neither IBatchHandler<{typeof(TMessage).Name}> " +
                $"nor IMessageHandler<{typeof(TMessage).Name}>."),
        };

    /// <summary>
    /// Delegate registration — consume <paramref name="topic"/> with a batch delegate, no handler
    /// class needed. The delegate is invoked once per batch, not once per message.
    /// </summary>
    /// <param name="builder">The host application builder.</param>
    /// <param name="topic">The topic to consume.</param>
    /// <param name="handler">The batch handler.</param>
    /// <returns>The builder, for chaining.</returns>
    public static IHostApplicationBuilder AddStream(
        this IHostApplicationBuilder builder,
        string topic,
        Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(handler);

        var options = Core(builder);
        var consumer = FindByTopic(options, topic) ?? new ConsumerOptions { Topic = topic };

        return Register(builder, options, consumer, handlerType: null, handler);
    }

    /// <summary>
    /// Registers a publisher for <paramref name="topic"/>, using the matching
    /// <c>Streams:Producers</c> entry when there is one and record defaults otherwise.
    /// </summary>
    /// <param name="builder">The host application builder.</param>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="buffered">
    /// Whether to buffer publishes. Omit it (the default) to take the answer from
    /// <c>Streams:Producers</c> when the topic is configured there; passing a value that contradicts
    /// the configured one throws rather than silently losing one of the two.
    /// </param>
    /// <returns>The builder, for chaining.</returns>
    /// <exception cref="StreamConfigurationException">
    /// Thrown when <paramref name="buffered"/> contradicts the configured <c>Buffered</c> for this topic.
    /// </exception>
    public static IHostApplicationBuilder AddStreamPublisher(
        this IHostApplicationBuilder builder,
        string topic,
        bool? buffered = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        var options = Core(builder);
        var configuredProducer = FindProducerByTopic(options, topic);

        // R-17: the parameter used to be silently dropped whenever a Streams:Producers entry existed,
        // so AddStreamPublisher(topic, buffered: true) against a configured topic produced a direct
        // publisher and no pump — a throughput cliff with nothing in the log to explain it. The
        // parameter is nullable so "not specified" is distinguishable from "specified as false":
        // unspecified defers to config, agreeing is fine, and disagreeing is refused with both
        // sources named. Config wins nothing by default here — neither does the argument — because a
        // silent winner is exactly the failure being removed.
        if (configuredProducer is not null && buffered is bool requested && requested != configuredProducer.Buffered)
        {
            throw new StreamConfigurationException(
                $"Streams: AddStreamPublisher(\"{topic}\", buffered: {(requested ? "true" : "false")}) contradicts " +
                $"{StreamConfigBinder.SectionName}:Producers for topic '{topic}', which sets Buffered to " +
                $"{(configuredProducer.Buffered ? "true" : "false")}. Drop the argument to take the configured value, or change the " +
                "configuration to match.");
        }

        var producer = configuredProducer ?? new ProducerOptions { Topic = topic, Buffered = buffered ?? false };

        var registry = Registry(builder);
        var before = registry.Publishers.Count;
        registry.Add(new StreamPublisherRegistration(producer));

        // A second AddStreamPublisher for the same topic is a no-op: one producer per topic is enough,
        // and registering the singleton twice would create two buffers with two pumps behind one interface.
        if (registry.Publishers.Count == before)
            return builder;

        var topicOptions = options.Topics.TryGetValue(topic, out var configured) ? configured : new TopicOptions();

        if (producer.Buffered)
        {
            // The buffered publisher owns a direct one internally: encode, route and trim live in
            // StreamPublisher alone, and the buffer adds nothing but batching.
            // object? not object: the keyed-factory delegate declares a nullable key. (It reads as
            // non-nullable through the ASP.NET shared framework's ref assemblies, which is what core
            // used to compile against before the R-25 split; object? is correct against both.)
            builder.Services.AddKeyedSingleton(topic, (IServiceProvider sp, object? _) =>
                new BufferedStreamPublisher(
                    CreateDirect(sp, topic, topicOptions),
                    producer,
                    topicOptions,
                    sp.GetService<ILoggerFactory>()?.CreateLogger("RedisEvents.Producer")));

            // Same instance under both interfaces — resolved, never re-created.
            builder.Services.AddKeyedSingleton<IStreamBufferedPublisher>(
                topic,
                (sp, key) => sp.GetRequiredKeyedService<BufferedStreamPublisher>(key));
            builder.Services.AddKeyedSingleton<IStreamPublisher>(
                topic,
                (sp, key) => sp.GetRequiredKeyedService<BufferedStreamPublisher>(key));

            // The pump. AddSingleton<IHostedService> rather than AddHostedService<T>, which
            // de-duplicates by type and would start only the first buffered topic's pump.
            builder.Services.AddSingleton<IHostedService>(
                sp => sp.GetRequiredKeyedService<BufferedStreamPublisher>(topic));
        }
        else
        {
            builder.Services.AddKeyedSingleton<IStreamPublisher>(
                topic,
                (sp, _) => CreateDirect(sp, topic, topicOptions));
        }

        RegisterDefaults(builder);
        return builder;
    }

    /// <summary>
    /// Registers the unkeyed <see cref="IStreamPublisher"/> / <see cref="IStreamBufferedPublisher"/>
    /// resolutions once per builder. They are deliberately resolved through the
    /// <see cref="StreamRegistry"/> at container-build time rather than at registration time, so the
    /// "is there exactly one publisher?" question is answered after every <c>AddStreamPublisher</c>
    /// call has run, not after the first.
    /// </summary>
    private static void RegisterDefaults(IHostApplicationBuilder builder)
    {
        if (builder.Properties.ContainsKey(PublisherDefaultsKey))
            return;

        builder.Properties[PublisherDefaultsKey] = true;

        builder.Services.AddSingleton(sp =>
            sp.GetRequiredKeyedService<IStreamPublisher>(SoleTopic(sp, typeof(IStreamPublisher))));

        builder.Services.AddSingleton(sp =>
            sp.GetRequiredKeyedService<IStreamBufferedPublisher>(SoleTopic(sp, typeof(IStreamBufferedPublisher), requireBuffered: true)));
    }

    /// <summary>
    /// The topic of the only registered publisher. A service publishing to more than one topic must
    /// ask for the one it means — <c>[FromKeyedServices("topic")]</c> — because an unkeyed
    /// resolution could otherwise silently pick the wrong stream.
    /// </summary>
    private static string SoleTopic(IServiceProvider services, Type contract, bool requireBuffered = false)
    {
        var publishers = services.GetRequiredService<StreamRegistry>().Publishers;

        if (publishers.Count == 1)
        {
            var only = publishers[0].Options;

            if (requireBuffered && !only.Buffered)
            {
                throw new StreamConfigurationException(
                    $"Streams: topic '{only.Topic}' has an unbuffered producer, so there is no {contract.Name} to resolve. " +
                    $"Register it with AddStreamPublisher(\"{only.Topic}\", buffered: true) or set {StreamConfigBinder.SectionName}:Producers Buffered to true.");
            }

            return only.Topic;
        }

        if (publishers.Count == 0)
        {
            throw new StreamConfigurationException(
                $"Streams: no publisher is registered, so {contract.Name} cannot be resolved. Call AddStreamPublisher(topic) first.");
        }

        var topics = string.Join(", ", publishers.Select(p => p.Options.Topic));
        throw new StreamConfigurationException(
            $"Streams: {publishers.Count} publishers are registered ({topics}), so an unkeyed {contract.Name} is ambiguous. " +
            $"Inject [FromKeyedServices(\"<topic>\")] {contract.Name} instead.");
    }

    /// <summary>
    /// Builds a direct publisher on the <b>shared</b> multiplexer. Writes never go through a
    /// consumer's dedicated reader connection, which may be parked in a blocking <c>XREAD</c>.
    /// </summary>
    private static StreamPublisher CreateDirect(IServiceProvider services, string topic, TopicOptions topicOptions) =>
        new(services.GetRequiredService<StreamsConnectionProvider>().Connection.GetDatabase(), topic, topicOptions);

    private static IHostApplicationBuilder Register(
        IHostApplicationBuilder builder,
        StreamOptions options,
        ConsumerOptions consumer,
        Type? handlerType,
        Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask>? handler,
        Func<object, object>? wrapAsTypedHandler = null)
    {
        var name = StreamConfigBinder.ResolveConsumerName(options, consumer);
        var registration = new StreamConsumerRegistration(consumer, name, handlerType, handler, wrapAsTypedHandler);
        Registry(builder).Add(registration);

        // One StreamConsumerHost per registration. It owns its partition workers, its position store
        // (RedisPositionStore, or none at all when Persist = None), its ownership claim, and — when
        // ReadMode = Block — a dedicated reader ConnectionMultiplexer that is deliberately NOT
        // registered as IConnectionMultiplexer so nothing else in the process can reuse it.
        //
        // AddSingleton<IHostedService> rather than AddHostedService<T>: the latter de-duplicates by
        // type, which would silently drop every consumer after the first in a service that declares
        // several. The handler is resolved once, inside Create, not per message.
        builder.Services.AddSingleton<IHostedService>(sp => StreamConsumerHost.Create(options, registration, sp));

        return builder;
    }

    /// <summary>
    /// Binds and validates the options once per builder, and registers the shared singletons.
    /// </summary>
    private static StreamOptions Core(IHostApplicationBuilder builder)
    {
        if (builder.Properties.TryGetValue(OptionsKey, out var cached) && cached is StreamOptions existing)
            return existing;

        var options = StreamConfigBinder.Bind(builder.Configuration);

        // Two passes, on purpose (R-17). This one throws: a bad key must stop the service before the
        // container is even built, and there is no logger this early. The advisory rules — the
        // "topic absent from Streams:Topics, here are its defaults" line, the Block-without-
        // co-location connection-count warning — need a real logger, so they are re-run by
        // StreamsConfigurationAdvisor once the host has one. Passing null here used to be the whole
        // story, which is why neither line had ever been printed.
        var environmentName = builder.Environment.EnvironmentName;
        StreamConfigBinder.Validate(options, environmentName, logger: null);

        builder.Properties[OptionsKey] = options;
        builder.Properties[RegistryKey] = new StreamRegistry();

        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton(_ => Registry(builder));

        // The shared multiplexer for writes, positions and admin. It is resolved lazily so the
        // reuse-or-connect decision — and its Information log line — happens once the container is
        // built and the logger factory exists.
        builder.Services.AddSingleton(sp => new StreamsConnectionProvider(
            options,
            sp,
            sp.GetService<ILoggerFactory>()?.CreateLogger("RedisEvents")));

        // Registered first, so its advisory lines land above the consumer hosts' startup logs.
        builder.Services.AddSingleton<IHostedService>(sp => new StreamsConfigurationAdvisor(
            options,
            environmentName,
            sp.GetService<ILoggerFactory>()?.CreateLogger("RedisEvents")));

        // R-26. The same failure as R-05: a complete component nothing constructed. Nothing in src
        // ever built a BackgroundTrimmer, so a topic that set BackgroundTrimIntervalSeconds and
        // RetentionSeconds got no time-based retention at all — only the inline MAXLEN on XADD,
        // which cannot express "keep six hours". Worse, the trimmer's own
        // WarnAboutIneffectiveConfiguration lives inside the type that was never built, so the
        // library could not even warn that the keys were inert.
        //
        // One trimmer per process, not one per topic: the type already sweeps every enabled topic
        // in Streams:Topics on its own loop each, so a second instance would double the XTRIM
        // traffic and race its own clamp reads. AddSingleton<IHostedService> rather than
        // AddHostedService<T> for consistency with the consumer hosts and buffered pumps above.
        //
        // The registration is conditional on an interval being configured somewhere, so a service
        // that never asked for background trimming carries no extra hosted service. The condition is
        // "an interval is set", NOT BackgroundTrimmer.IsEnabled: an interval paired with no
        // RetentionSeconds, or with Trim=None, is exactly the inert configuration
        // WarnAboutIneffectiveConfiguration exists to complain about, and it can only complain if
        // the trimmer is started.
        if (AnyBackgroundTrimInterval(options))
        {
            builder.Services.AddSingleton<IHostedService>(sp => new BackgroundTrimmer(
                options,
                sp.GetRequiredService<StreamsConnectionProvider>(),
                sp.GetService<ILoggerFactory>()?.CreateLogger("RedisEvents.Trimming")));
        }

        // No default IPositionStore registration here, and deliberately so: StreamConsumerHost.Create
        // resolves one with GetService (not GetRequiredService) and falls back to RedisPositionStore
        // when nothing is registered, so an ordinary service that never touches IPositionStore gets
        // exactly today's behaviour. A service that wants a substitute — Testing.MemoryPositionStore
        // in a test, or a custom store — registers it itself, in either order relative to AddStream:
        // builder.Services.AddSingleton<IPositionStore, MemoryPositionStore>().
        //
        // The health check is NOT registered here. R-25 moved StreamsHealthCheck (and the admin
        // endpoints) into RedisEvents.Web so this library needs neither ASP.NET Core nor the
        // health-check abstractions, and a headless worker can reference it alone. A service that
        // wants the readiness answer references that library and calls AddStreamsHealthCheck().
        // StreamsDiagnostics needs no registration — its ActivitySource and Meter are static.
        return options;
    }

    private static StreamRegistry Registry(IHostApplicationBuilder builder)
    {
        if (builder.Properties.TryGetValue(RegistryKey, out var value) && value is StreamRegistry registry)
            return registry;

        var created = new StreamRegistry();
        builder.Properties[RegistryKey] = created;
        return created;
    }

    private static ConsumerOptions? FindByTopic(StreamOptions options, string topic)
    {
        foreach (var consumer in options.Consumers)
        {
            if (string.Equals(consumer.Topic, topic, StringComparison.Ordinal))
                return consumer;
        }

        return null;
    }

    /// <summary>
    /// Whether any topic asks for background trimming at all — the gate on registering the
    /// <see cref="BackgroundTrimmer"/> hosted service.
    /// </summary>
    /// <param name="options">The bound root options.</param>
    /// <returns><see langword="true"/> when at least one topic sets a positive interval.</returns>
    /// <remarks>
    /// Deliberately looser than <see cref="BackgroundTrimmer.IsEnabled"/>: an interval set without a
    /// retention window, or with <see cref="TrimMode.None"/>, is a misconfiguration the trimmer warns
    /// about at startup, and it can only do that if it is started.
    /// </remarks>
    private static bool AnyBackgroundTrimInterval(StreamOptions options)
    {
        foreach (var (_, topic) in options.Topics)
        {
            if (topic is { BackgroundTrimIntervalSeconds: > 0 })
                return true;
        }

        return false;
    }

    private static ProducerOptions? FindProducerByTopic(StreamOptions options, string topic)
    {
        foreach (var producer in options.Producers)
        {
            if (string.Equals(producer.Topic, topic, StringComparison.Ordinal))
                return producer;
        }

        return null;
    }
}
