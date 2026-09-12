using System.Diagnostics.CodeAnalysis;
using global::MessagePack;
using Microsoft.Extensions.Hosting;
using RedisEvents.Config;
using RedisEvents.Extensions;

namespace RedisEvents.MessagePack;

/// <summary>
/// Typed registration for MessagePack handlers — the MessagePack sibling of the
/// <c>JsonTypeInfo&lt;TMessage&gt;</c>-based <c>AddStream&lt;THandler, TMessage&gt;</c> overloads in
/// core. Each overload here is a one-line call into core's serialiser-agnostic
/// <c>AddStream&lt;THandler, TMessage&gt;(..., Func&lt;ReadOnlyMemory&lt;byte&gt;, TMessage?&gt;)</c>
/// overload — no access to core's internal registration plumbing is needed.
/// </summary>
public static class MessagePackStreamsBuilderExtensions
{
    /// <summary>
    /// Zero-config typed registration: consume <paramref name="topic"/> with
    /// <typeparamref name="THandler"/>, deserialising every body as MessagePack before the handler is
    /// called. <typeparamref name="THandler"/> must implement <c>IBatchHandler&lt;TMessage&gt;</c> or
    /// <c>IMessageHandler&lt;TMessage&gt;</c>, exactly as for the JSON-typed <c>AddStream</c> overloads
    /// in core.
    /// </summary>
    /// <typeparam name="THandler">The handler class; registered as a singleton.</typeparam>
    /// <typeparam name="TMessage">The deserialised message type.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="topic">The topic to consume.</param>
    /// <param name="options">The MessagePack serialiser options.</param>
    /// <returns>The builder, for chaining.</returns>
    public static IHostApplicationBuilder AddStream<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler,
        TMessage>(
        this IHostApplicationBuilder builder,
        string topic,
        MessagePackSerializerOptions options)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(options);
        return builder.AddStream<THandler, TMessage>(topic, body => MessagePackSerializer.Deserialize<TMessage>(body, options));
    }

    /// <summary>Config-driven typed registration for a service with exactly one <c>Streams:Consumers</c> entry.</summary>
    /// <typeparam name="THandler">The handler class; registered as a singleton.</typeparam>
    /// <typeparam name="TMessage">The deserialised message type.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="options">The MessagePack serialiser options.</param>
    /// <returns>The builder, for chaining.</returns>
    public static IHostApplicationBuilder AddStream<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler,
        TMessage>(
        this IHostApplicationBuilder builder,
        MessagePackSerializerOptions options)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(options);
        return builder.AddStream<THandler, TMessage>(body => MessagePackSerializer.Deserialize<TMessage>(body, options));
    }

    /// <summary>Config-driven typed registration by position in <c>Streams:Consumers</c>.</summary>
    /// <typeparam name="THandler">The handler class; registered as a singleton.</typeparam>
    /// <typeparam name="TMessage">The deserialised message type.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="index">Zero-based index into <c>Streams:Consumers</c>.</param>
    /// <param name="options">The MessagePack serialiser options.</param>
    /// <returns>The builder, for chaining.</returns>
    public static IHostApplicationBuilder AddStream<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler,
        TMessage>(
        this IHostApplicationBuilder builder,
        int index,
        MessagePackSerializerOptions options)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(options);
        return builder.AddStream<THandler, TMessage>(index, body => MessagePackSerializer.Deserialize<TMessage>(body, options));
    }

    /// <summary>Fully explicit typed registration: <paramref name="configure"/> produces the effective consumer options.</summary>
    /// <typeparam name="THandler">The handler class; registered as a singleton.</typeparam>
    /// <typeparam name="TMessage">The deserialised message type.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="configure">Produces the effective consumer options.</param>
    /// <param name="options">The MessagePack serialiser options.</param>
    /// <returns>The builder, for chaining.</returns>
    public static IHostApplicationBuilder AddStream<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler,
        TMessage>(
        this IHostApplicationBuilder builder,
        Func<ConsumerOptions, ConsumerOptions> configure,
        MessagePackSerializerOptions options)
        where THandler : class
    {
        ArgumentNullException.ThrowIfNull(options);
        return builder.AddStream<THandler, TMessage>(configure, body => MessagePackSerializer.Deserialize<TMessage>(body, options));
    }
}
