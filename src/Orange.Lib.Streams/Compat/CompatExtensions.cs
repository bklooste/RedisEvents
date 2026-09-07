using System.Diagnostics.CodeAnalysis;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Orange.Lib.Streams.Compat;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Extensions;

namespace Orange.Lib.EventHubs.Consumer.BatchConsumer;

/// <summary>
/// The Kafka-era registration calls, backed by Orange.Lib.Streams. Registering a consumer through
/// these is the same thing as calling <c>AddStream&lt;THandler&gt;</c>: it produces one
/// <c>StreamConsumerHost</c> reading Redis Streams, with the handler wrapped so it still sees
/// <see cref="EventMsg"/><c>[]</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>What a migrating service changes.</b> One <c>.csproj</c> line —
/// <c>Orange.Lib.Kafka.Aot</c> → <c>Orange.Lib.Streams</c> — plus configuration, plus (in
/// <c>Program.cs</c> only) swapping <c>using Orange.Lib.Kafka.Aot;</c> for
/// <c>using Orange.Lib.EventHubs.Consumer.BatchConsumer;</c>, which is the namespace the handler
/// files already import. Handler bodies, <see cref="IBatchConsumer"/> implementations and the
/// numbered registrations themselves are untouched. The extra <c>using</c> is unavoidable: the
/// Kafka registration helper lived in the <c>Orange.Lib.Kafka.Aot</c> namespace, and declaring a
/// namespace of that name inside this library would be a worse lie than the one the shim already
/// tells.
/// </para>
/// <para>
/// <b>A service references either <c>Orange.Lib.Kafka.Aot</c> or <c>Orange.Lib.Streams</c>, never
/// both.</b> <see cref="EventMsg"/> and <see cref="IBatchConsumer"/> exist in this namespace in both
/// assemblies, so a service that references both fails to compile at every use of them. That is the
/// enforcement mechanism, not an accident: a transport cutover is atomic per service, and a
/// half-migrated service — some consumers on Kafka, some on Redis, one topic's producer on the other
/// side of the fence from its consumers — is the failure mode worth making impossible. The wire
/// formats are not compatible and there is no bridge, so a topic's producer and all of its consumers
/// move together.
/// </para>
/// <para>
/// <b>Configuration.</b> The existing <c>EventHubs:&lt;namespace&gt;:BatchConsumers[]</c> section is
/// read as-is and mapped onto <see cref="ConsumerOptions"/> by
/// <see cref="EventHubsCompatConfig"/> — topic, batch size, filter and consumer group — so numbered
/// registrations keep resolving to the same consumers. Anything the old section cannot express (read
/// mode, persistence, error policy, consumer groups, instances) is set by adding a
/// <c>Streams:Consumers</c> entry for that topic, which then replaces the mapped one. The
/// <c>Streams:</c> section also supplies the connection string and per-topic partitioning and
/// retention; every one of those has a default, so a service can start with no <c>Streams:</c>
/// section at all.
/// </para>
/// <para>
/// <b>What the shim does not paper over</b> — the per-service checklist in
/// <c>docs/plans/2026-09-06-orange-lib-streams/08-migration.md</c> is the list that matters, and its
/// first item is the big one: a throwing batch is logged and skipped rather than retried forever, so
/// a handler that leaned on Kafka's infinite retry now drops messages unless it retries internally or
/// throws a <c>DontIgnoreException</c> subclass.
/// </para>
/// <para>
/// <b>It is temporary.</b> See <see cref="EventMsg"/> for what the conversion costs per batch. Each
/// migrated service converts its handlers to <c>IBatchHandler</c> + <c>StreamMsg</c> in a follow-up
/// commit (P4-15), and this whole folder is deleted once nothing references it (P4-16).
/// </para>
/// </remarks>
public static class CompatExtensions
{
    /// <summary>
    /// Registers <typeparamref name="T"/> as the consumer for the <paramref name="index"/>-th
    /// configured batch consumer, exactly as the Kafka overload of the same name did.
    /// </summary>
    /// <typeparam name="T">The service's batch consumer; registered as a singleton.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="index">
    /// Zero-based index into the <em>flattened</em> <c>EventHubs:*:BatchConsumers</c> list —
    /// every namespace's array concatenated in configuration order. With the single
    /// <c>EventHubs:common</c> section services actually have, that is the position in that array;
    /// with more than one section it is not, and an entry added to the first section renumbers the
    /// second. Registering by topic name sidesteps it.
    /// </param>
    /// <returns>The builder, for chaining.</returns>
    /// <exception cref="Orange.Lib.Streams.Errors.StreamConfigurationException">
    /// The index names no configured consumer; the message lists the ones that were found.
    /// </exception>
    public static IHostApplicationBuilder AddBatchConsumerHostedServiceV2<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>(
        this IHostApplicationBuilder builder,
        int index = 0)
        where T : class, IBatchConsumer
    {
        ArgumentNullException.ThrowIfNull(builder);
        return Add<T>(builder, EventHubsCompatConfig.ResolveByIndex(builder.Configuration, index));
    }

    /// <summary>
    /// Registers <typeparamref name="T"/> as the consumer for the configured batch consumer whose
    /// <c>EventHubName</c> is <paramref name="topicName"/>.
    /// </summary>
    /// <typeparam name="T">The service's batch consumer; registered as a singleton.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="topicName">The topic, as spelled in <c>EventHubName</c>.</param>
    /// <returns>The builder, for chaining.</returns>
    /// <exception cref="Orange.Lib.Streams.Errors.StreamConfigurationException">
    /// No configured consumer names that topic; the message lists the ones that were found.
    /// </exception>
    public static IHostApplicationBuilder AddBatchConsumerHostedServiceV2<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>(
        this IHostApplicationBuilder builder,
        string topicName)
        where T : class, IBatchConsumer
    {
        ArgumentNullException.ThrowIfNull(builder);
        return Add<T>(builder, EventHubsCompatConfig.ResolveByTopic(builder.Configuration, topicName));
    }

    /// <summary>
    /// Registers the consumer and the adapter that feeds it, then hands the options to the ordinary
    /// <c>AddStream</c> path — so a shim consumer is a normal registration in the
    /// <c>StreamRegistry</c>, subject to the same duplicate-<c>(Topic, Consumer)</c> check and
    /// producing the same hosted service as a native one.
    /// </summary>
    /// <typeparam name="T">The service's batch consumer.</typeparam>
    /// <param name="builder">The host application builder.</param>
    /// <param name="options">The resolved consumer options.</param>
    /// <returns>The builder, for chaining.</returns>
    private static IHostApplicationBuilder Add<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>(
        IHostApplicationBuilder builder,
        ConsumerOptions options)
        where T : class, IBatchConsumer
    {
        // The consumer itself, so the adapter can take it as a constructor dependency. AddSingleton
        // rather than AddSingleton<IBatchConsumer> because a service registers several of these and
        // each must stay resolvable by its own type — the same reason the Kafka helper did it.
        builder.Services.AddSingleton<T>();

        // The configure overload is the only one that takes options from outside configuration; the
        // seed it passes is the Streams:Consumers entry, which EventHubsCompatConfig has already
        // consulted, so it is discarded here rather than merged.
        return builder.AddStream<CompatBatchHandler<T>>(_ => options);
    }
}
