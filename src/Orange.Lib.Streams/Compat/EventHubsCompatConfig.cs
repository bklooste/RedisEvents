using Microsoft.Extensions.Configuration;

using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Errors;

namespace Orange.Lib.Streams.Compat;

/// <summary>
/// Reads the Kafka-era <c>EventHubs:&lt;namespace&gt;:BatchConsumers[]</c> configuration and maps it
/// onto <see cref="ConsumerOptions"/>, so a service on the compatibility shim keeps its existing
/// configuration and its existing numbered registrations during the transition.
/// </summary>
/// <remarks>
/// <para>
/// <b>The mapping.</b> Four keys carry over and nothing else does:
/// </para>
/// <list type="table">
///   <listheader><term><c>EventHubs</c></term><description><see cref="ConsumerOptions"/></description></listheader>
///   <item><term><c>EventHubName</c></term><description><see cref="ConsumerOptions.Topic"/>, verbatim — no environment prefix is applied, matching what the Kafka batch-consumer path did.</description></item>
///   <item><term><c>BatchSize</c></term><description><see cref="ConsumerOptions.BatchSize"/>, defaulting to 100 exactly as the Kafka reader did.</description></item>
///   <item><term><c>Filter</c></term><description><see cref="ConsumerOptions.Filter"/>. Still a match on the type string; the reflective type registry — and its "type not registered" startup warning — is gone, so a filter listing a <c>FullName</c> keeps working and an incorrect one now fails silently. Test the filters.</description></item>
///   <item><term><c>ConsumerGroup</c></term><description><see cref="ConsumerOptions.Consumer"/> — the owner of the stored position. <c>$Default</c>, <c>$Dummy</c> and an absent value all map to <see langword="null"/>, which lets <c>Streams:Consumer</c> (or the entry assembly name) supply it, because those three meant "no group of my own" on the Kafka side.</description></item>
/// </list>
/// <para>
/// <c>PartitionOwnershipExpirationIntervalSeconds</c> is dropped: it configured Event Hubs' lease
/// renewal, and streams partition ownership is not leased that way. Everything else about a
/// consumer — read mode, persistence, error policy, backpressure, instances — takes
/// <see cref="ConsumerOptions"/> defaults, which is what the override below is for.
/// </para>
/// <para>
/// <b>A <c>Streams:Consumers</c> entry for the same topic wins outright.</b> A shim consumer is
/// still a real streams consumer, and half of the migration checklist — <c>UseConsumerGroup</c> for
/// a service that relied on Kafka rebalancing, <c>Persist</c>, <c>OnError</c>, <c>ReadMode</c> —
/// lives on keys the <c>EventHubs:</c> section cannot express. Adding a <c>Streams:Consumers</c>
/// entry naming the topic replaces the mapped entry wholesale (not key by key), which is the same
/// rule <c>AddStream&lt;T&gt;(topic)</c> already follows: one entry, one source, nothing merged
/// silently from two places.
/// </para>
/// <para>
/// <b>The index gotcha.</b> Index-based registration —
/// <c>AddBatchConsumerHostedServiceV2&lt;T&gt;(2)</c> — indexes the <em>flattened</em> list of every
/// namespace's <c>BatchConsumers</c>, concatenated in configuration order. With the single
/// <c>EventHubs:common</c> section that services actually have, that is simply the position in that
/// section's array; with two sections it is not, and the index of a consumer in the second section
/// shifts whenever the first section gains or loses an entry. This mirrors the producer-side gotcha
/// already recorded for <c>GetEventHubProducer(domain, index)</c>, where the index is relative to
/// that domain's array. The flattening is reproduced here on purpose, bug-for-bug: renumbering
/// during a transport migration would wire a service's handlers to the wrong topics with everything
/// still compiling. Registering by topic name avoids the whole question and is the better move for
/// any service touching these lines anyway.
/// </para>
/// <para>
/// <b>Namespace sections without a <c>Namespace</c> key are skipped</b>, exactly as the Kafka reader
/// skipped them, because that skipping is what determines the flattened index. Services set
/// <c>Namespace</c> from the environment (<c>DOTNET_EventHubs__common__Namespace</c>) rather than
/// from <c>appsettings.json</c>, so a service that drops that variable when it stops using Kafka
/// would find its consumers silently gone — hence the exception message below names the cause
/// rather than reporting an empty list.
/// </para>
/// </remarks>
public static class EventHubsCompatConfig
{
    /// <summary>The configuration section the Kafka-era consumer configuration is read from.</summary>
    public const string SectionName = "EventHubs";

    /// <summary>Consumer-group values that meant "no group of my own" on the Kafka/Event Hubs side.</summary>
    private const string DefaultConsumerGroup = "$Default";
    private const string DummyConsumerGroup = "$Dummy";

    /// <summary>
    /// Every configured batch consumer, mapped onto <see cref="ConsumerOptions"/> and flattened
    /// across namespaces in configuration order — the order index-based registration indexes into.
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The mapped consumers; empty when nothing is configured.</returns>
    public static ConsumerOptions[] ReadBatchConsumers(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        return Read(configuration, out _);
    }

    /// <summary>
    /// The consumer options for a numbered registration, after the
    /// <c>Streams:Consumers</c> override has been applied.
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <param name="index">Zero-based index into the flattened <c>BatchConsumers</c> list.</param>
    /// <returns>The options to register the consumer with.</returns>
    /// <exception cref="StreamConfigurationException">The index names no configured consumer.</exception>
    public static ConsumerOptions ResolveByIndex(IConfiguration configuration, int index)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var consumers = Read(configuration, out var skipped);

        if (index < 0 || index >= consumers.Length)
        {
            throw new StreamConfigurationException(
                $"Streams compat: AddBatchConsumerHostedServiceV2<T>({index}) is out of range — " +
                $"{SectionName}:*:BatchConsumers has {consumers.Length} entr{(consumers.Length == 1 ? "y" : "ies")}" +
                $"{Describe(consumers)}.{SkippedHint(skipped)}");
        }

        return Override(configuration, consumers[index]);
    }

    /// <summary>
    /// The consumer options for a registration by topic name, after the <c>Streams:Consumers</c>
    /// override has been applied.
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <param name="topic">The topic, as spelled in <c>EventHubName</c>.</param>
    /// <returns>The options to register the consumer with.</returns>
    /// <exception cref="StreamConfigurationException">No configured consumer names that topic.</exception>
    public static ConsumerOptions ResolveByTopic(IConfiguration configuration, string topic)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        var consumers = Read(configuration, out var skipped);

        foreach (var consumer in consumers)
        {
            if (string.Equals(consumer.Topic, topic, StringComparison.Ordinal))
            {
                return Override(configuration, consumer);
            }
        }

        throw new StreamConfigurationException(
            $"Streams compat: AddBatchConsumerHostedServiceV2<T>(\"{topic}\") found no {SectionName}:*:BatchConsumers entry " +
            $"with that EventHubName{Describe(consumers)}.{SkippedHint(skipped)}");
    }

    /// <summary>
    /// Reads and flattens the configured batch consumers.
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <param name="skipped">How many namespace sections were skipped for want of a <c>Namespace</c> key.</param>
    /// <returns>The mapped consumers, in flattened configuration order.</returns>
    private static ConsumerOptions[] Read(IConfiguration configuration, out int skipped)
    {
        skipped = 0;
        List<ConsumerOptions>? consumers = null;

        foreach (var namespaceSection in configuration.GetSection(SectionName).GetChildren())
        {
            // The Kafka reader dropped any namespace missing a Namespace value, and the flattened
            // index counts what survives that drop — so the shim drops them too. See the class
            // remarks: this is index parity, not a taste for Azure coordinates.
            if (string.IsNullOrEmpty(namespaceSection["Namespace"]))
            {
                if (namespaceSection.GetSection("BatchConsumers").GetChildren().Any())
                {
                    skipped++;
                }

                continue;
            }

            foreach (var entry in namespaceSection.GetSection("BatchConsumers").GetChildren())
            {
                var topic = entry["EventHubName"];
                if (string.IsNullOrEmpty(topic))
                {
                    // Matches the Kafka reader, which skipped a nameless entry rather than failing.
                    continue;
                }

                (consumers ??= []).Add(Map(entry, topic));
            }
        }

        return consumers is null ? [] : consumers.ToArray();
    }

    /// <summary>Maps one <c>BatchConsumers[i]</c> entry onto <see cref="ConsumerOptions"/>.</summary>
    /// <param name="entry">The configuration entry.</param>
    /// <param name="topic">Its <c>EventHubName</c>, already known to be non-empty.</param>
    /// <returns>The mapped options.</returns>
    private static ConsumerOptions Map(IConfigurationSection entry, string topic)
    {
        var options = new ConsumerOptions { Topic = topic };

        // int.TryParse-with-a-default, like the Kafka reader: a BatchSize of "ten" is ignored rather
        // than fatal, because that is the behaviour the existing configuration was validated against.
        if (int.TryParse(entry["BatchSize"], out var batchSize))
        {
            options.BatchSize = batchSize;
        }

        var group = entry["ConsumerGroup"];
        if (!string.IsNullOrEmpty(group) &&
            !string.Equals(group, DefaultConsumerGroup, StringComparison.Ordinal) &&
            !string.Equals(group, DummyConsumerGroup, StringComparison.Ordinal))
        {
            options.Consumer = group;
        }

        List<string>? filter = null;
        foreach (var type in entry.GetSection("Filter").GetChildren())
        {
            if (!string.IsNullOrEmpty(type.Value))
            {
                (filter ??= []).Add(type.Value);
            }
        }

        if (filter is not null)
        {
            options.Filter = filter.ToArray();
        }

        return options;
    }

    /// <summary>
    /// Replaces the mapped entry with the <c>Streams:Consumers</c> entry for the same topic when one
    /// exists — see the class remarks for why the override is wholesale rather than per key.
    /// </summary>
    /// <param name="configuration">The application configuration.</param>
    /// <param name="mapped">The entry mapped from <c>EventHubs:</c>.</param>
    /// <returns>The effective consumer options.</returns>
    private static ConsumerOptions Override(IConfiguration configuration, ConsumerOptions mapped)
    {
        var streams = StreamConfigBinder.Bind(configuration);

        foreach (var consumer in streams.Consumers)
        {
            if (string.Equals(consumer.Topic, mapped.Topic, StringComparison.Ordinal))
            {
                return consumer;
            }
        }

        return mapped;
    }

    /// <summary>Lists the topics found, for an exception message.</summary>
    private static string Describe(ConsumerOptions[] consumers)
    {
        if (consumers.Length == 0)
        {
            return string.Empty;
        }

        var topics = new string[consumers.Length];
        for (var i = 0; i < consumers.Length; i++)
        {
            topics[i] = $"[{i}] {consumers[i].Topic}";
        }

        return $" ({string.Join(", ", topics)})";
    }

    /// <summary>Names the skipped-namespace cause when it is the likely explanation.</summary>
    private static string SkippedHint(int skipped) =>
        skipped == 0
            ? string.Empty
            : $" {skipped} {SectionName} section{(skipped == 1 ? " was" : "s were")} ignored for having BatchConsumers but no Namespace value — " +
              "services set it from the environment (DOTNET_EventHubs__<ns>__Namespace), so check that variable still reaches this service.";
}
