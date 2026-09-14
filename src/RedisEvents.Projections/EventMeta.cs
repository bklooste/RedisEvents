namespace RedisEvents.Projections;

/// <summary>
/// The envelope facts about one decoded event, handed to an <see cref="IProjection{TEvent}"/>
/// alongside the event itself.
/// </summary>
/// <remarks>
/// <para>
/// <b>A value type passed by value, not <c>in RedisEvents.Wire.StreamMsg</c>.</b>
/// <see cref="EventProjector"/> could have handed projections the raw <c>StreamMsg</c> it read
/// off the wire — core's own <c>IMessageHandler</c> does exactly that, via
/// <c>HandleAsync(in StreamMsg …)</c>. Two reasons rule it out here. First, <c>async</c> methods
/// cannot take <c>in</c> or <c>ref</c> parameters, and every real projection — Redis, Azure
/// Tables, anything that talks to a store — is <c>async</c>; core's handlers can get away with
/// <c>in</c> because they are often synchronous and hot, a tradeoff that does not hold for
/// projections. Second, by the time a projection sees an event it has already been decoded by
/// <see cref="EventTypeRegistry.TryDecode"/>, so there is nothing left for it to do with
/// <c>StreamMsg.Body</c> except misuse the pooled buffer that memory aliases — <see cref="EventMeta"/>
/// carries forward only the facts a projection actually needs (<see cref="PartitionKey"/>,
/// <see cref="Id"/>, <see cref="CorrelationId"/>) and nothing that outlives the batch.
/// </para>
/// <para>
/// <b>No aggregate version here, by design.</b> The read side depends only on standard RedisEvents
/// streams — a topic's own partitioning and per-partition ordering — not on whether the write side
/// happens to be an event-sourced aggregate (<c>RedisEvents.EventSourcing.AggregateRoot</c>, in the
/// sibling package). Redis already assigns every entry a monotonically
/// increasing <see cref="Id"/> within its partition, and a topic is partitioned by key, so all events
/// for one key arrive at one projector instance in publish order for free. That is everything an
/// idempotent projection needs: a redelivery is a replay of an already-seen contiguous prefix, never
/// a reordering, so a running total or append-style update just needs to compare the incoming
/// <see cref="Id"/> (it implements <see cref="IComparable{T}"/>) against the last one it applied and
/// skip when it is not greater — no manufactured version number, and no dependency on the event
/// having come from <c>RedisEvents.EventSourcing</c>'s <c>AddEventStore</c> at all. A set-semantics update (replace the whole
/// view) needs no guard either way.
/// </para>
/// </remarks>
/// <param name="PartitionKey">
/// The wire message's own partition key — the key every event sharing one ordered history was
/// published under, whatever the write side that key happens to identify (an aggregate id is the
/// common case, but this field carries no assumption that one exists).
/// </param>
/// <param name="Id">
/// The Redis stream entry id the event was read at: unique and strictly increasing within its
/// partition, and identical on every redelivery of that entry. The natural ordering and dedupe key
/// for a projection — see the remarks above.
/// </param>
/// <param name="CorrelationId">The publisher's correlation id, or empty when none was set.</param>
public readonly record struct EventMeta(string PartitionKey, RedisEvents.Wire.StreamId Id, string CorrelationId);
