namespace RedisEvents.EventSourcing;

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
/// carries forward only the facts a projection actually needs (<see cref="AggregateId"/>,
/// <see cref="Version"/>, <see cref="Id"/>, <see cref="CorrelationId"/>) and nothing that outlives
/// the batch.
/// </para>
/// </remarks>
/// <param name="AggregateId">
/// The id of the aggregate that raised the event — the wire message's <c>PartitionKey</c>, since
/// every event of one aggregate is published with the aggregate id as its partition key.
/// </param>
/// <param name="Version">
/// The event's 1-based position in its aggregate's stream, read from the <c>es-version</c> header
/// <c>RedisEventRepository</c> stamps on every published event. Lets a projection assert
/// per-aggregate monotonicity or skip a stale at-least-once redelivery.
/// </param>
/// <param name="Id">The Redis stream entry id the event was read at.</param>
/// <param name="CorrelationId">The publisher's correlation id, or empty when none was set.</param>
public readonly record struct EventMeta(string AggregateId, int Version, RedisEvents.Wire.StreamId Id, string CorrelationId);
