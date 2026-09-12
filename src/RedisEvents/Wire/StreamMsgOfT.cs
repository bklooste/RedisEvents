namespace RedisEvents.Wire;

/// <summary>
/// A message paired with its deserialised value — what a typed handler (<c>IMessageHandler&lt;T&gt;</c>
/// / <c>IBatchHandler&lt;T&gt;</c>) receives instead of a raw <see cref="StreamMsg"/>.
/// </summary>
/// <remarks>
/// <see cref="Envelope"/> keeps every field a handler might still need — <see cref="StreamMsg.Id"/>
/// for idempotency, <see cref="StreamMsg.Partition"/>, headers, correlation id — so moving to a typed
/// handler costs nothing beyond the deserialisation itself.
/// </remarks>
/// <typeparam name="T">The deserialised message type.</typeparam>
/// <param name="Value">
/// The deserialised body, or <see langword="null"/> when the body was the JSON literal <c>null</c>.
/// </param>
/// <param name="Envelope">The original message, valid for the same duration <see cref="StreamMsg"/> is.</param>
public readonly record struct StreamMsg<T>(T? Value, StreamMsg Envelope);
