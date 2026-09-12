namespace RedisEvents.Producer;

/// <summary>
/// One event to append to a state stream and publish to its topic, for
/// <see cref="IStreamStore.AppendAndPublishAsync"/>.
/// </summary>
/// <remarks>
/// It is <see cref="OutboxPublish"/> minus everything the store already knows: the topic, the
/// partition key and the topic's options are properties of the call, not of the event, so a batch
/// cannot accidentally carry two different ones and split itself across partitions.
/// </remarks>
/// <param name="Body">
/// The event body. Passed to Redis by reference — not copied — so it must not be mutated until the
/// returned task completes.
/// </param>
/// <param name="Type">The event type string; consumers filter on it, so it is required.</param>
/// <param name="Options">Correlation id and headers. An explicit <see cref="PublishOptions.Partition"/> is honoured for the topic publish exactly as a plain publish honours it.</param>
public readonly record struct StateEvent(
    ReadOnlyMemory<byte> Body,
    string Type,
    PublishOptions Options = default);
