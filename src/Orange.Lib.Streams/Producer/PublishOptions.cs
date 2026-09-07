namespace Orange.Lib.Streams.Producer;

/// <summary>
/// Optional metadata for a stream publish operation.
/// </summary>
/// <param name="CorrelationId">Optional correlation id, carried through to the consumer's log scope.</param>
/// <param name="Headers">Optional custom headers, packed into one field on the entry.</param>
/// <param name="Partition">Optional explicit partition override; when null, the partition key is used for routing.</param>
public readonly record struct PublishOptions(
    string? CorrelationId = null,
    IReadOnlyList<KeyValuePair<string, string>>? Headers = null,
    int? Partition = null);
