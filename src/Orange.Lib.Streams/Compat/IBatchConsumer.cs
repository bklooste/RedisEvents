namespace Orange.Lib.EventHubs.Consumer.BatchConsumer;

/// <summary>
/// A Kafka-era batch consumer. Implemented by services; invoked once per batch.
/// </summary>
/// <remarks>
/// <para>
/// <b>Compatibility shim.</b> Duplicated from <c>Orange.Lib.Kafka.Aot</c> — same namespace, same
/// method name, same parameter name — so that a migrating service's handler classes compile
/// unchanged against Orange.Lib.Streams. See <see cref="EventMsg"/> for what the shim costs and for
/// the behaviour differences a handler has to be checked against.
/// </para>
/// <para>
/// A service references either <c>Orange.Lib.Kafka.Aot</c> or <c>Orange.Lib.Streams</c>, never both:
/// this type exists in this namespace in both assemblies, so referencing both fails to compile.
/// </para>
/// <para>
/// Register an implementation with
/// <see cref="CompatExtensions.AddBatchConsumerHostedServiceV2{T}(Microsoft.Extensions.Hosting.IHostApplicationBuilder, int)"/>.
/// The implementation is resolved from DI once, at startup, and the same instance handles every
/// batch — so it must be thread-safe if the topic has more than one partition.
/// </para>
/// </remarks>
public interface IBatchConsumer
{
    /// <summary>
    /// Processes one batch of messages.
    /// </summary>
    /// <param name="msgs">
    /// The batch. Never <see langword="null"/>, and may be empty when a batch was filtered down to
    /// nothing. The array and the messages in it belong to the handler — unlike the native
    /// <c>StreamMsg</c> batch, they may be retained past the call.
    /// </param>
    /// <param name="cancellationToken">Cancelled when the host is shutting down.</param>
    /// <returns>A task that completes when the batch has been processed.</returns>
    /// <remarks>
    /// Throwing logs the batch and skips it (<c>ErrorPolicy.BestEffort</c>, the default), which is
    /// <b>not</b> what Kafka did — it retried forever. To block the partition until the failure is
    /// resolved, throw a subclass of <see cref="Orange.Lib.Streams.Errors.DontIgnoreException"/> instead
    /// (it is abstract, so the service declares its own), or retry inside the handler.
    /// </remarks>
    public Task Consume(EventMsg[] msgs, CancellationToken cancellationToken = default);
}
