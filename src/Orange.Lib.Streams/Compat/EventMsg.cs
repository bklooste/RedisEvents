namespace Orange.Lib.EventHubs.Consumer.BatchConsumer;

/// <summary>
/// One message as a Kafka-era batch consumer expects to see it.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is a compatibility shim, not part of the Orange.Lib.Streams API.</b> It is duplicated —
/// name for name, parameter for parameter — from <c>Orange.Lib.Kafka.Aot</c>, which duplicated it
/// from <c>Orange.Lib.EventHubs.Aot</c> before that. Declaring it in the namespace services already
/// import is the entire trick: a service swaps its <c>.csproj</c> project reference and its handler
/// bodies, <c>using</c> statements and DI registrations keep compiling unchanged.
/// </para>
/// <para>
/// <b>A service references either <c>Orange.Lib.Kafka.Aot</c> or <c>Orange.Lib.Streams</c>, never
/// both.</b> Both assemblies declare this type in this namespace, so referencing both fails to
/// compile (CS0433) at every use of <c>EventMsg</c>. That is deliberate: it makes a cutover atomic
/// per service and impossible to half-do.
/// </para>
/// <para>
/// <b>What it costs.</b> The native pipeline hands a handler a
/// <c>ReadOnlyMemory&lt;Orange.Lib.Streams.Wire.StreamMsg&gt;</c> over a pooled array and allocates
/// nothing per message. Producing this type gives part of that back, per batch: one
/// <c>EventMsg[]</c>, one byte array holding every body, one <c>EventMsg</c> object per message,
/// one string per message for <see cref="OffsetString"/>, and one dictionary per message that
/// carries headers. It is a migration aid with a known price, not a destination — each service
/// converts to <c>IBatchHandler</c> + <c>StreamMsg</c> in a follow-up commit (P4-15) and the shim is
/// deleted once nothing references it (P4-16).
/// </para>
/// <para>
/// <b>Field-by-field differences from the Kafka original</b> — the fields carry the same meanings,
/// but two of them are worth knowing before a handler is trusted on the new transport:
/// </para>
/// <list type="bullet">
///   <item>
///     <description>
///     <see cref="OffsetString"/> is a Redis stream id (<c>"&lt;unix-millis&gt;-&lt;seq&gt;"</c>),
///     not a Kafka offset. It is still ordered within a partition and still unique, but it does not
///     parse as a number. A handler that did <c>long.Parse(msg.OffsetString)</c> — arithmetic on it,
///     or storing it in a numeric column — is the one thing here that breaks at runtime rather than
///     at compile time.
///     </description>
///   </item>
///   <item>
///     <description>
///     <see cref="EnqueuedTime"/> comes from the stream id's millisecond component, so it is
///     Redis's clock at <c>XADD</c> rather than the producer's. That removes a clock-skew caveat
///     rather than adding one, but it is a different number.
///     </description>
///   </item>
///   <item>
///     <description>
///     <see cref="Type"/> is <see langword="null"/> when the entry carried no type, exactly as the
///     Kafka path left it — several handlers branch on <c>msg.Type == null</c> to pick up untyped
///     payloads, and an empty string there would silently route them nowhere. The declared type is
///     non-nullable only because the original was.
///     </description>
///   </item>
///   <item>
///     <description>
///     <see cref="Body"/> is a private copy, not a window onto the transport's read buffer, so
///     retaining a message past <see cref="IBatchConsumer.Consume"/> is as safe as it was on Kafka.
///     That copy is the reason the shim allocates a byte array per batch; the native API asks
///     handlers to call <c>Copy()</c> instead and charges nothing to the handlers that do not.
///     </description>
///   </item>
/// </list>
/// <para>
/// <b>Error handling changed and the type cannot express it.</b> Kafka retried a throwing batch
/// forever; the streams pipeline logs and skips it under the default
/// <c>ErrorPolicy.BestEffort</c>. A handler that leaned on infinite retry now drops messages. See
/// the per-service checklist in <c>docs/plans/2026-09-06-orange-lib-streams/08-migration.md</c>,
/// item 1 — it is the single most important line in this migration.
/// </para>
/// </remarks>
/// <param name="Body">The raw message body, owned by this message.</param>
/// <param name="Type">The message type string, by convention <c>typeof(T).FullName</c>; <see langword="null"/> when the entry carried none.</param>
/// <param name="CorrelationId">The publisher's correlation id, or an empty string when none was set.</param>
/// <param name="OffsetString">The Redis stream entry id, formatted <c>"&lt;unix-millis&gt;-&lt;seq&gt;"</c>.</param>
/// <param name="PartitionKey">The key the message was routed on; empty for a round-robin publish.</param>
/// <param name="EnqueuedTime">When Redis accepted the entry.</param>
/// <param name="Headers">Custom headers, or <see langword="null"/> when the message carried none.</param>
public record EventMsg(
    ReadOnlyMemory<byte> Body,
    string Type,
    string CorrelationId,
    string OffsetString,
    string PartitionKey,
    DateTimeOffset EnqueuedTime,
    IReadOnlyDictionary<string, string>? Headers = null);
