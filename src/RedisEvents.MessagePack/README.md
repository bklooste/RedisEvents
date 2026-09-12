# RedisEvents.MessagePack

MessagePack-typed publish/consume for [RedisEvents](../RedisEvents/README.md), kept in a separate
package for the same reason [RedisEvents.Web](../RedisEvents.Web/README.md) is: MessagePack
(the `MessagePack` NuGet package) is a real dependency core must not carry. Reference this only if you
want MessagePack instead of — or alongside — the JSON typed convenience; the core byte-oriented API
needs no serializer at all, and a service can use JSON on one topic and MessagePack on another.

## Quickstart

```csharp
[MessagePackObject]
public sealed record Order([property: Key(0)] string Id, [property: Key(1)] decimal Stake);

public sealed class OrderHandler : IBatchHandler<Order>
{
    public ValueTask HandleAsync(ReadOnlyMemory<StreamMsg<Order>> batch, CancellationToken ct)
    {
        for (var i = 0; i < batch.Length; i++)
        {
            Order? order = batch.Span[i].Value;
            // ... handle it
        }

        return ValueTask.CompletedTask;
    }
}

builder.AddStream<OrderHandler, Order>("orders", MessagePackSerializerOptions.Standard);
```

Publishing:

```csharp
builder.AddStreamPublisher("orders");

// later, injected as IStreamPublisher
await publisher.PublishAsync(orderId, order, MessagePackSerializerOptions.Standard, ct: ct);
```

The same `PublishAsync<T>`/`PublishBatchAsync<T>` overloads exist, plus `EnqueueAsync<T>` on
`IStreamBufferedPublisher` — the same three-method shape as the JSON typed convenience in core, and
the same `IMessageHandler<T>`/`IBatchHandler<T>` interfaces, registered the same way via
`AddStream<THandler, TMessage>`. `Deserialize<T>` exists too, for a handler that wants to call it by
hand inside an untyped `IBatchHandler`/`IMessageHandler` rather than using the typed interfaces.

## Compression

A published body over 1024 bytes (uncompressed MessagePack encoding) is automatically re-serialised
with LZ4 compression (`MessagePackCompression.Lz4BlockArray`); a smaller one ships as plain MessagePack
bytes. **Decoding needs no special handling either way** — `Deserialize<T>` reads both compressed and
uncompressed bodies transparently, so a consumer never needs to know or care which one a given message
was.

Control it with `MessagePackCompressionOptions`, passed to any publish call:

```csharp
// Never compress, regardless of size:
await publisher.PublishAsync(orderId, order, options, compression: MessagePackCompressionOptions.Disabled, ct: ct);

// A different threshold:
await publisher.PublishAsync(orderId, order, options,
    compression: new MessagePackCompressionOptions { ThresholdBytes = 4096 }, ct: ct);
```

Omit `compression` and `MessagePackCompressionOptions.Default` applies — enabled, 1024-byte threshold.

**Why size-gated at all**: MessagePack-CSharp's LZ4 framing has its own fixed overhead, and running
the compressor at all costs CPU — worth it for a large payload, wasted on a small one. The library
always serialises once uncompressed to measure the actual encoded size, and only pays for a second,
compressed serialise pass above the threshold; most messages stay under it and pay for exactly one
pass.

## AOT note

None of this package's own code uses reflection — `MessagePackSerializer.Serialize`/`Deserialize` with
an explicit `MessagePackSerializerOptions` is all it calls. The caveat is MessagePack-CSharp's own
*default* resolver, which falls back to runtime code generation (`Reflection.Emit`) for types it
hasn't seen before — not AOT-safe. For a genuinely AOT-clean app, build your
`MessagePackSerializerOptions` from MessagePack-CSharp's source-generated resolver (mark your message
types `partial` with `[MessagePackObject]`; its own Roslyn generator emits the formatters at compile
time) rather than relying on the default resolver — the same discipline `JsonSerializerContext` already
asks for on the JSON side of RedisEvents.
