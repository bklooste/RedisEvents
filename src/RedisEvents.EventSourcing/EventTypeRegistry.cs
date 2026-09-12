using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

using RedisEvents.Errors;

namespace RedisEvents.EventSourcing;

/// <summary>
/// Maps event CLR types to a stable wire type string and back, and owns their (de)serialisation.
/// </summary>
/// <remarks>
/// <para>
/// <b>The wire type is chosen once and never derived from <c>.FullName</c> or <c>GetType().Name</c>.</b>
/// A stream of events outlives any particular assembly layout: renaming <c>ItemCreated</c> to
/// <c>ItemAdded</c>, or moving it to a different namespace, must never change what is already on the
/// wire. Registering an explicit string (<c>"inventory.created"</c>) means a class rename is a pure
/// refactor — replay of history recorded years earlier keeps working because the string, not the CLR
/// name, is what <see cref="IEventRepository"/> and <see cref="EventProjector"/> key off. This mirrors
/// why <see cref="AggregateRoot.AggregateName"/> is explicit rather than derived from the type.
/// </para>
/// <para>
/// <b>Pluggable serialisation.</b> <see cref="RegisterJson{TEvent}"/> covers the common,
/// AOT-compatible default via a source-generated <see cref="JsonTypeInfo{T}"/>.
/// <see cref="Register{TEvent}"/> accepts a raw lambda pair for anything else — MessagePack, a hand
/// rolled binary format, or a legacy codec being migrated away from — with no package dependency and
/// no reflection, mirroring the <c>Func&lt;ReadOnlyMemory&lt;byte&gt;, TMessage?&gt;</c> seam
/// <c>RedisEvents.MessagePack</c> already plugs into core through.
/// </para>
/// <para>
/// <b>Fail fast on developer error.</b> Two events sharing a wire type, or one CLR type registered
/// twice, is a startup bug — not a runtime condition a caller can recover from — so both throw
/// <see cref="StreamConfigurationException"/> immediately rather than silently overwriting the first
/// registration. Likewise, <see cref="Encode"/> on an unregistered CLR type is a service raising an
/// event it forgot to register: it throws loudly instead of dropping the event.
/// </para>
/// </remarks>
public sealed class EventTypeRegistry
{
    private readonly Dictionary<Type, Entry> byClrType = [];
    private readonly Dictionary<string, Entry> byWireType = [];

    /// <summary>
    /// Registers an event type with a hand rolled serialiser pair — the seam for MessagePack, or any
    /// other codec that isn't <see cref="System.Text.Json"/>.
    /// </summary>
    /// <typeparam name="TEvent">The event's CLR type.</typeparam>
    /// <param name="wireType">
    /// The stable string recorded on the wire and in stream headers. Chosen once; never derived from
    /// <c>typeof(TEvent).FullName</c> or <c>.Name</c>, so renaming <typeparamref name="TEvent"/> never
    /// breaks replay of history already written under this string.
    /// </param>
    /// <param name="serialize">Encodes an instance of <typeparamref name="TEvent"/> to its wire body.</param>
    /// <param name="deserialize">Decodes a wire body back into an instance of <typeparamref name="TEvent"/>.</param>
    /// <returns>This registry, so calls chain.</returns>
    /// <exception cref="StreamConfigurationException">
    /// <paramref name="wireType"/> or <typeparamref name="TEvent"/> is already registered.
    /// </exception>
    public EventTypeRegistry Register<TEvent>(
        string wireType,
        Func<TEvent, ReadOnlyMemory<byte>> serialize,
        Func<ReadOnlyMemory<byte>, TEvent> deserialize)
        where TEvent : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(wireType);
        ArgumentNullException.ThrowIfNull(serialize);
        ArgumentNullException.ThrowIfNull(deserialize);

        var entry = new Entry(
            wireType,
            typeof(TEvent),
            Encode: e => serialize((TEvent)e),
            Decode: body => deserialize(body),
            BindProjection: projection => projection is IProjection<TEvent>
                ? static (evt, proj, meta, ct) => ((IProjection<TEvent>)proj).HandleAsync((TEvent)evt, meta, ct)
                : null);

        Add(entry);
        return this;
    }

    /// <summary>
    /// Registers an event type using <see cref="System.Text.Json"/> with a source-generated
    /// <see cref="JsonTypeInfo{T}"/> — the default, AOT-safe path with no reflection.
    /// </summary>
    /// <typeparam name="TEvent">The event's CLR type.</typeparam>
    /// <param name="wireType">
    /// The stable string recorded on the wire and in stream headers. Chosen once; never derived from
    /// <c>typeof(TEvent).FullName</c> or <c>.Name</c>, so renaming <typeparamref name="TEvent"/> never
    /// breaks replay of history already written under this string.
    /// </param>
    /// <param name="typeInfo">
    /// The source-generated metadata for <typeparamref name="TEvent"/>, e.g. <c>MyEventsJson.Default.ItemCreated</c>.
    /// </param>
    /// <returns>This registry, so calls chain.</returns>
    /// <exception cref="StreamConfigurationException">
    /// <paramref name="wireType"/> or <typeparamref name="TEvent"/> is already registered.
    /// </exception>
    public EventTypeRegistry RegisterJson<TEvent>(string wireType, JsonTypeInfo<TEvent> typeInfo)
        where TEvent : class
    {
        ArgumentNullException.ThrowIfNull(typeInfo);

        return Register<TEvent>(
            wireType,
            e => JsonSerializer.SerializeToUtf8Bytes(e, typeInfo),
            body => JsonSerializer.Deserialize(body.Span, typeInfo)
                ?? throw new InvalidOperationException($"Decoded a null '{typeof(TEvent)}' from a non-empty event body."));
    }

    /// <summary>Encodes an event for the wire, using whichever serialiser its CLR type was registered with.</summary>
    /// <param name="event">The event instance. Its runtime type must have been registered.</param>
    /// <returns>The wire type string and the encoded body.</returns>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="event"/>'s runtime type was never registered — a service raising an event it
    /// forgot to register in <see cref="EventTypeRegistry"/> is a bug, so this is loud rather than
    /// silently dropping the event.
    /// </exception>
    public (string WireType, ReadOnlyMemory<byte> Body) Encode(object @event)
    {
        ArgumentNullException.ThrowIfNull(@event);

        var clrType = @event.GetType();
        if (!byClrType.TryGetValue(clrType, out var entry))
        {
            throw new InvalidOperationException(
                $"Event type '{clrType}' is not registered with the {nameof(EventTypeRegistry)}. " +
                $"Register it with '{nameof(Register)}' or '{nameof(RegisterJson)}' before raising it.");
        }

        return (entry.WireType, entry.Encode(@event));
    }

    /// <summary>Decodes a wire body back into an event instance, given the wire type string it was published under.</summary>
    /// <param name="wireType">The wire type string, as read from the stream entry.</param>
    /// <param name="body">The encoded event body.</param>
    /// <param name="event">The decoded event, or <see langword="null"/> if <paramref name="wireType"/> is unregistered.</param>
    /// <returns><see langword="true"/> if <paramref name="wireType"/> is registered and decoding succeeded.</returns>
    public bool TryDecode(string wireType, ReadOnlyMemory<byte> body, out object @event)
    {
        if (byWireType.TryGetValue(wireType, out var entry))
        {
            @event = entry.Decode(body);
            return true;
        }

        @event = null!;
        return false;
    }

    /// <summary>
    /// Binds a projection instance to the event type registered under <paramref name="wireType"/>, if
    /// the instance implements <see cref="IProjection{TEvent}"/> for it.
    /// </summary>
    /// <remarks>
    /// Built with zero runtime reflection: each <see cref="Register{TEvent}"/> /
    /// <see cref="RegisterJson{TEvent}"/> call captures the <c>is IProjection&lt;TEvent&gt;</c> check
    /// and the dispatch closure inside its own generic method body, where <c>TEvent</c> is statically
    /// known. This is what lets <see cref="EventProjector"/> wire a projection implementing
    /// <c>IProjection&lt;A&gt;, IProjection&lt;B&gt;</c> to both event types with no registration code
    /// of its own — see <c>PLAN-edge-cases.md</c>, "Why projections need no registration".
    /// </remarks>
    /// <param name="wireType">The wire type string.</param>
    /// <param name="projection">The projection instance to test and bind.</param>
    /// <returns>
    /// A closure that invokes <c>projection.HandleAsync</c> for the bound event type, or
    /// <see langword="null"/> if <paramref name="wireType"/> is unregistered or
    /// <paramref name="projection"/> does not implement <see cref="IProjection{TEvent}"/> for it.
    /// </returns>
    internal Func<object, object, EventMeta, CancellationToken, ValueTask>? TryBindProjection(string wireType, object projection)
    {
        ArgumentNullException.ThrowIfNull(projection);

        return byWireType.TryGetValue(wireType, out var entry) ? entry.BindProjection(projection) : null;
    }

    private void Add(Entry entry)
    {
        if (byWireType.ContainsKey(entry.WireType))
        {
            throw new StreamConfigurationException(
                $"Wire type '{entry.WireType}' is already registered with the {nameof(EventTypeRegistry)}.");
        }

        if (byClrType.TryGetValue(entry.ClrType, out var existing))
        {
            throw new StreamConfigurationException(
                $"Event type '{entry.ClrType}' is already registered under wire type '{existing.WireType}'; " +
                $"it cannot also be registered under '{entry.WireType}'.");
        }

        byWireType.Add(entry.WireType, entry);
        byClrType.Add(entry.ClrType, entry);
    }

    private sealed record Entry(
        string WireType,
        Type ClrType,
        Func<object, ReadOnlyMemory<byte>> Encode,
        Func<ReadOnlyMemory<byte>, object> Decode,
        Func<object, Func<object, object, EventMeta, CancellationToken, ValueTask>?> BindProjection);
}
