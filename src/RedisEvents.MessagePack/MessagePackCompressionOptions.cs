namespace RedisEvents.MessagePack;

/// <summary>
/// Controls automatic LZ4 compression of published MessagePack bodies.
/// </summary>
/// <remarks>
/// A publish-side-only concern: decoding never needs to know whether a body was compressed —
/// <c>MessagePackSerializer.Deserialize</c> auto-detects the LZ4 extension type in the byte stream
/// regardless of what compression setting the reader's own <c>MessagePackSerializerOptions</c> carry.
/// </remarks>
public sealed record MessagePackCompressionOptions
{
    /// <summary>Whether to auto-compress bodies over <see cref="ThresholdBytes"/>. Default: enabled.</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// The uncompressed encoding size, in bytes, above which a body is re-serialised with LZ4
    /// compression. Below it, compression overhead (the LZ4 framing, the CPU cost) costs more than it
    /// saves. Default: 1024.
    /// </summary>
    public int ThresholdBytes { get; init; } = 1024;

    /// <summary>The default: enabled, 1024-byte threshold.</summary>
    public static readonly MessagePackCompressionOptions Default = new();

    /// <summary>Never compresses, regardless of size.</summary>
    public static readonly MessagePackCompressionOptions Disabled = new() { Enabled = false };
}
