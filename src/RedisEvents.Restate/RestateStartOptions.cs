namespace RedisEvents.Restate;

/// <summary>
/// Settings for a <see cref="RestateStartHandler{T}"/>.
/// </summary>
/// <remarks>
/// The defaults are the ones the batch ack rule was designed around: eight submits in flight, five
/// attempts per item, 200 ms doubling to a 5 s ceiling. Past the attempts the batch is handed back to
/// the partition worker as a block (1 s to 30 s, forever), so raising <see cref="MaxAttempts"/> only
/// moves where the waiting happens; it never decides whether a start is kept.
/// </remarks>
public sealed class RestateStartOptions
{
    /// <summary>The handler name used when none is configured: Restate's conventional workflow entry point.</summary>
    public const string DefaultHandler = "run";

    /// <summary>
    /// The Restate ingress base address, e.g. <c>http://plat-restate:8080</c>. When <see langword="null"/>
    /// the <see cref="HttpClient.BaseAddress"/> of the client given to the handler is used; one of the two
    /// is required.
    /// </summary>
    public Uri? Ingress { get; set; }

    /// <summary>
    /// The workflow handler to start, the <c>{handler}</c> in <c>POST {ingress}/{workflow}/{key}/{handler}/send</c>.
    /// Restate handler names are case sensitive. Default <c>run</c>.
    /// </summary>
    public string Handler { get; set; } = DefaultHandler;

    /// <summary>How many submits one batch has in flight at once. Default 8.</summary>
    public int MaxParallelism { get; set; } = 8;

    /// <summary>Attempts per item inside one handler call, the first included. Default 5.</summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>The delay before the second attempt; it doubles per attempt up to <see cref="MaxRetryDelay"/>. Default 200 ms.</summary>
    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>The ceiling on the per-item retry delay. Default 5 s.</summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long one submit may take before it counts as a timeout and is retried. Default 10 s. This is
    /// separate from <see cref="HttpClient.Timeout"/>; whichever is shorter wins, and both are retried.
    /// </summary>
    public TimeSpan AttemptTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// The <c>handler</c> tag on this handler's metrics. Defaults to the handler's type name; set it
    /// when one type is registered more than once.
    /// </summary>
    public string? Name { get; set; }

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(this.Handler, nameof(this.Handler));
        ArgumentOutOfRangeException.ThrowIfLessThan(this.MaxParallelism, 1, nameof(this.MaxParallelism));
        ArgumentOutOfRangeException.ThrowIfLessThan(this.MaxAttempts, 1, nameof(this.MaxAttempts));
        ArgumentOutOfRangeException.ThrowIfLessThan(this.InitialRetryDelay, TimeSpan.Zero, nameof(this.InitialRetryDelay));
        ArgumentOutOfRangeException.ThrowIfLessThan(this.MaxRetryDelay, this.InitialRetryDelay, nameof(this.MaxRetryDelay));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(this.AttemptTimeout, TimeSpan.Zero, nameof(this.AttemptTimeout));
    }
}
