using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orange.Lib.Streams.Config;

/// <summary>
/// Re-runs <see cref="StreamConfigBinder.Validate(StreamOptions, string?, ILogger?)"/> at host start
/// with a real <see cref="ILogger"/>, so the advisory half of validation actually reaches the log.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists (R-17).</b> Validation has two kinds of rule: ones that throw, and ones that
/// only tell you something — "this topic is not in <c>Streams:Topics</c>, here are the defaults it
/// will run on", "Block mode with <c>CoLocatePartitions: false</c> and 8 partitions will open eight
/// dedicated Redis connections". The registration-time call runs before the container exists, so it
/// has no logger to give them, and it passed <see langword="null"/> — which meant every advisory
/// line in the binder was dead code. Both of the rules above were written, tested for, and had
/// never once been printed.
/// </para>
/// <para>
/// <b>Why a hosted service rather than a logger at registration time.</b> A logger built during
/// registration is not the application's logger: it has none of the configured providers, filters or
/// scopes, so its output would go nowhere (or, worse, to a second console sink). The first moment a
/// real <see cref="ILoggerFactory"/> exists is after the container is built, which is exactly when a
/// hosted service starts. This one is registered by the first <c>AddStream</c>/<c>AddStreamPublisher</c>
/// call, so it runs ahead of every consumer host and its lines appear above them in the log.
/// </para>
/// <para>
/// <b>Running validation twice is deliberate and free.</b> The registration-time pass still throws on
/// a bad key, so a misconfigured service fails before the host is even built. By the time this pass
/// runs, every throwing rule has already passed; it walks a handful of records and emits at most a
/// few log lines, once per process.
/// </para>
/// </remarks>
internal sealed class StreamsConfigurationAdvisor : IHostedService
{
    private readonly StreamOptions options;
    private readonly string? environmentName;
    private readonly ILogger? logger;

    /// <summary>Creates the advisor.</summary>
    /// <param name="options">The bound options, as validated at registration time.</param>
    /// <param name="environmentName">The host environment name, for the environment-sensitive rules.</param>
    /// <param name="logger">The application's logger; <see langword="null"/> disables the pass.</param>
    internal StreamsConfigurationAdvisor(StreamOptions options, string? environmentName, ILogger? logger)
    {
        ArgumentNullException.ThrowIfNull(options);

        this.options = options;
        this.environmentName = environmentName;
        this.logger = logger;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (this.logger is not null)
        {
            StreamConfigBinder.Validate(this.options, this.environmentName, this.logger);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
