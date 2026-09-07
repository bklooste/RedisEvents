using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Orange.Lib.Streams.Extensions;

namespace Orange.Lib.Streams.Web;

/// <summary>
/// Registration for <see cref="StreamsHealthCheck"/>.
/// </summary>
/// <remarks>
/// <para>
/// This is the hook that used to be the <c>TODO(P1/P3)</c> in
/// <c>Orange.Lib.Streams.Extensions.StreamsBuilderExtensions.Core</c>. It lives here, and not in the
/// core library, because <see cref="IHealthCheck"/> comes from
/// <c>Microsoft.Extensions.Diagnostics.HealthChecks</c>, which core deliberately does not reference
/// after the R-25 headless split — a worker with no web host must be able to reference
/// <c>Orange.Lib.Streams</c> alone.
/// </para>
/// <para>
/// Registration is therefore explicit: a service that wants the readiness answer calls
/// <see cref="AddStreamsHealthCheck(IHostApplicationBuilder)"/> after its <c>AddStream</c> calls.
/// </para>
/// </remarks>
public static class StreamsHealthCheckExtensions
{
    private const string RegisteredKey = "Orange.Lib.Streams.Web:HealthCheck";

    /// <summary>
    /// Registers <see cref="StreamsHealthCheck"/> under the name and tag <c>streams</c>.
    /// Calling it more than once on the same builder registers the check once — a duplicate name
    /// would throw when <c>HealthCheckService</c> is first resolved.
    /// </summary>
    /// <param name="builder">The host application builder.</param>
    /// <returns>The builder, for chaining.</returns>
    public static IHostApplicationBuilder AddStreamsHealthCheck(this IHostApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        if (builder.Properties.ContainsKey(RegisteredKey))
            return builder;

        builder.Properties[RegisteredKey] = true;
        builder.Services.AddStreamsHealthCheck();

        return builder;
    }

    /// <summary>
    /// Registers <see cref="StreamsHealthCheck"/> under the name and tag <c>streams</c>, for a
    /// service collection that is not being built through an <see cref="IHostApplicationBuilder"/>.
    /// Unlike the builder overload this does not de-duplicate; call it once.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection, for chaining.</returns>
    public static IServiceCollection AddStreamsHealthCheck(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // A factory registration, not AddCheck<T>: the check's two dependencies are optional (a
        // publish-only process has neither), and the consumer hosts can only be reached as
        // IEnumerable<IHostedService> because they are registered one-per-consumer rather than by
        // type. A factory also keeps this AOT-clean — AddCheck<T> goes through ActivatorUtilities.
        services.AddHealthChecks().Add(new HealthCheckRegistration(
            StreamsHealthCheck.Name,
            sp => new StreamsHealthCheck(
                sp.GetService<StreamsConnectionProvider>(),
                sp.GetServices<IHostedService>()),
            failureStatus: null,
            tags: [StreamsHealthCheck.Name]));

        return services;
    }
}
