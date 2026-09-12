// Both sample services' entry points are the implicit `Program` class a top-level-statements
// Program.cs generates. Referencing both projects unaliased would make `Program` an ambiguous
// CS0433 reference in this assembly, so each ProjectReference in the .csproj carries its own
// extern alias instead of the default "global" one.
extern alias CommandApi;
extern alias ViewApi;

using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

using CommandApiProgram = CommandApi::Program;
using ViewApiProgram = ViewApi::Program;

namespace RedisEvents.Tests;

/// <summary>
/// Builds a <see cref="WebApplicationFactory{TEntryPoint}"/> for one of the two Inventory sample
/// services, pointed at the fixture's real Testcontainers Redis instead of the `localhost:6379`
/// placeholder each service's own <c>appsettings.json</c> ships with.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why environment variables, not <c>ConfigureAppConfiguration</c>/<c>UseSetting</c>.</b> Both
/// <c>AddEventStore</c> and <c>AddEventProjector</c> read <c>Streams:ConnectionString</c> eagerly —
/// <c>StreamsBuilderExtensions</c> binds and caches <c>StreamOptions</c> the first time any
/// <c>AddStream…</c> call runs on the builder, i.e. synchronously inside <c>Program.cs</c>, well
/// before <c>Build()</c>. <see cref="WebApplicationFactory{TEntryPoint}"/>'s configuration overrides
/// for minimal-hosting apps only reach <c>builder.Configuration</c> as part of intercepting
/// <c>Build()</c> itself — too late for a value the library already read and cached during its own
/// registration calls. Environment variables sidestep this entirely: <c>WebApplication.CreateBuilder(args)</c>
/// includes <c>AddEnvironmentVariables()</c> among its default sources, read synchronously as the very
/// first thing <c>Program.cs</c> does, so setting one on this (in-process) test process before the
/// service's <c>Program.Main</c> runs is visible to every line of <c>Program.cs</c>, not just calls
/// made after some later hook fires.
/// </para>
/// <para>
/// Both services hard-code their topic name ("inventory") in <c>Program.cs</c>, exactly as the
/// worked example in the package README does — it is not configuration, so there is nothing to
/// override there. Only the connection string (both services) and the consumer name (the read side,
/// so each test's projector starts its own consumer group instead of resuming another test's
/// position) need overriding.
/// </para>
/// </remarks>
public static class ServiceTestHosts
{
    private const string ConnectionStringVariable = "Streams__ConnectionString";
    private const string ConsumerVariable = "Streams__Consumer";

    /// <summary>A factory hosting the command-side service (<c>RedisEvents.EventSourcing.Sample.Inventory.CommandApi</c>).</summary>
    public static WebApplicationFactory<CommandApiProgram> CommandApi(RedisStreamsFixture fixture) =>
        Build<CommandApiProgram>(fixture, consumer: null);

    /// <summary>A factory hosting the read-side service (<c>RedisEvents.EventSourcing.Sample.Inventory.ViewApi</c>), consuming under <paramref name="consumer"/>.</summary>
    public static WebApplicationFactory<ViewApiProgram> ViewApi(RedisStreamsFixture fixture, string consumer) =>
        Build<ViewApiProgram>(fixture, consumer);

    /// <summary>
    /// Builds and fully starts a <see cref="WebApplicationFactory{TEntryPoint}"/>, with the
    /// connection string (and, when given, the consumer name) visible to the service's own
    /// <c>Program.cs</c> from its very first line — see the type's remarks for why that has to be an
    /// environment variable rather than a post-hoc configuration override.
    /// </summary>
    private static WebApplicationFactory<TEntryPoint> Build<TEntryPoint>(RedisStreamsFixture fixture, string? consumer)
        where TEntryPoint : class
    {
        var previousConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
        var previousConsumer = Environment.GetEnvironmentVariable(ConsumerVariable);

        try
        {
            Environment.SetEnvironmentVariable(ConnectionStringVariable, fixture.ConnectionString);
            if (consumer is not null)
            {
                Environment.SetEnvironmentVariable(ConsumerVariable, consumer);
            }

            var factory = new WebApplicationFactory<TEntryPoint>().WithWebHostBuilder(builder =>
            {
                // AddEventProjector registers a deliberate, never-actually-resolved placeholder
                // AddSingleton<EventProjector>() alongside the real factory-based registration that
                // supersedes it (see the <remarks> on EventSourcingBuilderExtensions.AddEventProjector)
                // — safe in Production, where WebApplicationBuilder leaves
                // ServiceProviderOptions.ValidateOnBuild off, but WebApplicationFactory otherwise
                // defaults to the Development environment, which turns that eager validation on and
                // makes Build() try to construct the placeholder. The library's own host-based tests
                // hit the same thing and fix it the same way.
                builder.UseEnvironment("Production");
            });

            // Force the host to build (and, for the read side, start its hosted consumer) now, while
            // the environment variables above are still set on this process.
            _ = factory.Server;
            return factory;
        }
        finally
        {
            Environment.SetEnvironmentVariable(ConnectionStringVariable, previousConnectionString);
            if (consumer is not null)
            {
                Environment.SetEnvironmentVariable(ConsumerVariable, previousConsumer);
            }
        }
    }

    /// <summary>
    /// Disposes a <see cref="WebApplicationFactory{TEntryPoint}"/>, tolerating one specific, benign
    /// disposal-time race: a read-side host stopped very soon after starting can hit
    /// <c>StreamConsumerHost.StopAsync</c> racing its own startup-generation teardown, surfacing as
    /// <see cref="ObjectDisposedException"/> on an internal linked <c>CancellationTokenSource</c>
    /// after the consumer has already done everything a test asked of it. Every assertion in this
    /// suite runs and completes before a factory is ever disposed, so an exception here reflects
    /// nothing about correctness — only about how quickly this test tore the host back down, which
    /// none of this sample's tests have reason to slow down for.
    /// </summary>
    public static async ValueTask DisposeQuietlyAsync<TEntryPoint>(this WebApplicationFactory<TEntryPoint> factory)
        where TEntryPoint : class
    {
        try
        {
            await factory.DisposeAsync();
        }
        catch (ObjectDisposedException)
        {
        }
    }
}
