using System.Text;

using FluentAssertions;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RedisEvents.Extensions;
using RedisEvents.Trimming;

namespace RedisEvents.UnitTests;

/// <summary>
/// R-26. <see cref="BackgroundTrimmer"/> was 884 lines of complete, service-tested code that
/// <b>nothing in <c>src</c> ever constructed</b>. A service that set
/// <c>BackgroundTrimIntervalSeconds</c> and <c>RetentionSeconds</c> got no time-based retention at
/// all — only the inline <c>MAXLEN</c> on <c>XADD</c>, which cannot express "keep six hours" — and
/// it could not even find out, because the trimmer's own
/// <c>WarnAboutIneffectiveConfiguration</c> lives inside the type that was never built.
/// </summary>
/// <remarks>
/// <para>
/// The same failure mode as R-05 (the lag sampler, complete and never started). The reason it hid
/// for so long is precisely that the service tests construct the trimmer by hand: hand-construction
/// proves the sweep works and says nothing about whether a real host ever performs one. These
/// assertions go through <c>AddStream</c> / <c>AddStreamPublisher</c> and the built container, which
/// is the path a service actually takes.
/// </para>
/// <para>
/// Resolving the hosted services is as far as this can go without Redis — the trimmer's connection
/// is resolved lazily inside its sweep loop, so construction connects to nothing. That the started
/// trimmer really sweeps through the host is asserted against real Redis in
/// <c>TrimmingTests.S15_background_trimmer_runs_when_the_generic_host_starts_it</c>.
/// </para>
/// </remarks>
public class TrimmerRegistrationTests
{
    private const string TrimmedTopic = """
    {
      "Streams": {
        "Consumers": [ { "Topic": "orders" } ],
        "Topics": {
          "orders": { "Partitions": 1, "MaxLen": 100000, "RetentionSeconds": 3600, "BackgroundTrimIntervalSeconds": 30 }
        }
      }
    }
    """;

    /// <summary>A topic that opts in gets a trimmer, and it is the topic the trimmer will sweep.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_topic_with_a_background_trim_interval_registers_the_trimmer()
    {
        var builder = Builder(TrimmedTopic);
        builder.AddStream<TrimHandler>("orders");

        var trimmer = TrimmerOf(builder);

        trimmer.Should().NotBeNull("without this registration RetentionSeconds is silently inert in production");
        trimmer!.Topics.Should().ContainSingle().Which.Should().Be("orders");
        BackgroundTrimmer.IsEnabled(new Config.TopicOptions
        {
            MaxLen = 100_000,
            RetentionSeconds = 3_600,
            BackgroundTrimIntervalSeconds = 30,
        }).Should().BeTrue();
    }

    /// <summary>A publish-only service gets one too: retention is the topic's, not the consumer's.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_publish_only_service_also_registers_the_trimmer()
    {
        var builder = Builder(TrimmedTopic);
        builder.AddStreamPublisher("orders");

        TrimmerOf(builder)!.Topics.Should().ContainSingle().Which.Should().Be("orders");
    }

    /// <summary>
    /// No interval anywhere means no hosted service at all. Registering one unconditionally would be
    /// harmless but dishonest — the service list is what an operator reads to see what a pod runs.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_topic_without_an_interval_registers_no_trimmer()
    {
        var builder = Builder("""
        {
          "Streams": {
            "Topics": { "orders": { "Partitions": 1, "MaxLen": 100000 } }
          }
        }
        """);

        builder.AddStreamPublisher("orders");

        TrimmerOf(builder).Should().BeNull("nothing asked for background trimming");
    }

    /// <summary>
    /// An interval with no retention window is inert, and the trimmer is <b>still</b> registered —
    /// that inert configuration is exactly what <c>WarnAboutIneffectiveConfiguration</c> exists to
    /// complain about at startup, and it can only complain if something starts it. Gating the
    /// registration on <see cref="BackgroundTrimmer.IsEnabled"/> would have silenced the warning for
    /// the only configuration that needs it.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_interval_without_retention_still_registers_so_the_warning_can_fire()
    {
        var builder = Builder("""
        {
          "Streams": {
            "Topics": { "orders": { "Partitions": 1, "MaxLen": 100000, "BackgroundTrimIntervalSeconds": 30 } }
          }
        }
        """);

        builder.AddStreamPublisher("orders");

        var trimmer = TrimmerOf(builder);

        trimmer.Should().NotBeNull();
        trimmer!.Topics.Should().BeEmpty("no retention window means no sweep — only the warning");
    }

    /// <summary>Several <c>AddStream</c> calls still produce exactly one trimmer for the process.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Several_registrations_still_produce_exactly_one_trimmer()
    {
        var builder = Builder("""
        {
          "Streams": {
            "Topics": {
              "orders":   { "Partitions": 1, "MaxLen": 100000, "RetentionSeconds": 3600, "BackgroundTrimIntervalSeconds": 30 },
              "payments": { "Partitions": 1, "MaxLen": 100000, "RetentionSeconds": 7200, "BackgroundTrimIntervalSeconds": 60 }
            }
          }
        }
        """);

        builder.AddStream<TrimHandler>("orders");
        builder.AddStream<OtherTrimHandler>("payments");
        builder.AddStreamPublisher("orders");

        using var host = builder.Build();

        var trimmers = host.Services.GetServices<IHostedService>().OfType<BackgroundTrimmer>().ToArray();

        trimmers.Should().ContainSingle(
            "one trimmer per process sweeps every enabled topic; a second would double the XTRIM traffic and race its own clamp reads");

        trimmers[0].Topics.Should().BeEquivalentTo(["orders", "payments"]);
    }

    private static HostApplicationBuilder Builder(string json)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
        {
            EnvironmentName = "Production",
        });

        builder.Configuration.AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(json)));
        return builder;
    }

    private static BackgroundTrimmer? TrimmerOf(HostApplicationBuilder builder)
    {
        using var host = builder.Build();
        return host.Services.GetServices<IHostedService>().OfType<BackgroundTrimmer>().SingleOrDefault();
    }

    /// <summary>A real handler: resolving the hosted services constructs the consumer hosts too.</summary>
    private sealed class TrimHandler : Consumer.IBatchHandler
    {
        public ValueTask HandleAsync(ReadOnlyMemory<Wire.StreamMsg> batch, CancellationToken ct) => default;
    }

    private sealed class OtherTrimHandler : Consumer.IBatchHandler
    {
        public ValueTask HandleAsync(ReadOnlyMemory<Wire.StreamMsg> batch, CancellationToken ct) => default;
    }
}
