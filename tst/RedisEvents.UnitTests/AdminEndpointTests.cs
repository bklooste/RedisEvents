using FluentAssertions;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using RedisEvents.Config;
using RedisEvents.Web;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// Unit tests for <see cref="StreamAdminEndpoints"/>: the SCAN glob, the route-parameter alphabet,
/// target resolution, and the authorization metadata the mapped routes carry.
/// </summary>
/// <remarks>
/// <para>
/// R-11 in <c>docs/plans/2026-09-06-orange-lib-streams/12-remediation.md</c>. These endpoints had
/// zero tests, which is why three separate defects survived review: an ownership glob built by C#
/// interpolation (<c>o:feed_x:*</c>) that can never match the literal-brace keys the library writes
/// (<c>o:{feed_x}:*</c>), routes with no authorization at all, and unvalidated route parameters where
/// a <c>{</c> re-tags the Redis key.
/// </para>
/// <para>
/// No Redis and no web host. Everything here is either a pure helper or the endpoint metadata that
/// <see cref="StreamAdminEndpoints.MapRoutes(IEndpointRouteBuilder, StreamAdminEndpointOptions)"/>
/// attaches, which is exactly the layer the three defects lived in.
/// </para>
/// </remarks>
public class AdminEndpointTests
{
    private const string Topic = "feed_source_racing";

    // ------------------------------------------------------------ the SCAN glob

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Ownership_scan_glob_keeps_the_literal_hash_tag_braces()
    {
        var pattern = StreamAdminEndpoints.OwnershipKeyPattern(Topic);
        var ns = KeyNamespace.Prefix();

        pattern.Should().Be(
            ns + "o:{" + Topic + "}:*",
            "the braces are Redis Cluster hash tags and are part of the key, so a glob without them matches nothing");

        pattern.Should().NotBe(
            $"{ns}o:{Topic}:*",
            "C# interpolation substitutes the topic for the braces, which is the bug this test exists for");

        pattern.Should().Be(
            StreamKeys.Ownership(Topic, "*").ToString(),
            "the glob is built from the same key builder the writer uses, so the two cannot drift");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Ownership_key_prefix_strips_back_to_the_consumer_name()
    {
        var prefix = StreamAdminEndpoints.OwnershipKeyPrefix(Topic);
        var key = StreamKeys.Ownership(Topic, "svc-0").ToString();

        prefix.Should().Be(KeyNamespace.Prefix() + "o:{" + Topic + "}:");
        key.Should().StartWith(prefix);
        key[prefix.Length..].Should().Be("svc-0", "the endpoint reports what it trims off as the consumer name");
    }

    // ------------------------------------------------------------ route parameter validation

    [Theory]
    [InlineData("feed_source_racing")]
    [InlineData("orders-v2")]
    [InlineData("svc.worker:0")]
    [InlineData("A0")]
    [Trait("TestType", "UnitTest")]
    public void Ordinary_names_are_accepted(string name)
        => StreamAdminEndpoints.NameError(name, "topic").Should().BeNull();

    [Theory]
    [InlineData("{evil}")]
    [InlineData("orders}")]
    [InlineData("{orders")]
    [Trait("TestType", "UnitTest")]
    public void A_name_with_braces_is_refused(string name)
        => StreamAdminEndpoints.NameError(name, "topic").Should().NotBeNull(
            "braces re-tag the key: 'p:{a}b:{c}' hashes to a different slot and addresses a different hash");

    [Theory]
    [InlineData("orders*")]
    [InlineData("or?ers")]
    [InlineData("orders[0-9]")]
    [InlineData("orders\\x")]
    [Trait("TestType", "UnitTest")]
    public void A_name_with_glob_metacharacters_is_refused(string name)
        => StreamAdminEndpoints.NameError(name, "topic").Should().NotBeNull(
            "the ownership route puts the topic straight into a SCAN pattern");

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [Trait("TestType", "UnitTest")]
    public void An_empty_name_is_refused(string? name)
        => StreamAdminEndpoints.NameError(name, "consumer").Should().Contain("consumer");

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_over_long_name_is_refused()
        => StreamAdminEndpoints.NameError(new string('a', StreamAdminEndpoints.MaxNameLength + 1), "topic")
            .Should().NotBeNull();

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_name_at_the_length_limit_is_accepted()
        => StreamAdminEndpoints.NameError(new string('a', StreamAdminEndpoints.MaxNameLength), "topic")
            .Should().BeNull();

    // ------------------------------------------------------------ reset target resolution

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_reset_with_no_target_is_refused()
    {
        StreamAdminEndpoints.TryResolveTarget(Request(), out _, out var error).Should().BeFalse();
        error.Should().Contain("exactly one", "the old endpoint only understood from= and said so only for a bad date");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Two_targets_at_once_are_refused()
    {
        StreamAdminEndpoints.TryResolveTarget(
            Request(from: "2026-09-01T00:00:00Z", id: "5-0"), out _, out var error).Should().BeFalse();

        error.Should().Contain("mutually exclusive");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_date_target_resolves_to_that_instant()
    {
        StreamAdminEndpoints.TryResolveTarget(Request(from: "2026-09-01T00:00:00Z"), out var target, out var error)
            .Should().BeTrue();

        error.Should().BeNull();
        target.Kind.Should().Be(StreamAdminEndpoints.ResetTargetKind.Date);
        target.Date.Should().Be(new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_unparseable_date_is_refused()
    {
        StreamAdminEndpoints.TryResolveTarget(Request(from: "yesterday"), out _, out var error).Should().BeFalse();
        error.Should().Contain("ISO-8601");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_exact_id_target_resolves()
    {
        StreamAdminEndpoints.TryResolveTarget(Request(id: "1757000000000-4"), out var target, out _).Should().BeTrue();

        target.Kind.Should().Be(StreamAdminEndpoints.ResetTargetKind.Exact);
        target.Id.Should().Be(new StreamId(1757000000000, 4));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_unparseable_id_is_refused()
    {
        StreamAdminEndpoints.TryResolveTarget(Request(id: "not-an-id"), out _, out var error).Should().BeFalse();
        error.Should().Contain("stream id");
    }

    // The expected kind travels as an int: ResetTargetKind is internal, and a public xUnit theory
    // method cannot take an internal parameter type.
    [Theory]
    [InlineData("start", (int)StreamAdminEndpoints.ResetTargetKind.Start)]
    [InlineData("END", (int)StreamAdminEndpoints.ResetTargetKind.End)]
    [Trait("TestType", "UnitTest")]
    public void To_start_and_to_end_resolve(string to, int expected)
    {
        StreamAdminEndpoints.TryResolveTarget(Request(to: to), out var target, out _).Should().BeTrue();
        ((int)target.Kind).Should().Be(expected);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_unknown_to_value_is_refused()
    {
        StreamAdminEndpoints.TryResolveTarget(Request(to: "middle"), out _, out var error).Should().BeFalse();
        error.Should().Contain("start or end");
    }

    // ------------------------------------------------------------ whenMissing

    [Theory]
    [InlineData(null, StartFrom.Beginning)]
    [InlineData("", StartFrom.Beginning)]
    [InlineData("beginning", StartFrom.Beginning)]
    [InlineData("NOW", StartFrom.Now)]
    [Trait("TestType", "UnitTest")]
    public void When_missing_parses(string? value, StartFrom expected)
    {
        StreamAdminEndpoints.TryParseWhenMissing(value, out var parsed, out var error).Should().BeTrue();
        error.Should().BeNull();
        parsed.Should().Be(expected);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void An_unknown_when_missing_is_refused()
    {
        StreamAdminEndpoints.TryParseWhenMissing("stored", out _, out var error).Should().BeFalse();
        error.Should().Contain("beginning or now");
    }

    // ------------------------------------------------------------ mapping and authorization

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_three_routes_are_mapped()
    {
        var endpoints = Map();

        endpoints.Select(e => e.RoutePattern.RawText).Should().BeEquivalentTo(
            ["{topic}/{consumer}/reset/preview", "{topic}/{consumer}/reset", "{topic}/ownership"]);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Every_route_requires_authorization_by_default()
    {
        foreach (var endpoint in Map())
        {
            endpoint.Metadata.GetMetadata<IAuthorizeData>().Should().NotBeNull(
                $"'{endpoint.RoutePattern.RawText}' rewinds or fast-forwards a production consumer; " +
                "relying on the caller to remember a group-level policy is how it shipped open");
        }
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void A_named_policy_is_carried_onto_every_route()
    {
        foreach (var endpoint in Map(new StreamAdminEndpointOptions { PolicyName = "streams-admin" }))
        {
            endpoint.Metadata.GetMetadata<IAuthorizeData>()!.Policy.Should().Be("streams-admin");
        }
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Authorization_can_be_opted_out_of_explicitly()
    {
        foreach (var endpoint in Map(new StreamAdminEndpointOptions { RequireAuthorization = false }))
        {
            endpoint.Metadata.GetMetadata<IAuthorizeData>().Should().BeNull(
                "the opt-out exists for test hosts, and it has to be written down in the caller's code");
        }
    }

    // ------------------------------------------------------------ helpers

    private static StreamAdminEndpoints.ResetRequest Request(
        string? from = null,
        string? id = null,
        string? to = null)
        => new(from, id, to, Partition: null, MaxCountScan: null, WhenMissing: null, Force: false);

    /// <summary>
    /// Maps the routes onto a bare route builder and returns the built endpoints, which is when
    /// conventions such as <c>RequireAuthorization</c> are applied.
    /// </summary>
    private static IReadOnlyList<RouteEndpoint> Map(StreamAdminEndpointOptions? options = null)
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var builder = new TestRouteBuilder(services);

        StreamAdminEndpoints.MapRoutes(builder, options);

        var endpoints = new List<RouteEndpoint>();
        foreach (var source in builder.DataSources)
        {
            foreach (var endpoint in source.Endpoints)
            {
                endpoints.Add((RouteEndpoint)endpoint);
            }
        }

        return endpoints;
    }

    /// <summary>The smallest thing minimal APIs will map onto: no web host, no server, no pipeline.</summary>
    private sealed class TestRouteBuilder(IServiceProvider services) : IEndpointRouteBuilder
    {
        public IServiceProvider ServiceProvider { get; } = services;

        public ICollection<EndpointDataSource> DataSources { get; } = [];

        public IApplicationBuilder CreateApplicationBuilder() => new ApplicationBuilder(this.ServiceProvider);
    }
}
