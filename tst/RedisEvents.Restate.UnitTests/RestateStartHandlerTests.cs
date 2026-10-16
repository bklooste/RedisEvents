using System.Diagnostics;
using System.Net;
using System.Net.Http;

using FluentAssertions;

using RedisEvents.Errors;
using RedisEvents.Restate;

namespace RedisEvents.Restate.UnitTests;

/// <summary>
/// <see cref="RestateStartHandler{T}"/> against a fake Restate ingress: filter before I/O, one call per
/// kept message, no <c>Idempotency-Key</c>, per-item retry, and the exception boundary (only
/// <see cref="RestateStartBlockedException"/> ever leaves <c>HandleAsync</c>).
/// </summary>
public class RestateStartHandlerTests
{
    private const string AcceptedName = "restate.start.accepted";

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_batch_of_1000_with_10_kept_makes_exactly_10_calls_to_their_own_workflow_keys()
    {
        var ingress = new FakeIngress();
        using var metrics = new RestateMetrics(nameof(this.A_batch_of_1000_with_10_kept_makes_exactly_10_calls_to_their_own_workflow_keys));
        var handler = new PayoutStarter(ingress.Client(), Fast.Options(nameof(this.A_batch_of_1000_with_10_kept_makes_exactly_10_calls_to_their_own_workflow_keys)));

        await handler.HandleAsync(Batches.EveryNth(1_000, 100), CancellationToken.None);

        var seen = ingress.Requests.ToArray();
        seen.Should().HaveCount(10);
        seen.Select(s => s.Path).Should().BeEquivalentTo(Enumerable.Range(0, 10).Select(i => $"/BonusPayout/c{i * 100}/run/send"));
        seen.Should().OnlyContain(s => !s.HasIdempotencyKey);
        seen.Should().OnlyContain(s => s.ContentType == "application/json");
        seen.Single(s => s.Path == "/BonusPayout/c300/run/send").Body.Should().Be("{\"BetId\":\"bet300\"}");

        metrics["restate.start.kept"].Should().Be(10);
        metrics["restate.start.filtered"].Should().Be(990);
        metrics[AcceptedName].Should().Be(10);
        metrics["restate.start.already_started"].Should().Be(0);
        metrics["restate.start.blocked"].Should().Be(0);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_batch_with_nothing_kept_makes_no_call_and_returns()
    {
        var ingress = new FakeIngress();
        var handler = new PayoutStarter(ingress.Client(), Fast.Options(nameof(this.A_batch_with_nothing_kept_makes_no_call_and_returns)));

        await handler.HandleAsync(Batches.Of(500, _ => false), CancellationToken.None);
        await handler.HandleAsync(ReadOnlyMemory<RedisEvents.Wire.StreamMsg<Bet>>.Empty, CancellationToken.None);

        ingress.Requests.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Three_items_failing_past_their_attempts_block_the_batch_and_the_retry_sends_all_ten_again()
    {
        const string name = nameof(this.Three_items_failing_past_their_attempts_block_the_batch_and_the_retry_sends_all_ten_again);
        var ingress = new FakeIngress();
        using var metrics = new RestateMetrics(name);
        var failing = new HashSet<string> { "BonusPayout/c700", "BonusPayout/c800", "BonusPayout/c900" };
        var outage = true;
        ingress.Respond = (seen, _) => Task.FromResult(
            outage && failing.Contains(seen.WorkflowKey)
                ? FakeIngress.Json(HttpStatusCode.ServiceUnavailable, "{\"message\":\"unavailable\"}")
                : null);

        var handler = new PayoutStarter(ingress.Client(), Fast.Options(name));
        var batch = Batches.EveryNth(1_000, 100);

        // First delivery: 7 accepted, 3 fail all 5 attempts -> the batch is not acked.
        var first = async () => await handler.HandleAsync(batch, CancellationToken.None);
        var blocked = (await first.Should().ThrowExactlyAsync<RestateStartBlockedException>()).Which;

        blocked.Should().BeAssignableTo<DontIgnoreException>();
        blocked.FailedKeys.Should().BeEquivalentTo(failing);
        blocked.ShuttingDown.Should().BeFalse();
        blocked.Message.Should().Contain("HTTP 503");
        ingress.Requests.Should().HaveCount(7 + (3 * 5));
        ingress.AnswersSnapshot().Should().HaveCount(7).And.OnlyContain(a => a.Status == "Accepted");

        // The partition retries the same batch once the ingress is back: all 10 go out again, the 7
        // already started answer PreviouslyAccepted, the 3 start now, and the call returns (ack).
        outage = false;
        ingress.Requests.Clear();
        ingress.Answers.Clear();

        await handler.HandleAsync(batch, CancellationToken.None);

        ingress.Requests.Should().HaveCount(10);
        var answers = ingress.AnswersSnapshot();
        answers.Where(a => a.Status == "PreviouslyAccepted").Should().HaveCount(7);
        answers.Where(a => a.Status == "Accepted").Select(a => a.WorkflowKey).Should().BeEquivalentTo(failing);

        metrics["restate.start.kept"].Should().Be(20);
        metrics["restate.start.filtered"].Should().Be(1_980);
        metrics[AcceptedName].Should().Be(7 + 10);
        metrics["restate.start.already_started"].Should().Be(7);
        metrics["restate.start.retried"].Should().Be(3 * 4);
        metrics["restate.start.blocked"].Should().Be(1);
    }

    public static TheoryData<string> TransientFailures =>
    [
        "HttpRequestException",
        "attempt timeout",
        "HttpClient.Timeout",
        "500",
        "502",
        "503",
        "408",
        "429",
        "unexpected exception",
    ];

    [Theory]
    [MemberData(nameof(TransientFailures))]
    [Trait("TestType", "UnitTest")]
    public async Task A_transient_failure_is_retried_in_place_and_the_batch_is_acked_once_it_goes_through(string failure)
    {
        var ingress = new FakeIngress();
        var calls = 0;
        ingress.Respond = (_, ct) => Interlocked.Increment(ref calls) <= 2 ? Fail(failure, ct) : Task.FromResult<HttpResponseMessage?>(null);

        var handler = new PayoutStarter(
            ingress.Client(),
            Fast.Options($"transient-ok-{failure}", attemptTimeout: TimeSpan.FromMilliseconds(100)));

        await handler.HandleAsync(Batches.Of(1, _ => true), CancellationToken.None);

        ingress.Requests.Should().HaveCount(3);
        ingress.AnswersSnapshot().Should().ContainSingle().Which.Status.Should().Be("Accepted");
    }

    [Theory]
    [MemberData(nameof(TransientFailures))]
    [Trait("TestType", "UnitTest")]
    public async Task A_transient_failure_that_outlasts_the_attempts_blocks_and_never_escapes_as_anything_else(string failure)
    {
        var ingress = new FakeIngress { Respond = (_, ct) => Fail(failure, ct) };
        var handler = new PayoutStarter(
            ingress.Client(),
            Fast.Options($"transient-block-{failure}", attemptTimeout: TimeSpan.FromMilliseconds(100)));

        var act = async () => await handler.HandleAsync(Batches.Of(1, _ => true), CancellationToken.None);

        var blocked = (await act.Should().ThrowExactlyAsync<RestateStartBlockedException>()).Which;
        blocked.FailedKeys.Should().Equal("BonusPayout/c0");
        blocked.ShuttingDown.Should().BeFalse();
        ingress.Requests.Should().HaveCount(5);
    }

    [Theory]
    [InlineData(400)]
    [InlineData(401)]
    [InlineData(403)]
    [InlineData(404)]
    [InlineData(409)]
    [InlineData(422)]
    [Trait("TestType", "UnitTest")]
    public async Task A_4xx_other_than_already_started_blocks_without_retrying_and_never_skips(int status)
    {
        var ingress = new FakeIngress
        {
            Respond = (_, _) => Task.FromResult<HttpResponseMessage?>(
                FakeIngress.Json((HttpStatusCode)status, "{\"code\":" + status + ",\"message\":\"no\"}")),
        };
        var handler = new PayoutStarter(ingress.Client(), Fast.Options($"4xx-{status}"));

        var act = async () => await handler.HandleAsync(Batches.Of(3, i => i == 1), CancellationToken.None);

        var blocked = (await act.Should().ThrowExactlyAsync<RestateStartBlockedException>()).Which;
        blocked.FailedKeys.Should().Equal("BonusPayout/c1");
        blocked.Message.Should().Contain($"HTTP {status}");
        ingress.Requests.Should().ContainSingle();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Two_handler_instances_given_the_same_batch_concurrently_produce_one_accepted_start_per_key()
    {
        // The fake dedupes on the workflow key in the path, as Restate does, and answers 400 to any
        // Idempotency-Key, as Restate 1.8 does. The handler takes no lock: exclusivity is the key's.
        var ingress = new FakeIngress { Latency = TimeSpan.FromMilliseconds(2) };
        var batch = Batches.Of(200, i => i % 4 == 0);
        var a = new PayoutStarter(ingress.Client(), Fast.Options("exclusive-a"));
        var b = new PayoutStarter(ingress.Client(), Fast.Options("exclusive-b"));

        for (var round = 0; round < 5; round++)
        {
            await Task.WhenAll(
                Task.Run(async () => await a.HandleAsync(batch, CancellationToken.None)),
                Task.Run(async () => await b.HandleAsync(batch, CancellationToken.None)));
        }

        ingress.IdempotencyKeySeen.Should().BeFalse("a workflow submit must never carry Idempotency-Key");
        ingress.Requests.Should().HaveCount(50 * 2 * 5);

        var answers = ingress.AnswersSnapshot();
        var accepted = answers.Where(x => x.Status == "Accepted").Select(x => x.WorkflowKey).ToArray();
        accepted.Should().HaveCount(50).And.OnlyHaveUniqueItems();
        accepted.Should().BeEquivalentTo(Enumerable.Range(0, 50).Select(i => $"BonusPayout/c{i * 4}"));
        answers.Count(x => x.Status == "PreviouslyAccepted").Should().Be((50 * 2 * 5) - 50);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_fake_ingress_rejects_an_Idempotency_Key_like_Restate_so_a_handler_that_sent_one_would_block()
    {
        // Guards the exclusivity test above: if the header were ever sent, Restate 1.8 never starts the
        // run, and this is what the handler would see.
        var ingress = new FakeIngress();
        using var client = ingress.Client();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/BonusPayout/k/run/send");
        request.Headers.Add("Idempotency-Key", "k");

        using var response = await client.SendAsync(request);

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        ingress.IdempotencyKeySeen.Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Submits_are_bounded_parallel_at_the_configured_degree()
    {
        var ingress = new FakeIngress { Latency = TimeSpan.FromMilliseconds(20) };
        var handler = new PayoutStarter(ingress.Client(), Fast.Options("parallel-default"));

        await handler.HandleAsync(Batches.Of(40, _ => true), CancellationToken.None);

        ingress.MaxInFlight.Should().BeInRange(2, 8);

        var narrow = new FakeIngress { Latency = TimeSpan.FromMilliseconds(20) };
        var options = Fast.Options("parallel-3");
        options.MaxParallelism = 3;
        await new PayoutStarter(narrow.Client(), options).HandleAsync(Batches.Of(20, _ => true), CancellationToken.None);

        narrow.MaxInFlight.Should().BeInRange(2, 3);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Any_2xx_is_accepted_and_PreviouslyAccepted_counts_as_already_started()
    {
        const string name = nameof(this.Any_2xx_is_accepted_and_PreviouslyAccepted_counts_as_already_started);
        using var metrics = new RestateMetrics(name);
        var ingress = new FakeIngress
        {
            Respond = (seen, _) => Task.FromResult<HttpResponseMessage?>(seen.WorkflowKey switch
            {
                "BonusPayout/c0" => FakeIngress.Json(HttpStatusCode.Accepted, "{\"invocationId\":\"inv_1\",\"status\":\"PreviouslyAccepted\"}"),
                "BonusPayout/c1" => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("not json") },
                "BonusPayout/c2" => new HttpResponseMessage(HttpStatusCode.NoContent),
                _ => null,
            }),
        };
        var handler = new PayoutStarter(ingress.Client(), Fast.Options(name));

        await handler.HandleAsync(Batches.Of(4, _ => true), CancellationToken.None);

        metrics[AcceptedName].Should().Be(4);
        metrics["restate.start.already_started"].Should().Be(1);
        metrics["restate.start.retried"].Should().Be(0);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_throwing_Map_blocks_naming_the_message_and_the_other_items_are_still_sent()
    {
        var ingress = new FakeIngress();
        var handler = new PayoutStarter(
            ingress.Client(),
            Fast.Options("map-throws"),
            map: bet => bet.Id == "bet2" ? throw new InvalidOperationException("bad payload") : ("BonusPayout", bet.Claim!, new StartInput(bet.Id)));

        var act = async () => await handler.HandleAsync(Batches.Of(5, _ => true), CancellationToken.None);

        var blocked = (await act.Should().ThrowExactlyAsync<RestateStartBlockedException>()).Which;
        blocked.FailedKeys.Should().Equal("stream id 1002-0");
        blocked.Message.Should().Contain("Map threw InvalidOperationException: bad payload");
        blocked.InnerException.Should().BeOfType<InvalidOperationException>();
        ingress.Requests.Should().HaveCount(4);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_throwing_Keep_blocks_rather_than_skipping()
    {
        var ingress = new FakeIngress();
        var handler = new PayoutStarter(
            ingress.Client(),
            Fast.Options("keep-throws"),
            keep: bet => bet.Id == "bet1" ? throw new FormatException("bad") : bet.Claim is not null);

        var act = async () => await handler.HandleAsync(Batches.Of(3, i => i != 1), CancellationToken.None);

        var blocked = (await act.Should().ThrowExactlyAsync<RestateStartBlockedException>()).Which;
        blocked.FailedKeys.Should().Equal("stream id 1001-0");
        ingress.Requests.Should().HaveCount(2);
    }

    [Theory]
    [InlineData("", "key")]
    [InlineData("BonusPayout", "")]
    [Trait("TestType", "UnitTest")]
    public async Task An_empty_workflow_or_key_blocks(string workflow, string key)
    {
        var ingress = new FakeIngress();
        var handler = new PayoutStarter(ingress.Client(), Fast.Options($"empty-{workflow}-{key}"), map: _ => (workflow, key, null));

        var act = async () => await handler.HandleAsync(Batches.Of(1, _ => true), CancellationToken.None);

        await act.Should().ThrowExactlyAsync<RestateStartBlockedException>();
        ingress.Requests.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_input_type_the_serializer_does_not_know_blocks_naming_the_key()
    {
        var ingress = new FakeIngress();
        var handler = new PayoutStarter(
            ingress.Client(),
            Fast.Options("unknown-input"),
            map: bet => ("BonusPayout", bet.Claim!, new UnknownInput(1)));

        var act = async () => await handler.HandleAsync(Batches.Of(1, _ => true), CancellationToken.None);

        var blocked = (await act.Should().ThrowExactlyAsync<RestateStartBlockedException>()).Which;
        blocked.FailedKeys.Should().Equal("BonusPayout/c0");
        blocked.Message.Should().Contain("serializing the UnknownInput input");
        ingress.Requests.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_null_input_sends_no_body()
    {
        var ingress = new FakeIngress();
        var handler = new PayoutStarter(ingress.Client(), Fast.Options("null-input"), map: bet => ("Verify", bet.Claim!, null));

        await handler.HandleAsync(Batches.Of(1, _ => true), CancellationToken.None);

        var seen = ingress.Requests.Should().ContainSingle().Which;
        seen.Body.Should().BeEmpty();
        seen.ContentType.Should().BeNull();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Keys_and_workflow_names_are_escaped_into_one_path_segment_each()
    {
        var ingress = new FakeIngress();
        var options = Fast.Options("escaping");
        options.Handler = "Run";
        var handler = new PayoutStarter(ingress.Client("http://restate.test:8080/prefix/"), options, map: _ => ("Verify", "cust:42/1", null));

        await handler.HandleAsync(Batches.Of(1, _ => true), CancellationToken.None);

        ingress.Requests.Should().ContainSingle().Which.Path.Should().Be("/prefix/Verify/cust%3A42%2F1/Run/send");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Cancellation_mid_batch_is_reported_as_a_blocked_batch_never_as_an_OperationCanceledException()
    {
        // Core treats an OperationCanceledException as shutdown only when the worker's own token is
        // cancelled; anything else is skipped under BestEffort. So a cancelled handler token is still
        // reported as RestateStartBlockedException, which no policy can skip.
        using var cts = new CancellationTokenSource();
        var ingress = new FakeIngress
        {
            Respond = async (_, ct) =>
            {
                await cts.CancelAsync();
                await Task.Delay(Timeout.Infinite, ct);
                return null;
            },
        };
        using var metrics = new RestateMetrics("cancelled");
        var handler = new PayoutStarter(ingress.Client(), Fast.Options("cancelled"));

        var act = async () => await handler.HandleAsync(Batches.Of(30, _ => true), cts.Token);

        var blocked = (await act.Should().ThrowExactlyAsync<RestateStartBlockedException>()).Which;
        blocked.ShuttingDown.Should().BeTrue();
        blocked.FailedKeys.Should().HaveCount(30);
        metrics["restate.start.blocked"].Should().Be(0, "a shutdown is not an outage");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_already_cancelled_token_sends_nothing_and_still_blocks_a_batch_with_kept_items()
    {
        var ingress = new FakeIngress();
        var handler = new PayoutStarter(ingress.Client(), Fast.Options("pre-cancelled"));

        var act = async () => await handler.HandleAsync(Batches.Of(3, _ => true), new CancellationToken(canceled: true));

        (await act.Should().ThrowExactlyAsync<RestateStartBlockedException>()).Which.ShuttingDown.Should().BeTrue();
        ingress.Requests.Should().BeEmpty();

        // Nothing kept: nothing to lose, so it returns and the batch is acked.
        await handler.HandleAsync(Batches.Of(3, _ => false), new CancellationToken(canceled: true));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_traceparent_continues_the_trace_of_the_message_that_caused_the_start()
    {
        const string producer = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        var ingress = new FakeIngress();
        var handler = new PayoutStarter(ingress.Client(), Fast.Options("trace"), map: bet => ("TraceFlow", bet.Claim!, null));

        await handler.HandleAsync(Batches.Of(1, _ => true, _ => producer), CancellationToken.None);

        // With or without a listener on the package's source (other tests may have one): the same trace.
        var sent = ingress.Requests.Should().ContainSingle().Which.TraceParent;
        sent.Should().NotBeNull();
        ActivityContext.Parse(sent!, null).TraceId.ToHexString().Should().Be("0af7651916cd43dd8448eb211c80319c");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task With_tracing_on_each_start_is_a_client_span_parented_on_the_producer_and_sent_as_the_traceparent()
    {
        const string producer = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        var captured = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == RestateOpenTelemetryExtensions.TelemetrySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.GetTagItem("restate.workflow") as string == "SpanFlow")
                {
                    lock (captured)
                    {
                        captured.Add(activity);
                    }
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        var ingress = new FakeIngress();
        var handler = new PayoutStarter(ingress.Client(), Fast.Options("spans"), map: bet => ("SpanFlow", bet.Claim!, null));

        await handler.HandleAsync(Batches.Of(1, _ => true, _ => producer), CancellationToken.None);

        var span = captured.Should().ContainSingle().Which;
        span.Kind.Should().Be(ActivityKind.Client);
        span.DisplayName.Should().Be("restate.start SpanFlow");
        span.ParentSpanId.ToHexString().Should().Be("00f067aa0ba902b7");
        span.TraceId.ToHexString().Should().Be("4bf92f3577b34da6a3ce929d0e0e4736");
        span.GetTagItem("restate.workflow_key").Should().Be("c0");
        ingress.Requests.Should().ContainSingle().Which.TraceParent.Should().Be(span.Id);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_handler_refuses_to_start_without_an_ingress_address_or_with_bad_options()
    {
        using var noBase = new HttpClient();

        var noIngress = () => new PayoutStarter(noBase);
        noIngress.Should().Throw<ArgumentException>().WithMessage("*no ingress address*");

        var relative = () => new PayoutStarter(noBase, new RestateStartOptions { Ingress = new Uri("/x", UriKind.Relative) });
        relative.Should().Throw<ArgumentException>();

        var zeroParallel = () => new PayoutStarter(noBase, new RestateStartOptions { Ingress = new Uri("http://r:8080"), MaxParallelism = 0 });
        zeroParallel.Should().Throw<ArgumentOutOfRangeException>();

        var zeroAttempts = () => new PayoutStarter(noBase, new RestateStartOptions { Ingress = new Uri("http://r:8080"), MaxAttempts = 0 });
        zeroAttempts.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_defaults_are_the_ones_the_ack_rule_was_designed_around()
    {
        var options = new RestateStartOptions();

        options.Handler.Should().Be("run");
        options.MaxParallelism.Should().Be(8);
        options.MaxAttempts.Should().Be(5);
        options.InitialRetryDelay.Should().Be(TimeSpan.FromMilliseconds(200));
        options.MaxRetryDelay.Should().Be(TimeSpan.FromSeconds(5));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void The_blocked_exception_names_at_most_twenty_keys_then_counts_the_rest()
    {
        var keys = Enumerable.Range(0, 25).Select(i => $"W/k{i}").ToArray();
        var ex = new RestateStartBlockedException(keys, 30, keys.Select(_ => "HTTP 503").ToArray(), shuttingDown: false, innerException: null);

        ex.Message.Should().Contain("25 of 30").And.Contain("W/k19 (HTTP 503)").And.NotContain("W/k20").And.Contain("and 5 more");
        ex.FailedKeys.Should().HaveCount(25);
    }

    private static async Task<HttpResponseMessage?> Fail(string failure, CancellationToken ct)
    {
        switch (failure)
        {
            case "HttpRequestException":
                throw new HttpRequestException("connection refused");
            case "attempt timeout":
                // Hangs until the handler's per-attempt timeout cancels the request.
                await Task.Delay(Timeout.Infinite, ct);
                return null;
            case "HttpClient.Timeout":
                // What HttpClient throws when its own Timeout elapses: an OperationCanceledException
                // on a token that is not the handler's.
                throw new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout", new TimeoutException());
            case "unexpected exception":
                throw new InvalidOperationException("something odd");
            default:
                return FakeIngress.Json((HttpStatusCode)int.Parse(failure, System.Globalization.CultureInfo.InvariantCulture), "{\"message\":\"later\"}");
        }
    }
}
