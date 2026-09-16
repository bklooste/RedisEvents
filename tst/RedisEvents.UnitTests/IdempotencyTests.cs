using System.Reflection;
using FluentAssertions;
using RedisEvents.Config;
using RedisEvents.Errors;
using RedisEvents.Producer;
using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// R-19: <see cref="Idempotency"/> had zero tests. These cover the three things the review found
/// wrong — the cancellation token was ignored, nothing validated the arguments (so a zero TTL went
/// to Redis as <c>PX 0</c> and came back as a server error), and the key carried no Redis Cluster
/// hash tag while every other key in the library does.
/// </summary>
/// <remarks>
/// No Redis. The database is a <see cref="DispatchProxy"/> stand-in that records the
/// <c>SET</c> it is asked for, so the key shape and the NX/PX arguments are asserted directly.
/// </remarks>
public class IdempotencyTests
{
    // -------------------------------------------------------------------------------------------
    // Key shape
    // -------------------------------------------------------------------------------------------

    /// <summary>
    /// The dedupe key is hash-tagged on the scope, so a scope's markers share a Redis Cluster slot
    /// with the topic of the same name — which is what lets a marker ride inside an outbox
    /// transaction alongside the state it guards.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void KeyIsHashTaggedOnScope()
    {
        var ns = KeyNamespace.Prefix();

        Idempotency.Key("bets", "abc").ToString().Should().Be($"{ns}dedupe:{{bets}}:abc");

        // Same tag as the topic's stream and state keys, which is the whole point.
        Outbox.StateKey("bets", "abc").ToString().Should().Be($"{ns}{{bets}}:state:abc");
        HashTag(Idempotency.Key("bets", "abc").ToString()!).Should().Be("bets");
        HashTag(Outbox.StateKey("bets", "abc").ToString()!).Should().Be("bets");
    }

    /// <summary>
    /// Two scopes do not collide, and two ids inside one scope do not collide.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void KeysAreDistinctPerScopeAndId()
    {
        Idempotency.Key("a", "1").ToString().Should().NotBe(Idempotency.Key("b", "1").ToString());
        Idempotency.Key("a", "1").ToString().Should().NotBe(Idempotency.Key("a", "2").ToString());
    }

    /// <summary>
    /// A brace in the scope would open a hash tag nobody intended and scatter the scope's markers
    /// across slots, so it is refused rather than silently accepted.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void BracedScopeIsRefused()
    {
        var act = () => Idempotency.Key("be{ts}", "1");
        act.Should().Throw<StreamConfigurationException>().WithMessage("*brace*");
    }

    // -------------------------------------------------------------------------------------------
    // Argument validation — none of this reached Redis before R-19
    // -------------------------------------------------------------------------------------------

    /// <summary>
    /// A null database, or an empty scope or id, is a caller bug and is named as one.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task NullOrEmptyArgumentsAreRefused()
    {
        var db = new RecordingDatabase().Database;

        var nullDb = async () => await Idempotency.TryBeginAsync(null!, "s", "1", TimeSpan.FromMinutes(1));
        await nullDb.Should().ThrowAsync<ArgumentNullException>();

        var emptyScope = async () => await Idempotency.TryBeginAsync(db, "  ", "1", TimeSpan.FromMinutes(1));
        await emptyScope.Should().ThrowAsync<ArgumentException>();

        var emptyId = async () => await Idempotency.TryBeginAsync(db, "s", "", TimeSpan.FromMinutes(1));
        await emptyId.Should().ThrowAsync<ArgumentException>();
    }

    /// <summary>
    /// Redis rejects <c>PX 0</c>, and StackExchange.Redis renders a <see cref="TimeSpan"/> as whole
    /// milliseconds — so anything under a millisecond used to reach the server as <c>PX 0</c> and
    /// fail there, with a message that said nothing about the caller's TimeSpan.
    /// </summary>
    [Theory]
    [Trait("TestType", "UnitTest")]
    [InlineData(0)]
    [InlineData(-1000)]
    [InlineData(0.4)]
    public async Task TtlBelowOneMillisecondIsRefusedBeforeTheCommand(double milliseconds)
    {
        var recorder = new RecordingDatabase();

        var act = async () => await Idempotency.TryBeginAsync(
            recorder.Database, "s", "1", TimeSpan.FromMilliseconds(milliseconds));

        await act.Should().ThrowAsync<ArgumentOutOfRangeException>();

        // The point of validating up front: nothing was sent.
        recorder.Calls.Should().Be(0);
    }

    /// <summary>
    /// The token was accepted and ignored before R-19. StackExchange.Redis takes no token on the
    /// command, so the contract is the same as the outbox's: observed before the command is issued.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task CancelledTokenIsObservedBeforeTheCommand()
    {
        var recorder = new RecordingDatabase();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var act = async () => await Idempotency.TryBeginAsync(
            recorder.Database, "s", "1", TimeSpan.FromMinutes(1), cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        recorder.Calls.Should().Be(0);
    }

    // -------------------------------------------------------------------------------------------
    // The command itself
    // -------------------------------------------------------------------------------------------

    /// <summary>
    /// One round trip: <c>SET dedupe:{scope}:&lt;id&gt; 1 NX PX &lt;ttl&gt;</c>. NX is what makes
    /// the first caller win, and the TTL is what bounds the keyspace.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task IssuesSetNxWithTheTtl()
    {
        var recorder = new RecordingDatabase { Result = true };

        var first = await Idempotency.TryBeginAsync(
            recorder.Database, "bet-placed", "bet-123", TimeSpan.FromHours(24));

        first.Should().BeTrue();
        recorder.Calls.Should().Be(1);
        recorder.LastKey.Should().Be($"{KeyNamespace.Prefix()}dedupe:{{bet-placed}}:bet-123");
        recorder.LastWhen.Should().Be(When.NotExists);
        recorder.LastExpiry.Should().Be(TimeSpan.FromHours(24));
    }

    /// <summary>
    /// A second sighting inside the window is reported as such — the caller skips the work.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task SecondSightingReturnsFalse()
    {
        var recorder = new RecordingDatabase { Result = false };

        var seen = await Idempotency.TryBeginAsync(
            recorder.Database, "bet-placed", "bet-123", TimeSpan.FromHours(24));

        seen.Should().BeFalse();
    }

    private static string HashTag(string key)
    {
        var open = key.IndexOf('{', StringComparison.Ordinal);
        var close = key.IndexOf('}', StringComparison.Ordinal);
        return open >= 0 && close > open ? key[(open + 1)..close] : key;
    }

    /// <summary>
    /// A stand-in <see cref="IDatabase"/> built from <see cref="DispatchProxy"/>, so no mocking
    /// package is needed. Only <c>StringSetAsync</c> is answered; anything else throws, because
    /// <see cref="Idempotency"/> should not be calling it.
    /// </summary>
    internal sealed class RecordingDatabase
    {
        internal RecordingDatabase()
        {
            var db = DispatchProxy.Create<IDatabase, Proxy>();
            ((Proxy)db).Owner = this;
            this.Database = db;
        }

        internal IDatabase Database { get; }

        /// <summary>What the stand-in SET reports; true means "first sighting".</summary>
        internal bool Result { get; init; } = true;

        internal int Calls { get; private set; }

        internal string? LastKey { get; private set; }

        internal TimeSpan? LastExpiry { get; private set; }

        internal When LastWhen { get; private set; } = When.Always;

        private object Invoke(MethodInfo method, object?[] args)
        {
            if (method.Name != "StringSetAsync")
            {
                throw new InvalidOperationException($"Idempotency called {method.Name}; it should only issue SET.");
            }

            this.Calls++;

            // Read the arguments by type rather than by position: SE.Redis has several
            // StringSetAsync overloads and the test should not care which one bound.
            foreach (var arg in args)
            {
                switch (arg)
                {
                    case RedisKey key:
                        this.LastKey = key.ToString();
                        break;
                    case TimeSpan expiry:
                        this.LastExpiry = expiry;
                        break;
                    case When condition:
                        this.LastWhen = condition;
                        break;
                    default:
                        break;
                }
            }

            return Task.FromResult(this.Result);
        }

        public class Proxy : DispatchProxy
        {
            internal RecordingDatabase Owner { get; set; } = null!;

            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
                => this.Owner.Invoke(targetMethod!, args ?? []);
        }
    }
}
