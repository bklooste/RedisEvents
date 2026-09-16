using System.Diagnostics.Metrics;
using System.Reflection;
using System.Runtime.CompilerServices;

using FluentAssertions;

using RedisEvents.Config;
using RedisEvents.Errors;
using RedisEvents.Producer;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.UnitTests;

/// <summary>
/// R-21 (P3 test gap 10). The parts of <see cref="Outbox"/> nothing reached: the
/// <see cref="Outbox.WriteAndPublishManyAsync"/> commit path (ids in publish order, one count per
/// entry), <c>RequireStreamKeysInOneSlot</c> — which is the only thing standing between a
/// multi-publish transaction and a <c>CROSSSLOT</c> in production — and the abandoned transaction:
/// when <c>EXEC</c> does not run, SE.Redis completes the queued command tasks as cancelled or
/// faulted and nobody awaits them, so an unobserved fault would surface later as a process-level
/// unhandled task exception in a service that did nothing wrong.
/// </summary>
/// <remarks>
/// No Redis. The database and its transaction are <see cref="DispatchProxy"/> stand-ins that behave
/// the way SE.Redis does on the paths under test: a queued command's task does not complete until
/// <c>EXEC</c> runs, and an abandoned transaction cancels or faults it instead.
/// </remarks>
public class OutboxTransactionTests
{
    private const string Topic = "outbox-tests";

    private static ReadOnlyMemory<byte> Body(string text) => System.Text.Encoding.UTF8.GetBytes(text);

    private static TopicOptions Options(int partitions = 4, bool coLocate = true)
        => new() { Partitions = partitions, CoLocatePartitions = coLocate };

    // ------------------------------------------------------------------ the commit path

    /// <summary>
    /// The many-publish commit path: every entry gets the id its own <c>XADD</c> returned, in publish
    /// order, and each is counted once towards <c>streams.published</c>.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task WriteAndPublishManyAsync_returns_one_id_per_publish_in_order()
    {
        var redis = new FakeTransaction();
        using var published = new PublishProbe();

        var publishes = new[]
        {
            new OutboxPublish(Topic, "customer-1", Body("a"), "BetPlaced", TopicOptions: Options()),
            new OutboxPublish(Topic, "customer-2", Body("b"), "BetMatched", TopicOptions: Options()),
            new OutboxPublish(Topic, "customer-3", Body("c"), "BetSettled", TopicOptions: Options()),
        };

        var stateWrites = 0;

        var ids = await Outbox.WriteAndPublishManyAsync(
            redis.Database,
            tran =>
            {
                stateWrites++;
                _ = tran.StringSetAsync(Outbox.StateKey(Topic, "1"), "v");
            },
            publishes,
            stateKeys: new[] { Outbox.StateKey(Topic, "1") });

        ids.Should().NotBeNull();
        ids!.Should().HaveCount(3);
        ids.Select(id => id.Seq).Should().Equal(new long[] { 0, 1, 2 }, "each entry carries the id its own queued XADD returned");

        stateWrites.Should().Be(1, "the state delegate is queued once, ahead of the publishes");
        redis.TransactionsCreated.Should().Be(1, "one MULTI/EXEC covers the whole outbox call");
        redis.Executed.Should().Be(1);
        published.TotalFor(Topic).Should().Be(3, "an outbox publish is still a publish on the dashboard");
    }

    /// <summary>
    /// Routing inside the transaction is the publisher's routing: the key decides the partition, an
    /// explicit partition overrides it, and the stream key is the topic's hash-tagged one.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task WriteAndPublishManyAsync_routes_each_publish_and_uses_the_tagged_stream_keys()
    {
        var redis = new FakeTransaction();

        var publishes = new[]
        {
            new OutboxPublish(Topic, "customer-1", Body("a"), "T", new PublishOptions(Partition: 2), Options()),
            new OutboxPublish(Topic, "customer-1", Body("b"), "T", TopicOptions: Options()),
        };

        var ids = await Outbox.WriteAndPublishManyAsync(redis.Database, _ => { }, publishes);

        ids.Should().NotBeNull();
        redis.StreamKeys.Should().HaveCount(2);
        redis.StreamKeys[0].Should().Be($"{KeyNamespace.Prefix()}s:{{{Topic}}}:2", "an explicit partition wins outright");

        var routed = PartitionRouter.ForKey(System.Text.Encoding.UTF8.GetBytes("customer-1"), 4);
        redis.StreamKeys[1].Should().Be($"{KeyNamespace.Prefix()}s:{{{Topic}}}:{routed}", "an unrouted publish hashes its key exactly as StreamPublisher does");
    }

    /// <summary>An explicit partition outside the topic's range is a caller bug, caught before anything is queued.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_publish_naming_a_partition_outside_the_topic_is_rejected()
    {
        var redis = new FakeTransaction();

        var act = async () => await Outbox.WriteAndPublishManyAsync(
            redis.Database,
            _ => { },
            new[] { new OutboxPublish(Topic, "k", Body("a"), "T", new PublishOptions(Partition: 9), Options(partitions: 4)) });

        await act.Should().ThrowAsync<ArgumentOutOfRangeException>();
        redis.TransactionsCreated.Should().Be(0);
    }

    // ------------------------------------------------------------------ one slot per transaction

    /// <summary>
    /// Two topics in one transaction is a <c>CROSSSLOT</c> on a cluster and works fine on a single
    /// node, which is exactly why it has to be refused here rather than discovered in production.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Publishes_to_two_topics_are_refused_before_anything_is_queued()
    {
        var redis = new FakeTransaction();

        var act = async () => await Outbox.WriteAndPublishManyAsync(
            redis.Database,
            _ => { },
            new[]
            {
                new OutboxPublish(Topic, "k", Body("a"), "T", TopicOptions: Options()),
                new OutboxPublish("another-topic", "k", Body("b"), "T", TopicOptions: Options()),
            });

        var thrown = await act.Should().ThrowAsync<StreamConfigurationException>();
        thrown.Which.Message.Should().Contain(Topic).And.Contain("another-topic").And.Contain("cannot span topics");

        redis.TransactionsCreated.Should().Be(0, "the slot check runs before the transaction is built");
    }

    /// <summary>
    /// The same topic can still span slots: with <c>CoLocatePartitions=false</c> the partition
    /// streams are deliberately spread, so two partitions of one topic cannot share a transaction.
    /// The message has to say so, because "same topic" is exactly when this looks impossible.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Two_partitions_of_a_non_co_located_topic_are_refused_with_that_named_as_the_reason()
    {
        var redis = new FakeTransaction();
        var spread = Options(partitions: 4, coLocate: false);

        var act = async () => await Outbox.WriteAndPublishManyAsync(
            redis.Database,
            _ => { },
            new[]
            {
                new OutboxPublish(Topic, "k", Body("a"), "T", new PublishOptions(Partition: 0), spread),
                new OutboxPublish(Topic, "k", Body("b"), "T", new PublishOptions(Partition: 1), spread),
            });

        var thrown = await act.Should().ThrowAsync<StreamConfigurationException>();
        thrown.Which.Message.Should().Contain("CoLocatePartitions=false");
    }

    /// <summary>Two publishes to the same partition of a spread topic share one slot and are fine.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task Two_publishes_to_one_partition_of_a_non_co_located_topic_are_accepted()
    {
        var redis = new FakeTransaction();
        var spread = Options(partitions: 4, coLocate: false);

        var ids = await Outbox.WriteAndPublishManyAsync(
            redis.Database,
            _ => { },
            new[]
            {
                new OutboxPublish(Topic, "k", Body("a"), "T", new PublishOptions(Partition: 1), spread),
                new OutboxPublish(Topic, "k", Body("b"), "T", new PublishOptions(Partition: 1), spread),
            });

        ids.Should().NotBeNull().And.HaveCount(2);
        redis.StreamKeys.Should().AllBe($"{KeyNamespace.Prefix()}s:{Topic}:1");
    }

    /// <summary>
    /// A state key is checked against the <em>first</em> publish's stream key, so a key tagged on the
    /// topic is accepted and an untagged one is refused — the same rule the single-publish overload
    /// applies, asserted on the many overload because it takes a different code path to it.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task WriteAndPublishManyAsync_checks_the_declared_state_keys_against_the_stream_slot()
    {
        var redis = new FakeTransaction();
        var publishes = new[] { new OutboxPublish(Topic, "k", Body("a"), "T", TopicOptions: Options()) };

        var act = async () => await Outbox.WriteAndPublishManyAsync(
            redis.Database,
            _ => { },
            publishes,
            stateKeys: new RedisKey[] { "customer:123" });

        var thrown = await act.Should().ThrowAsync<StreamConfigurationException>();
        thrown.Which.Message.Should().Contain("customer:123").And.Contain(Topic);
        redis.TransactionsCreated.Should().Be(0);
    }

    // ------------------------------------------------------------------ argument contracts

    /// <summary>A transaction with no publishes is a plain write; the API says so rather than doing nothing.</summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_empty_publish_list_is_refused_with_an_explanation()
    {
        var redis = new FakeTransaction();

        var act = async () => await Outbox.WriteAndPublishManyAsync(
            redis.Database,
            _ => { },
            ReadOnlyMemory<OutboxPublish>.Empty);

        var thrown = await act.Should().ThrowAsync<ArgumentException>();
        thrown.Which.Message.Should().Contain("only writes state");
    }

    /// <summary>A publish naming no topic or no type is named by index, because it is one of many.</summary>
    [Theory]
    [Trait("TestType", "UnitTest")]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_publish_missing_its_topic_or_type_is_reported_by_index(bool missingTopic)
    {
        var redis = new FakeTransaction();

        var publishes = new[]
        {
            new OutboxPublish(Topic, "k", Body("a"), "T", TopicOptions: Options()),
            missingTopic
                ? new OutboxPublish(" ", "k", Body("b"), "T", TopicOptions: Options())
                : new OutboxPublish(Topic, "k", Body("b"), null!, TopicOptions: Options()),
        };

        var act = async () => await Outbox.WriteAndPublishManyAsync(redis.Database, _ => { }, publishes);

        var thrown = await act.Should().ThrowAsync<ArgumentException>();
        thrown.Which.Message.Should().Contain("publish 1");
    }

    /// <summary>
    /// A consumer's dedicated reader multiplexer is read-only and may be parked in a blocking
    /// <c>XREAD</c>; a transaction on it would queue behind the block. Both entry points refuse it.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_database_on_the_reader_multiplexer_is_refused_by_both_entry_points()
    {
        var redis = new FakeTransaction { ClientName = "streams-rd:svc:pod-0" };

        var many = async () => await Outbox.WriteAndPublishManyAsync(
            redis.Database,
            _ => { },
            new[] { new OutboxPublish(Topic, "k", Body("a"), "T", TopicOptions: Options()) });

        var one = async () => await Outbox.WriteAndPublishAsync(
            redis.Database, _ => { }, Topic, "k", Body("a"), "T", topicOptions: Options());

        (await many.Should().ThrowAsync<StreamConfigurationException>()).Which.Message.Should().Contain("read-only");
        (await one.Should().ThrowAsync<StreamConfigurationException>()).Which.Message.Should().Contain("streams-rd:svc:pod-0");

        redis.TransactionsCreated.Should().Be(0);
    }

    // ------------------------------------------------------------------ the abandoned transaction

    /// <summary>
    /// An abandoned transaction — a failed condition — reports itself as a null id rather than
    /// letting the cancelled queued tasks surface as a <see cref="TaskCanceledException"/> nobody
    /// asked for. The many overload behaves the same, and neither leaves state unwritten.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task An_abandoned_transaction_never_leaks_the_cancellation_of_its_queued_commands()
    {
        var redis = new FakeTransaction { Commits = false };

        var ids = await Outbox.WriteAndPublishManyAsync(
            redis.Database,
            _ => { },
            new[]
            {
                new OutboxPublish(Topic, "k", Body("a"), "T", TopicOptions: Options()),
                new OutboxPublish(Topic, "k", Body("b"), "T", TopicOptions: Options()),
            },
            conditions: new[] { Condition.KeyExists(Outbox.StateKey(Topic, "guard")) });

        ids.Should().BeNull("nothing was applied, and that is a normal outcome rather than a failure");
        redis.ConditionsAdded.Should().Be(1);
    }

    /// <summary>
    /// <c>EXEC</c> failing is a transport failure, wrapped so callers catch the one documented
    /// exception — and it says how many messages did not land, because "the state writes may or may
    /// not have applied" is the operator's next question.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task A_failed_exec_is_wrapped_as_a_transport_failure_that_names_the_batch()
    {
        var redis = new FakeTransaction
        {
            ExecuteThrows = new RedisConnectionException(ConnectionFailureType.SocketFailure, "the connection went away"),
        };

        var act = async () => await Outbox.WriteAndPublishManyAsync(
            redis.Database,
            _ => { },
            new[]
            {
                new OutboxPublish(Topic, "k", Body("a"), "T", TopicOptions: Options()),
                new OutboxPublish(Topic, "k", Body("b"), "T", TopicOptions: Options()),
            });

        var thrown = await act.Should().ThrowAsync<StreamTransportException>();
        thrown.Which.Message.Should().Contain("2 message(s)").And.Contain(Topic);
        thrown.Which.InnerException.Should().BeOfType<RedisConnectionException>();
    }

    /// <summary>
    /// The half of the abandoned path that has no visible symptom until much later: the queued
    /// command tasks are faulted and nobody awaits them, so they must be observed here. An
    /// unobserved one is finalised into <see cref="TaskScheduler.UnobservedTaskException"/> —
    /// arriving in an unrelated part of the process, minutes later, with no outbox call in the stack.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public async Task The_queued_commands_of_an_abandoned_transaction_are_observed()
    {
        var seen = new List<Exception>();

        void OnUnobserved(object? sender, UnobservedTaskExceptionEventArgs args)
        {
            foreach (var inner in args.Exception.InnerExceptions)
            {
                if (inner is OutboxSentinelException)
                {
                    lock (seen)
                    {
                        seen.Add(inner);
                    }
                }
            }
        }

        TaskScheduler.UnobservedTaskException += OnUnobserved;
        try
        {
            await RunAbandonedAsync();

            // The queued tasks are unreachable now; only a finaliser can report them, so provoke one.
            for (var i = 0; i < 3; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();

            lock (seen)
            {
                seen.Should().BeEmpty("the outbox observes the tasks of a transaction that did not execute");
            }
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= OnUnobserved;
        }
    }

    /// <summary>Runs — and drops every reference to — an outbox call whose queued commands fault.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static async Task RunAbandonedAsync()
    {
        var redis = new FakeTransaction
        {
            Commits = false,
            QueuedFault = () => new OutboxSentinelException(),
        };

        var ids = await Outbox.WriteAndPublishManyAsync(
            redis.Database,
            _ => { },
            new[]
            {
                new OutboxPublish(Topic, "k", Body("a"), "T", TopicOptions: Options()),
                new OutboxPublish(Topic, "k", Body("b"), "T", TopicOptions: Options()),
            });

        ids.Should().BeNull();
    }

    /// <summary>A fault type no other test can produce, so the assertion cannot pick up a neighbour's task.</summary>
    private sealed class OutboxSentinelException : Exception
    {
        internal OutboxSentinelException()
            : base("outbox queued command")
        {
        }
    }

    /// <summary>Counts <c>streams.published</c> per topic while it is alive.</summary>
    private sealed class PublishProbe : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly Dictionary<string, long> totals = new(StringComparer.Ordinal);

        internal PublishProbe()
        {
            this.listener.InstrumentPublished = (instrument, listening) =>
            {
                if (instrument.Meter.Name == "RedisEvents" &&
                    string.Equals(instrument.Name, "streams.published", StringComparison.Ordinal))
                {
                    listening.EnableMeasurementEvents(instrument);
                }
            };

            this.listener.SetMeasurementEventCallback<long>(this.OnMeasurement);
            this.listener.Start();
        }

        internal long TotalFor(string topic)
        {
            lock (this.totals)
            {
                return this.totals.TryGetValue(topic, out var total) ? total : 0L;
            }
        }

        public void Dispose() => this.listener.Dispose();

        private void OnMeasurement(
            Instrument instrument,
            long measurement,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state)
        {
            _ = instrument;
            _ = state;

            foreach (var tag in tags)
            {
                if (!string.Equals(tag.Key, "topic", StringComparison.Ordinal))
                {
                    continue;
                }

                var topic = tag.Value?.ToString() ?? string.Empty;
                lock (this.totals)
                {
                    this.totals[topic] = (this.totals.TryGetValue(topic, out var current) ? current : 0L) + measurement;
                }
            }
        }
    }

    /// <summary>
    /// An <see cref="IDatabase"/> and <see cref="ITransaction"/> stand-in that behaves the way
    /// SE.Redis does around <c>MULTI</c>/<c>EXEC</c>: queued commands hand back tasks that only
    /// complete once <c>EXEC</c> has run, and an abandoned transaction cancels or faults them.
    /// </summary>
    private sealed class FakeTransaction
    {
        internal const long IdMs = 1_700_000_000_000;

        private readonly List<string> streamKeys = [];
        private int queued;

        internal FakeTransaction()
        {
            var multiplexer = DispatchProxy.Create<IConnectionMultiplexer, Proxy>();
            ((Proxy)multiplexer).Owner = this;

            var db = DispatchProxy.Create<IDatabase, Proxy>();
            ((Proxy)db).Owner = this;
            ((Proxy)db).Multiplexer = multiplexer;

            this.Database = db;
        }

        internal IDatabase Database { get; }

        /// <summary>Whether <c>EXEC</c> reports the transaction as committed.</summary>
        internal bool Commits { get; init; } = true;

        /// <summary>When set, <c>EXEC</c> throws this instead of returning.</summary>
        internal Exception? ExecuteThrows { get; init; }

        /// <summary>When set, a queued command's task faults with this rather than being cancelled.</summary>
        internal Func<Exception>? QueuedFault { get; init; }

        /// <summary>The multiplexer's client name; the outbox refuses a reader connection.</summary>
        internal string ClientName { get; init; } = "streams-sh:tests";

        internal int TransactionsCreated { get; private set; }

        internal int ConditionsAdded { get; private set; }

        internal int Executed { get; private set; }

        /// <summary>The stream keys the queued <c>XADD</c>s were issued against, in order.</summary>
        internal IReadOnlyList<string> StreamKeys => this.streamKeys;

        private object? Invoke(Proxy proxy, MethodInfo method, object?[] args)
        {
            switch (method.Name)
            {
                case "get_ClientName":
                    return this.ClientName;

                case "get_Multiplexer":
                    return proxy.Multiplexer
                        ?? throw new NotSupportedException("Multiplexer was asked of the multiplexer proxy itself.");

                case "CreateTransaction":
                    this.TransactionsCreated++;
                    var tran = DispatchProxy.Create<ITransaction, Proxy>();
                    ((Proxy)tran).Owner = this;
                    return tran;

                case "AddCondition":
                    this.ConditionsAdded++;
                    return null;

                case "StringSetAsync":
                    return this.Queue(true);

                case "StreamAddAsync":
                    this.streamKeys.Add(((RedisKey)args[0]!).ToString() ?? string.Empty);
                    return this.Queue((RedisValue)$"{IdMs}-{this.queued++}");

                case "ExecuteAsync":
                    this.Executed++;
                    return this.ExecuteThrows is { } boom
                        ? Task.FromException<bool>(boom)
                        : Task.FromResult(this.Commits);

                default:
                    throw new NotSupportedException(
                        $"FakeTransaction does not implement {method.Name}; the outbox should not be calling it.");
            }
        }

        /// <summary>
        /// A queued command's task: completed on a transaction that commits; cancelled — or faulted,
        /// when the test asks for it — on one that does not, which is what SE.Redis does.
        /// </summary>
        private Task<T> Queue<T>(T value)
        {
            if (this.Commits && this.ExecuteThrows is null)
            {
                return Task.FromResult(value);
            }

            return this.QueuedFault is { } fault
                ? Task.FromException<T>(fault())
                : Task.FromCanceled<T>(new CancellationToken(canceled: true));
        }

        /// <summary>The generated proxy's base; public because <see cref="DispatchProxy"/> subclasses it.</summary>
        public class Proxy : DispatchProxy
        {
            internal FakeTransaction Owner { get; set; } = null!;

            internal IConnectionMultiplexer? Multiplexer { get; set; }

            /// <inheritdoc />
            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            {
                ArgumentNullException.ThrowIfNull(targetMethod);
                return this.Owner.Invoke(this, targetMethod, args ?? []);
            }
        }
    }
}
