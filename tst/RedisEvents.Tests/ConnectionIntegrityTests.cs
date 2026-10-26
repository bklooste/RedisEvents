using System.Text;

using FluentAssertions;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using RedisEvents.Config;
using RedisEvents.Consumer;
using RedisEvents.Diagnostics;
using RedisEvents.Errors;
using RedisEvents.Extensions;
using RedisEvents.Ownership;
using RedisEvents.Producer;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>
/// Which connection trusts which replies. Partition ownership — and only that — runs on a dedicated
/// connection with StackExchange.Redis <c>highIntegrity</c> on, so a desynchronised reply can never
/// be taken for a lease. Publishes and position writes stay on the shared connection. The streaming reader does not pay an extra ECHO per
/// <c>XREAD</c>: it validates every reply, and a reply that fails validation rebuilds the reader
/// connection and resumes from the recorded position.
/// </summary>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class ConnectionIntegrityTests(RedisStreamsFixture fixture)
{
    private static bool HasHighIntegrity(string configuration)
        => ConfigurationOptions.Parse(configuration).HighIntegrity;

    /// <summary>
    /// The library created the shared multiplexer: ownership gets its own clone with highIntegrity,
    /// the shared one and the reader keep it off, the lease is written over the verified connection,
    /// and that connection goes with the host.
    /// </summary>
    [Fact]
    public async Task Ownership_runs_on_its_own_high_integrity_connection_and_the_reader_does_not()
    {
        var (options, consumerOptions, topic, consumer) = this.Setup("integrity-own");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new StreamsConnectionProvider(options, services: null, logger: null);
        var handler = new TestHandler();
        var host = new StreamConsumerHost(options, consumerOptions, consumer, ((IBatchHandler)handler).HandleAsync, connection);

        try
        {
            await host.StartAsync(ct);

            var ownership = host.OwnershipConnection;
            ownership.Should().NotBeNull();
            ownership.Should().NotBeSameAs(connection.Connection, "ownership traffic must not share the general-purpose connection");
            HasHighIntegrity(ownership!.Configuration).Should().BeTrue("a lease or position reply is checksum-verified");
            ownership.ClientName.Should().StartWith("streams-own:");

            HasHighIntegrity(connection.Connection.Configuration).Should().BeFalse("the shared connection is left as configured");

            host.ReaderConnection.Should().NotBeNull();
            HasHighIntegrity(host.ReaderConnection!.Configuration).Should().BeFalse(
                "an extra ECHO per XREAD is not paid on the streaming path; replies are validated instead");

            var claims = OwnershipRegistry.ReadOwners(await fixture.Db.HashGetAllAsync(StreamKeys.Ownership(topic, consumer)));
            claims.Should().NotBeEmpty("the lease was taken over the verified connection");

            // Positions still land — over the shared connection, not this one (see the routing test).
            var publisher = new StreamPublisher(fixture.Db, topic, options.Topics[topic]);
            await publisher.PublishAsync("k", Encoding.UTF8.GetBytes("x"), "test.event", new PublishOptions(Partition: 0));
            await handler.WaitForAsync(1, TimeSpan.FromSeconds(10), ct);

            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await fixture.Db.HashLengthAsync(StreamKeys.Positions(topic, consumer))) > 0,
                TimeSpan.FromSeconds(10),
                "the position to be written",
                ct);

            await host.StopAsync(ct);

            host.OwnershipConnection.Should().BeNull("the ownership connection is disposed with the host");
            ownership.IsConnected.Should().BeFalse();
        }
        finally
        {
            await host.DisposeAsync();
        }
    }

    /// <summary>
    /// The high-integrity connection carries partition ownership and nothing else. Watched with
    /// <c>MONITOR</c>: it runs the lease script and its ECHO checksums, while the publish
    /// (<c>XADD</c>), the read (<c>XREAD</c>) and the position write (<c>HSET</c>/<c>HMSET</c> on
    /// the positions hash) all come from other connections — and the reader sends no checksum ECHO.
    /// </summary>
    [Fact]
    public async Task Only_ownership_commands_travel_the_high_integrity_connection()
    {
        var (options, consumerOptions, topic, consumer) = this.Setup("integrity-routing");
        var ct = TestContext.Current.CancellationToken;
        var positionsKey = (string)StreamKeys.Positions(topic, consumer)!;

        using var monitor = await RedisMonitor.StartAsync(fixture.Endpoint, ct);

        await using var connection = new StreamsConnectionProvider(options, services: null, logger: null);
        var handler = new TestHandler();
        var host = new StreamConsumerHost(options, consumerOptions, consumer, ((IBatchHandler)handler).HandleAsync, connection);

        try
        {
            await host.StartAsync(ct);

            // Published the way an application publishes: over the library's shared connection.
            var publisher = new StreamPublisher(connection.Connection.GetDatabase(), topic, options.Topics[topic]);
            await publisher.PublishAsync("k", Encoding.UTF8.GetBytes("routed"), "test.event", new PublishOptions(Partition: 0));
            await handler.WaitForAsync(1, TimeSpan.FromSeconds(10), ct);

            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await fixture.Db.HashLengthAsync(positionsKey)) > 0,
                TimeSpan.FromSeconds(10),
                "the position to be written",
                ct);

            // At least one more lease cycle, so the ownership connection is seen working.
            await Task.Delay(TimeSpan.FromSeconds(1.5), ct);

            await using var admin = await fixture.ConnectAsync(allowAdmin: true);
            var clients = await admin.GetServer(admin.GetEndPoints()[0]).ClientListAsync();

            var ownershipName = host.OwnershipConnection!.ClientName;
            var ownership = clients.Where(c => c.Name == ownershipName).Select(c => Address(c.Address)).ToHashSet();
            var readers = clients.Where(c => c.Name?.StartsWith("streams-rd:", StringComparison.Ordinal) == true)
                .Select(c => Address(c.Address)).ToHashSet();

            ownership.Should().NotBeEmpty();
            readers.Should().NotBeEmpty();

            var seen = monitor.Lines();
            var fromOwnership = seen.Where(l => ownership.Contains(l.Client)).ToArray();
            var fromOthers = seen.Where(l => !ownership.Contains(l.Client) && l.Client != "lua").ToArray();

            fromOwnership.Should().Contain(l => l.Command == "EVALSHA" || l.Command == "EVAL", "the lease script runs over the ownership connection");
            // Every StackExchange.Redis connection sends one ECHO in its handshake; highIntegrity adds
            // one after every command on top.
            fromOwnership.Count(l => l.Command == "ECHO").Should().BeGreaterThan(
                ownership.Count,
                "highIntegrity checksums every ownership reply, beyond the one handshake ECHO per connection");
            fromOwnership.Should().NotContain(l => l.Command == "XADD", "no publish is routed through the ownership connection");
            fromOwnership.Should().NotContain(l => l.Command == "XREAD", "stream reads are not ownership traffic");
            fromOwnership.Should().NotContain(
                l => (l.Command == "HSET" || l.Command == "HMSET") && l.Key == positionsKey,
                "position writes are not ownership traffic");

            fromOthers.Should().Contain(l => l.Command == "XADD", "the publish went over the shared connection");
            fromOthers.Should().Contain(
                l => (l.Command == "HSET" || l.Command == "HMSET") && l.Key == positionsKey,
                "the position went over the shared connection");
            var fromReader = seen.Where(l => readers.Contains(l.Client)).ToArray();
            fromReader.Count(l => l.Command == "XREAD").Should().BeGreaterThan(1);
            fromReader.Count(l => l.Command == "ECHO").Should().BeLessThanOrEqualTo(
                readers.Count,
                "the reader pays no highIntegrity ECHO per XREAD — only the one in each connection's handshake");

            await host.StopAsync(ct);
        }
        finally
        {
            await host.DisposeAsync();
        }
    }

    private static string Address(System.Net.EndPoint? endpoint) => endpoint switch
    {
        System.Net.IPEndPoint ip => $"{ip.Address}:{ip.Port}",
        null => string.Empty,
        _ => endpoint.ToString() ?? string.Empty,
    };

    /// <summary>
    /// The host reuses an <see cref="IConnectionMultiplexer"/> the application registered: the
    /// ownership connection is cloned from that multiplexer's own configuration, and disposing the
    /// host leaves the application's multiplexer alone.
    /// </summary>
    [Fact]
    public async Task A_reused_application_multiplexer_is_cloned_for_ownership_and_left_untouched()
    {
        var (options, consumerOptions, _, consumer) = this.Setup("integrity-reuse");
        var ct = TestContext.Current.CancellationToken;

        await using var application = await fixture.ConnectAsync();
        await using var services = new ServiceCollection().AddSingleton(application).BuildServiceProvider();

        await using var connection = new StreamsConnectionProvider(options, services, logger: null);
        var handler = new TestHandler();
        var host = new StreamConsumerHost(options, consumerOptions, consumer, ((IBatchHandler)handler).HandleAsync, connection);

        try
        {
            await host.StartAsync(ct);

            connection.Source.Should().Be(StreamsConnectionSource.Reused);
            connection.Connection.Should().BeSameAs(application);

            host.OwnershipConnection.Should().NotBeSameAs(application);
            HasHighIntegrity(host.OwnershipConnection!.Configuration).Should().BeTrue();
            HasHighIntegrity(application.Configuration).Should().BeFalse("the application's own multiplexer is not changed");
            host.OwnershipConnection.GetEndPoints().Should().BeEquivalentTo(application.GetEndPoints());

            await host.StopAsync(ct);
        }
        finally
        {
            await host.DisposeAsync();
        }

        application.IsConnected.Should().BeTrue("the host disposes only the connection it opened, never the application's");
    }

    /// <summary>
    /// A reply on the reader that fails validation rebuilds the reader connection, and the loop
    /// resumes from the position its handler last recorded: every entry is delivered, in order, and
    /// the only repeats are read-ahead entries past that position — at-least-once, never a gap.
    /// </summary>
    [Fact]
    public async Task An_untrusted_reply_rebuilds_the_reader_and_resumes_with_nothing_lost()
    {
        var (options, consumerOptions, topic, consumer) = this.Setup("integrity-rebuild");
        var ct = TestContext.Current.CancellationToken;
        const int Total = 40;

        var topicOptions = options.Topics[topic];
        var publisher = new StreamPublisher(fixture.Db, topic, topicOptions);

        for (var i = 0; i < Total; i++)
        {
            await publisher.PublishAsync("k", Encoding.UTF8.GetBytes(i.ToString(System.Globalization.CultureInfo.InvariantCulture)), "test.event", new PublishOptions(Partition: 0));
        }

        var key = StreamKeys.Stream(topic, 0, topicOptions.CoLocatePartitions);
        var reader = await StreamReaderConnection.CreateAsync(options, consumerOptions, consumer, index: 0, NullLogger.Instance);

        try
        {
            var block = new BlockFetch(reader, key, StreamId.Min, batchSize: 5);
            var monitor = StreamLag.Track(topic, consumer, 0, key, unhealthyStalledSeconds: 300);
            var channel = PartitionWorker.CreateChannel(4);
            var ctx = new PartitionContext(fixture.Db, key, 0, topic, consumer, 5, null, ErrorPolicy.BestEffort, channel.Writer, NullLogger.Instance)
            {
                Monitor = monitor,
            };

            var calls = 0;
            StreamId? recordedAtPoison = null;

            async ValueTask<StreamEntryBatch> Fetch(CancellationToken token)
            {
                if (Interlocked.Increment(ref calls) == 4)
                {
                    // Once the handler has recorded something, so read-ahead sits past the resume point.
                    await RedisStreamsFixture.WaitUntilAsync(() => monitor.LastProcessed > StreamId.Min, TimeSpan.FromSeconds(10), "a recorded position", token);
                    recordedAtPoison = monitor.Position;
                    throw new StreamTransportException("the reply carried stream key 's:other:1', which was not requested");
                }

                return await block.FetchAsync(token);
            }

            var seen = new List<int>();
            using var cts = new CancellationTokenSource();

            var read = PartitionWorker.ReadLoopAsync(ctx, StreamId.Min, Fetch, cts.Token, seek: block.SeekTo, rebuildReader: block.RebuildReaderAsync);
            var process = Task.Run(() => PartitionWorker.ProcessLoopAsync(
                ctx,
                async (batch, token) =>
                {
                    lock (seen)
                    {
                        foreach (var msg in batch.Span)
                        {
                            seen.Add(int.Parse(Encoding.UTF8.GetString(msg.Body.Span), System.Globalization.CultureInfo.InvariantCulture));
                        }
                    }

                    // Slow enough that the reader runs ahead of the handler.
                    await Task.Delay(20, token);
                },
                static (_, _) => { },
                cts.Token,
                channel.Reader));

            await RedisStreamsFixture.WaitUntilAsync(
                () =>
                {
                    lock (seen)
                    {
                        return seen.Distinct().Count() == Total;
                    }
                },
                TimeSpan.FromSeconds(30),
                "every entry to be delivered after the rebuild",
                ct);

            await cts.CancelAsync();
            await read;
            await process;

            reader.Generation.Should().Be(1, "exactly one rebuild, for the one untrusted reply");
            HasHighIntegrity(reader.Configuration).Should().BeFalse("the rebuilt reader is the same kind of connection");
            recordedAtPoison.Should().NotBeNull();

            int[] delivered;
            lock (seen)
            {
                delivered = [.. seen];
            }

            delivered.Distinct().Should().BeEquivalentTo(Enumerable.Range(0, Total), "nothing is skipped");

            // Each repeat is a replay of read-ahead: the stream again from the resume point onwards.
            var firstRepeat = delivered.FindIndex((v, i) => Array.IndexOf(delivered, v) < i);
            if (firstRepeat >= 0)
            {
                delivered[..firstRepeat].Should().BeInAscendingOrder();
                delivered[firstRepeat..].Should().BeInAscendingOrder("the replay runs forward from the recorded position");
            }
            else
            {
                delivered.Should().BeInAscendingOrder();
            }
        }
        finally
        {
            await reader.DisposeAsync();
        }
    }

    private (StreamOptions Options, ConsumerOptions Consumer, string Topic, string ConsumerName) Setup(string hint)
    {
        var topic = fixture.NewTopic(hint);
        var consumer = fixture.NewConsumer(hint);
        var topicOptions = new TopicOptions { Partitions = 1 };

        var options = new StreamOptions
        {
            ConnectionString = fixture.ConnectionString,
            Topics = new Dictionary<string, TopicOptions>(StringComparer.Ordinal) { [topic] = topicOptions },
        };

        var consumerOptions = new ConsumerOptions
        {
            Topic = topic,
            BatchSize = 5,
            ReadMode = ReadMode.Block,
            Instances = new InstanceOptions { Mode = InstanceMode.Lease, LeaseTtlSeconds = 3, LeaseRenewSeconds = 1 },
        };

        return (options, consumerOptions, topic, consumer);
    }
}

file static class FindIndexExtensions
{
    internal static int FindIndex(this int[] values, Func<int, int, bool> match)
    {
        for (var i = 0; i < values.Length; i++)
        {
            if (match(values[i], i))
            {
                return i;
            }
        }

        return -1;
    }
}

/// <summary>
/// A raw <c>MONITOR</c> session: every command the server executes, with the client address that
/// sent it. StackExchange.Redis does not expose MONITOR, so this speaks RESP over a plain socket.
/// </summary>
internal sealed class RedisMonitor : IDisposable
{
    private readonly System.Net.Sockets.TcpClient tcp;
    private readonly CancellationTokenSource stop = new();
    private readonly List<MonitoredCommand> lines = [];
    private readonly Task pump;

    private RedisMonitor(System.Net.Sockets.TcpClient tcp)
    {
        this.tcp = tcp;
        this.pump = Task.Run(this.PumpAsync);
    }

    internal static async Task<RedisMonitor> StartAsync(string endpoint, CancellationToken ct)
    {
        var at = endpoint.LastIndexOf(':');
        var tcp = new System.Net.Sockets.TcpClient();
        await tcp.ConnectAsync(endpoint[..at], int.Parse(endpoint[(at + 1)..], System.Globalization.CultureInfo.InvariantCulture), ct);

        var stream = tcp.GetStream();
        await stream.WriteAsync(Encoding.ASCII.GetBytes("MONITOR\r\n"), ct);

        var monitor = new RedisMonitor(tcp);

        // MONITOR answers +OK before streaming; wait for it so nothing after this call is missed.
        await RedisStreamsFixture.WaitUntilAsync(() => monitor.started, TimeSpan.FromSeconds(5), "MONITOR to start", ct);
        return monitor;
    }

    private volatile bool started;

    internal IReadOnlyList<MonitoredCommand> Lines()
    {
        lock (this.lines)
        {
            return [.. this.lines];
        }
    }

    public void Dispose()
    {
        this.stop.Cancel();
        this.tcp.Dispose();

        try
        {
            this.pump.Wait(TimeSpan.FromSeconds(2));
        }
        catch (AggregateException)
        {
            // The socket was closed under the pump; that is how it ends.
        }

        this.stop.Dispose();
    }

    private async Task PumpAsync()
    {
        using var reader = new StreamReader(this.tcp.GetStream(), Encoding.UTF8);

        while (!this.stop.IsCancellationRequested)
        {
            string? line;

            try
            {
                line = await reader.ReadLineAsync(this.stop.Token);
            }
            catch (Exception)
            {
                return;
            }

            if (line is null)
            {
                return;
            }

            if (line == "+OK")
            {
                this.started = true;
                continue;
            }

            if (MonitoredCommand.TryParse(line, out var command))
            {
                lock (this.lines)
                {
                    this.lines.Add(command);
                }
            }
        }
    }
}

/// <summary>One MONITOR line: <c>+&lt;time&gt; [&lt;db&gt; &lt;client&gt;] "CMD" "key" …</c>.</summary>
internal readonly record struct MonitoredCommand(string Client, string Command, string? Key)
{
    internal static bool TryParse(string line, out MonitoredCommand command)
    {
        command = default;

        var open = line.IndexOf('[', StringComparison.Ordinal);
        var close = line.IndexOf(']', StringComparison.Ordinal);
        if (open < 0 || close < open)
        {
            return false;
        }

        var bracket = line[(open + 1)..close].Split(' ', 2);
        var client = bracket.Length == 2 ? bracket[1] : bracket[0];

        var args = new List<string>();
        var rest = line[(close + 1)..];
        var i = 0;

        while (i < rest.Length && args.Count < 2)
        {
            var start = rest.IndexOf('"', i);
            if (start < 0)
            {
                break;
            }

            var sb = new StringBuilder();
            var j = start + 1;

            for (; j < rest.Length; j++)
            {
                if (rest[j] == '\\' && j + 1 < rest.Length)
                {
                    sb.Append(rest[++j]);
                    continue;
                }

                if (rest[j] == '"')
                {
                    break;
                }

                sb.Append(rest[j]);
            }

            args.Add(sb.ToString());
            i = j + 1;
        }

        if (args.Count == 0)
        {
            return false;
        }

        command = new MonitoredCommand(client, args[0].ToUpperInvariant(), args.Count > 1 ? args[1] : null);
        return true;
    }
}
