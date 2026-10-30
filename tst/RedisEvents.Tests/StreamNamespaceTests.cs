using System.Diagnostics;
using System.Globalization;
using System.Text;

using FluentAssertions;

using RedisEvents.Admin;
using RedisEvents.Config;
using RedisEvents.Positions;
using RedisEvents.Producer;
using RedisEvents.Wire;

using StackExchange.Redis;

namespace RedisEvents.Tests;

/// <summary>
/// <see cref="TopicOptions.StreamNamespace"/> end to end: two environments, <c>dev</c> and
/// <c>tst</c>, consume one topic whose stream lives in a shared namespace <c>np</c>. Each must
/// receive every message, and each must keep its own cursor — positions for
/// <see cref="DeliveryMode.Ordered"/>, a consumer group for <see cref="DeliveryMode.WorkQueue"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why child processes.</b> The process namespace comes from <c>ASPNETCORE_ENVIRONMENT</c>, which
/// is process-wide, so two hosts in this test process would share one namespace — and with it one
/// position hash and one group, the very collision under test. Each environment is therefore a real
/// process (<c>RedisEvents.NamespaceProbe</c>) configured exactly as a deployed service is: the
/// environment variable and a <c>Streams</c> section. A wrong key (a position or a group left on the
/// shared stream's namespace) makes the two environments share a cursor, which shows up here as one
/// of them missing messages.
/// </para>
/// <para>
/// The publisher runs in this process with the topic's namespace set, as a tier producer would.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class StreamNamespaceTests(RedisStreamsFixture fixture)
{
    private const string SharedNamespace = "np";
    private const string Consumer = "probe";
    private const int Partitions = 2;

    /// <summary>
    /// Ordered delivery: both environments receive all twenty messages, their positions live under
    /// <c>dev:re:p:</c> and <c>tst:re:p:</c> (never <c>np:re:p:</c>), and a later run of one
    /// environment resumes from its own position without moving the other's.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Two_environments_consume_one_shared_stream_with_independent_positions()
    {
        var topic = fixture.NewTopic();
        var db = fixture.Db;

        var first = await this.PublishAsync(topic, from: 0, count: 20);

        var runs = await Task.WhenAll(
            this.RunProbeAsync("dev", topic, "Ordered", expect: first.Length),
            this.RunProbeAsync("tst", topic, "Ordered", expect: first.Length));

        runs[0].Received.Should().BeEquivalentTo(first, "dev must receive every message on the shared stream");
        runs[1].Received.Should().BeEquivalentTo(first, "tst must receive every message on the shared stream, too");

        // The stream and its meta are in the shared namespace and nowhere else.
        (await db.KeyExistsAsync(SharedKey('m', topic))).Should().BeTrue();
        (await StreamLengthAsync(db, SharedNamespace, topic)).Should().Be(first.Length);
        (await StreamLengthAsync(db, "dev", topic)).Should().Be(0, "dev reads the shared stream; it has none of its own");
        (await StreamLengthAsync(db, "tst", topic)).Should().Be(0);

        // Each environment's cursor is in its own namespace, and the shared namespace holds none.
        (await this.PositionKeysAsync(topic)).Should().BeEquivalentTo(
            [PositionsKey("dev", topic), PositionsKey("tst", topic)],
            "positions are the consumer's, so they stay in the process namespace");

        var tstBefore = await ReadPositionsAsync(db, PositionsKey("tst", topic));
        tstBefore.Should().NotBeEmpty();

        // Five more, then dev alone: it resumes from its own position, and tst's does not move.
        var second = await this.PublishAsync(topic, from: first.Length, count: 5);

        var devAgain = await this.RunProbeAsync("dev", topic, "Ordered", expect: second.Length);
        devAgain.Received.Should().BeEquivalentTo(second, "dev resumes after the twenty it already processed");

        (await ReadPositionsAsync(db, PositionsKey("tst", topic))).Should().BeEquivalentTo(
            tstBefore,
            "dev's progress must not move tst's cursor");

        // ...and tst, still behind, gets exactly the five it has not seen.
        var tstAgain = await this.RunProbeAsync("tst", topic, "Ordered", expect: second.Length);
        tstAgain.Received.Should().BeEquivalentTo(second);
    }

    /// <summary>
    /// Work-queue delivery: the group lives on the shared stream key, so each environment's group
    /// carries its process namespace. With one bare group name the two environments would join one
    /// group and split the stream, each missing what the other was handed.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Two_environments_in_work_queue_mode_each_get_their_own_group_on_the_shared_stream()
    {
        var topic = fixture.NewTopic();
        var db = fixture.Db;

        var published = await this.PublishAsync(topic, from: 0, count: 20);

        var runs = await Task.WhenAll(
            this.RunProbeAsync("dev", topic, "WorkQueue", expect: published.Length),
            this.RunProbeAsync("tst", topic, "WorkQueue", expect: published.Length));

        runs[0].Received.Should().BeEquivalentTo(published, "dev's group must be handed every message");
        runs[1].Received.Should().BeEquivalentTo(published, "tst's group must be handed every message, not the other half");

        for (var p = 0; p < Partitions; p++)
        {
            var key = (RedisKey)SharedKey('s', topic, p);
            var groups = await db.StreamGroupInfoAsync(key);

            groups.Select(static g => g.Name).Should().BeEquivalentTo(
                ["dev:re:" + Consumer, "tst:re:" + Consumer],
                $"partition {p} carries one group per environment and no bare '{Consumer}' group");

            groups.Should().OnlyContain(static g => g.PendingMessageCount == 0, "every delivered entry was acknowledged");
        }
    }

    /// <summary>
    /// The admin path an operator takes on a namespaced topic: the layout is read from the shared
    /// <c>m:</c> hash and stream, and a reset to the end lands on the shared stream's tails — but is
    /// written to this process's position hash, never to the shared namespace.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task An_admin_reset_on_a_namespaced_topic_reads_the_shared_stream_and_writes_local_positions()
    {
        var topic = fixture.NewTopic();
        var db = fixture.Db;

        await this.PublishAsync(topic, from: 0, count: 10);

        (await StreamAdmin.ReadTopicLayoutAsync(fixture.Redis, topic)).Partitions
            .Should().Be(0, "this process's own namespace has no such topic");

        var layout = await StreamAdmin.ReadTopicLayoutAsync(fixture.Redis, topic, SharedNamespace);
        layout.Partitions.Should().Be(Partitions);
        layout.StreamNamespace.Should().Be(SharedNamespace);

        var targets = await StreamAdmin.ResetPositionToEndAsync(fixture.Redis, topic, Consumer, options: layout);

        var tails = await StreamAdmin.ReadTopicTailAsync(fixture.Redis, topic, layout);
        targets.Should().BeEquivalentTo(Enumerable.Range(0, Partitions).Select(p => (p, tails[p])));
        tails.Should().Contain(static t => t != StreamId.Min, "the shared stream has entries to move to");

        var local = await new RedisPositionStore(fixture.Redis).LoadAsync(topic, Consumer, CancellationToken.None);
        local.Should().BeEquivalentTo(targets.ToDictionary(static t => t.Partition, static t => t.Target));

        (await this.PositionKeysAsync(topic)).Should().BeEquivalentTo(
            [KeyNamespace.Prefix() + $"p:{{{topic}}}:{Consumer}"],
            "the reset is this process's cursor, so it is written in this process's namespace only");
    }

    private static string SharedKey(char kind, string topic, int? partition = null)
        => partition is { } p
            ? string.Create(CultureInfo.InvariantCulture, $"{SharedNamespace}:re:{kind}:{{{topic}}}:{p}")
            : string.Create(CultureInfo.InvariantCulture, $"{SharedNamespace}:re:{kind}:{{{topic}}}");

    private static string PositionsKey(string environment, string topic)
        => $"{environment}:re:p:{{{topic}}}:{Consumer}";

    private static async Task<long> StreamLengthAsync(IDatabase db, string ns, string topic)
    {
        var total = 0L;
        for (var p = 0; p < Partitions; p++)
        {
            total += await db.StreamLengthAsync(string.Create(CultureInfo.InvariantCulture, $"{ns}:re:s:{{{topic}}}:{p}"));
        }

        return total;
    }

    private static async Task<Dictionary<string, string>> ReadPositionsAsync(IDatabase db, string key)
        => (await db.HashGetAllAsync(key)).ToDictionary(static e => e.Name.ToString(), static e => e.Value.ToString());

    /// <summary>Every <c>&lt;ns&gt;:re:p:{topic}:probe</c> key, in any namespace.</summary>
    private async Task<string[]> PositionKeysAsync(string topic)
    {
        var keys = new List<string>();
        foreach (var endpoint in fixture.Redis.GetEndPoints())
        {
            var server = fixture.Redis.GetServer(endpoint);
            await foreach (var key in server.KeysAsync(pattern: $"*:re:p:{{{topic}}}:{Consumer}"))
            {
                keys.Add(key.ToString());
            }
        }

        return [.. keys];
    }

    private async Task<string[]> PublishAsync(string topic, int from, int count)
    {
        var publisher = new StreamPublisher(
            fixture.Redis,
            topic,
            new TopicOptions { Partitions = Partitions, StreamNamespace = SharedNamespace });

        var bodies = new string[count];
        for (var i = 0; i < count; i++)
        {
            var n = from + i;
            bodies[i] = string.Create(CultureInfo.InvariantCulture, $"m{n}");
            await publisher.PublishAsync(
                string.Create(CultureInfo.InvariantCulture, $"k{n % 7}"),
                Encoding.UTF8.GetBytes(bodies[i]),
                "test.event");
        }

        return bodies;
    }

    /// <summary>
    /// Runs one environment's consumer as a child process until it has received
    /// <paramref name="expect"/> messages (plus a short settle, so an extra delivery would be seen),
    /// and returns what it received.
    /// </summary>
    private async Task<ProbeRun> RunProbeAsync(string environment, string topic, string delivery, int expect)
    {
        var probe = Path.Combine(AppContext.BaseDirectory, "RedisEvents.NamespaceProbe.dll");
        File.Exists(probe).Should().BeTrue($"the probe is built beside the tests ({probe})");

        var start = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        start.ArgumentList.Add(probe);
        foreach (var arg in new[]
        {
            $"--Streams:ConnectionString={fixture.ConnectionString}",
            $"--Streams:Consumer={Consumer}",
            $"--Streams:Topics:{topic}:Partitions={Partitions}",
            $"--Streams:Topics:{topic}:StreamNamespace={SharedNamespace}",
            $"--Streams:Consumers:0:Topic={topic}",
            $"--Streams:Consumers:0:Delivery={delivery}",
            "--Streams:Consumers:0:PersistIntervalMs=100",
            "--Streams:Consumers:0:BlockMs=200",
            $"--Probe:Topic={topic}",
            string.Create(CultureInfo.InvariantCulture, $"--Probe:Expect={expect}"),
            "--Probe:TimeoutSeconds=30",
            "--Logging:LogLevel:Default=Warning",
        })
        {
            start.ArgumentList.Add(arg);
        }

        // The environment decides the process namespace; nothing else may override it.
        start.Environment["ASPNETCORE_ENVIRONMENT"] = environment;
        start.Environment.Remove("DOTNET_ENVIRONMENT");
        start.Environment.Remove("STREAMS_KEY_PREFIX");
        start.Environment["POD_NAME"] = environment + "-0";

        using var process = Process.Start(start)
            ?? throw new InvalidOperationException("The probe process did not start.");

        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();

        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        try
        {
            await process.WaitForExitAsync(limit.Token);
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException($"The {environment} probe did not exit within 90s.\n{await stdout}\n{await stderr}");
        }

        var output = await stdout;
        var errors = await stderr;

        process.ExitCode.Should().Be(0, $"the {environment} probe should receive {expect} message(s).\nstdout:\n{output}\nstderr:\n{errors}");

        var received = output
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(static line => line.StartsWith("msg:", StringComparison.Ordinal))
            .Select(static line => line["msg:".Length..])
            .ToArray();

        return new ProbeRun(received);
    }

    private sealed record ProbeRun(string[] Received);
}
