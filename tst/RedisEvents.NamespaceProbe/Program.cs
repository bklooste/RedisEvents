// One consumer service for StreamNamespaceTests: consume Probe:Topic until Probe:Expect messages
// have arrived, print each body as "msg:<body>", then stop cleanly so the consumer's positions are
// flushed. Everything else — the Streams section, the environment (ASPNETCORE_ENVIRONMENT, which
// decides the process key namespace) — comes from the command line and the environment, exactly as
// it would for a deployed service.
using System.Text;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

using RedisEvents.Extensions;

var builder = Host.CreateApplicationBuilder(args);

var topic = builder.Configuration["Probe:Topic"]
    ?? throw new InvalidOperationException("Probe:Topic is required.");
var expect = builder.Configuration.GetValue("Probe:Expect", 0);
var timeout = TimeSpan.FromSeconds(builder.Configuration.GetValue("Probe:TimeoutSeconds", 60));

// Enough to see a duplicate or an extra delivery arrive after the expected count.
var settle = TimeSpan.FromMilliseconds(builder.Configuration.GetValue("Probe:SettleMs", 750));

var received = new List<string>();
var gate = new Lock();
var enough = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

builder.AddStream(topic, (batch, _) =>
{
    lock (gate)
    {
        foreach (var message in batch.Span)
        {
            received.Add(Encoding.UTF8.GetString(message.BodySpan));
        }

        if (received.Count >= expect)
        {
            enough.TrySetResult();
        }
    }

    return ValueTask.CompletedTask;
});

using var host = builder.Build();
await host.StartAsync();

var finished = await Task.WhenAny(enough.Task, Task.Delay(timeout)) == enough.Task;
if (finished)
{
    await Task.Delay(settle);
}

// A clean stop flushes the consumer's positions, which the test then reads.
await host.StopAsync();

string[] snapshot;
lock (gate)
{
    snapshot = [.. received];
}

foreach (var body in snapshot)
{
    Console.Out.WriteLine("msg:" + body);
}

Console.Out.WriteLine(finished ? "done" : $"timeout after {snapshot.Length} of {expect}");
Console.Out.Flush();

return finished ? 0 : 2;
