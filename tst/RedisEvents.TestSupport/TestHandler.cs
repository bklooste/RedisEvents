using System.Text;

using RedisEvents.Consumer;
using RedisEvents.Wire;

namespace RedisEvents.Tests;

/// <summary>
/// A handler that records everything it is given and can be awaited on "N messages seen".
/// </summary>
/// <remarks>
/// <para>
/// Nearly every service test in this project needs the same two things: a handler that keeps what it
/// received, and a way to wait for delivery without a hard-coded sleep. This is both, in one place,
/// so the polling loop is written once rather than copy-pasted into every file and left to rot.
/// </para>
/// <para>
/// <b>Messages are deep-copied on arrival.</b> A batch handed to a handler is a window onto a pooled
/// array and onto the Redis read buffer, and both are recycled the moment the handler returns —
/// keeping the memory is the one genuinely dangerous mistake the consumer API allows. Every recorded
/// message therefore goes through <see cref="StreamBatchExtensions.Copy(in StreamMsg)"/> and owns its
/// own storage, so assertions made minutes later still read the right bytes.
/// </para>
/// <para>
/// <b>Recording happens before <see cref="OnBatch"/> runs.</b> A test that makes the handler throw
/// still gets a record of what was delivered, which is what the error-policy tests assert on: a
/// batch that is retried appears in <see cref="Messages"/> once per delivery.
/// </para>
/// <para>
/// The type implements both <see cref="IBatchHandler"/> and <see cref="IMessageHandler"/>, so it can
/// be wired to either shape of consumer without a second class.
/// </para>
/// </remarks>
public sealed class TestHandler : IBatchHandler, IMessageHandler
{
    private readonly Lock gate = new();
    private readonly List<StreamMsg> messages = [];
    private readonly List<int> threadIds = [];
    private readonly List<Waiter> waiters = [];

    private int batches;
    private int inFlight;
    private int peakInFlight;

    /// <summary>
    /// Invoked once per batch, after the batch has been recorded. Throw from here to exercise an
    /// error policy, or await from here to make the handler slow.
    /// </summary>
    public Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask>? OnBatch { get; set; }

    /// <summary>
    /// Time spent inside each batch before returning. For backpressure tests that need a handler
    /// slower than the reader — not a synchronisation device.
    /// </summary>
    public TimeSpan Delay { get; set; } = TimeSpan.Zero;

    /// <summary>Every message seen, in arrival order, each owning its own storage.</summary>
    public IReadOnlyList<StreamMsg> Messages
    {
        get
        {
            lock (this.gate)
            {
                return this.messages.ToArray();
            }
        }
    }

    /// <summary>How many messages have been seen. Cheap; safe to poll.</summary>
    public int Count
    {
        get
        {
            lock (this.gate)
            {
                return this.messages.Count;
            }
        }
    }

    /// <summary>How many batches have been delivered.</summary>
    public int Batches => Volatile.Read(ref this.batches);

    /// <summary>
    /// The largest number of messages that were ever inside the handler at once — what the
    /// backpressure tests bound against <c>Capacity × BatchSize</c>.
    /// </summary>
    public int PeakInFlight => Volatile.Read(ref this.peakInFlight);

    /// <summary>The managed thread ids batches were handled on, deduplicated (S21d).</summary>
    public IReadOnlyList<int> ThreadIds
    {
        get
        {
            lock (this.gate)
            {
                return this.threadIds.Distinct().ToArray();
            }
        }
    }

    /// <summary>Message bodies decoded as UTF-8, in arrival order.</summary>
    public IReadOnlyList<string> Bodies
    {
        get
        {
            lock (this.gate)
            {
                return this.messages.Select(static m => Encoding.UTF8.GetString(m.Body.Span)).ToArray();
            }
        }
    }

    /// <summary>Waits until at least <paramref name="count"/> messages have been seen.</summary>
    /// <remarks>
    /// Completes the instant the count is reached — there is no polling interval to tune, because
    /// the waiter is signalled from the recording path itself.
    /// </remarks>
    /// <param name="count">The number of messages to wait for.</param>
    /// <param name="timeout">How long to wait; 30 seconds by default.</param>
    /// <param name="ct">Cancels the wait.</param>
    /// <exception cref="TimeoutException">Fewer than <paramref name="count"/> messages arrived in time.</exception>
    public async Task WaitForAsync(int count, TimeSpan? timeout = null, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);

        var limit = timeout ?? TimeSpan.FromSeconds(30);
        Waiter waiter;

        lock (this.gate)
        {
            if (this.messages.Count >= count)
            {
                return;
            }

            waiter = new Waiter(count);
            this.waiters.Add(waiter);
        }

        try
        {
            await waiter.Task.WaitAsync(limit, ct).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException(
                $"Timed out after {limit.TotalSeconds:0.##}s waiting for {count} message(s); " +
                $"saw {this.Count} in {this.Batches} batch(es).");
        }
        finally
        {
            lock (this.gate)
            {
                _ = this.waiters.Remove(waiter);
            }
        }
    }

    /// <summary>
    /// Waits for <paramref name="count"/> messages and then keeps watching for
    /// <paramref name="settle"/> to confirm no more arrive — the "exactly N, not N+1" assertion.
    /// </summary>
    /// <exception cref="TimeoutException">Fewer than <paramref name="count"/> messages arrived in time.</exception>
    /// <exception cref="InvalidOperationException">More than <paramref name="count"/> messages arrived.</exception>
    public async Task WaitForExactlyAsync(
        int count,
        TimeSpan? settle = null,
        TimeSpan? timeout = null,
        CancellationToken ct = default)
    {
        await this.WaitForAsync(count, timeout, ct).ConfigureAwait(false);
        await Task.Delay(settle ?? TimeSpan.FromMilliseconds(500), ct).ConfigureAwait(false);

        var seen = this.Count;
        if (seen != count)
        {
            throw new InvalidOperationException(
                $"Expected exactly {count} message(s) but {seen} were delivered — duplicates or over-delivery.");
        }
    }

    /// <inheritdoc />
    public async ValueTask HandleAsync(ReadOnlyMemory<StreamMsg> batch, CancellationToken ct)
    {
        var depth = Interlocked.Add(ref this.inFlight, batch.Length);
        Bump(ref this.peakInFlight, depth);
        _ = Interlocked.Increment(ref this.batches);

        try
        {
            this.Record(batch.Span);

            if (this.Delay > TimeSpan.Zero)
            {
                await Task.Delay(this.Delay, ct).ConfigureAwait(false);
            }

            if (this.OnBatch is { } hook)
            {
                await hook(batch, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            _ = Interlocked.Add(ref this.inFlight, -batch.Length);
        }
    }

    /// <inheritdoc />
    public ValueTask HandleAsync(in StreamMsg msg, CancellationToken ct)
    {
        // The single-message shape has no batch to hand the hook, so it gets a one-element copy.
        StreamMsg[] one = [msg.Copy()];
        return this.HandleAsync(one.AsMemory(), ct);
    }

    private static void Bump(ref int target, int candidate)
    {
        var current = Volatile.Read(ref target);
        while (candidate > current)
        {
            var seen = Interlocked.CompareExchange(ref target, candidate, current);
            if (seen == current)
            {
                return;
            }

            current = seen;
        }
    }

    private void Record(ReadOnlySpan<StreamMsg> batch)
    {
        List<Waiter>? ready = null;

        lock (this.gate)
        {
            for (var i = 0; i < batch.Length; i++)
            {
                this.messages.Add(batch[i].Copy());
            }

            this.threadIds.Add(Environment.CurrentManagedThreadId);

            var total = this.messages.Count;
            for (var i = this.waiters.Count - 1; i >= 0; i--)
            {
                if (this.waiters[i].Target <= total)
                {
                    (ready ??= []).Add(this.waiters[i]);
                    this.waiters.RemoveAt(i);
                }
            }
        }

        // Completed outside the lock: a continuation running inline must not re-enter it.
        if (ready is not null)
        {
            foreach (var waiter in ready)
            {
                waiter.Complete();
            }
        }
    }

    private sealed class Waiter(int target)
    {
        private readonly TaskCompletionSource source =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal int Target { get; } = target;

        internal Task Task => this.source.Task;

        internal void Complete() => this.source.TrySetResult();
    }
}
