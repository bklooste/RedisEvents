using RedisEvents.Errors;

namespace RedisEvents.Restate;

/// <summary>
/// Thrown by <see cref="RestateStartHandler{T}"/> when one or more kept messages of a batch could not
/// be started, so that the partition blocks and retries the whole batch instead of acking it.
/// </summary>
/// <remarks>
/// <para>
/// A <see cref="DontIgnoreException"/>: the partition worker never advances past a batch that throws
/// it, under any <c>ErrorPolicy</c>. It retries the same batch at 1 s to 30 s until the handler
/// returns. The starts that were accepted on an earlier attempt are sent again and answer
/// <c>202 PreviouslyAccepted</c>, because the workflow key is the dedupe.
/// </para>
/// <para>
/// It is the only exception type <see cref="RestateStartHandler{T}.HandleAsync"/> lets out. Any other
/// failure (a <see cref="HttpRequestException"/>, a timeout, a throwing <c>Keep</c> or <c>Map</c>, a
/// serializer that does not know the input type, a cancelled token) is caught and reported through
/// this type, because an ordinary exception would be logged and <em>skipped</em> by the default
/// <c>ErrorPolicy.BestEffort</c>, silently dropping the starts.
/// </para>
/// </remarks>
public sealed class RestateStartBlockedException : DontIgnoreException
{
    /// <summary>How many failures the message names before it summarises the rest as a count.</summary>
    internal const int MaxNamedInMessage = 20;

    /// <summary>Creates the exception for a batch with failed starts.</summary>
    /// <param name="failedKeys">One entry per failed item: <c>{workflow}/{key}</c>, or <c>stream id {id}</c> when the item failed before it had a key.</param>
    /// <param name="kept">How many messages of the batch were kept.</param>
    /// <param name="reasons">The last reason per failed item, in the same order as <paramref name="failedKeys"/>.</param>
    /// <param name="shuttingDown">Whether the handler's token was cancelled before every start was accepted.</param>
    /// <param name="innerException">The first underlying exception, if any.</param>
    internal RestateStartBlockedException(
        IReadOnlyList<string> failedKeys,
        int kept,
        IReadOnlyList<string> reasons,
        bool shuttingDown,
        Exception? innerException)
        : base(BuildMessage(failedKeys, kept, reasons, shuttingDown), innerException)
    {
        this.FailedKeys = failedKeys;
        this.Kept = kept;
        this.ShuttingDown = shuttingDown;
    }

    /// <summary>
    /// The items that were not accepted: <c>{workflow}/{key}</c>, or <c>stream id {id}</c> for a
    /// message whose <c>Keep</c>, <c>Map</c> or input serialization threw.
    /// </summary>
    public IReadOnlyList<string> FailedKeys { get; }

    /// <summary>How many messages of the batch were kept (accepted plus failed).</summary>
    public int Kept { get; }

    /// <summary>
    /// <see langword="true"/> when the token passed to <c>HandleAsync</c> was cancelled before every
    /// start was accepted. The batch was not acked and redelivers; the worker stops on the cancelled
    /// token rather than blocking.
    /// </summary>
    public bool ShuttingDown { get; }

    private static string BuildMessage(IReadOnlyList<string> failedKeys, int kept, IReadOnlyList<string> reasons, bool shuttingDown)
    {
        var builder = new System.Text.StringBuilder();
        builder.Append("Restate: ")
            .Append(failedKeys.Count)
            .Append(" of ")
            .Append(kept)
            .Append(" kept start(s) were not accepted");

        if (shuttingDown)
        {
            builder.Append(" before the handler's token was cancelled");
        }

        builder.Append("; the batch is not acked and is retried whole (accepted starts dedupe on their workflow keys). Failed: ");

        var named = Math.Min(failedKeys.Count, MaxNamedInMessage);
        for (var i = 0; i < named; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            builder.Append(failedKeys[i]);
            if (i < reasons.Count && !string.IsNullOrEmpty(reasons[i]))
            {
                builder.Append(" (").Append(reasons[i]).Append(')');
            }
        }

        if (failedKeys.Count > named)
        {
            builder.Append(", and ").Append(failedKeys.Count - named).Append(" more");
        }

        builder.Append('.');
        return builder.ToString();
    }
}
