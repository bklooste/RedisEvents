namespace Orange.Lib.Streams.Errors;

/// <summary>
/// Exception thrown when a transport-level operation on Redis Streams fails.
///
/// Examples include <c>XREAD</c> or <c>XADD</c> command failures, codec version mismatches,
/// or other communication failures with the Redis server.
///
/// When raised at runtime, <see cref="StreamTransportException"/> receives the
/// <see cref="DontIgnoreException"/> blocking-retry treatment: the partition blocks and retries
/// the failing batch indefinitely with exponential backoff, without advancing the position.
///
/// This ensures that temporary Redis unavailability does not result in silent data loss,
/// but also means the service's lag will grow during an outage. The
/// <c>streams.lag.ms</c> and <c>streams.lag.entries</c> metrics must be monitored and alerted
/// to prevent the producer's <c>MAXLEN</c> trimming from deleting unprocessed entries.
/// </summary>
public sealed class StreamTransportException : DontIgnoreException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="StreamTransportException"/> class.
    /// </summary>
    public StreamTransportException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamTransportException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The error message that explains the reason for the exception.</param>
    public StreamTransportException(string? message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamTransportException"/> class with a specified error message
    /// and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The error message that explains the reason for the exception.</param>
    /// <param name="innerException">The exception that is the cause of the current exception, or <c>null</c> if no inner exception is specified.</param>
    public StreamTransportException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
