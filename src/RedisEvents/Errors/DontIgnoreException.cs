namespace RedisEvents.Errors;

/// <summary>
/// Base exception class for errors that must block and retry indefinitely with backoff.
///
/// When a <see cref="DontIgnoreException"/> (or any subclass) is thrown at runtime, the affected
/// partition blocks and retries the failing batch indefinitely with exponential backoff (1s → 2s → 4s → 8s → 16s → 30s cap).
/// The position never advances, ensuring no data is skipped. Other partitions continue running normally.
///
/// Services can derive their own subclasses to declare failures that must not be silently skipped.
///
/// Contrast with other exceptions, which are logged at Error level and skipped: the position advances,
/// the next batch is processed, and the service continues operating (best effort).
///
/// Note: <see cref="StreamConfigurationException"/> during startup validation fails fast without retry,
/// as no amount of backoff fixes a configuration issue. Only exceptions thrown at runtime from handlers
/// or the transport layer receive the blocking-retry treatment.
/// </summary>
public abstract class DontIgnoreException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DontIgnoreException"/> class.
    /// </summary>
    protected DontIgnoreException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DontIgnoreException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The error message that explains the reason for the exception.</param>
    protected DontIgnoreException(string? message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DontIgnoreException"/> class with a specified error message
    /// and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The error message that explains the reason for the exception.</param>
    /// <param name="innerException">The exception that is the cause of the current exception, or <c>null</c> if no inner exception is specified.</param>
    protected DontIgnoreException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
