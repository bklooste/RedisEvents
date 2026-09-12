namespace RedisEvents.Errors;

/// <summary>
/// Exception thrown when a stream configuration is invalid.
///
/// Examples include a partition decrease or invalid configuration parameters such as a negative <c>BlockMs</c>.
///
/// When raised during startup validation, <see cref="StreamConfigurationException"/> fails fast and does not retry,
/// as no amount of backoff can fix a configuration error. The pod is faulted and Kubernetes restarts it,
/// signaling that the configuration must be corrected.
///
/// Contrast with runtime exceptions, which are not usually configuration errors and receive the
/// <see cref="DontIgnoreException"/> blocking-retry treatment.
/// </summary>
public sealed class StreamConfigurationException : DontIgnoreException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="StreamConfigurationException"/> class.
    /// </summary>
    public StreamConfigurationException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamConfigurationException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The error message that explains the reason for the exception.</param>
    public StreamConfigurationException(string? message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamConfigurationException"/> class with a specified error message
    /// and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The error message that explains the reason for the exception.</param>
    /// <param name="innerException">The exception that is the cause of the current exception, or <c>null</c> if no inner exception is specified.</param>
    public StreamConfigurationException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
