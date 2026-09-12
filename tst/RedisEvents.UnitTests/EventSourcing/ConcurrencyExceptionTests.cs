using FluentAssertions;

using RedisEvents.Errors;
using RedisEvents.EventSourcing;

namespace RedisEvents.UnitTests.EventSourcing;

/// <summary>
/// Unit tests for <see cref="ConcurrencyException"/>: the three properties a caller branches on, the
/// message an operator reads, and the base class it deliberately does <b>not</b> have.
/// </summary>
[Trait("TestType", "UnitTest")]
public sealed class ConcurrencyExceptionTests
{
    /// <summary>
    /// The aggregate family, the id and the version that was required are all carried, so a retry
    /// loop can log or branch without parsing the message.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Carries_the_aggregate_the_id_and_the_expected_version()
    {
        var exception = new ConcurrencyException("Inventory", "42", 7);

        exception.AggregateName.Should().Be("Inventory");
        exception.Id.Should().Be("42");
        exception.ExpectedVersion.Should().Be(7);
    }

    /// <summary>
    /// The message names all three, because the first thing an operator asks of a concurrency log
    /// line is "which aggregate, and at what version".
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Message_names_the_aggregate_the_id_and_the_expected_version()
    {
        var exception = new ConcurrencyException("Inventory", "42", 7);

        exception.Message.Should().Be("'Inventory' '42': expected version 7, but it has changed.");
    }

    /// <summary>
    /// Not a <see cref="DontIgnoreException"/>, and that is a contract rather than an oversight:
    /// blocking a partition to retry an identical save forever could never succeed, because only a
    /// reload moves the expected version. A service that wants escalation wraps this in its own
    /// <see cref="DontIgnoreException"/> subclass after N attempts.
    /// </summary>
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Is_a_plain_exception_and_not_a_dont_ignore_exception()
    {
        var exception = new ConcurrencyException("Inventory", "42", 0);

        exception.Should().BeAssignableTo<Exception>();
        exception.Should().NotBeAssignableTo<DontIgnoreException>(
            "a concurrency loss is caller-resolvable; blocking and retrying the same batch would wedge the partition");
    }
}
