using System.Text.Json;

using FluentAssertions;

using RedisEvents.Consumer;
using RedisEvents.Wire;

namespace RedisEvents.UnitTests;

/// <summary>
/// <see cref="TypedConsumeExtensions"/>: the read-side mirror of <see cref="TypedPublishTests"/>.
/// Reuses <see cref="TypedOrder"/> and <see cref="TypedPublishJson"/> so a round trip through both
/// extension classes is exercised with the same payload type.
/// </summary>
public class TypedConsumeTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Deserialize_reads_a_single_message_body()
    {
        var msg = Build(new TypedOrder(7, "seven"));

        var order = msg.Deserialize(TypedPublishJson.Default.TypedOrder);

        order.Should().Be(new TypedOrder(7, "seven"));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Deserialize_rejects_a_null_typeInfo()
    {
        var msg = Build(new TypedOrder(1, "a"));

        var act = () => msg.Deserialize<TypedOrder>(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Deserialize_batch_reads_every_message_in_order()
    {
        ReadOnlyMemory<StreamMsg> batch = new[]
        {
            Build(new TypedOrder(1, "one")),
            Build(new TypedOrder(2, "two")),
            Build(new TypedOrder(3, "three")),
        };

        var orders = batch.Deserialize(TypedPublishJson.Default.TypedOrder);

        orders.Should().Equal(
            new TypedOrder(1, "one"),
            new TypedOrder(2, "two"),
            new TypedOrder(3, "three"));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Deserialize_batch_with_no_messages_returns_an_empty_array()
    {
        var orders = ReadOnlyMemory<StreamMsg>.Empty.Deserialize(TypedPublishJson.Default.TypedOrder);

        orders.Should().BeEmpty();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Deserialize_batch_rejects_a_null_typeInfo()
    {
        ReadOnlyMemory<StreamMsg> batch = new[] { Build(new TypedOrder(1, "a")) };

        var act = () => batch.Deserialize<TypedOrder>(null!);

        act.Should().Throw<ArgumentNullException>();
    }

    private static StreamMsg Build(TypedOrder order)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(order, TypedPublishJson.Default.TypedOrder);
        return new StreamMsg(body, typeof(TypedOrder).FullName!, new StreamId(1, 0), 0, "k", string.Empty, null, HeaderBlock.Empty);
    }
}
