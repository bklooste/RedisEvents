using FluentAssertions;

using RedisEvents.Projections;
using RedisEvents.Wire;

namespace RedisEvents.Projections.UnitTests;

/// <summary>
/// Trivial value-equality coverage for <see cref="EventMeta"/> — it is a plain record struct with no
/// behaviour of its own, so there is little else to test.
/// </summary>
public class EventMetaTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Two_instances_with_the_same_values_are_equal()
    {
        var id = new StreamId(1, 0);

        var a = new EventMeta("agg-1", id, "corr-1");
        var b = new EventMeta("agg-1", id, "corr-1");

        a.Should().Be(b);
        (a == b).Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Instances_differing_by_id_are_not_equal()
    {
        var a = new EventMeta("agg-1", new StreamId(1, 0), "corr-1");
        var b = new EventMeta("agg-1", new StreamId(1, 1), "corr-1");

        a.Should().NotBe(b);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Properties_round_trip_the_constructor_arguments()
    {
        var id = new StreamId(42, 1);

        var meta = new EventMeta("agg-9", id, "corr-9");

        meta.PartitionKey.Should().Be("agg-9");
        meta.Id.Should().Be(id);
        meta.CorrelationId.Should().Be("corr-9");
    }
}
