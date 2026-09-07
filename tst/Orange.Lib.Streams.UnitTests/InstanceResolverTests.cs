using FluentAssertions;
using Orange.Lib.Streams.Config;
using Orange.Lib.Streams.Errors;

namespace Orange.Lib.Streams.UnitTests;

/// <summary>
/// Unit tests for <see cref="InstanceResolver"/> — identity precedence, pod-ordinal parsing, and the
/// partition ownership arithmetic that must tile <c>[0, partitions)</c> with no gap and no overlap.
/// </summary>
public class InstanceResolverTests
{
    /// <summary>An environment lookup backed by a dictionary, so no process state is mutated.</summary>
    private static Func<string, string?> Env(params (string Key, string Value)[] entries)
    {
        var map = entries.ToDictionary(e => e.Key, e => e.Value, StringComparer.Ordinal);
        return key => map.TryGetValue(key, out var value) ? value : null;
    }

    private static readonly Func<string, string?> EmptyEnv = _ => null;

    // ---------------------------------------------------------------- ownership tiling

    public static TheoryData<int, int> Topologies =>
        new()
        {
            { 1, 1 },
            { 8, 1 },
            { 8, 4 },
            { 8, 3 },
            { 4, 2 },
            { 5, 2 },
            { 2, 4 },
        };

    [Theory]
    [MemberData(nameof(Topologies))]
    [Trait("TestType", "UnitTest")]
    public void Owned_TilesPartitionSpace_NoGapsNoOverlaps(int partitions, int count)
    {
        var owner = new int?[partitions];

        for (var index = 0; index < count; index++)
        {
            var range = InstanceResolver.Owned(partitions, count, index);

            range.Length.Should().BeGreaterThanOrEqualTo(0);
            range.ToArray().Should().HaveCount(range.Length);

            foreach (var partition in range.ToArray())
            {
                partition.Should().BeInRange(0, partitions - 1);
                owner[partition].Should().BeNull(
                    "partition {0} must be owned by exactly one instance, but {1} and {2} both claim it",
                    partition, owner[partition], index);
                owner[partition] = index;

                range.Contains(partition).Should().BeTrue();
            }
        }

        owner.Should().OnlyContain(o => o.HasValue, "every partition must be owned by someone");
    }

    [Theory]
    [MemberData(nameof(Topologies))]
    [Trait("TestType", "UnitTest")]
    public void Owned_IsBalancedToWithinOnePartition(int partitions, int count)
    {
        var lengths = Enumerable.Range(0, count)
            .Select(i => InstanceResolver.Owned(partitions, count, i).Length)
            .ToArray();

        lengths.Sum().Should().Be(partitions);
        (lengths.Max() - lengths.Min()).Should().BeLessThanOrEqualTo(1);
    }

    [Theory]
    [MemberData(nameof(Topologies))]
    [Trait("TestType", "UnitTest")]
    public void OwnerOf_AgreesWithOwned(int partitions, int count)
    {
        for (var partition = 0; partition < partitions; partition++)
        {
            var owner = InstanceResolver.OwnerOf(partition, partitions, count);

            owner.Should().BeInRange(0, count - 1);
            InstanceResolver.Owned(partitions, count, owner).Contains(partition).Should().BeTrue();
        }
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Owned_FourPartitionsTwoInstances_SplitsInHalf()
    {
        InstanceResolver.Owned(4, 2, 0).Should().Be(new PartitionRange(0, 2));
        InstanceResolver.Owned(4, 2, 1).Should().Be(new PartitionRange(2, 2));

        InstanceResolver.Owned(4, 2, 0).ToArray().Should().Equal(0, 1);
        InstanceResolver.Owned(4, 2, 1).ToArray().Should().Equal(2, 3);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Owned_FivePartitionsTwoInstances_RemainderGoesToTheEarlierInstance()
    {
        InstanceResolver.Owned(5, 2, 0).Should().Be(new PartitionRange(0, 3));
        InstanceResolver.Owned(5, 2, 1).Should().Be(new PartitionRange(3, 2));

        InstanceResolver.Owned(5, 2, 0).ToArray().Should().Equal(0, 1, 2);
        InstanceResolver.Owned(5, 2, 1).ToArray().Should().Equal(3, 4);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Owned_SurplusInstances_OwnNothing()
    {
        InstanceResolver.Owned(2, 4, 2).IsEmpty.Should().BeTrue();
        InstanceResolver.Owned(2, 4, 3).IsEmpty.Should().BeTrue();
        InstanceResolver.Owned(2, 4, 3).ToArray().Should().BeEmpty();
        InstanceResolver.Owned(2, 4, 3).ToString().Should().Be("[]");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Owned_IndexBeyondCount_OwnsNothing()
    {
        var identity = new InstanceIdentity(2, 5, InstanceSource.Environment, InstanceSource.PodOrdinal, "svc-5");

        InstanceResolver.Owned(8, identity).IsEmpty.Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Owned_RejectsInvalidArguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => InstanceResolver.Owned(-1, 1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => InstanceResolver.Owned(4, 0, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => InstanceResolver.Owned(4, 2, -1));
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void DescribeOwnership_NamesEveryPartitionsPod()
    {
        var identity = new InstanceIdentity(2, 0, InstanceSource.Environment, InstanceSource.PodOrdinal, "svc-0");

        var line = InstanceResolver.DescribeOwnership(4, identity);

        line.Should().Be("instance 0/2 (pod svc-0) owns partitions [0,1] of 4 | full map: 0->svc-0 1->svc-0 2->svc-1 3->svc-1");
    }

    // ---------------------------------------------------------------- pod ordinal parsing

    [Theory]
    [InlineData("svc-0", 0)]
    [InlineData("svc-12", 12)]
    [InlineData("bet-processing-3", 3)]
    [InlineData("svc--3", 3)]
    [Trait("TestType", "UnitTest")]
    public void TryParsePodOrdinal_ParsesStatefulSetNames(string podName, int expected)
    {
        InstanceResolver.TryParsePodOrdinal(podName, out var ordinal).Should().BeTrue();
        ordinal.Should().Be(expected);
    }

    [Theory]
    [InlineData("svc-abc")]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("svc")]
    [InlineData("-0")]
    [InlineData("svc-")]
    [InlineData("svc-7d4f8b-x2k9p")]
    [InlineData("svc- 3")]
    [InlineData("svc-+3")]
    [InlineData("svc-99999999999999999999")]
    [Trait("TestType", "UnitTest")]
    public void TryParsePodOrdinal_FallsThroughOnNonOrdinalNames(string? podName)
    {
        InstanceResolver.TryParsePodOrdinal(podName, out var ordinal).Should().BeFalse();
        ordinal.Should().Be(0);
    }

    // ---------------------------------------------------------------- precedence

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_ConfigBeatsEnvironmentAndPodOrdinal()
    {
        var identity = InstanceResolver.Resolve(
            new InstanceOptions { Count = 3, Index = 2 },
            Env(("STREAMS_INSTANCE_COUNT", "8"), ("STREAMS_INSTANCE_INDEX", "7"), ("POD_NAME", "svc-5")));

        identity.Count.Should().Be(3);
        identity.Index.Should().Be(2);
        identity.CountSource.Should().Be(InstanceSource.Config);
        identity.IndexSource.Should().Be(InstanceSource.Config);
        identity.PodName.Should().Be("svc-5");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_EnvironmentBeatsPodOrdinal()
    {
        var identity = InstanceResolver.Resolve(
            options: null,
            Env(("STREAMS_INSTANCE_COUNT", "8"), ("STREAMS_INSTANCE_INDEX", "7"), ("POD_NAME", "svc-5")));

        identity.Count.Should().Be(8);
        identity.Index.Should().Be(7);
        identity.CountSource.Should().Be(InstanceSource.Environment);
        identity.IndexSource.Should().Be(InstanceSource.Environment);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_PodOrdinalBeatsFallback()
    {
        var identity = InstanceResolver.Resolve(
            options: null,
            Env(("STREAMS_INSTANCE_COUNT", "4"), ("POD_NAME", "svc-2")));

        identity.Count.Should().Be(4);
        identity.Index.Should().Be(2);
        identity.CountSource.Should().Be(InstanceSource.Environment);
        identity.IndexSource.Should().Be(InstanceSource.PodOrdinal);
        identity.PodName.Should().Be("svc-2");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_NothingConfigured_FallsBackToSingleInstanceOwningEverything()
    {
        var identity = InstanceResolver.Resolve(options: null, EmptyEnv);

        identity.Count.Should().Be(1);
        identity.Index.Should().Be(0);
        identity.CountSource.Should().Be(InstanceSource.Fallback);
        identity.IndexSource.Should().Be(InstanceSource.Fallback);
        identity.PodName.Should().BeNull();

        InstanceResolver.Owned(8, identity).ToArray().Should().Equal(0, 1, 2, 3, 4, 5, 6, 7);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_NullPodName_FallsThroughToIndexZero()
    {
        var identity = InstanceResolver.Resolve(options: null, Env(("STREAMS_INSTANCE_COUNT", "3")));

        identity.Index.Should().Be(0);
        identity.IndexSource.Should().Be(InstanceSource.Fallback);
        identity.PodName.Should().BeNull();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_NonOrdinalPodName_FallsThroughToIndexZero()
    {
        var identity = InstanceResolver.Resolve(options: null, Env(("POD_NAME", "svc-abc")));

        identity.Index.Should().Be(0);
        identity.IndexSource.Should().Be(InstanceSource.Fallback);
        identity.PodName.Should().Be("svc-abc");
    }

    [Theory]
    [InlineData("nonsense")]
    [InlineData("0")]
    [InlineData("-2")]
    [Trait("TestType", "UnitTest")]
    public void Resolve_MalformedCountVariable_IsNotFatal(string raw)
    {
        var identity = InstanceResolver.Resolve(options: null, Env(("STREAMS_INSTANCE_COUNT", raw)));

        identity.Count.Should().Be(1);
        identity.CountSource.Should().Be(InstanceSource.Fallback);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_MalformedIndexVariable_FallsThroughToPodOrdinal()
    {
        var identity = InstanceResolver.Resolve(
            options: null,
            Env(("STREAMS_INSTANCE_COUNT", "4"), ("STREAMS_INSTANCE_INDEX", "not-a-number"), ("POD_NAME", "svc-3")));

        identity.Index.Should().Be(3);
        identity.IndexSource.Should().Be(InstanceSource.PodOrdinal);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_ConfigCountWithEnvironmentIndex_MixesSources()
    {
        var identity = InstanceResolver.Resolve(
            new InstanceOptions { Count = 2 },
            Env(("STREAMS_INSTANCE_INDEX", "1"), ("POD_NAME", "svc-0")));

        identity.Count.Should().Be(2);
        identity.CountSource.Should().Be(InstanceSource.Config);
        identity.Index.Should().Be(1);
        identity.IndexSource.Should().Be(InstanceSource.Environment);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_ContradictoryExplicitConfig_Throws()
    {
        var ex = Assert.Throws<StreamConfigurationException>(
            () => InstanceResolver.Resolve(new InstanceOptions { Count = 2, Index = 2 }, EmptyEnv));

        ex.Message.Should().Contain("Streams:Instances");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_NegativeConfigValues_Throw()
    {
        Assert.Throws<StreamConfigurationException>(
            () => InstanceResolver.Resolve(new InstanceOptions { Count = 0 }, EmptyEnv))
            .Message.Should().Contain("Streams:Instances:Count");

        Assert.Throws<StreamConfigurationException>(
            () => InstanceResolver.Resolve(new InstanceOptions { Index = -1 }, EmptyEnv))
            .Message.Should().Contain("Streams:Instances:Index");
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_DriftedPoolFromAmbientSources_DoesNotThrow()
    {
        // Scale-down drift: pod ordinal 5 survives while the declared count is 2. This must be
        // reported by the ownership registry, not crash the process at startup.
        var identity = InstanceResolver.Resolve(
            options: null,
            Env(("STREAMS_INSTANCE_COUNT", "2"), ("POD_NAME", "svc-5")));

        identity.Count.Should().Be(2);
        identity.Index.Should().Be(5);
        InstanceResolver.Owned(8, identity).IsEmpty.Should().BeTrue();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_RequiresAnEnvironmentLookup()
        => Assert.Throws<ArgumentNullException>(() => InstanceResolver.Resolve(null, environment: null!));

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void Resolve_TrimsWhitespaceAroundAmbientValues()
    {
        var identity = InstanceResolver.Resolve(
            options: null,
            Env(("STREAMS_INSTANCE_COUNT", " 4 "), ("POD_NAME", " svc-1 ")));

        identity.Count.Should().Be(4);
        identity.Index.Should().Be(1);
        identity.PodName.Should().Be("svc-1");
    }
}
