using FluentAssertions;

using RedisEvents.Config;
using RedisEvents.Ownership;
using RedisEvents.Positions;

namespace RedisEvents.UnitTests;

/// <summary>
/// The writer identity itself (R-01, option A): what a pod stamps on its claims and positions, and
/// why it survives a restart.
/// </summary>
/// <remarks>
/// This used to be <see cref="OwnershipRegistry.ProcessInstanceId"/> — a fresh GUID per process — so
/// a bounced pod read its own predecessor's positions as a rival's and stood its partitions down on
/// the first flush. The identity of a writer is the pod.
/// </remarks>
public class InstanceIdentityTests
{
    [Fact]
    [Trait("TestType", "UnitTest")]
    public void InstanceIdFor_IsStableForOnePodNameAndDistinctBetweenPods()
    {
        var first = OwnershipRegistry.InstanceIdFor("svc-1");
        var second = OwnershipRegistry.InstanceIdFor("svc-1");

        second.Should().Be(first, "two lives of one pod are one writer — this is the whole of finding P0-1");
        OwnershipRegistry.InstanceIdFor("svc-0").Should().NotBe(first, "different ordinals are different writers");
        OwnershipRegistry.InstanceIdFor("deploy-abc12-7f4dq").Should().NotBe(
            OwnershipRegistry.InstanceIdFor("deploy-abc12-x2k9p"),
            "two pods of a Deployment still differ, which is what keeps the overlap detectable");

        first.Should().NotBe(Guid.Empty, "an empty id reads as 'unattributed' everywhere else");
        first.Should().NotBe(OwnershipRegistry.ProcessInstanceId);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void InstanceIdFor_WithNoName_FallsBackToTheProcessId()
    {
        // Nothing identifies the instance, so there is no stable identity to be had. Falling back to
        // the per-process id keeps two processes on one developer machine distinguishable, rather
        // than silently merging them into one writer.
        OwnershipRegistry.InstanceIdFor(null).Should().Be(OwnershipRegistry.ProcessInstanceId);
        OwnershipRegistry.InstanceIdFor("   ").Should().Be(OwnershipRegistry.ProcessInstanceId);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void ResolveInstanceId_PrefersPodNameThenIndexThenTheProcessId()
    {
        var fromPod = OwnershipRegistry.ResolveInstanceId(name => name switch
        {
            InstanceResolver.PodNameVariable => "svc-2",
            InstanceResolver.IndexVariable => "9",
            _ => null,
        });

        fromPod.Should().Be(
            OwnershipRegistry.InstanceIdFor("svc-2"),
            "the pod name is the strongest identity available, exactly as it is for the partition arithmetic");

        var fromIndex = OwnershipRegistry.ResolveInstanceId(name =>
            name == InstanceResolver.IndexVariable ? "9" : null);

        fromIndex.Should().NotBe(fromPod);
        fromIndex.Should().NotBe(OwnershipRegistry.ProcessInstanceId, "an explicit ordinal is an identity");
        fromIndex.Should().Be(
            OwnershipRegistry.ResolveInstanceId(name => name == InstanceResolver.IndexVariable ? "9" : null),
            "and it is stable across a restart too");

        OwnershipRegistry.ResolveInstanceId(_ => null).Should().Be(OwnershipRegistry.ProcessInstanceId);
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void PresenceField_IsNotAPartitionAndNotAMarker()
    {
        var field = OwnershipRegistry.PresenceField(OwnershipRegistry.InstanceIdFor("svc-1")).ToString();

        field.Should().StartWith("i:");

        // The ownership hash is read by field name: a presence field that parsed as a partition
        // number would show up as a phantom owner in the map and in the admin endpoint.
        int.TryParse(field, out _).Should().BeFalse();
    }

    [Fact]
    [Trait("TestType", "UnitTest")]
    public void AdminInstanceId_IsFixedAndIsNobodysWriterIdentity()
    {
        // Fixed, because the flusher on the other side of a reset has nothing else to recognise it
        // by — it is a different process, so it cannot be told at run time.
        RedisPositionStore.AdminInstanceId.Should().Be(new Guid("6f72616e-6765-4164-6d69-6e5265736574"));
        RedisPositionStore.AdminInstanceId.Should().NotBe(OwnershipRegistry.StableInstanceId);
        RedisPositionStore.AdminInstanceId.Should().NotBe(OwnershipRegistry.ProcessInstanceId);
        RedisPositionStore.AdminInstanceId.Should().NotBe(Guid.Empty);
    }
}
