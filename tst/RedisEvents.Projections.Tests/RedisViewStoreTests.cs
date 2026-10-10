using System.Text.Json.Serialization;

using FluentAssertions;

using RedisEvents.Config;
using RedisEvents.Projections;

namespace RedisEvents.Tests;

/// <summary>
/// Service tests for <see cref="RedisViewStore{TView}"/> against a real Redis: the hash-per-view-type
/// storage shape, get/set/delete/list, and that two view names on the same topic get separate keys.
/// </summary>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class RedisViewStoreTests(RedisStreamsFixture fixture)
{
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Set_then_get_round_trips_a_view()
    {
        var topic = fixture.NewTopic();
        var store = new RedisViewStore<Detail>(fixture.Db, topic, "detail", RedisViewStoreTestsJson.Default.Detail);
        var view = new Detail("a1", "Widget", 1);

        await store.SetAsync("a1", view);
        var read = await store.GetAsync("a1");

        read.Should().Be(view);
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Get_on_a_missing_id_returns_null()
    {
        var topic = fixture.NewTopic();
        var store = new RedisViewStore<Detail>(fixture.Db, topic, "detail", RedisViewStoreTestsJson.Default.Detail);

        var read = await store.GetAsync("missing");

        read.Should().BeNull();
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Delete_removes_it_and_a_subsequent_get_returns_null()
    {
        var topic = fixture.NewTopic();
        var store = new RedisViewStore<Detail>(fixture.Db, topic, "detail", RedisViewStoreTestsJson.Default.Detail);
        await store.SetAsync("a1", new Detail("a1", "Widget", 1));

        await store.DeleteAsync("a1");

        (await store.GetAsync("a1")).Should().BeNull();
    }

    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task List_returns_every_set_view()
    {
        var topic = fixture.NewTopic();
        var store = new RedisViewStore<Detail>(fixture.Db, topic, "detail", RedisViewStoreTestsJson.Default.Detail);
        var views = new[]
        {
            new Detail("a1", "One", 1),
            new Detail("a2", "Two", 1),
            new Detail("a3", "Three", 1),
        };

        foreach (var view in views)
            await store.SetAsync(view.Id, view);

        var listed = new List<Detail>();
        await foreach (var view in store.ListAsync())
            listed.Add(view);

        listed.Should().BeEquivalentTo(views);
    }

    /// <summary>
    /// Two view names on the same topic are two separate hashes, so listing one never sees the
    /// other's entries even though both views share the same id.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Two_different_view_names_on_the_same_topic_do_not_collide()
    {
        var topic = fixture.NewTopic();
        var details = new RedisViewStore<Detail>(fixture.Db, topic, "detail", RedisViewStoreTestsJson.Default.Detail);
        var summaries = new RedisViewStore<Detail>(fixture.Db, topic, "summary", RedisViewStoreTestsJson.Default.Detail);

        await details.SetAsync("a1", new Detail("a1", "Detail view", 1));
        await summaries.SetAsync("a1", new Detail("a1", "Summary view", 7));

        (await details.GetAsync("a1")).Should().Be(new Detail("a1", "Detail view", 1));
        (await summaries.GetAsync("a1")).Should().Be(new Detail("a1", "Summary view", 7));

        var detailList = new List<Detail>();
        await foreach (var view in details.ListAsync())
            detailList.Add(view);

        detailList.Should().ContainSingle().Which.Should().Be(new Detail("a1", "Detail view", 1));
    }

    /// <summary>The default key shape is unchanged: the entry assembly's name is the owner segment.</summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Default_key_uses_the_entry_assembly_name_as_owner()
    {
        var topic = fixture.NewTopic();
        var store = new RedisViewStore<Detail>(fixture.Db, topic, "detail", RedisViewStoreTestsJson.Default.Detail);

        await store.SetAsync("a1", new Detail("a1", "Widget", 1));

        var expected = $"{KeyNamespace.Prefix()}{{{topic}}}:view:{KeyNamespace.DefaultServiceName()}:detail";
        (await fixture.Db.HashExistsAsync(expected, "a1")).Should().BeTrue();
    }

    /// <summary>An explicit owner replaces the assembly name in the key.</summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Explicit_owner_is_used_in_the_key()
    {
        var topic = fixture.NewTopic();
        var store = new RedisViewStore<Detail>(fixture.Db, topic, "detail", RedisViewStoreTestsJson.Default.Detail, "wallet-views");

        await store.SetAsync("a1", new Detail("a1", "Widget", 1));

        var expected = $"{KeyNamespace.Prefix()}{{{topic}}}:view:wallet-views:detail";
        (await fixture.Db.HashExistsAsync(expected, "a1")).Should().BeTrue();
        var assemblyKey = $"{KeyNamespace.Prefix()}{{{topic}}}:view:{KeyNamespace.DefaultServiceName()}:detail";
        (await fixture.Db.KeyExistsAsync(assemblyKey)).Should().BeFalse();
    }

    /// <summary>
    /// The split-process case: a writer and a separate reader instance sharing an owner agree on the
    /// key; a store with a different owner sees nothing.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task A_store_written_with_an_owner_is_read_by_another_store_with_the_same_owner_only()
    {
        var topic = fixture.NewTopic();
        var json = RedisViewStoreTestsJson.Default.Detail;
        var writer = new RedisViewStore<Detail>(fixture.Db, topic, "detail", json, "wallet-views");
        var reader = new RedisViewStore<Detail>(fixture.Db, topic, "detail", json, "wallet-views");
        var stranger = new RedisViewStore<Detail>(fixture.Db, topic, "detail", json, "other-owner");
        var view = new Detail("a1", "Widget", 1);

        await writer.SetAsync("a1", view);

        (await reader.GetAsync("a1")).Should().Be(view);
        (await stranger.GetAsync("a1")).Should().BeNull();

        var listed = new List<Detail>();
        await foreach (var v in stranger.ListAsync())
            listed.Add(v);
        listed.Should().BeEmpty();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    [Trait("TestType", "ServiceTest")]
    public void A_blank_owner_is_rejected(string? owner)
    {
        var act = () => new RedisViewStore<Detail>(fixture.Db, "t", "detail", RedisViewStoreTestsJson.Default.Detail, owner!);

        act.Should().Throw<ArgumentException>();
    }
}

internal sealed record Detail(string Id, string Name, int Version);

[JsonSerializable(typeof(Detail))]
internal sealed partial class RedisViewStoreTestsJson : JsonSerializerContext;
