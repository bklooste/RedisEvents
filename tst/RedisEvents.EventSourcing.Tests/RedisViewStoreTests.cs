using System.Text.Json.Serialization;

using FluentAssertions;

using RedisEvents.EventSourcing;

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
}

internal sealed record Detail(string Id, string Name, int Version);

[JsonSerializable(typeof(Detail))]
internal sealed partial class RedisViewStoreTestsJson : JsonSerializerContext;
