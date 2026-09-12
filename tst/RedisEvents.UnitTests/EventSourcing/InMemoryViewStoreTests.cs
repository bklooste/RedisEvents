using FluentAssertions;
using RedisEvents.EventSourcing;

namespace RedisEvents.UnitTests.EventSourcing;

/// <summary>
/// Basic get/set/delete/list round-trip behaviour of <see cref="InMemoryViewStore{TView}"/>, which
/// also stands in for the contract every <see cref="IViewStore{TView}"/> implementation must satisfy.
/// </summary>
public class InMemoryViewStoreTests
{
    private sealed record TestView(string Id, string Name, int Version);

    [Fact]
    public async Task Set_then_get_round_trips_the_view()
    {
        var store = new InMemoryViewStore<TestView>();
        var view = new TestView("a1", "Widget", 1);

        await store.SetAsync("a1", view);
        var read = await store.GetAsync("a1");

        read.Should().Be(view);
    }

    [Fact]
    public async Task Get_on_a_missing_id_returns_null()
    {
        var store = new InMemoryViewStore<TestView>();

        var read = await store.GetAsync("missing");

        read.Should().BeNull();
    }

    [Fact]
    public async Task Set_again_replaces_the_prior_value()
    {
        var store = new InMemoryViewStore<TestView>();

        await store.SetAsync("a1", new TestView("a1", "Widget", 1));
        await store.SetAsync("a1", new TestView("a1", "Widget renamed", 2));

        var read = await store.GetAsync("a1");

        read.Should().Be(new TestView("a1", "Widget renamed", 2));
    }

    [Fact]
    public async Task Delete_removes_the_view_and_a_subsequent_get_returns_null()
    {
        var store = new InMemoryViewStore<TestView>();
        await store.SetAsync("a1", new TestView("a1", "Widget", 1));

        await store.DeleteAsync("a1");

        (await store.GetAsync("a1")).Should().BeNull();
    }

    [Fact]
    public async Task Delete_on_a_missing_id_does_not_throw()
    {
        var store = new InMemoryViewStore<TestView>();

        var act = async () => await store.DeleteAsync("missing");

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task List_returns_every_set_view()
    {
        var store = new InMemoryViewStore<TestView>();
        var views = new[]
        {
            new TestView("a1", "One", 1),
            new TestView("a2", "Two", 1),
            new TestView("a3", "Three", 1),
        };

        foreach (var view in views)
            await store.SetAsync(view.Id, view);

        var listed = new List<TestView>();
        await foreach (var view in store.ListAsync())
            listed.Add(view);

        listed.Should().BeEquivalentTo(views);
    }

    [Fact]
    public async Task List_on_an_empty_store_yields_nothing()
    {
        var store = new InMemoryViewStore<TestView>();

        var listed = new List<TestView>();
        await foreach (var view in store.ListAsync())
            listed.Add(view);

        listed.Should().BeEmpty();
    }
}
