using FluentAssertions;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RedisEvents.Config;
using RedisEvents.EventSourcing;
using RedisEvents.EventSourcing.Sample.Inventory;
using RedisEvents.Positions;
using RedisEvents.Producer;
using RedisEvents.Wire;

namespace RedisEvents.Tests;

/// <summary>
/// The final proof for <c>RedisEvents.EventSourcing</c>: a real <see cref="IHost"/>, wired with the
/// public <c>AddEventStore</c>/<c>AddEventProjector</c>/<c>AddProjection</c>/<c>AddRedisViewStore</c>
/// surface exactly as a service would use it, against the fixture's real Redis — no fakes anywhere in
/// this file. Every test uses the sample project's <see cref="InventoryItem"/>,
/// <see cref="InventoryEventTypes"/>, <see cref="InventoryDetailProjection"/> and
/// <see cref="InventoryDetail"/> as its fixtures, per PLAN.md's "both test projects reference it".
/// </summary>
/// <remarks>
/// <para>
/// Three things are proven here that no unit test can: a save really does flow, through a live
/// <c>Outbox</c> transaction and a live consumer, into a real Redis view (<see cref="Save_flows_through_the_real_projector_into_the_real_view"/>);
/// a second host pointed at the same topic and consumer name resumes from the stored position
/// instead of replaying history (<see cref="Consumer_restart_resumes_without_replay"/>); and two
/// racing loads really do leave exactly one winner and a view consistent with it
/// (<see cref="Two_racing_saves_leave_one_winner_and_the_view_matches"/>).
/// </para>
/// <para>
/// Every host is built fresh per test, over its own topic from <see cref="RedisStreamsFixture.NewTopic"/>,
/// and is stopped and disposed before the test returns — a hosted consumer left running would keep
/// polling Redis in the background for the rest of the suite.
/// </para>
/// </remarks>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class EventSourcingEndToEndTests(RedisStreamsFixture fixture)
{
    /// <summary>
    /// Save, rename, check in and remove stock through a real <see cref="IEventRepository"/>; a real
    /// <see cref="EventProjector"/> running inside the started host must carry every one of those
    /// changes into a real <see cref="RedisViewStore{TView}"/> — the whole pipeline, aggregate to
    /// outbox transaction to topic to consumer to projection to view, with nothing stubbed.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Save_flows_through_the_real_projector_into_the_real_view()
    {
        var topic = fixture.NewTopic();
        var host = BuildHost(topic, fixture.NewConsumer());

        try
        {
            await host.StartAsync();

            var repository = host.Services.GetRequiredService<IEventRepository>();
            var views = host.Services.GetRequiredService<IViewStore<InventoryDetail>>();

            var item = InventoryItem.Create("item-1", "Widget");
            await repository.SaveAsync(item);

            item.Rename("Widget v2");
            item.CheckIn(5);
            await repository.SaveAsync(item);

            item.Remove(2);
            await repository.SaveAsync(item);

            await RedisStreamsFixture.WaitUntilAsync(
                async () =>
                {
                    var view = await views.GetAsync("item-1");
                    return view is { Name: "Widget v2", CurrentCount: 3, Version: 4 };
                },
                TimeSpan.FromSeconds(15),
                "the real projector to carry every save into the real Redis view");

            var final = await views.GetAsync("item-1");
            final.Should().NotBeNull();
            final!.Id.Should().Be("item-1");
            final.Name.Should().Be("Widget v2");
            final.Active.Should().BeTrue();
            final.CurrentCount.Should().Be(3);
            final.Version.Should().Be(4);
        }
        finally
        {
            await Stop(host);
        }
    }

    /// <summary>
    /// Two "command handlers" both load the same version, both decide, and both save: exactly one
    /// wins and the other gets <see cref="ConcurrencyException"/>. The real projector, still running,
    /// must settle the view on the winner's outcome — not the loser's, and not some mix of both.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Two_racing_saves_leave_one_winner_and_the_view_matches()
    {
        var topic = fixture.NewTopic();
        var host = BuildHost(topic, fixture.NewConsumer());

        try
        {
            await host.StartAsync();

            var repository = host.Services.GetRequiredService<IEventRepository>();
            var views = host.Services.GetRequiredService<IViewStore<InventoryDetail>>();

            await repository.SaveAsync(InventoryItem.Create("item-2", "Original"));

            // Two independent loads of the same version — simulating two command handlers racing.
            var first = await repository.LoadAsync<InventoryItem>("item-2");
            var second = await repository.LoadAsync<InventoryItem>("item-2");

            first!.Version.Should().Be(1);
            second!.Version.Should().Be(1);

            first.Rename("From first");
            second.Rename("From second");

            (await repository.SaveAsync(first)).Should().Be(2, "the first save wins the version check");

            var act = async () => await repository.SaveAsync(second);
            (await act.Should().ThrowAsync<ConcurrencyException>())
                .Which.AggregateName.Should().Be("Inventory");

            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await views.GetAsync("item-2"))?.Version == 2,
                TimeSpan.FromSeconds(15),
                "the real projector to catch up to the winning save");

            var view = await views.GetAsync("item-2");
            view.Should().NotBeNull();
            view!.Name.Should().Be("From first", "the losing save must never have reached the view");
            view.Version.Should().Be(2);
        }
        finally
        {
            await Stop(host);
        }
    }

    /// <summary>
    /// A second host, pointed at the same topic and the same consumer name, resumes from the stored
    /// position rather than replaying the first host's whole history. Proven two ways: a spy
    /// projection registered on both hosts counts exactly the events each host's own consumer
    /// actually dispatched — the second host's count must be the handful raised after it started, not
    /// the first host's full history plus that handful — and the raw position hash in Redis is read
    /// directly before and after, showing it advances rather than resets to the beginning.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task Consumer_restart_resumes_without_replay()
    {
        var topic = fixture.NewTopic();
        var consumer = fixture.NewConsumer();
        const int HistoryCheckIns = 12;

        var host1 = BuildHost(topic, consumer, b => b.AddProjection<CheckInCountingSpy>());

        try
        {
            await host1.StartAsync();

            var repository1 = host1.Services.GetRequiredService<IEventRepository>();
            var spy1 = host1.Services.GetRequiredService<CheckInCountingSpy>();

            var item = InventoryItem.Create("item-3", "First");
            await repository1.SaveAsync(item);

            for (var i = 0; i < HistoryCheckIns; i++)
            {
                item.CheckIn(1);
                await repository1.SaveAsync(item);
            }

            // Let the first host's own projector fully catch up before it is stopped, so its position
            // flush on shutdown covers the whole history rather than a partial batch.
            await RedisStreamsFixture.WaitUntilAsync(
                () => Volatile.Read(ref spy1.Count) >= HistoryCheckIns,
                TimeSpan.FromSeconds(15),
                "the first host's projector to process every check-in before it is stopped");
        }
        finally
        {
            await Stop(host1);
        }

        // The graceful stop must have flushed a real position, not left the hash empty (which would
        // make the second host's StartFromWhenMissing=Beginning fallback kick in — indistinguishable
        // from a resume for a topic this short, but not what this test is proving).
        var positionsAfterStop = await ReadPositions(topic, consumer);
        positionsAfterStop.Should().ContainKey(0, "the first host must have flushed a position for the topic's single partition on a graceful stop");
        var storedPosition = positionsAfterStop[0].Id;
        storedPosition.Should().BeGreaterThan(StreamId.Min, "a real position was recorded, not the empty/zero marker");

        // One more event, saved independently of either host — a fresh repository against the same
        // Redis, exactly as a redeployed writer pod would be.
        var freshRepository = new RedisEventRepository(new StreamStore(fixture.Db, topic, new TopicOptions()), Registry());
        var reloaded = await freshRepository.LoadAsync<InventoryItem>("item-3");
        reloaded!.Version.Should().Be(HistoryCheckIns + 1);
        reloaded.CheckIn(1);
        await freshRepository.SaveAsync(reloaded);

        var host2 = BuildHost(topic, consumer, b => b.AddProjection<CheckInCountingSpy>());

        try
        {
            await host2.StartAsync();

            var spy2 = host2.Services.GetRequiredService<CheckInCountingSpy>();
            var views2 = host2.Services.GetRequiredService<IViewStore<InventoryDetail>>();

            await RedisStreamsFixture.WaitUntilAsync(
                async () => (await views2.GetAsync("item-3"))?.Version == HistoryCheckIns + 2,
                TimeSpan.FromSeconds(15),
                "the second host to pick up the one new event, resuming from the stored position");

            // The deterministic proof: the second host's own projector instance saw exactly the one
            // new check-in, not the whole history plus one. A bug that reset the position to the
            // beginning would make this HistoryCheckIns + 1 instead.
            spy2.Count.Should().Be(1, "the second host must resume from the stored position, not replay the first host's history");

            // Position writes are async (PersistMode.AsyncBatch, flushed roughly every second), so the
            // view already reflecting the new event does not mean the position hash has been written
            // yet — poll for it rather than reading once and racing the flusher.
            await RedisStreamsFixture.WaitUntilAsync(
                async () =>
                {
                    var positions = await ReadPositions(topic, consumer);
                    return positions.TryGetValue(0, out var record) && record.Id > storedPosition;
                },
                TimeSpan.FromSeconds(5),
                "the second host's position flush to advance forward from where the first host left it");
        }
        finally
        {
            await Stop(host2);
        }
    }

    /// <summary>Builds a host wired exactly as a service would wire the Inventory sample, over one topic.</summary>
    private IHost BuildHost(string topic, string consumer, Action<IHostApplicationBuilder>? configure = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
        {
            EnvironmentName = "Production",
        });

        builder.Configuration["Streams:ConnectionString"] = fixture.ConnectionString;
        builder.Configuration["Streams:Consumer"] = consumer;

        builder.AddEventStore(topic, InventoryEventTypes.Register)
               .AddEventProjector(topic)
               .AddRedisViewStore<InventoryDetail>(topic, "detail", InventoryJsonContext.Default.InventoryDetail)
               .AddProjection<InventoryDetailProjection>();

        configure?.Invoke(builder);

        return builder.Build();
    }

    /// <summary>Stops then disposes a host, swallowing nothing — a leaked hosted consumer would keep polling Redis for the rest of the suite.</summary>
    private static async Task Stop(IHost host)
    {
        await host.StopAsync();
        host.Dispose();
    }

    /// <summary>Reads the raw position hash for one topic/consumer directly off Redis, bypassing the host entirely.</summary>
    private async Task<IReadOnlyDictionary<int, PositionRecord>> ReadPositions(string topic, string consumer)
    {
        var entries = await fixture.Db.HashGetAllAsync(StreamKeys.Positions(topic, consumer));
        return RedisPositionStore.Read(entries);
    }

    /// <summary>The registry a fresh, host-independent repository needs — the same registrations <see cref="InventoryEventTypes.Register"/> makes.</summary>
    private static EventTypeRegistry Registry()
    {
        var registry = new EventTypeRegistry();
        InventoryEventTypes.Register(registry);
        return registry;
    }

    /// <summary>
    /// Counts <see cref="ItemsCheckedIn"/> events this specific projector instance actually
    /// dispatched. Registered fresh on each host via <see cref="EventSourcingBuilderExtensions.AddProjection{TProjection}"/>,
    /// so a new host gets a new instance starting at zero — the count is exactly what that host's own
    /// consumer processed, not a running total across restarts.
    /// </summary>
    private sealed class CheckInCountingSpy : IProjection<ItemsCheckedIn>
    {
        public int Count;

        public ValueTask HandleAsync(ItemsCheckedIn @event, EventMeta meta, CancellationToken ct)
        {
            Interlocked.Increment(ref this.Count);
            return ValueTask.CompletedTask;
        }
    }
}
