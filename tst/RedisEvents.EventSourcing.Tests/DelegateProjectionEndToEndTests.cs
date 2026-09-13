using System.Text.Json.Serialization;

using FluentAssertions;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using RedisEvents.EventSourcing;
using RedisEvents.Extensions;
using RedisEvents.Producer;

namespace RedisEvents.Tests;

/// <summary>
/// Proves the delegate-based <c>AddProjection</c> overloads end to end, against a real Redis and a
/// real <see cref="EventProjector"/> — no fakes, and deliberately no <see cref="AggregateRoot"/> or
/// <c>AddEventStore</c> anywhere in this file. Events are published with the ordinary
/// <see cref="IStreamPublisher"/>, exactly as a plain domain-event producer would, to demonstrate that
/// the read side needs nothing from an event-sourced write side to work.
/// </summary>
[Collection(RedisStreamsCollection.Name)]
[Trait("TestType", "ServiceTest")]
public sealed class DelegateProjectionEndToEndTests(RedisStreamsFixture fixture)
{
    /// <summary>
    /// The general delegate overload — <c>AddProjection&lt;TEvent&gt;(Func&lt;IServiceProvider, TEvent,
    /// EventMeta, CancellationToken, ValueTask&gt;)</c> — resolving <see cref="IViewStore{TView}"/> from
    /// the handed-in <see cref="IServiceProvider"/> itself, with no projection class anywhere.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task General_delegate_overload_resolves_the_view_store_and_writes_through_it()
    {
        var topic = fixture.NewTopic();
        var host = BuildHost(topic, builder => builder
            .AddProjection<Widget>((sp, e, meta, ct) =>
                sp.GetRequiredService<IViewStore<WidgetView>>()
                  .SetAsync(meta.PartitionKey, new WidgetView(e.Id, e.Name), ct)));

        try
        {
            await host.StartAsync();

            var publisher = host.Services.GetRequiredKeyedService<IStreamPublisher>(topic);
            await publisher.PublishAsync(
                "widget-1",
                System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new Widget("widget-1", "Gadget"), WidgetsJson.Default.Widget),
                "widget");

            var views = host.Services.GetRequiredService<IViewStore<WidgetView>>();

            await RedisStreamsFixture.WaitUntilAsync(
                async () => await views.GetAsync("widget-1") is { Name: "Gadget" },
                TimeSpan.FromSeconds(15),
                "the delegate projection to write the view for a plainly-published event");
        }
        finally
        {
            await Stop(host);
        }
    }

    /// <summary>
    /// The view-store shorthand — <c>AddProjection&lt;TEvent, TView&gt;(Func&lt;TEvent, EventMeta,
    /// TView&gt;)</c> — where <c>SetAsync(meta.PartitionKey, ...)</c> happens for you.
    /// </summary>
    [Fact]
    [Trait("TestType", "ServiceTest")]
    public async Task View_store_shorthand_overload_maps_the_event_and_replaces_the_view()
    {
        var topic = fixture.NewTopic();
        var host = BuildHost(topic, builder => builder
            .AddProjection<Widget, WidgetView>((e, meta) => new WidgetView(e.Id, e.Name)));

        try
        {
            await host.StartAsync();

            var publisher = host.Services.GetRequiredKeyedService<IStreamPublisher>(topic);
            await publisher.PublishAsync(
                "widget-2",
                System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new Widget("widget-2", "Doohickey"), WidgetsJson.Default.Widget),
                "widget");

            var views = host.Services.GetRequiredService<IViewStore<WidgetView>>();

            await RedisStreamsFixture.WaitUntilAsync(
                async () => await views.GetAsync("widget-2") is { Name: "Doohickey" },
                TimeSpan.FromSeconds(15),
                "the shorthand projection to map and store the view for a plainly-published event");
        }
        finally
        {
            await Stop(host);
        }
    }

    private IHost BuildHost(string topic, Action<IHostApplicationBuilder> configureProjection)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
        {
            EnvironmentName = "Production",
        });

        builder.Configuration["Streams:ConnectionString"] = fixture.ConnectionString;
        builder.Configuration["Streams:Consumer"] = fixture.NewConsumer();

        builder.AddStreamPublisher(topic)
               .AddEventProjector(topic, types => types.RegisterJson("widget", WidgetsJson.Default.Widget))
               .AddRedisViewStore<WidgetView>(topic, "widget", WidgetsJson.Default.WidgetView);

        configureProjection(builder);

        return builder.Build();
    }

    private static async Task Stop(IHost host)
    {
        await host.StopAsync();
        host.Dispose();
    }
}

internal sealed record Widget(string Id, string Name);

internal sealed record WidgetView(string Id, string Name);

[JsonSerializable(typeof(Widget))]
[JsonSerializable(typeof(WidgetView))]
internal sealed partial class WidgetsJson : JsonSerializerContext;
