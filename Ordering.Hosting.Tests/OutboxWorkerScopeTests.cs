using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Ordering.Application.Outbox;
using Ordering.Application.Tests;
using Ordering.Infrastructure.Kafka;

namespace Ordering.Hosting.Tests;

public sealed class OutboxWorkerScopeTests
{
    [Fact]
    public async Task ValidatedHostUsesDistinctScopedRelaysAndStoresWithRootOwnedProducerAndDataSource()
    {
        var time = new ControlledTimeProvider();
        var calls = Channel.CreateUnbounded<Store>();
        var relays = new List<OutboxRelay>();
        var host = new HostBuilder().UseDefaultServiceProvider(o => { o.ValidateScopes = true; o.ValidateOnBuild = true; })
            .ConfigureLogging(l => l.ClearProviders()).ConfigureServices(s =>
            {
                s.Configure<HostOptions>(o => { o.ServicesStopConcurrently = true; o.ShutdownTimeout = Timeout.InfiniteTimeSpan; });
                s.AddSingleton(_ => NpgsqlDataSource.Create("Host=localhost;Database=synthetic;Username=synthetic;Password=synthetic"));
                s.AddSingleton(_ => new KafkaOutboxEventPublisher(new()
                {
                    BootstrapServers = "127.0.0.1:1", Topic = "scope-test", DeliveryTimeout = TimeSpan.FromSeconds(1)
                }));
                s.AddSingleton<IOutboxEventPublisher>(p => p.GetRequiredService<KafkaOutboxEventPublisher>());
                s.AddSingleton(new OutboxRelayOptions());
                s.AddScoped<IOutboxProcessingStore>(p => new Store(p.GetRequiredService<NpgsqlDataSource>(),
                    p.GetRequiredService<IOutboxEventPublisher>(), calls.Writer));
                s.AddScoped(p =>
                {
                    var relay = new OutboxRelay(p.GetRequiredService<IOutboxProcessingStore>(),
                        p.GetRequiredService<IOutboxEventPublisher>(), p.GetRequiredService<OutboxRelayOptions>());
                    relays.Add(relay);
                    return relay;
                });
                s.AddSingleton<IHostedService>(p => new OutboxBackgroundService(
                    () => OutboxWorkerExecution.CreateScopedAsync(p.GetRequiredService<IServiceScopeFactory>()),
                    new(), time, p.GetRequiredService<ILogger<OutboxBackgroundService>>()));
            }).Build();
        var coordinator = new OutboxShutdownCoordinator(host, TimeSpan.FromSeconds(9), time);
        try
        {
            Assert.Throws<InvalidOperationException>(() => host.Services.GetRequiredService<OutboxRelay>());
            await host.StartAsync();
            var first = await calls.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            (await time.NextTimerAsync(TimeSpan.FromSeconds(5))).Fire();
            var second = await calls.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            await time.NextTimerAsync(TimeSpan.FromSeconds(5));
            Assert.NotSame(first, second);
            Assert.NotSame(relays[0], relays[1]);
            Assert.True(first.Disposed);
            Assert.True(second.Disposed);
            Assert.Same(first.Source, second.Source);
            Assert.Same(first.Publisher, second.Publisher);
            using var connection = first.Source.CreateConnection();
            var result = await coordinator.ShutdownAsync().WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(result.RootDisposalAuthorized);
        }
        finally
        {
            await coordinator.ShutdownAsync();
            await coordinator.Completion;
            await ((IAsyncDisposable)host).DisposeAsync();
        }
    }

    [Fact]
    public async Task ScopedFactoryDisposesPartiallyResolvedDependenciesOnResolutionFailure()
    {
        var resource = new Resource();
        var services = new ServiceCollection();
        services.AddScoped(_ => resource);
        services.AddScoped<OutboxRelay>(p =>
        {
            p.GetRequiredService<Resource>();
            throw new IOException("controlled resolution failure");
        });
        await using var root = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await Assert.ThrowsAsync<IOException>(async () => await OutboxWorkerExecution.CreateScopedAsync(root.GetRequiredService<IServiceScopeFactory>()));
        Assert.True(resource.Disposed);
    }

    private sealed class Resource : IAsyncDisposable
    {
        public bool Disposed;
        public ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    }

    private sealed class Store(NpgsqlDataSource source, IOutboxEventPublisher publisher, ChannelWriter<Store> calls)
        : IOutboxProcessingStore, IAsyncDisposable
    {
        public NpgsqlDataSource Source => source;
        public IOutboxEventPublisher Publisher => publisher;
        public bool Disposed;
        public Task<OutboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan duration, CancellationToken ct)
        {
            calls.TryWrite(this);
            return Task.FromResult(new OutboxAcquisitionResult(OutboxAcquisitionStatus.NoEligibleWork, null));
        }
        public Task<OutboxMutationResult> RenewAsync(OutboxLease l, TimeSpan d, CancellationToken ct) => throw new NotSupportedException();
        public Task<OutboxMutationResult> MarkPublishedAsync(OutboxLease l, CancellationToken ct) => throw new NotSupportedException();
        public Task<OutboxMutationResult> ScheduleRetryAsync(OutboxLease l, TimeSpan d, CancellationToken ct) => throw new NotSupportedException();
        public ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    }
}
