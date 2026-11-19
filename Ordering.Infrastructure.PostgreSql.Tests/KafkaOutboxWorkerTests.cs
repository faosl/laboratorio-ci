using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Ordering.Application.Outbox;
using Ordering.Application.Tests;
using Ordering.Hosting;
using Ordering.Infrastructure.Kafka;

namespace Ordering.Infrastructure.PostgreSql.Tests;

public sealed partial class KafkaOutboxRelayTests
{
    private IHost WorkerHost(ControlledTimeProvider time, Func<IOutboxEventPublisher, IOutboxEventPublisher>? decorate = null)
        => new HostBuilder().UseDefaultServiceProvider(o => { o.ValidateScopes = true; o.ValidateOnBuild = true; })
            .ConfigureLogging(l => l.ClearProviders()).ConfigureServices(s =>
            {
                s.Configure<HostOptions>(o => { o.ServicesStopConcurrently = true; o.ShutdownTimeout = Timeout.InfiniteTimeSpan; });
                // Dependency ownership: each root owns one pool and one producer, reused across scopes.
                s.AddSingleton(_ => NpgsqlDataSource.Create(database.ConnectionString));
                s.AddSingleton(_ => Publisher(TimeSpan.FromSeconds(2)));
                s.AddSingleton<IOutboxEventPublisher>(p => decorate?.Invoke(p.GetRequiredService<KafkaOutboxEventPublisher>())
                    ?? p.GetRequiredService<KafkaOutboxEventPublisher>());
                s.AddSingleton(RelayOptions with { RetryDelay = TimeSpan.Zero });
                s.AddScoped<IOutboxProcessingStore, PostgresOutboxProcessingStore>();
                s.AddScoped<OutboxRelay>();
                s.AddSingleton<IHostedService>(p => new OutboxBackgroundService(
                    () => OutboxWorkerExecution.CreateScopedAsync(p.GetRequiredService<IServiceScopeFactory>()),
                    new() { PublicationFailureDelay = TimeSpan.FromSeconds(7) }, time,
                    p.GetRequiredService<ILogger<OutboxBackgroundService>>()));
            }).Build();

    [Fact]
    public async Task OutboxWorker_AutomaticallyPublishesTwoOrdersWithRealBrokerAcknowledgements()
    {
        var first = await Seed();
        var second = await Seed();
        Assert.NotEqual(first.AggregateId, second.AggregateId);
        var time = new ControlledTimeProvider();
        var receipts = new ReceiptPublisher();
        var host = WorkerHost(time, receipts.Wrap);
        var shutdown = new OutboxShutdownCoordinator(host, TimeSpan.FromSeconds(9), time);
        try
        {
            await host.StartAsync();
            // Idle timer exists only after both sequential relay calls and their scopes finish.
            await time.NextTimerAsync(TimeSpan.FromSeconds(5));
            using var consumer = Consumer();
            foreach (var message in new[] { first, second })
            {
                var record = Observe(consumer, message);
                var result = receipts.Results[message.EventId];
                Assert.Equal(OutboxDeliveryStatus.Acknowledged, result.Status);
                Assert.Equal(new OutboxDeliveryReceipt(_topic, record.Partition.Value, record.Offset.Value), result.Receipt);
                await AssertState(message, "Published");
            }
            Assert.Equal(OutboxShutdownStatus.Drained, (await shutdown.ShutdownAsync()).Status);
        }
        finally { await StopAndDisposeWorkerHost(host, shutdown); }
    }

    [Fact]
    public async Task OutboxWorker_RealBrokerRejectionWaitsConfiguredIntervalAndAutomaticallyRecovers()
    {
        var message = await Seed();
        await SetMinimumReplicas("2");
        var time = new ControlledTimeProvider();
        var receipts = new ReceiptPublisher();
        var host = WorkerHost(time, receipts.Wrap);
        var shutdown = new OutboxShutdownCoordinator(host, TimeSpan.FromSeconds(9), time);
        try
        {
            await host.StartAsync();
            var retry = await time.NextTimerAsync(TimeSpan.FromSeconds(7));
            await AssertState(message, "RetryScheduled");
            Assert.Equal(OutboxDeliveryStatus.Unconfirmed, receipts.Results[message.EventId].Status);
            Assert.Null(receipts.Results[message.EventId].Receipt);
            using var consumer = Consumer();
            Assert.Equal(0, consumer.QueryWatermarkOffsets(new(_topic, 0), TimeSpan.FromSeconds(10)).High.Value);
            await SetMinimumReplicas("1");
            Assert.Equal(1, receipts.Calls);
            retry.Fire();
            await time.NextTimerAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(2, receipts.Calls);
            Assert.Equal(OutboxDeliveryStatus.Acknowledged, receipts.Results[message.EventId].Status);
            Observe(consumer, message);
            await AssertState(message, "Published");
        }
        finally
        {
            await SetMinimumReplicas("1");
            await StopAndDisposeWorkerHost(host, shutdown);
        }
    }

    [Fact]
    public async Task OutboxWorker_SyntheticPendingPublicationBoundaryRetainsRootPastDeadline()
    {
        var message = await Seed();
        var time = new ControlledTimeProvider();
        var boundary = new SupervisedPublicationBoundary();
        var host = WorkerHost(time, boundary.Wrap);
        var shutdown = new OutboxShutdownCoordinator(host, TimeSpan.FromSeconds(9), time);
        try
        {
            await host.StartAsync();
            await boundary.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var stopping = shutdown.ShutdownAsync();
            await boundary.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
            (await time.NextTimerAsync(TimeSpan.FromSeconds(9))).Fire();
            var result = await stopping.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(OutboxShutdownStatus.DeadlineExceeded, result.Status);
            Assert.False(result.RootDisposalAuthorized);
            Assert.False(shutdown.Completion.IsCompleted);
            Assert.False(result.Workers[0].ScopesDisposed);
            using var connection = host.Services.GetRequiredService<NpgsqlDataSource>().CreateConnection();
            await AssertState(message, "Processing");
            boundary.Release.SetResult();
            await shutdown.Completion.WaitAsync(TimeSpan.FromSeconds(10));
            using var consumer = Consumer();
            Observe(consumer, message);
            // ACK after cancellation cannot authorize MarkPublished; lease recovery remains needed.
            await AssertState(message, "Processing");
            Assert.Same(result, await shutdown.ShutdownAsync());
        }
        finally
        {
            boundary.Release.TrySetResult();
            await StopAndDisposeWorkerHost(host, shutdown);
        }
    }

    private static async Task StopAndDisposeWorkerHost(IHost host, OutboxShutdownCoordinator shutdown)
    {
        await shutdown.ShutdownAsync().WaitAsync(TimeSpan.FromSeconds(30));
        await shutdown.Completion.WaitAsync(TimeSpan.FromSeconds(30));
        // Test-only cleanup: all synthetic work is released and known, including after a deadline.
        // Production callers cannot infer disposal authorization from Completion alone.
        await ((IAsyncDisposable)host).DisposeAsync();
    }

    private sealed class ReceiptPublisher : IOutboxEventPublisher
    {
        private IOutboxEventPublisher _inner = null!;
        public Dictionary<Guid, OutboxPublicationResult> Results { get; } = [];
        public int Calls;
        public TimeSpan DeliveryTimeout => _inner.DeliveryTimeout;
        public IOutboxEventPublisher Wrap(IOutboxEventPublisher inner) { _inner = inner; return this; }
        public async Task<OutboxPublicationResult> PublishAsync(OutboxMessage message, CancellationToken ct)
        {
            Calls++;
            return Results[message.EventId] = await _inner.PublishAsync(message, ct);
        }
    }

    // Synthetic in-flight supervision barrier, not a physical broker interruption test.
    private sealed class SupervisedPublicationBoundary : IOutboxEventPublisher
    {
        private IOutboxEventPublisher _inner = null!;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TimeSpan DeliveryTimeout => _inner.DeliveryTimeout;
        public IOutboxEventPublisher Wrap(IOutboxEventPublisher inner) { _inner = inner; return this; }
        public async Task<OutboxPublicationResult> PublishAsync(OutboxMessage message, CancellationToken ct)
        {
            // Enqueue starts before the barrier. Its Task remains observed, even on assertion cleanup.
            var sending = _inner.PublishAsync(message, CancellationToken.None);
            using var registration = ct.Register(() => Cancelled.TrySetResult());
            Entered.SetResult();
            try { await Release.Task; }
            finally { await sending; }
            return await sending;
        }
    }
}
