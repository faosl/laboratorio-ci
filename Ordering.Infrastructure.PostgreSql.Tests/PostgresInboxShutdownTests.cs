using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Ordering.Application.Inbox;
using Ordering.Application.Tests;
using Ordering.Contracts.Partners;
using Ordering.Hosting;

namespace Ordering.Infrastructure.PostgreSql.Tests;

public sealed class PostgresInboxShutdownTests(DatabaseFixture database) : IClassFixture<DatabaseFixture>
{
    [Fact]
    public async Task ShutdownWithRealBlockedRenewal_RetainsScopeAndRootUntilSupervisedCompletion()
    {
        var eventId = Guid.NewGuid().ToString();
        var notification = new InboxNotification(new("partner", "shutdown", eventId),
            new OrderNotification(eventId, "order"), 1, [0, 255, 10]);
        var inserted = await new PostgresInboxStore(database.Source).RegisterAsync(notification, CancellationToken.None);
        Assert.Equal(InboxRegistrationStatus.Inserted, inserted.Status);

        var time = new ControlledTimeProvider();
        var options = new InboxProcessingOptions(TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(7),
            TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(3));
        var globalBudget = TimeSpan.FromSeconds(5);
        var renewalSession = Signal<int>();
        var resultReturned = Signal<InboxCoordinationResult>();
        var processorCreated = Signal<ControlledProcessor>();
        var storeCreated = Signal<ObservedStore>();
        var attempts = 0;
        var lockCalls = 0;

        var host = new HostBuilder()
            .UseDefaultServiceProvider(settings => { settings.ValidateScopes = true; settings.ValidateOnBuild = true; })
            .ConfigureLogging(logging => logging.ClearProviders())
            .ConfigureServices(services =>
            {
                services.Configure<HostOptions>(settings =>
                {
                    settings.ServicesStopConcurrently = true;
                    settings.ShutdownTimeout = Timeout.InfiniteTimeSpan;
                });
                services.AddSingleton<TimeProvider>(time);
                // Factory registration makes the root own this pool, independently of the fixture.
                services.AddSingleton(_ => NpgsqlDataSource.Create(database.ConnectionString));
                services.AddSingleton(options);
                services.AddScoped(provider => new PostgresInboxProcessingStore(
                    provider.GetRequiredService<NpgsqlDataSource>(), new(BeforeLock: async (connection, transaction, token) =>
                    {
                        if (Interlocked.Increment(ref lockCalls) != 2) return; // Acquisition, then renewal.
                        await using var limits = new NpgsqlCommand(
                            "SET LOCAL lock_timeout = '8s'; SET LOCAL statement_timeout = '9s'", connection, transaction);
                        await limits.ExecuteNonQueryAsync(token);
                        renewalSession.TrySetResult(connection.ProcessID);
                    })));
                // Observation only: every operation delegates to the scoped real store.
                services.AddScoped<IInboxProcessingStore>(provider =>
                {
                    var store = new ObservedStore(provider.GetRequiredService<PostgresInboxProcessingStore>());
                    storeCreated.TrySetResult(store);
                    return store;
                });
                services.AddScoped<IInboxNotificationProcessor>(_ =>
                {
                    var processor = new ControlledProcessor();
                    processorCreated.TrySetResult(processor);
                    return processor;
                });
                services.AddScoped<InboxProcessingCoordinator>();
                services.AddSingleton(provider => new InboxBackgroundService(() =>
                {
                    Interlocked.Increment(ref attempts);
                    var scope = provider.GetRequiredService<IServiceScopeFactory>().CreateAsyncScope();
                    return new(async (owner, token) =>
                    {
                        var result = await scope.ServiceProvider.GetRequiredService<InboxProcessingCoordinator>()
                            .RunOnceAsync(owner, token);
                        resultReturned.TrySetResult(result);
                        return result;
                    }, scope);
                }, new(), time, NullLogger<InboxBackgroundService>.Instance));
                services.AddHostedService(provider => provider.GetRequiredService<InboxBackgroundService>());
            }).Build();

        var source = host.Services.GetRequiredService<NpgsqlDataSource>();
        Assert.NotSame(database.Source, source);
        var worker = host.Services.GetRequiredService<InboxBackgroundService>();
        var shutdown = new InboxShutdownCoordinator(host, globalBudget, time);
        await using var blocker = await database.Source.OpenConnectionAsync();
        NpgsqlTransaction? transaction = null;
        ControlledProcessor? processor = null;
        try
        {
            await Finish(host.StartAsync());
            processor = await Finish(processorCreated.Task);
            var store = await Finish(storeCreated.Task);
            Assert.Equal(notification.Key, (await Finish(processor.Started.Task)).Key);
            var acquired = await new PostgresInboxStore(database.Source).GetByKeyAsync(notification.Key, CancellationToken.None);
            Assert.Equal(inserted.InboxId, acquired!.InboxId);
            Assert.Equal("Processing", acquired.State);
            Assert.Equal(1, store.Acquisitions);

            transaction = await blocker.BeginTransactionAsync();
            await using (var rowLock = new NpgsqlCommand(
                "SELECT inbox_id FROM public.ordering_inbox WHERE inbox_id = $1 FOR UPDATE", blocker, transaction))
            {
                rowLock.Parameters.AddWithValue(acquired.InboxId);
                Assert.Equal(acquired.InboxId, await rowLock.ExecuteScalarAsync());
            }
            (await time.NextTimerAsync(options.RenewalInterval)).Fire();
            var pid = await Finish(renewalSession.Task);
            await WaitForLock(pid, blocker.ProcessID);
            Assert.Equal(1, store.Renewals);
            Assert.False(store.RenewalFinished.Task.IsCompleted);

            var stopping = shutdown.ShutdownAsync();
            await Finish(processor.Cancelled.Task);
            Assert.True(worker.ShutdownState.StopRequested);
            Assert.False(stopping.IsCompleted);
            AssertAlive();

            // Expire the local drain and then the global budget while the processor remains held.
            // Npgsql may finish cancellation before the blocker is released; no post-cancel lock claim.
            (await time.NextTimerAsync(options.ShutdownTimeout)).Fire();
            var coordination = await Finish(resultReturned.Task);
            Assert.True(coordination.ShutdownTimedOut);
            Assert.False(coordination.Quiescence.IsCompleted);
            Assert.Equal(InboxCoordinationStatus.Cancelled, coordination.Status);
            (await time.NextTimerAsync(globalBudget)).Fire();
            var snapshot = await Finish(stopping);
            Assert.Equal(InboxShutdownStatus.DeadlineExceeded, snapshot.Status);
            Assert.False(snapshot.RootDisposalAuthorized);
            Assert.False(snapshot.HostStopCompleted);
            Assert.False(snapshot.HostStopFailed);
            Assert.Equal(worker.ShutdownState, Assert.Single(snapshot.Workers));
            AssertAlive();
            await using (var probe = source.CreateCommand("SELECT 1"))
                Assert.Equal(1, await probe.ExecuteScalarAsync());

            await transaction.RollbackAsync();
            await transaction.DisposeAsync();
            transaction = null;
            await Finish(store.RenewalFinished.Task);
            // Real renewal has settled, but the processor still owns its scoped dependencies.
            AssertAlive();
            processor.Release.TrySetResult();
            await Finish(shutdown.Completion);
            await Finish(coordination.Quiescence);
            Assert.True(worker.ExecuteTask!.IsCompletedSuccessfully);
            Assert.Equal(new InboxWorkerShutdownState(true, true, true, true, InboxSupervisionFailure.None), worker.ShutdownState);
            Assert.True(store.Disposed);
            Assert.True(processor.Disposed);
            Assert.Equal(1, store.Acquisitions);
            Assert.Equal(1, store.Renewals);
            Assert.Equal(0, store.TerminalCalls);
            Assert.Equal(1, Volatile.Read(ref attempts));
            Assert.Equal(0, time.ActiveTimerCount);
            Assert.Same(snapshot, await shutdown.ShutdownAsync()); // Deadline evidence is immutable.

            // Inspect committed data instead of interpreting cancellation as rollback.
            await using var state = database.Source.CreateCommand(
                "SELECT state = 'Processing' AND lease_generation = 1 AND lease_owner IS NOT NULL FROM public.ordering_inbox WHERE inbox_id = $1");
            state.Parameters.AddWithValue(acquired.InboxId);
            Assert.Equal(true, await state.ExecuteScalarAsync());

            void AssertAlive()
            {
                Assert.False(store.Disposed);
                Assert.False(processor.Disposed);
                Assert.False(worker.ShutdownState.OperationsCompleted);
                Assert.False(worker.ShutdownState.ScopesDisposed);
                Assert.False(worker.ExecuteTask!.IsCompleted);
                Assert.Equal(InboxSupervisionFailure.None, worker.ShutdownState.SupervisionFailures);
                Assert.Equal(1, store.Acquisitions);
                Assert.Equal(1, Volatile.Read(ref attempts));
            }
        }
        finally
        {
            // Release real locks and synthetic work even if an assertion/observation fails.
            try { if (transaction is not null) await transaction.DisposeAsync(); }
            finally
            {
                if (processorCreated.Task.IsCompletedSuccessfully) (await processorCreated.Task).Release.TrySetResult();
                try
                {
                    if (worker.ExecuteTask is not null)
                    {
                        _ = shutdown.ShutdownAsync();
                        await Finish(shutdown.Completion);
                    }
                }
                finally { await ((IAsyncDisposable)host).DisposeAsync(); }
            }
        }
        await Assert.ThrowsAsync<ObjectDisposedException>(() => source.OpenConnectionAsync().AsTask());
        await using var independent = database.Source.CreateCommand("SELECT 1");
        Assert.Equal(1, await independent.ExecuteScalarAsync());
    }

    private async Task WaitForLock(int waitingPid, int blockingPid)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (true)
        {
            await using var query = database.Source.CreateCommand("""
                SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                    WHERE pid = $1 AND state = 'active' AND wait_event_type = 'Lock'
                      AND query LIKE 'SELECT inbox_id%FOR UPDATE'
                      AND $2 = ANY(pg_blocking_pids(pid)))
                """);
            query.Parameters.AddWithValue(waitingPid);
            query.Parameters.AddWithValue(blockingPid);
            if ((bool)(await query.ExecuteScalarAsync(timeout.Token))!) return;
            await Task.Delay(10, timeout.Token);
        }
    }

    private static TaskCompletionSource<T> Signal<T>() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    private static Task Finish(Task task) => task.WaitAsync(TimeSpan.FromSeconds(10));
    private static Task<T> Finish<T>(Task<T> task) => task.WaitAsync(TimeSpan.FromSeconds(10));

    private sealed class ControlledProcessor : IInboxNotificationProcessor, IAsyncDisposable
    {
        public TaskCompletionSource<InboxNotification> Started { get; } = Signal<InboxNotification>();
        public TaskCompletionSource Cancelled { get; } = Signal();
        public TaskCompletionSource Release { get; } = Signal();
        public bool Disposed { get; private set; }
        public async Task<InboxNotificationProcessingResult> ProcessAsync(InboxNotification notification, CancellationToken token)
        {
            using var registration = token.Register(() => Cancelled.TrySetResult());
            Started.TrySetResult(notification);
            await Release.Task; // Deliberately outlives cancellation to exercise supervision.
            Assert.False(Disposed);
            return InboxNotificationProcessingResult.Succeeded;
        }
        public ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    }

    private sealed class ObservedStore(PostgresInboxProcessingStore inner) : IInboxProcessingStore, IAsyncDisposable
    {
        public int Acquisitions;
        public int Renewals;
        public int TerminalCalls;
        public bool Disposed { get; private set; }
        public TaskCompletionSource RenewalFinished { get; } = Signal();
        public Task<InboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan duration, CancellationToken token)
        {
            Interlocked.Increment(ref Acquisitions);
            return inner.TryAcquireAsync(owner, duration, token);
        }
        public async Task<InboxMutationResult> RenewAsync(InboxLease lease, TimeSpan duration, CancellationToken token)
        {
            Interlocked.Increment(ref Renewals);
            try { return await inner.RenewAsync(lease, duration, token); }
            finally { Assert.False(Disposed); RenewalFinished.TrySetResult(); }
        }
        public Task<InboxMutationResult> CompleteAsync(InboxLease lease, CancellationToken token)
        {
            Interlocked.Increment(ref TerminalCalls);
            return inner.CompleteAsync(lease, token);
        }
        public Task<InboxMutationResult> ScheduleRetryAsync(InboxLease lease, TimeSpan delay, CancellationToken token)
        {
            Interlocked.Increment(ref TerminalCalls);
            return inner.ScheduleRetryAsync(lease, delay, token);
        }
        public ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    }
}
