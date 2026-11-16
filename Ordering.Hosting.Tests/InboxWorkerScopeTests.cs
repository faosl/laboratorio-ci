using System.Collections.Concurrent;
using System.Data;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Ordering.Application.Inbox;
using Ordering.Application.Tests;

namespace Ordering.Hosting.Tests;

public sealed class InboxWorkerScopeTests
{
    [Fact]
    public async Task ValidatedHostUsesFreshScopesSharedDataSourceAndStableOwnerUntilRestart()
    {
        var fixture = new Fixture();
        string owner;
        NpgsqlDataSource source;
        using (var host = fixture.BuildHost())
        {
            Assert.Throws<InvalidOperationException>(() => host.Services.GetRequiredService<InboxProcessingCoordinator>());
            Assert.Throws<InvalidOperationException>(() => host.Services.GetRequiredService<IInboxProcessingStore>());
            Assert.Throws<InvalidOperationException>(() => host.Services.GetRequiredService<IInboxNotificationProcessor>());
            Assert.Same(host.Services.GetRequiredService<InboxBackgroundService>(),
                Assert.Single(host.Services.GetServices<IHostedService>()));
            source = host.Services.GetRequiredService<NpgsqlDataSource>();
            await Finish(host.StartAsync());
            var first = await fixture.NextAcquisition();
            owner = first.Owner;
            InboxProcessingLimits.ValidateOwner(owner);
            first.NoWork();
            var polling = await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(5));
            AssertDisposed(first.Attempt);
            Assert.Equal(1, fixture.CreatedScopes);
            // Creating a connection object does not open a database connection. The root-owned
            // data source is still usable after disposal of the first child scope.
            using (var connection = source.CreateConnection()) Assert.Equal(ConnectionState.Closed, connection.State);
            polling.Fire();
            var second = await fixture.NextAcquisition();
            Assert.Equal(owner, second.Owner);
            AssertDisposed(first.Attempt);
            Assert.NotSame(first.Attempt.Coordinator, second.Attempt.Coordinator);
            Assert.NotSame(first.Attempt.Store, second.Attempt.Store);
            Assert.NotSame(first.Attempt.Processor, second.Attempt.Processor);
            Assert.Same(source, first.Attempt.Store.Source);
            Assert.Same(source, first.Attempt.Processor.Source);
            Assert.Same(source, second.Attempt.Store.Source);
            Assert.Same(source, second.Attempt.Processor.Source);
            var cancelled = Signal();
            using var registration = second.Token.Register(() => cancelled.TrySetResult());
            var stop = host.StopAsync();
            await Finish(cancelled.Task);
            Assert.True(second.Token.IsCancellationRequested);
            second.NoWork();
            await Finish(stop);
            AssertDisposed(second.Attempt);
            Assert.Equal(2, fixture.CreatedScopes);
            Assert.True(fixture.Worker!.ExecuteTask!.IsCompletedSuccessfully);
            Assert.Equal(0, fixture.Time.ActiveTimerCount);
        }

        using var restarted = fixture.BuildHost();
        await Finish(restarted.StartAsync());
        var next = await fixture.NextAcquisition();
        InboxProcessingLimits.ValidateOwner(next.Owner);
        Assert.NotEqual(owner, next.Owner);
        Assert.NotSame(source, next.Attempt.Store.Source);
        var shutdown = restarted.StopAsync();
        next.NoWork();
        await Finish(shutdown);
        AssertDisposed(next.Attempt);
        Assert.Equal(3, fixture.CreatedScopes);
    }

    [Fact]
    public void ValidateOnBuildRejectsCaptiveScopedCoordinator()
    {
        var fixture = new Fixture();
        var error = Assert.Throws<AggregateException>(() => fixture.BuildHost(services =>
            services.AddSingleton<CaptiveCoordinator>()));
        Assert.Contains("Cannot consume scoped service", error.ToString());
        Assert.Equal(0, fixture.CreatedScopes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ScopeRemainsAliveUntilQuiescenceAndAsyncDisposalFinish(bool shutdownTimedOut)
    {
        var fixture = new Fixture();
        using var host = fixture.BuildHost();
        await Finish(host.StartAsync());
        var first = await fixture.NextAcquisition();
        var quiescence = Signal();
        first.Attempt.ExtraQuiescence = quiescence.Task;
        first.Attempt.ShutdownTimedOut = shutdownTimedOut;
        first.Attempt.Store.DisposalRelease = Signal();
        first.NoWork();
        await Finish(first.Attempt.ResultReturned.Task);
        AssertAlive(first.Attempt);
        Assert.Equal(1, fixture.CreatedScopes);
        // No polling timer is registered until both quiescence and async disposal finish.
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
        quiescence.SetResult();
        await Finish(first.Attempt.Store.DisposalStarted.Task);
        Assert.False(first.Attempt.Store.Disposed);
        Assert.Equal(1, fixture.CreatedScopes);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
        first.Attempt.Store.DisposalRelease.SetResult();
        var delay = await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(5));
        AssertDisposed(first.Attempt);
        delay.Fire();
        var second = await fixture.NextAcquisition();
        AssertDisposed(first.Attempt);
        var stop = host.StopAsync();
        second.NoWork();
        await Finish(stop);
        AssertDisposed(second.Attempt);
        Assert.Equal(2, fixture.CreatedScopes);
    }

    [Fact]
    public async Task LateQuiescenceFaultIsLoggedDisposesScopeAndStopsNewAcquisitions()
    {
        var fixture = new Fixture();
        using var host = fixture.BuildHost();
        await Finish(host.StartAsync());
        var call = await fixture.NextAcquisition();
        var quiescence = Signal();
        var error = new IOException("late synthetic quiescence failure");
        call.Attempt.ExtraQuiescence = quiescence.Task;
        call.NoWork();
        await Finish(call.Attempt.ResultReturned.Task);
        AssertAlive(call.Attempt);
        quiescence.SetException(error);
        await Finish(fixture.Worker!.ExecuteTask!);
        AssertDisposed(call.Attempt);
        Assert.Contains(fixture.Log.Entries, entry => entry.Level == LogLevel.Critical &&
            entry.Error == error && entry.Message.Contains("Acquisitions stopped"));
        Assert.Equal(1, fixture.CreatedScopes);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
        await Finish(host.StopAsync());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RealCoordinatorAndHostDeadlinesDoNotDisposePendingScopedResources(bool lateStoreFault)
    {
        var fixture = new Fixture();
        using var host = fixture.BuildHost();
        await Finish(host.StartAsync());
        var call = await fixture.NextAcquisition();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(1), fixture.Time);
        var cancelled = Signal();
        using var registration = call.Token.Register(() => cancelled.TrySetResult());
        var stop = host.StopAsync(deadline.Token);
        await Finish(cancelled.Task);
        Assert.True(call.Token.IsCancellationRequested);
        (await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(1))).Fire();
        await Finish(stop);
        Assert.False(fixture.Worker!.ExecuteTask!.IsCompleted);
        AssertAlive(call.Attempt);
        (await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(3))).Fire();
        await Finish(fixture.Log.CoordinatorTimedOut.Task);
        AssertAlive(call.Attempt);
        Assert.False(fixture.Worker.ExecuteTask.IsCompleted);
        Assert.Equal(1, fixture.CreatedScopes);
        // The root is deliberately kept alive until local quiescence. These synthetic tests
        // do not establish safe use of a data source after root-container disposal.
        if (lateStoreFault) call.Result.SetException(new IOException("late synthetic store failure"));
        else call.NoWork();
        await Finish(fixture.Worker.ExecuteTask);
        AssertDisposed(call.Attempt);
        Assert.Equal(1, fixture.CreatedScopes);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
        Assert.Contains(fixture.Log.Entries, entry => entry.Message.Contains("host shutdown deadline"));
    }

    [Fact]
    public async Task HostCancellationDuringAsyncDisposalPreventsTheNextScopeAndWait()
    {
        var fixture = new Fixture();
        using var host = fixture.BuildHost();
        await Finish(host.StartAsync());
        var call = await fixture.NextAcquisition();
        call.Attempt.Store.DisposalRelease = Signal();
        call.NoWork();
        await Finish(call.Attempt.Store.DisposalStarted.Task);
        var stop = host.StopAsync();
        Assert.False(stop.IsCompleted);
        call.Attempt.Store.DisposalRelease.SetResult();
        await Finish(stop);
        AssertDisposed(call.Attempt);
        Assert.Equal(1, fixture.CreatedScopes);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
    }

    [Fact]
    public async Task HostCancellationWhileQuiescenceIsPendingPreventsNewScopes()
    {
        var fixture = new Fixture();
        using var host = fixture.BuildHost();
        await Finish(host.StartAsync());
        var call = await fixture.NextAcquisition();
        var quiescence = Signal();
        call.Attempt.ExtraQuiescence = quiescence.Task;
        call.NoWork();
        await Finish(call.Attempt.ResultReturned.Task);
        var stop = host.StopAsync();
        AssertAlive(call.Attempt);
        Assert.False(stop.IsCompleted);
        quiescence.SetResult();
        await Finish(stop);
        AssertDisposed(call.Attempt);
        Assert.Equal(1, fixture.CreatedScopes);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
    }

    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Wall-clock timeout only guards deadlocks; signals and controlled timers order all assertions.
    private static Task Finish(Task task) => task.WaitAsync(TimeSpan.FromSeconds(10));
    private static void AssertAlive(Attempt attempt)
    {
        Assert.False(attempt.Store.DisposalStarted.Task.IsCompleted);
        Assert.False(attempt.Processor.Disposed);
    }
    private static void AssertDisposed(Attempt attempt)
    {
        Assert.True(attempt.Store.Disposed);
        Assert.True(attempt.Processor.Disposed);
    }

    private sealed class Fixture
    {
        private readonly Channel<Acquisition> _acquisitions = Channel.CreateUnbounded<Acquisition>();
        private int _createdScopes;
        public int CreatedScopes => Volatile.Read(ref _createdScopes);
        public ControlledTimeProvider Time { get; } = new();
        public TestLogger Log { get; } = new();
        public InboxBackgroundService? Worker { get; private set; }
        public Task<Acquisition> NextAcquisition() => _acquisitions.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        public IHost BuildHost(Action<IServiceCollection>? configure = null) => new HostBuilder()
            .UseDefaultServiceProvider(options =>
            {
                options.ValidateScopes = true;
                options.ValidateOnBuild = true;
            })
            .ConfigureLogging(logging => logging.ClearProviders())
            .ConfigureServices(services =>
            {
                services.AddSingleton<TimeProvider>(Time);
                // No connections are opened. Only the root container owns this shared pool.
                services.AddSingleton(_ => NpgsqlDataSource.Create(
                    "Host=localhost;Database=synthetic_only;Username=synthetic_only;Enlist=false"));
                services.AddSingleton(new InboxProcessingOptions(TimeSpan.FromSeconds(30),
                    TimeSpan.FromSeconds(7), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(3)));
                services.AddScoped<IInboxProcessingStore, SyntheticStore>();
                services.AddScoped<IInboxNotificationProcessor, SyntheticProcessor>();
                services.AddScoped<InboxProcessingCoordinator>();
                services.AddSingleton(provider =>
                {
                    var scopes = provider.GetRequiredService<IServiceScopeFactory>();
                    Worker = new InboxBackgroundService(() => CreateExecution(scopes), new(), Time, Log);
                    return Worker;
                });
                services.AddHostedService(provider => provider.GetRequiredService<InboxBackgroundService>());
                configure?.Invoke(services);
            }).Build();

        private InboxWorkerExecution CreateExecution(IServiceScopeFactory scopes)
        {
            var scope = scopes.CreateAsyncScope();
            Interlocked.Increment(ref _createdScopes);
            // Resolution occurs inside the owned delegate, after the worker has installed
            // its await-using boundary, so even a resolution exception releases the scope.
            return new(async (owner, token) =>
            {
                var provider = scope.ServiceProvider;
                var coordinator = provider.GetRequiredService<InboxProcessingCoordinator>();
                var store = (SyntheticStore)provider.GetRequiredService<IInboxProcessingStore>();
                var processor = (SyntheticProcessor)provider.GetRequiredService<IInboxNotificationProcessor>();
                Assert.Same(coordinator, provider.GetRequiredService<InboxProcessingCoordinator>());
                Assert.Same(store, provider.GetRequiredService<IInboxProcessingStore>());
                Assert.Same(processor, provider.GetRequiredService<IInboxNotificationProcessor>());
                var attempt = new Attempt(coordinator, store, processor);
                store.OnAcquire = (acquiredOwner, acquiredToken) =>
                {
                    var acquisition = new Acquisition(attempt, acquiredOwner, acquiredToken);
                    _acquisitions.Writer.TryWrite(acquisition);
                    return acquisition.Result.Task;
                };
                var result = await coordinator.RunOnceAsync(owner, token);
                // Only the explicit quiescence tests decorate the real coordinator's result
                // to simulate a late supervision fault or a separately signalled local task.
                if (attempt.ExtraQuiescence is { } extra)
                    result = result with { Quiescence = Task.WhenAll(result.Quiescence, extra),
                        ShutdownTimedOut = attempt.ShutdownTimedOut };
                attempt.ResultReturned.SetResult();
                return result;
            }, scope);
        }
    }

    private sealed class Attempt(InboxProcessingCoordinator coordinator, SyntheticStore store, SyntheticProcessor processor)
    {
        public InboxProcessingCoordinator Coordinator { get; } = coordinator;
        public SyntheticStore Store { get; } = store;
        public SyntheticProcessor Processor { get; } = processor;
        public TaskCompletionSource ResultReturned { get; } = Signal();
        public Task? ExtraQuiescence { get; set; }
        public bool ShutdownTimedOut { get; set; }
    }

    private sealed record Acquisition(Attempt Attempt, string Owner, CancellationToken Token)
    {
        public TaskCompletionSource<InboxAcquisitionResult> Result { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void NoWork() => Result.SetResult(new(InboxAcquisitionStatus.NoEligibleWork, null));
    }

    private sealed class SyntheticStore(NpgsqlDataSource source) : IInboxProcessingStore, IAsyncDisposable
    {
        public NpgsqlDataSource Source { get; } = source;
        public Func<string, CancellationToken, Task<InboxAcquisitionResult>> OnAcquire { get; set; } = null!;
        public TaskCompletionSource DisposalStarted { get; } = Signal();
        public TaskCompletionSource? DisposalRelease { get; set; }
        public bool Disposed { get; private set; }
        public async Task<InboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan leaseDuration, CancellationToken cancellationToken)
        {
            try { return await OnAcquire(owner, cancellationToken); }
            finally { Assert.False(Disposed); }
        }
        public Task<InboxMutationResult> RenewAsync(InboxLease lease, TimeSpan leaseDuration, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("No synthetic work was acquired.");
        public Task<InboxMutationResult> CompleteAsync(InboxLease lease, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("No synthetic work was acquired.");
        public Task<InboxMutationResult> ScheduleRetryAsync(InboxLease lease, TimeSpan retryDelay, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("No synthetic work was acquired.");
        public async ValueTask DisposeAsync()
        {
            Assert.False(Disposed);
            DisposalStarted.SetResult();
            if (DisposalRelease is { } release) await release.Task;
            Disposed = true;
        }
    }

    private sealed class SyntheticProcessor(NpgsqlDataSource source) : IInboxNotificationProcessor, IAsyncDisposable
    {
        public NpgsqlDataSource Source { get; } = source;
        public bool Disposed { get; private set; }
        public Task<InboxNotificationProcessingResult> ProcessAsync(InboxNotification notification, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("No synthetic work was acquired.");
        public ValueTask DisposeAsync()
        {
            Assert.False(Disposed);
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class CaptiveCoordinator(InboxProcessingCoordinator coordinator)
    {
        public InboxProcessingCoordinator Coordinator { get; } = coordinator;
    }

    private sealed class TestLogger : ILogger<InboxBackgroundService>
    {
        public ConcurrentQueue<(LogLevel Level, string Message, Exception? Error)> Entries { get; } = new();
        public TaskCompletionSource CoordinatorTimedOut { get; } = Signal();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            Entries.Enqueue((logLevel, message, exception));
            if (message.Contains("coordinator shutdown")) CoordinatorTimedOut.TrySetResult();
        }
    }
}
