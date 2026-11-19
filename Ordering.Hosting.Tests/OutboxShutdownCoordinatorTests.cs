using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ordering.Application.Outbox;
using Ordering.Application.Tests;

namespace Ordering.Hosting.Tests;

public sealed class OutboxShutdownCoordinatorTests
{
    [Fact]
    public async Task SingleStopOwnerDrainsAndAuthorizesRootDisposal()
    {
        var f = new Fixture();
        await f.Start();
        try
        {
            var stop = f.Coordinator.ShutdownAsync();
            Assert.Same(stop, f.Coordinator.ShutdownAsync());
            await f.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.False(f.Scope.Disposed);
            Assert.False(f.Root.Disposed);
            f.Result.SetResult(new(OutboxRelayStatus.Cancelled, OutboxRelayStage.Publication));
            var result = await stop.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(OutboxShutdownStatus.Drained, result.Status);
            Assert.True(result.RootDisposalAuthorized);
            Assert.True(f.Scope.Disposed);
            Assert.Equal(1, f.Lifetime.Stops);
            Assert.False(f.Root.Disposed);
        }
        finally { await f.Cleanup(); }
        Assert.True(f.Root.Disposed);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeadlineRetainsRootAndObservesLateCompletionOrFailure(bool lateFailure)
    {
        var f = new Fixture();
        await f.Start();
        try
        {
            var stop = f.Coordinator.ShutdownAsync();
            await f.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
            (await f.Time.NextTimerAsync(f.Budget)).Fire();
            var result = await stop.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(OutboxShutdownStatus.DeadlineExceeded, result.Status);
            Assert.False(result.RootDisposalAuthorized);
            Assert.False(f.Coordinator.Completion.IsCompleted);
            Assert.False(f.Scope.Disposed);
            Assert.False(f.Root.Disposed);
            if (lateFailure) f.Result.SetException(new IOException("late failure"));
            else f.Result.SetResult(new(OutboxRelayStatus.Cancelled, OutboxRelayStage.Publication));
            await f.Coordinator.Completion.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(f.Scope.Disposed);
            Assert.Same(result, await f.Coordinator.ShutdownAsync());
            Assert.False(result.RootDisposalAuthorized);
            Assert.Equal(lateFailure ? OutboxSupervisionFailure.Execution : OutboxSupervisionFailure.None,
                f.Worker.ShutdownState.SupervisionFailures);
            Assert.Equal(1, f.Calls);
        }
        finally { await f.Cleanup(); }
    }

    [Theory]
    [InlineData("execution")]
    [InlineData("disposal")]
    [InlineData("host")]
    public async Task FailuresPreventRootDisposalAuthorization(string failure)
    {
        var f = new Fixture();
        await f.Start();
        try
        {
            f.Scope.Fail = failure == "disposal";
            f.Lifetime.Fail = failure == "host";
            var stop = f.Coordinator.ShutdownAsync();
            await f.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
            if (failure == "execution") f.Result.SetException(new IOException("execution failure"));
            else f.Result.SetResult(new(OutboxRelayStatus.Cancelled, OutboxRelayStage.Publication));
            var result = await stop.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(OutboxShutdownStatus.Unverified, result.Status);
            Assert.False(result.RootDisposalAuthorized);
            Assert.False(f.Root.Disposed);
            if (failure == "host") Assert.True(result.HostStopFailed);
            else Assert.NotEqual(OutboxSupervisionFailure.None, result.Workers[0].SupervisionFailures);
        }
        finally { await f.Cleanup(); }
    }

    [Fact]
    public async Task GlobalDeadlineIncludesPendingScopeDisposal()
    {
        var f = new Fixture();
        await f.Start();
        try
        {
            f.Scope.Hold = true;
            var stop = f.Coordinator.ShutdownAsync();
            f.Result.SetResult(new(OutboxRelayStatus.Cancelled, OutboxRelayStage.Publication));
            await f.Scope.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(f.Worker.ShutdownState.OperationsCompleted);
            (await f.Time.NextTimerAsync(f.Budget)).Fire();
            var result = await stop.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(OutboxShutdownStatus.DeadlineExceeded, result.Status);
            Assert.False(result.Workers[0].ScopesDisposed);
            Assert.False(f.Root.Disposed);
            f.Scope.Release.SetResult();
            await f.Coordinator.Completion.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally { await f.Cleanup(); }
    }

    [Fact]
    public async Task InvalidBudgetsAndShutdownBeforeStartAreRejected()
    {
        var f = new Fixture();
        try
        {
            foreach (var budget in new[] { TimeSpan.Zero, Timeout.InfiniteTimeSpan, TimeSpan.MaxValue })
                Assert.Throws<ArgumentOutOfRangeException>(() => new OutboxShutdownCoordinator(f.Host, budget, f.Time));
            Assert.Throws<InvalidOperationException>(() => { _ = f.Coordinator.ShutdownAsync(); });
            Assert.Throws<InvalidOperationException>(() => { _ = f.Coordinator.Completion; });
        }
        finally { await ((IAsyncDisposable)f.Host).DisposeAsync(); }
    }

    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    private sealed class Fixture
    {
        public ControlledTimeProvider Time { get; } = new();
        public TimeSpan Budget { get; } = TimeSpan.FromSeconds(9);
        public Resource Scope { get; } = new();
        public Resource Root { get; } = new();
        public Lifetime Lifetime { get; } = new();
        public TaskCompletionSource Started { get; } = Signal();
        public TaskCompletionSource Cancelled { get; } = Signal();
        public TaskCompletionSource<OutboxRelayResult> Result { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public OutboxBackgroundService Worker { get; }
        public IHost Host { get; }
        public OutboxShutdownCoordinator Coordinator { get; }
        public int Calls;
        public Fixture()
        {
            Worker = new(() => ValueTask.FromResult(new OutboxWorkerExecution(async (_, ct) =>
            {
                Calls++;
                using var registration = ct.Register(() => Cancelled.TrySetResult());
                Started.TrySetResult();
                return await Result.Task;
            }, Scope)), new(), Time, NullLogger<OutboxBackgroundService>.Instance);
            Host = new HostBuilder().UseDefaultServiceProvider(o => { o.ValidateScopes = true; o.ValidateOnBuild = true; })
                .ConfigureLogging(l => l.ClearProviders()).ConfigureServices(s =>
                {
                    s.Configure<HostOptions>(o =>
                    {
                        o.ServicesStopConcurrently = true; o.ShutdownTimeout = Timeout.InfiniteTimeSpan;
                        o.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.Ignore;
                    });
                    s.AddSingleton<IHostLifetime>(Lifetime);
                    s.AddSingleton(_ => Root);
                    s.AddSingleton<IHostedService>(Worker);
                }).Build();
            Host.Services.GetRequiredService<Resource>();
            Coordinator = new(Host, Budget, Time);
        }
        public async Task Start() { await Host.StartAsync(); await Started.Task.WaitAsync(TimeSpan.FromSeconds(10)); }
        public async Task Cleanup()
        {
            Result.TrySetResult(new(OutboxRelayStatus.Cancelled, OutboxRelayStage.Publication));
            Scope.Release.TrySetResult();
            await Coordinator.ShutdownAsync().WaitAsync(TimeSpan.FromSeconds(10));
            await Coordinator.Completion.WaitAsync(TimeSpan.FromSeconds(10));
            // Controlled failures are settled and all synthetic resources are known to this test.
            await ((IAsyncDisposable)Host).DisposeAsync();
        }
    }
    private sealed class Resource : IAsyncDisposable
    {
        public bool Disposed;
        public bool Hold;
        public bool Fail;
        public TaskCompletionSource Entered { get; } = Signal();
        public TaskCompletionSource Release { get; } = Signal();
        public async ValueTask DisposeAsync()
        {
            Entered.TrySetResult();
            if (Hold) await Release.Task;
            if (Fail) throw new IOException("disposal failure");
            Disposed = true;
        }
    }
    private sealed class Lifetime : IHostLifetime
    {
        public int Stops;
        public bool Fail;
        public Task WaitForStartAsync(CancellationToken ct) => Task.CompletedTask;
        public Task StopAsync(CancellationToken ct)
        {
            Stops++;
            return Fail ? Task.FromException(new IOException("host failure")) : Task.CompletedTask;
        }
    }
}
