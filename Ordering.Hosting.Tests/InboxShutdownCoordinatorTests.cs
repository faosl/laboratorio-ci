using System.Data;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Ordering.Application.Inbox;
using Ordering.Application.Tests;

namespace Ordering.Hosting.Tests;

public sealed class InboxShutdownCoordinatorTests
{
    [Fact]
    public async Task ThreeWorkersStopTogetherAndDrainOnlyAfterEveryExecutionAndScope()
    {
        var fixture = new Fixture(3);
        await fixture.StartAsync();
        try
        {
            var shutdown = fixture.Coordinator.ShutdownAsync();
            var concurrentObservers = Enumerable.Range(0, 5)
                .Select(_ => Task.Run(() => fixture.Coordinator.ShutdownAsync())).ToArray();
            await fixture.AllCancelled();
            Assert.Same(shutdown, fixture.Coordinator.ShutdownAsync());
            var deadline = await fixture.Time.NextTimerAsync(fixture.Budget);
            Assert.Equal(1, fixture.Time.ActiveTimerCount);
            Assert.False(shutdown.IsCompleted);

            foreach (var index in new[] { 1, 2 })
            {
                fixture.Participants[index].Return();
                await Finish(fixture.Participants[index].Worker.ExecuteTask!);
                fixture.Time.Advance(TimeSpan.FromSeconds(1));
                Assert.False(shutdown.IsCompleted);
                fixture.AssertDataSourceAlive();
            }

            fixture.Participants[0].Return();
            var result = await Finish(shutdown);
            Assert.All(await Finish(Task.WhenAll(concurrentObservers)), observed => Assert.Same(result, observed));
            Assert.Equal(1, fixture.Host.Services.GetRequiredService<RecordingLifetime>().Stops);
            Assert.Equal(InboxShutdownStatus.Drained, result.Status);
            Assert.True(result.RootDisposalAuthorized);
            Assert.True(result.HostStopCompleted);
            Assert.False(result.HostStopFailed);
            Assert.Equal(3, result.Workers.Count);
            Assert.All(result.Workers, state =>
            {
                Assert.True(state.StopRequested);
                Assert.True(state.OperationsCompleted);
                Assert.True(state.ExecuteTaskCompleted);
                Assert.True(state.ScopesDisposed);
                Assert.Equal(InboxSupervisionFailure.None, state.SupervisionFailures);
            });
            Assert.False(deadline.Active);
            Assert.Equal(0, fixture.Time.ActiveTimerCount);
            fixture.AssertDataSourceAlive();
            // Advancing time after stop must never trigger another acquisition or scope.
            fixture.Time.Advance(TimeSpan.FromDays(1));
            Assert.All(fixture.Participants, participant => Assert.Equal(1, participant.Calls));
            Assert.All(fixture.Participants, participant => Assert.Equal(1, participant.Scopes));
        }
        finally { await fixture.CleanupAsync(); }
        await Assert.ThrowsAsync<ObjectDisposedException>(() => fixture.Source.OpenConnectionAsync().AsTask());
    }

    [Fact]
    public async Task QuiescenceAndAsyncScopeDisposalShareTheSingleGlobalBudget()
    {
        var fixture = new Fixture(2);
        await fixture.StartAsync();
        try
        {
            var first = fixture.Participants[0];
            var second = fixture.Participants[1];
            var shutdown = fixture.Coordinator.ShutdownAsync();
            await fixture.AllCancelled();
            var deadline = await fixture.Time.NextTimerAsync(fixture.Budget);
            first.Return(first.Quiescence.Task);
            second.Return();
            await Finish(second.Worker.ExecuteTask!);
            Assert.False(shutdown.IsCompleted);
            Assert.False(first.Worker.ShutdownState.OperationsCompleted);
            Assert.False(first.Worker.ShutdownState.ScopesDisposed);
            Assert.False(first.Worker.ExecuteTask!.IsCompleted);
            Assert.False(first.Resource!.DisposalStarted.Task.IsCompleted);
            fixture.AssertDataSourceAlive();

            fixture.Time.Advance(TimeSpan.FromSeconds(2));
            first.Resource.HoldDisposal = true;
            first.Quiescence.SetResult();
            await Finish(first.Resource.DisposalStarted.Task);
            Assert.True(first.Worker.ShutdownState.OperationsCompleted);
            Assert.False(first.Worker.ShutdownState.ScopesDisposed);
            Assert.False(first.Worker.ExecuteTask.IsCompleted);
            Assert.False(shutdown.IsCompleted);
            Assert.Equal(1, fixture.Time.ActiveTimerCount);

            fixture.Time.Advance(fixture.Budget - TimeSpan.FromSeconds(2));
            deadline.Fire();
            var result = await Finish(shutdown);
            Assert.Equal(InboxShutdownStatus.DeadlineExceeded, result.Status);
            Assert.False(result.RootDisposalAuthorized);
            Assert.False(result.HostStopCompleted);
            Assert.True(result.Workers[0].OperationsCompleted);
            Assert.False(result.Workers[0].ScopesDisposed);
            fixture.AssertDataSourceAlive();

            first.Resource.DisposalRelease.SetResult();
            await Finish(fixture.Coordinator.Completion);
            Assert.True(first.Worker.ShutdownState.ScopesDisposed);
            Assert.Same(result, await fixture.Coordinator.ShutdownAsync());
            Assert.False(result.RootDisposalAuthorized);
            Assert.All(fixture.Participants, participant => Assert.Equal(1, participant.Calls));
        }
        finally { await fixture.CleanupAsync(); }
    }

    [Fact]
    public async Task DeadlineWhileQuiescenceIsPendingRetainsRootAndSupervisionUntilRelease()
    {
        var fixture = new Fixture(3);
        await fixture.StartAsync();
        try
        {
            var shutdown = fixture.Coordinator.ShutdownAsync();
            await fixture.AllCancelled();
            var pending = fixture.Participants[1];
            pending.Return(pending.Quiescence.Task);
            fixture.Participants[0].Return();
            fixture.Participants[2].Return();
            await Finish(fixture.Participants[0].Worker.ExecuteTask!);
            await Finish(fixture.Participants[2].Worker.ExecuteTask!);
            (await fixture.Time.NextTimerAsync(fixture.Budget)).Fire();

            var result = await Finish(shutdown);
            Assert.Equal(InboxShutdownStatus.DeadlineExceeded, result.Status);
            Assert.False(result.RootDisposalAuthorized);
            Assert.False(result.HostStopCompleted);
            Assert.False(result.Workers[1].OperationsCompleted);
            Assert.False(result.Workers[1].ScopesDisposed);
            Assert.False(result.Workers[1].ExecuteTaskCompleted);
            Assert.False(fixture.Coordinator.Completion.IsCompleted);
            fixture.AssertDataSourceAlive();
            pending.Quiescence.SetResult();
            await Finish(fixture.Coordinator.Completion);
            Assert.True(pending.Worker.ExecuteTask!.IsCompletedSuccessfully);
            Assert.Equal(1, pending.Calls);
            Assert.Equal(0, fixture.Time.ActiveTimerCount);
        }
        finally { await fixture.CleanupAsync(); }
    }

    [Fact]
    public async Task CaughtQuiescenceFailureIsUnverifiedEvenWhenExecuteTaskSucceeds()
    {
        var fixture = new Fixture(1);
        await fixture.StartAsync();
        try
        {
            var shutdown = fixture.Coordinator.ShutdownAsync();
            await fixture.AllCancelled();
            var participant = fixture.Participants[0];
            participant.Return(Task.FromException(new IOException("synthetic private diagnostic")));
            var result = await Finish(shutdown);
            Assert.True(participant.Worker.ExecuteTask!.IsCompletedSuccessfully);
            Assert.True(result.Workers[0].ExecuteTaskCompleted);
            Assert.True(result.Workers[0].ScopesDisposed);
            Assert.Equal(InboxSupervisionFailure.ResultSupervision, result.Workers[0].SupervisionFailures);
            AssertUnverified(result);
            Assert.DoesNotContain("synthetic private diagnostic", result.ToString());
            fixture.AssertDataSourceAlive();
        }
        finally { await fixture.CleanupAsync(); }
    }

    [Fact]
    public async Task UnexpectedExecutionFailureIsUnverifiedWithoutRetryOrAssumingOrphanedWork()
    {
        var fixture = new Fixture(2);
        await fixture.StartAsync();
        try
        {
            var failed = fixture.Participants[0];
            failed.Result.SetException(new IOException("synthetic execution failure"));
            await Assert.ThrowsAsync<IOException>(() => Finish(failed.Worker.ExecuteTask!));
            var shutdown = fixture.Coordinator.ShutdownAsync();
            await Finish(fixture.Participants[1].Cancelled.Task);
            fixture.Participants[1].Return();
            var result = await Finish(shutdown);
            Assert.Equal(InboxSupervisionFailure.Execution, result.Workers[0].SupervisionFailures);
            Assert.True(result.Workers[0].ScopesDisposed);
            Assert.Equal(1, failed.Calls);
            AssertUnverified(result);
            Assert.Equal(0, fixture.Time.ActiveTimerCount);
        }
        finally { await fixture.CleanupAsync(); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExplicitUntrackedWorkRemainsUnverifiedIncludingWhenGlobalBudgetExpires(bool expire)
    {
        var fixture = new Fixture(1);
        await fixture.StartAsync();
        try
        {
            var participant = fixture.Participants[0];
            // Represents a delegate that explicitly knows it started work absent from Quiescence.
            participant.DetachedTask = participant.DetachedRelease.Task;
            participant.Worker.ReportUntrackedWork();
            var shutdown = fixture.Coordinator.ShutdownAsync();
            await fixture.AllCancelled();
            participant.Return(expire ? participant.Quiescence.Task : Task.CompletedTask);
            if (expire) (await fixture.Time.NextTimerAsync(fixture.Budget)).Fire();
            var result = await Finish(shutdown);
            AssertUnverified(result);
            Assert.Equal(InboxSupervisionFailure.KnownUntrackedWork, result.Workers[0].SupervisionFailures);
            Assert.False(participant.DetachedTask.IsCompleted);
            fixture.AssertDataSourceAlive();
            participant.DetachedRelease.SetResult();
            participant.Quiescence.TrySetResult();
            await Finish(participant.DetachedTask);
            await Finish(fixture.Coordinator.Completion);
        }
        finally { await fixture.CleanupAsync(); }
    }

    [Fact]
    public async Task ScopeDisposalFailurePreventsAuthorization()
    {
        var fixture = new Fixture(1);
        await fixture.StartAsync();
        try
        {
            var participant = fixture.Participants[0];
            participant.Resource!.FailDisposal = true;
            var shutdown = fixture.Coordinator.ShutdownAsync();
            await fixture.AllCancelled();
            participant.Return();
            var result = await Finish(shutdown);
            AssertUnverified(result);
            Assert.True(result.Workers[0].OperationsCompleted);
            Assert.False(result.Workers[0].ScopesDisposed);
            Assert.True(result.Workers[0].SupervisionFailures.HasFlag(InboxSupervisionFailure.ScopeDisposal));
            fixture.AssertDataSourceAlive();
        }
        finally { await fixture.CleanupAsync(); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SupervisionFailureIsVisibleBeforePendingScopeDisposalAndExecuteTaskFinish(bool quiescenceFailure)
    {
        var fixture = new Fixture(1);
        await fixture.StartAsync();
        try
        {
            var participant = fixture.Participants[0];
            participant.Resource!.HoldDisposal = true;
            var shutdown = fixture.Coordinator.ShutdownAsync();
            await fixture.AllCancelled();
            var error = new IOException("synthetic failure before disposal");
            if (quiescenceFailure) participant.Return(Task.FromException(error));
            else participant.Result.SetException(error);
            await Finish(participant.Resource.DisposalStarted.Task);
            Assert.False(participant.Worker.ExecuteTask!.IsCompleted);
            (await fixture.Time.NextTimerAsync(fixture.Budget)).Fire();
            var result = await Finish(shutdown);
            AssertUnverified(result);
            Assert.Equal(quiescenceFailure ? InboxSupervisionFailure.ResultSupervision : InboxSupervisionFailure.Execution,
                result.Workers[0].SupervisionFailures);
            Assert.False(result.Workers[0].ExecuteTaskCompleted);
            Assert.False(result.Workers[0].ScopesDisposed);
            fixture.AssertDataSourceAlive();
            participant.Resource.DisposalRelease.SetResult();
            await Finish(fixture.Coordinator.Completion);
        }
        finally { await fixture.CleanupAsync(); }
    }

    [Fact]
    public async Task HostStopFailureIsObservedAndPreventsAuthorization()
    {
        var fixture = new Fixture(1, services => services.AddSingleton<IHostLifetime, FailingStopLifetime>());
        await fixture.StartAsync();
        try
        {
            var shutdown = fixture.Coordinator.ShutdownAsync();
            await fixture.AllCancelled();
            fixture.Participants[0].Return();
            var result = await Finish(shutdown);
            AssertUnverified(result);
            Assert.True(result.HostStopCompleted);
            Assert.True(result.HostStopFailed);
            Assert.True(result.Workers[0].ScopesDisposed);
            fixture.AssertDataSourceAlive();
        }
        finally { await fixture.CleanupAsync(); }
    }

    [Fact]
    public async Task InvalidCompositionBudgetAndShutdownBeforeStartAreRejected()
    {
        var fixture = new Fixture(1);
        try
        {
            foreach (var budget in new[] { TimeSpan.Zero, Timeout.InfiniteTimeSpan, TimeSpan.MaxValue })
                Assert.Throws<ArgumentOutOfRangeException>(() => new InboxShutdownCoordinator(fixture.Host, budget, fixture.Time));
            Assert.Throws<InvalidOperationException>(() => { _ = fixture.Coordinator.ShutdownAsync(); });
            Assert.Throws<InvalidOperationException>(() => { _ = fixture.Coordinator.Completion; });
        }
        finally { await fixture.CleanupAsync(); }

        foreach (var (concurrent, timeout) in new[] { (false, Timeout.InfiniteTimeSpan), (true, TimeSpan.FromSeconds(1)) })
        {
            var invalid = new Fixture(1, services => services.Configure<HostOptions>(options =>
            {
                options.ServicesStopConcurrently = concurrent;
                options.ShutdownTimeout = timeout;
            }), createCoordinator: false);
            try
            {
                Assert.Throws<ArgumentException>(() => new InboxShutdownCoordinator(invalid.Host, invalid.Budget, invalid.Time));
            }
            finally { await invalid.CleanupAsync(); }
        }

        var empty = new Fixture(0, createCoordinator: false);
        try
        {
            Assert.Throws<ArgumentException>(() => new InboxShutdownCoordinator(empty.Host, empty.Budget, empty.Time));
        }
        finally { await empty.CleanupAsync(); }
    }

    private static void AssertUnverified(InboxShutdownResult result)
    {
        Assert.Equal(InboxShutdownStatus.Unverified, result.Status);
        Assert.False(result.RootDisposalAuthorized);
    }

    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Wall-clock time only guards against deadlock. Signals and the controlled timer order tests.
    private static Task Finish(Task task) => task.WaitAsync(TimeSpan.FromSeconds(10));
    private static Task<T> Finish<T>(Task<T> task) => task.WaitAsync(TimeSpan.FromSeconds(10));

    private sealed class Fixture
    {
        public TimeSpan Budget { get; } = TimeSpan.FromSeconds(9);
        public ControlledTimeProvider Time { get; } = new();
        public Participant[] Participants { get; }
        public IHost Host { get; }
        public NpgsqlDataSource Source { get; }
        public InboxShutdownCoordinator Coordinator { get; } = null!;

        public Fixture(int count, Action<IServiceCollection>? configure = null, bool createCoordinator = true)
        {
            Participants = Enumerable.Range(0, count).Select(_ => new Participant()).ToArray();
            Host = new HostBuilder().UseDefaultServiceProvider(options =>
            {
                options.ValidateScopes = true;
                options.ValidateOnBuild = true;
            }).ConfigureLogging(logging => logging.ClearProviders()).ConfigureServices(services =>
            {
                services.Configure<HostOptions>(options =>
                {
                    options.ServicesStopConcurrently = true;
                    options.ShutdownTimeout = Timeout.InfiniteTimeSpan;
                });
                services.AddSingleton<RecordingLifetime>();
                services.AddSingleton<IHostLifetime>(provider => provider.GetRequiredService<RecordingLifetime>());
                services.AddSingleton(_ => NpgsqlDataSource.Create("Host=localhost;Database=synthetic;Username=synthetic;Password=synthetic;Enlist=false"));
                services.AddScoped<ScopedResource>();
                foreach (var participant in Participants)
                {
                    services.AddSingleton<IHostedService>(provider => participant.CreateWorker(
                        provider.GetRequiredService<IServiceScopeFactory>(), Time));
                }
                configure?.Invoke(services);
            }).Build();
            Source = Host.Services.GetRequiredService<NpgsqlDataSource>();
            if (createCoordinator) Coordinator = new(Host, Budget, Time);
        }

        public async Task StartAsync()
        {
            await Finish(Host.StartAsync());
            await Finish(Task.WhenAll(Participants.Select(participant => participant.Started.Task)));
        }

        public Task AllCancelled() => Finish(Task.WhenAll(Participants.Select(participant => participant.Cancelled.Task)));

        public void AssertDataSourceAlive()
        {
            Assert.Same(Source, Host.Services.GetRequiredService<NpgsqlDataSource>());
            foreach (var participant in Participants)
                Assert.Same(Source, participant.Resource!.Source);
            using var connection = Source.CreateConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        public async Task CleanupAsync()
        {
            // Tests retain explicit lifetime ownership. Release all synthetic work before disposal,
            // including on assertion failure, and never issue an independent concurrent StopAsync.
            foreach (var participant in Participants)
            {
                participant.Result.TrySetResult(new(InboxCoordinationStatus.NoEligibleWork, InboxCoordinationStage.Acquisition));
                participant.Quiescence.TrySetResult();
                participant.DetachedRelease.TrySetResult();
                participant.Resource?.DisposalRelease.TrySetResult();
            }
            if (Participants.Any(participant => participant.Worker?.ExecuteTask is not null))
            {
                await Finish(Coordinator.ShutdownAsync());
                await Finish(Coordinator.Completion);
                await Finish(Task.WhenAll(Participants.Where(participant => participant.DetachedTask is not null)
                    .Select(participant => participant.DetachedTask!)));
            }
            await ((IAsyncDisposable)Host).DisposeAsync();
        }
    }

    private sealed class Participant
    {
        public TaskCompletionSource Started { get; } = Signal();
        public TaskCompletionSource Cancelled { get; } = Signal();
        public TaskCompletionSource Quiescence { get; } = Signal();
        public TaskCompletionSource DetachedRelease { get; } = Signal();
        public Task? DetachedTask { get; set; }
        public TaskCompletionSource<InboxCoordinationResult> Result { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public InboxBackgroundService Worker { get; private set; } = null!;
        public ScopedResource? Resource { get; private set; }
        public int Calls { get; private set; }
        public int Scopes { get; private set; }

        public void Return(Task? quiescence = null) => Result.SetResult(new(InboxCoordinationStatus.NoEligibleWork,
            InboxCoordinationStage.Acquisition) { Quiescence = quiescence ?? Task.CompletedTask });

        public InboxBackgroundService CreateWorker(IServiceScopeFactory scopes, TimeProvider time)
        {
            Worker = new(() =>
            {
                Scopes++;
                var scope = scopes.CreateAsyncScope();
                return new(async (_, token) =>
                {
                    Calls++;
                    Resource = scope.ServiceProvider.GetRequiredService<ScopedResource>();
                    using var registration = token.Register(() => Cancelled.TrySetResult());
                    Started.TrySetResult();
                    return await Result.Task;
                }, scope);
            }, new(), time, NullLogger<InboxBackgroundService>.Instance);
            return Worker;
        }
    }

    private sealed class ScopedResource(NpgsqlDataSource source) : IAsyncDisposable
    {
        public NpgsqlDataSource Source { get; } = source;
        public TaskCompletionSource DisposalStarted { get; } = Signal();
        public TaskCompletionSource DisposalRelease { get; } = Signal();
        public bool HoldDisposal { get; set; }
        public bool FailDisposal { get; set; }
        public async ValueTask DisposeAsync()
        {
            DisposalStarted.TrySetResult();
            if (HoldDisposal) await DisposalRelease.Task;
            if (FailDisposal) throw new IOException("synthetic disposal failure");
        }
    }

    private sealed class FailingStopLifetime : IHostLifetime
    {
        public Task WaitForStartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.FromException(new IOException("synthetic lifetime failure"));
    }

    private sealed class RecordingLifetime : IHostLifetime
    {
        public int Stops { get; private set; }
        public Task WaitForStartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken)
        {
            Stops++;
            return Task.CompletedTask;
        }
    }
}
