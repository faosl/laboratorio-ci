using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Ordering.Application.Inbox;
using Ordering.Application.Tests;

namespace Ordering.Hosting.Tests;

public sealed class InboxBackgroundServiceTests
{
    [Fact]
    public async Task NoEligibleWorkPollsAfterFiveSecondsAndHostCancellationReleasesTheWait()
    {
        var fixture = new Fixture();
        using var worker = fixture.Create();
        await worker.StartAsync(default);
        var first = await fixture.NextCall();
        first.Return(InboxCoordinationStatus.NoEligibleWork);
        var delay = await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, fixture.Calls);
        delay.Fire();
        var second = await fixture.NextCall();
        second.Return(InboxCoordinationStatus.NoEligibleWork);
        await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(5));
        await Finish(worker.StopAsync(default));
        Assert.True(second.Token.IsCancellationRequested);
        Assert.Equal(2, fixture.Calls);
        Assert.True(worker.ExecuteTask!.IsCompletedSuccessfully);
        Assert.Equal(2, fixture.Disposals);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
    }

    [Theory]
    [InlineData(InboxCoordinationStatus.Completed)]
    [InlineData(InboxCoordinationStatus.RetryScheduled)]
    [InlineData(InboxCoordinationStatus.LeaseLost)]
    public async Task IsolatedImmediateResultDoesNotWait(InboxCoordinationStatus status)
    {
        var fixture = new Fixture();
        using var worker = fixture.Create();
        await worker.StartAsync(default);
        (await fixture.NextCall()).Return(status);
        var next = await fixture.NextCall();
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
        await Stop(worker, next);
    }

    [Fact]
    public async Task StoreFailureBackoffGrowsCapsAndResetsAfterEveryOtherStatus()
    {
        foreach (var reset in Enum.GetValues<InboxCoordinationStatus>().Where(status => status != InboxCoordinationStatus.StoreFailed))
        {
            var fixture = new Fixture();
            using var worker = fixture.Create();
            await worker.StartAsync(default);
            foreach (var seconds in new[] { 2, 4, 8, 16, 30, 30, 30 })
            {
                var call = await fixture.NextCall();
                call.Return(InboxCoordinationStatus.StoreFailed);
                var delay = await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(seconds));
                Assert.Equal(call.Number, fixture.Calls);
                delay.Fire();
            }
            (await fixture.NextCall()).Return(reset);
            if (reset is InboxCoordinationStatus.NoEligibleWork or InboxCoordinationStatus.ProcessingFailed
                or InboxCoordinationStatus.OutcomeUnknown or InboxCoordinationStatus.Cancelled)
                (await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(5))).Fire();
            (await fixture.NextCall()).Return(InboxCoordinationStatus.StoreFailed);
            (await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(2))).Fire();
            await Stop(worker, await fixture.NextCall());
        }
    }

    [Theory]
    [InlineData(InboxCoordinationStatus.ProcessingFailed)]
    [InlineData(InboxCoordinationStatus.OutcomeUnknown)]
    [InlineData(InboxCoordinationStatus.Cancelled)]
    public async Task FailuresWaitAndAreLogged(InboxCoordinationStatus status)
    {
        var fixture = new Fixture();
        var error = new IOException("synthetic failure");
        using var worker = fixture.Create();
        await worker.StartAsync(default);
        (await fixture.NextCall()).Result.SetResult(new(status, InboxCoordinationStage.Processing, Error: error));
        var delay = await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, fixture.Calls);
        Assert.Contains(fixture.Log.Entries, entry => entry.Error == error && entry.Message.Contains(status.ToString()));
        delay.Fire();
        await Stop(worker, await fixture.NextCall());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PendingQuiescencePreventsNewAcquisitionsEvenForConfirmedCompletion(bool shutdownTimedOut)
    {
        var fixture = new Fixture();
        var quiescence = Signal();
        using var worker = fixture.Create();
        await worker.StartAsync(default);
        (await fixture.NextCall()).Result.SetResult(new(InboxCoordinationStatus.Completed, InboxCoordinationStage.Completion)
        {
            ShutdownTimedOut = shutdownTimedOut,
            Quiescence = quiescence.Task
        });
        // The delegate publishes its result without advancing any worker delay.
        await fixture.ResultObserved.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(1, fixture.Calls);
        Assert.Equal(1, fixture.CreatedExecutions);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
        Assert.Equal(0, fixture.Disposals);
        quiescence.SetResult();
        await Stop(worker, await fixture.NextCall());
        Assert.Equal(shutdownTimedOut, fixture.Log.Entries.Any(entry => entry.Message.Contains("coordinator shutdown")));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HostDeadlineBoundsStopWhileDisposeRetainsSupervisionAndDependencies(bool lateFault)
    {
        var fixture = new Fixture();
        var quiescence = Signal();
        using var worker = fixture.Create();
        await worker.StartAsync(default);
        var call = await fixture.NextCall();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(3), fixture.Time);
        var stop = worker.StopAsync(deadline.Token);
        Assert.True(call.Token.IsCancellationRequested);
        Assert.False(stop.IsCompleted);
        (await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(3))).Fire();
        await Finish(stop);
        worker.Dispose();
        Assert.False(worker.ExecuteTask!.IsCompleted);
        Assert.Equal(0, fixture.Disposals);

        // The coordinator can return even after the host deadline. Supervision follows its result.
        call.Result.SetResult(new(InboxCoordinationStatus.Cancelled, InboxCoordinationStage.Processing)
        {
            ShutdownTimedOut = true,
            Quiescence = quiescence.Task
        });
        await fixture.ResultObserved.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(worker.ExecuteTask.IsCompleted);
        Assert.Equal(0, fixture.Disposals);
        if (lateFault) quiescence.SetException(new IOException("late failure"));
        else quiescence.SetResult();
        await Finish(worker.ExecuteTask);
        Assert.Equal(1, fixture.Calls);
        Assert.Equal(1, fixture.Disposals);
        Assert.Contains(fixture.Log.Entries, entry => entry.Message.Contains("host shutdown deadline"));
        Assert.Equal(lateFault, fixture.Log.Entries.Any(entry => entry.Error?.Message == "late failure"));
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
    }

    [Fact]
    public async Task HostStopWhileAlreadyWaitingForQuiescenceDoesNotAcquireAgain()
    {
        var fixture = new Fixture();
        var quiescence = Signal();
        using var worker = fixture.Create();
        await worker.StartAsync(default);
        (await fixture.NextCall()).Result.SetResult(new(InboxCoordinationStatus.OutcomeUnknown, InboxCoordinationStage.Renewal)
        {
            Quiescence = quiescence.Task,
            ShutdownTimedOut = true
        });
        await fixture.ResultObserved.Task.WaitAsync(TimeSpan.FromSeconds(10));
        using var deadline = new CancellationTokenSource();
        var stop = worker.StopAsync(deadline.Token);
        deadline.Cancel();
        await Finish(stop);
        Assert.False(worker.ExecuteTask!.IsCompleted);
        Assert.Equal(0, fixture.Disposals);
        quiescence.SetResult();
        await Finish(worker.ExecuteTask);
        Assert.Equal(1, fixture.Calls);
        Assert.Equal(1, fixture.Disposals);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RealCoordinatorRetainsAnUncooperativeStoreThroughBothShutdownDeadlines(bool lateFault)
    {
        var fixture = new Fixture();
        var store = new PendingAcquisitionStore();
        var coordinator = new InboxProcessingCoordinator(store, new UnusedProcessor(),
            new(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(7), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(3)),
            fixture.Time);
        using var worker = new InboxBackgroundService(() => new(coordinator.RunOnceAsync, store),
            new(), fixture.Time, fixture.Log);
        await worker.StartAsync(default);
        await Finish(store.Entered.Task);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(1), fixture.Time);
        var stop = worker.StopAsync(deadline.Token);
        (await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(1))).Fire();
        await Finish(stop);
        worker.Dispose();
        Assert.False(store.IsDisposed);
        var coordinatorDeadline = await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(3));
        coordinatorDeadline.Fire();
        await Finish(fixture.Log.CoordinatorTimedOut.Task);
        Assert.False(worker.ExecuteTask!.IsCompleted);
        Assert.False(store.IsDisposed);
        if (lateFault) store.Acquisition.SetException(new IOException("late store failure"));
        else store.Acquisition.SetResult(new(InboxAcquisitionStatus.NoEligibleWork, null));
        await Finish(worker.ExecuteTask);
        Assert.True(store.IsDisposed);
        Assert.Equal(1, store.Calls);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
    }

    [Fact]
    public async Task QuiescenceFaultStopsAcquisitionsAndObservesTheError()
    {
        var fixture = new Fixture();
        using var worker = fixture.Create();
        await worker.StartAsync(default);
        (await fixture.NextCall()).Result.SetResult(new(InboxCoordinationStatus.Completed, InboxCoordinationStage.Completion)
        {
            Quiescence = Task.FromException(new IOException("quiescence failure"))
        });
        await Finish(worker.ExecuteTask!);
        Assert.Equal(1, fixture.Calls);
        Assert.Equal(1, fixture.Disposals);
        Assert.Contains(fixture.Log.Entries, entry => entry.Error?.Message == "quiescence failure");
    }

    [Fact]
    public async Task LoggingFailureCannotDisposeDependenciesWhileQuiescenceIsPending()
    {
        var fixture = new Fixture();
        var quiescence = Signal();
        fixture.Log.FailOnCoordinatorTimeout = true;
        using var worker = fixture.Create();
        await worker.StartAsync(default);
        (await fixture.NextCall()).Result.SetResult(new(InboxCoordinationStatus.OutcomeUnknown, InboxCoordinationStage.Acquisition)
        {
            ShutdownTimedOut = true,
            Quiescence = quiescence.Task
        });
        await Finish(fixture.Log.LoggingFailed.Task);
        Assert.False(worker.ExecuteTask!.IsCompleted);
        Assert.Equal(0, fixture.Disposals);
        quiescence.SetResult();
        await Finish(worker.ExecuteTask);
        Assert.Equal(1, fixture.Calls);
        Assert.Equal(1, fixture.Disposals);
        Assert.Contains(fixture.Log.Entries, entry => entry.Error?.Message == "logger failure");
    }

    [Theory]
    [InlineData(InboxCoordinationStatus.LeaseLost)]
    [InlineData(InboxCoordinationStatus.Completed)]
    [InlineData(InboxCoordinationStatus.RetryScheduled)]
    public async Task RepeatedFastResultsAreThrottledAfterConfiguredLimit(InboxCoordinationStatus status)
    {
        var fixture = new Fixture();
        using var worker = fixture.Create(new() { MaximumImmediateResults = 3 });
        await worker.StartAsync(default);
        for (var burst = 0; burst < 2; burst++)
        {
            for (var index = 0; index < 3; index++) (await fixture.NextCall()).Return(status);
            var delay = await fixture.Time.NextTimerAsync(TimeSpan.FromMilliseconds(100));
            Assert.Equal((burst + 1) * 3, fixture.Calls);
            delay.Fire();
        }
        await Stop(worker, await fixture.NextCall());
    }

    [Fact]
    public async Task SynchronouslyCompletedAttemptsCannotSpinInsideStartAsync()
    {
        var fixture = new Fixture();
        var calls = 0;
        using var worker = new InboxBackgroundService(() => new((_, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(new InboxCoordinationResult(InboxCoordinationStatus.LeaseLost,
                InboxCoordinationStage.Acquisition));
        }, fixture.CreateDependencies()), new() { MaximumImmediateResults = 3 }, fixture.Time, fixture.Log);
        await worker.StartAsync(default);
        await fixture.Time.NextTimerAsync(TimeSpan.FromMilliseconds(100));
        Assert.Equal(3, Volatile.Read(ref calls));
        await Finish(worker.StopAsync(default));
        Assert.Equal(3, fixture.Disposals);
    }

    [Fact]
    public async Task SlowResultsAndNormalWaitsResetTheImmediateResultStreak()
    {
        var fixture = new Fixture();
        using var worker = fixture.Create(new() { MaximumImmediateResults = 3 });
        await worker.StartAsync(default);
        (await fixture.NextCall()).Return(InboxCoordinationStatus.LeaseLost);
        (await fixture.NextCall()).Return(InboxCoordinationStatus.Completed);
        var slow = await fixture.NextCall();
        fixture.Time.Advance(TimeSpan.FromSeconds(1));
        slow.Return(InboxCoordinationStatus.LeaseLost);
        (await fixture.NextCall()).Return(InboxCoordinationStatus.RetryScheduled);
        (await fixture.NextCall()).Return(InboxCoordinationStatus.NoEligibleWork);
        (await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(5))).Fire();
        (await fixture.NextCall()).Return(InboxCoordinationStatus.LeaseLost);
        (await fixture.NextCall()).Return(InboxCoordinationStatus.Completed);
        var pending = await fixture.NextCall();
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
        await Stop(worker, pending);
    }

    [Fact]
    public async Task OwnersAreValidStableWithinExecutionAndDifferentAfterRestart()
    {
        var fixture = new Fixture();
        string previous;
        using (var worker = fixture.Create())
        {
            await worker.StartAsync(default);
            var first = await fixture.NextCall();
            InboxProcessingLimits.ValidateOwner(first.Owner);
            previous = first.Owner;
            first.Return(InboxCoordinationStatus.Completed);
            var second = await fixture.NextCall();
            Assert.Equal(previous, second.Owner);
            await Stop(worker, second);
        }
        using var restarted = fixture.Create();
        await restarted.StartAsync(default);
        var next = await fixture.NextCall();
        InboxProcessingLimits.ValidateOwner(next.Owner);
        Assert.NotEqual(previous, next.Owner);
        await Stop(restarted, next);
    }

    [Fact]
    public async Task ConfigurationControlsEachDelay()
    {
        var options = new InboxWorkerOptions
        {
            PollingDelay = TimeSpan.FromSeconds(11), InitialStoreFailureDelay = TimeSpan.FromSeconds(3),
            MaximumStoreFailureDelay = TimeSpan.FromSeconds(5), ProcessingFailureDelay = TimeSpan.FromSeconds(12),
            UnknownOutcomeDelay = TimeSpan.FromSeconds(13)
        };
        var fixture = new Fixture();
        using var worker = fixture.Create(options);
        await worker.StartAsync(default);
        foreach (var (status, seconds) in new[]
        {
            (InboxCoordinationStatus.StoreFailed, 3), (InboxCoordinationStatus.StoreFailed, 5),
            (InboxCoordinationStatus.NoEligibleWork, 11), (InboxCoordinationStatus.ProcessingFailed, 12),
            (InboxCoordinationStatus.OutcomeUnknown, 13)
        })
        {
            (await fixture.NextCall()).Return(status);
            (await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(seconds))).Fire();
        }
        await Stop(worker, await fixture.NextCall());
    }

    [Fact]
    public async Task ConcurrentWorkersHaveIndependentBackoffAndDistinctOwners()
    {
        var first = new Fixture();
        var second = new Fixture();
        using var firstWorker = first.Create();
        using var secondWorker = second.Create();
        await firstWorker.StartAsync(default);
        (await first.NextCall()).Return(InboxCoordinationStatus.StoreFailed);
        (await first.Time.NextTimerAsync(TimeSpan.FromSeconds(2))).Fire();
        var firstCall = await first.NextCall();
        firstCall.Return(InboxCoordinationStatus.StoreFailed);
        await first.Time.NextTimerAsync(TimeSpan.FromSeconds(4));

        await secondWorker.StartAsync(default);
        var secondCall = await second.NextCall();
        Assert.NotEqual(firstCall.Owner, secondCall.Owner);
        secondCall.Return(InboxCoordinationStatus.StoreFailed);
        await second.Time.NextTimerAsync(TimeSpan.FromSeconds(2));
        await Finish(firstWorker.StopAsync(default));
        await Finish(secondWorker.StopAsync(default));
    }

    [Fact]
    public async Task CancellationBeforeStartDoesNotCreateAnAttempt()
    {
        var fixture = new Fixture();
        using var worker = fixture.Create();
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();
        await worker.StartAsync(stopping.Token);
        await Finish(worker.ExecuteTask!);
        Assert.Equal(0, fixture.CreatedExecutions);
        Assert.Equal(0, fixture.Calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnexpectedRunOnceExceptionFaultsExecutionWithoutRetry(bool synchronous)
    {
        var fixture = new Fixture();
        var error = new IOException("unexpected delegate failure");
        var entered = Signal();
        var failure = new TaskCompletionSource<InboxCoordinationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        using var worker = new InboxBackgroundService(() =>
        {
            attempts++;
            return new((_, _) =>
            {
                entered.SetResult();
                if (synchronous) throw error;
                return failure.Task;
            }, fixture.CreateDependencies());
        }, new(), fixture.Time, fixture.Log);
        // BackgroundService.StartAsync propagates a fault if execution already completed.
        var start = worker.StartAsync(default);
        await Finish(entered.Task);
        if (!synchronous) failure.SetException(error);
        Assert.Same(error, await Assert.ThrowsAsync<IOException>(() => Finish(worker.ExecuteTask!)));
        if (start.IsFaulted) Assert.Same(error, await Assert.ThrowsAsync<IOException>(() => start));
        else await start;
        Assert.Equal(1, attempts);
        Assert.Equal(1, fixture.Disposals);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
    }

    [Fact]
    public void DisposeBeforeStartDoesNotCreateResources()
    {
        var fixture = new Fixture();
        var worker = fixture.Create();
        worker.Dispose();
        Assert.Equal(0, fixture.CreatedExecutions);
        Assert.Equal(0, fixture.Disposals);
    }

    [Fact]
    public void InvalidConfigurationIsRejectedBeforeCreatingResources()
    {
        var fixture = new Fixture();
        foreach (var delay in new[] { TimeSpan.Zero, TimeSpan.FromTicks(1), TimeSpan.FromMilliseconds(-1), TimeSpan.MaxValue })
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => fixture.Create(new() { PollingDelay = delay }));
            Assert.Throws<ArgumentOutOfRangeException>(() => fixture.Create(new() { InitialStoreFailureDelay = delay }));
            Assert.Throws<ArgumentOutOfRangeException>(() => fixture.Create(new() { MaximumStoreFailureDelay = delay }));
            Assert.Throws<ArgumentOutOfRangeException>(() => fixture.Create(new() { ProcessingFailureDelay = delay }));
            Assert.Throws<ArgumentOutOfRangeException>(() => fixture.Create(new() { UnknownOutcomeDelay = delay }));
            Assert.Throws<ArgumentOutOfRangeException>(() => fixture.Create(new() { ImmediateResultDelay = delay }));
        }
        Assert.Throws<ArgumentOutOfRangeException>(() => fixture.Create(new() { MaximumImmediateResults = 1 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => fixture.Create(new() { MaximumStoreFailureDelay = TimeSpan.FromSeconds(1) }));
        Assert.Equal(0, fixture.CreatedExecutions);
    }

    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Wall time is only a deadlock guard. Test ordering uses calls, timers and task completion.
    private static Task Finish(Task task) => task.WaitAsync(TimeSpan.FromSeconds(10));

    private static async Task Stop(InboxBackgroundService worker, Call pending)
    {
        var stop = worker.StopAsync(default);
        pending.Return(InboxCoordinationStatus.Cancelled);
        await Finish(stop);
    }

    private sealed record Call(int Number, string Owner, CancellationToken Token)
    {
        public TaskCompletionSource<InboxCoordinationResult> Result { get; } = new();
        public void Return(InboxCoordinationStatus status) => Result.SetResult(new(status, InboxCoordinationStage.Acquisition));
    }

    private sealed class Fixture
    {
        private readonly Channel<Call> _calls = Channel.CreateUnbounded<Call>();
        private int _callCount;
        public int Calls => Volatile.Read(ref _callCount);
        public int CreatedExecutions { get; private set; }
        public ControlledTimeProvider Time { get; } = new();
        private readonly ConcurrentQueue<Dependencies> _dependencies = new();
        public int Disposals => _dependencies.Sum(dependencies => dependencies.Disposals);
        public Dependencies CreateDependencies()
        {
            var dependencies = new Dependencies();
            _dependencies.Enqueue(dependencies);
            return dependencies;
        }
        public TestLogger Log { get; } = new();
        public TaskCompletionSource ResultObserved { get; } = Signal();
        public Task<Call> NextCall() => _calls.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        public InboxBackgroundService Create(InboxWorkerOptions? options = null) => new(() =>
        {
            CreatedExecutions++;
            return new(RunOnce, CreateDependencies());
        }, options ?? new(), Time, Log);

        private async Task<InboxCoordinationResult> RunOnce(string owner, CancellationToken token)
        {
            // Every status policy must release the previous attempt before acquiring again.
            Assert.Equal(CreatedExecutions - 1, Disposals);
            var call = new Call(Interlocked.Increment(ref _callCount), owner, token);
            _calls.Writer.TryWrite(call);
            var result = await call.Result.Task;
            ResultObserved.TrySetResult();
            return result;
        }
    }

    private sealed class Dependencies : IAsyncDisposable
    {
        private int _disposals;
        public int Disposals => Volatile.Read(ref _disposals);
        public ValueTask DisposeAsync()
        {
            Assert.Equal(1, Interlocked.Increment(ref _disposals));
            return ValueTask.CompletedTask;
        }
    }

    private sealed class PendingAcquisitionStore : IInboxProcessingStore, IAsyncDisposable
    {
        public TaskCompletionSource Entered { get; } = Signal();
        public TaskCompletionSource<InboxAcquisitionResult> Acquisition { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool IsDisposed { get; private set; }
        public int Calls { get; private set; }

        public async Task<InboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan leaseDuration, CancellationToken cancellationToken)
        {
            Calls++;
            Entered.SetResult();
            try { return await Acquisition.Task; }
            finally { Assert.False(IsDisposed); }
        }

        public Task<InboxMutationResult> RenewAsync(InboxLease lease, TimeSpan leaseDuration, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Unexpected renewal.");
        public Task<InboxMutationResult> CompleteAsync(InboxLease lease, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Unexpected completion.");
        public Task<InboxMutationResult> ScheduleRetryAsync(InboxLease lease, TimeSpan retryDelay, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Unexpected retry.");
        public ValueTask DisposeAsync()
        {
            Assert.False(IsDisposed);
            IsDisposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class UnusedProcessor : IInboxNotificationProcessor
    {
        public Task<InboxNotificationProcessingResult> ProcessAsync(InboxNotification notification, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Unconfirmed acquisition must not process.");
    }

    private sealed class TestLogger : ILogger<InboxBackgroundService>
    {
        public ConcurrentQueue<(string Message, Exception? Error)> Entries { get; } = new();
        public TaskCompletionSource CoordinatorTimedOut { get; } = Signal();
        public TaskCompletionSource LoggingFailed { get; } = Signal();
        public bool FailOnCoordinatorTimeout { get; set; }
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            if (FailOnCoordinatorTimeout && message.Contains("coordinator shutdown"))
            {
                LoggingFailed.TrySetResult();
                throw new IOException("logger failure");
            }
            Entries.Enqueue((message, exception));
            if (message.Contains("coordinator shutdown")) CoordinatorTimedOut.TrySetResult();
        }
    }
}
