using Ordering.Application.Inbox;

namespace Ordering.Application.Tests;

public sealed class InboxProcessingCoordinatorTests
{
    private static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan RenewalInterval = TimeSpan.FromSeconds(7);
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan ShutdownTimeout = TimeSpan.FromSeconds(3);
    private static readonly InboxProcessingOptions Options = new(LeaseDuration, RenewalInterval, OperationTimeout, ShutdownTimeout);

    [Fact]
    public async Task ConfirmedAcquisitionProcessesExactlyOnceAndCompletesWithSameLease()
    {
        var fixture = new Fixture();
        var result = await Finished(fixture.Run());

        Assert.Equal(InboxCoordinationStatus.Completed, result.Status);
        Assert.Equal(InboxCoordinationStage.Completion, result.Stage);
        Assert.Same(fixture.Store.Work.Lease, result.Lease);
        Assert.Same(fixture.Store.Work.Content, fixture.Processor.Notification);
        Assert.Equal(new[] { "acquire", "complete" }, fixture.Store.Calls);
        Assert.Equal(1, fixture.Processor.Calls);
        Assert.False(result.ShutdownTimedOut);
        Assert.True(result.Quiescence.IsCompletedSuccessfully);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
    }

    [Theory]
    [InlineData(InboxAcquisitionStatus.NoEligibleWork, InboxCoordinationStatus.NoEligibleWork)]
    [InlineData(InboxAcquisitionStatus.OutcomeUnknown, InboxCoordinationStatus.OutcomeUnknown)]
    public async Task UnconfirmedAcquisitionNeverProcessesEvenIfStoreExposesContent(
        InboxAcquisitionStatus acquisition, InboxCoordinationStatus expected)
    {
        var fixture = new Fixture();
        fixture.Store.Acquire = _ => Task.FromResult(new InboxAcquisitionResult(acquisition, fixture.Store.Work));
        var result = await Finished(fixture.Run());

        Assert.Equal(expected, result.Status);
        Assert.Equal(InboxCoordinationStage.Acquisition, result.Stage);
        Assert.Null(result.Lease);
        Assert.Equal(0, fixture.Processor.Calls);
        Assert.Equal(new[] { "acquire" }, fixture.Store.Calls);
    }

    [Fact]
    public async Task DoesNotProcessBeforeAcquisitionConfirmation()
    {
        var fixture = new Fixture();
        var acquisition = Signal<InboxAcquisitionResult>();
        fixture.Store.Acquire = _ => acquisition.Task;
        var run = fixture.Run();
        await fixture.Time.NextTimerAsync(OperationTimeout);
        Assert.Equal(0, fixture.Processor.Calls);
        Assert.False(run.IsCompleted);

        acquisition.SetResult(new(InboxAcquisitionStatus.Acquired, fixture.Store.Work));
        Assert.Equal(InboxCoordinationStatus.Completed, (await Finished(run)).Status);
    }

    [Fact]
    public async Task MissingConfirmedContentIsAStoreFailure()
    {
        var fixture = new Fixture();
        fixture.Store.Acquire = _ => Task.FromResult(new InboxAcquisitionResult(InboxAcquisitionStatus.Acquired, null));
        var result = await Finished(fixture.Run());
        Assert.Equal(InboxCoordinationStatus.StoreFailed, result.Status);
        Assert.IsType<InvalidOperationException>(result.Error);
        Assert.Equal(0, fixture.Processor.Calls);
    }

    [Fact]
    public async Task PeriodicRenewalIsSequentialAndUsesTheOriginalLease()
    {
        var fixture = new Fixture();
        var processing = Signal<InboxNotificationProcessingResult>();
        var firstRenewal = Signal<InboxMutationResult>();
        fixture.Processor.Process = _ => processing.Task;
        fixture.Store.Renew = _ => firstRenewal.Task;
        var run = fixture.Run();

        (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        await fixture.Store.RenewEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(1, fixture.Store.RenewCalls);
        Assert.Equal(0, fixture.Store.TerminalCalls);
        fixture.Store.Renew = _ => Task.FromResult(Success());
        firstRenewal.SetResult(Success());
        (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        // The next interval is registered only after the second confirmed renewal.
        await fixture.Time.NextTimerAsync(RenewalInterval);
        Assert.Equal(2, fixture.Store.RenewCalls);

        processing.SetResult(InboxNotificationProcessingResult.Succeeded);
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.Completed, result.Status);
        Assert.Equal(new[] { "acquire", "renew", "renew", "complete" }, fixture.Store.Calls);
    }

    [Theory]
    [InlineData(InboxMutationStatus.LeaseLost, InboxCoordinationStatus.LeaseLost)]
    [InlineData(InboxMutationStatus.OutcomeUnknown, InboxCoordinationStatus.OutcomeUnknown)]
    public async Task UnconfirmedRenewalCancelsProcessingAndNeverFinalizes(
        InboxMutationStatus mutation, InboxCoordinationStatus expected)
    {
        var fixture = new Fixture();
        fixture.Processor.Process = async token =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
            return InboxNotificationProcessingResult.Succeeded;
        };
        fixture.Store.Renew = _ => Task.FromResult(new InboxMutationResult(mutation));
        var run = fixture.Run();
        (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        var result = await Finished(run);

        Assert.Equal(expected, result.Status);
        Assert.Equal(InboxCoordinationStage.Renewal, result.Stage);
        Assert.True(fixture.Processor.Token.IsCancellationRequested);
        Assert.Equal(0, fixture.Store.TerminalCalls);
        Assert.Equal(1, fixture.Store.RenewCalls);
        Assert.False(result.ShutdownTimedOut);
    }

    [Theory]
    [InlineData(InboxMutationStatus.LeaseLost)]
    [InlineData(InboxMutationStatus.OutcomeUnknown)]
    public async Task RenewalRejectionBeforeProcessingReturnsSuccessPreventsCompletion(InboxMutationStatus status)
    {
        var fixture = new Fixture();
        var processing = Signal<InboxNotificationProcessingResult>();
        fixture.Processor.Process = _ => processing.Task;
        fixture.Store.Renew = _ => Task.FromResult(new InboxMutationResult(status));
        var run = fixture.Run();
        (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        await fixture.Time.NextTimerAsync(ShutdownTimeout);
        Assert.True(fixture.Processor.Token.IsCancellationRequested);
        processing.SetResult(InboxNotificationProcessingResult.Succeeded);
        var result = await Finished(run);
        Assert.Equal(status == InboxMutationStatus.LeaseLost ? InboxCoordinationStatus.LeaseLost : InboxCoordinationStatus.OutcomeUnknown, result.Status);
        Assert.Equal(0, fixture.Store.TerminalCalls);
    }

    [Theory]
    [InlineData(InboxMutationStatus.Succeeded, InboxCoordinationStatus.Completed)]
    [InlineData(InboxMutationStatus.LeaseLost, InboxCoordinationStatus.LeaseLost)]
    [InlineData(InboxMutationStatus.OutcomeUnknown, InboxCoordinationStatus.OutcomeUnknown)]
    public async Task CompletionResultIsNotRetried(InboxMutationStatus mutation, InboxCoordinationStatus expected)
    {
        var fixture = new Fixture();
        fixture.Store.Complete = _ => Task.FromResult(new InboxMutationResult(mutation));
        var result = await Finished(fixture.Run());
        Assert.Equal(expected, result.Status);
        Assert.Equal(InboxCoordinationStage.Completion, result.Stage);
        Assert.Equal(1, fixture.Store.TerminalCalls);
        Assert.Equal(1, fixture.Processor.Calls);
    }

    [Theory]
    [InlineData(InboxMutationStatus.Succeeded, InboxCoordinationStatus.RetryScheduled)]
    [InlineData(InboxMutationStatus.LeaseLost, InboxCoordinationStatus.LeaseLost)]
    [InlineData(InboxMutationStatus.OutcomeUnknown, InboxCoordinationStatus.OutcomeUnknown)]
    public async Task OnlyExplicitSafeRetrySchedulesWork(InboxMutationStatus mutation, InboxCoordinationStatus expected)
    {
        var fixture = new Fixture();
        fixture.Processor.Process = _ => Task.FromResult(InboxNotificationProcessingResult.RetryAfter(TimeSpan.FromMinutes(2)));
        fixture.Store.Retry = _ => Task.FromResult(new InboxMutationResult(mutation));
        var result = await Finished(fixture.Run());
        Assert.Equal(expected, result.Status);
        Assert.Equal(InboxCoordinationStage.Retry, result.Stage);
        Assert.Equal(TimeSpan.FromMinutes(2), fixture.Store.RetryDelay);
        Assert.Equal(new[] { "acquire", "retry" }, fixture.Store.Calls);
        Assert.Equal(1, fixture.Processor.Calls);
    }

    [Fact]
    public async Task UncertainBusinessEffectsAreNotAutomaticallyRetried()
    {
        var fixture = new Fixture();
        fixture.Processor.Process = _ => Task.FromResult(InboxNotificationProcessingResult.OutcomeUnknown);
        var result = await Finished(fixture.Run());
        Assert.Equal(InboxCoordinationStatus.OutcomeUnknown, result.Status);
        Assert.Equal(InboxCoordinationStage.Processing, result.Stage);
        Assert.Equal(new[] { "acquire" }, fixture.Store.Calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ProcessorExceptionDoesNotImplySafeRetry(bool synchronous)
    {
        var fixture = new Fixture();
        var error = new InvalidOperationException("External effect may have happened.");
        fixture.Processor.Process = _ => synchronous ? throw error : Task.FromException<InboxNotificationProcessingResult>(error);
        var result = await Finished(fixture.Run());
        Assert.Equal(InboxCoordinationStatus.ProcessingFailed, result.Status);
        Assert.Same(error, result.Error);
        Assert.Equal(0, fixture.Store.TerminalCalls);
    }

    [Theory]
    [InlineData(InboxCoordinationStage.Acquisition)]
    [InlineData(InboxCoordinationStage.Renewal)]
    [InlineData(InboxCoordinationStage.Completion)]
    [InlineData(InboxCoordinationStage.Retry)]
    public async Task StoreExceptionsAreReportedWithoutRepeatingAnyCall(InboxCoordinationStage stage)
    {
        var fixture = new Fixture();
        var error = new InboxStorageUnavailableException();
        if (stage == InboxCoordinationStage.Acquisition) fixture.Store.Acquire = _ => throw error;
        if (stage == InboxCoordinationStage.Completion) fixture.Store.Complete = _ => throw error;
        if (stage == InboxCoordinationStage.Retry)
        {
            fixture.Processor.Process = _ => Task.FromResult(InboxNotificationProcessingResult.RetryAfter(TimeSpan.Zero));
            fixture.Store.Retry = _ => throw error;
        }
        if (stage == InboxCoordinationStage.Renewal)
        {
            fixture.Processor.Process = async token =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return InboxNotificationProcessingResult.Succeeded;
            };
            fixture.Store.Renew = _ => Task.FromException<InboxMutationResult>(error);
        }
        var run = fixture.Run();
        if (stage == InboxCoordinationStage.Renewal) (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.StoreFailed, result.Status);
        Assert.Equal(stage, result.Stage);
        Assert.Same(error, result.Error);
        Assert.Equal(1, fixture.Store.Calls.Count(call => call == stage switch
        {
            InboxCoordinationStage.Acquisition => "acquire",
            InboxCoordinationStage.Renewal => "renew",
            InboxCoordinationStage.Completion => "complete",
            _ => "retry"
        }));
    }

    [Theory]
    [InlineData(InboxMutationStatus.LeaseLost, false)]
    [InlineData(InboxMutationStatus.OutcomeUnknown, false)]
    [InlineData(InboxMutationStatus.LeaseLost, true)]
    [InlineData(InboxMutationStatus.OutcomeUnknown, true)]
    public async Task ConfirmedCompletionWinsOverLateRenewalInEitherResponseOrder(InboxMutationStatus renewalStatus, bool completionFirst)
    {
        var fixture = new Fixture();
        var processing = Signal<InboxNotificationProcessingResult>();
        var renewal = Signal<InboxMutationResult>();
        var completion = Signal<InboxMutationResult>();
        fixture.Processor.Process = _ => processing.Task;
        fixture.Store.Renew = _ => renewal.Task;
        fixture.Store.Complete = _ => completion.Task;
        var run = fixture.Run();
        (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        await fixture.Store.RenewEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        processing.SetResult(InboxNotificationProcessingResult.Succeeded);
        await fixture.Store.TerminalEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        if (completionFirst)
        {
            completion.SetResult(Success());
            await fixture.Time.NextTimerAsync(ShutdownTimeout);
            renewal.SetResult(new(renewalStatus));
        }
        else
        {
            renewal.SetResult(new(renewalStatus));
            // Completion is deliberately held open: renewal failure must not end finalization.
            Assert.False(run.IsCompleted);
            completion.SetResult(Success());
        }
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.Completed, result.Status);
        Assert.False(result.ShutdownTimedOut);
        Assert.Equal(new[] { "acquire", "renew", "complete" }, fixture.Store.Calls);
    }

    [Fact]
    public async Task ConfirmedRetryAlsoWinsOverLateRenewalRejection()
    {
        var fixture = new Fixture();
        var processing = Signal<InboxNotificationProcessingResult>();
        var renewal = Signal<InboxMutationResult>();
        fixture.Processor.Process = _ => processing.Task;
        fixture.Store.Renew = _ => renewal.Task;
        var run = fixture.Run();
        (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        await fixture.Store.RenewEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        processing.SetResult(InboxNotificationProcessingResult.RetryAfter(TimeSpan.Zero));
        await fixture.Time.NextTimerAsync(ShutdownTimeout);
        renewal.SetResult(new(InboxMutationStatus.LeaseLost));
        Assert.Equal(InboxCoordinationStatus.RetryScheduled, (await Finished(run)).Status);
    }

    [Theory]
    [InlineData(InboxMutationStatus.Succeeded)]
    [InlineData(InboxMutationStatus.LeaseLost)]
    [InlineData(InboxMutationStatus.OutcomeUnknown)]
    public async Task UncertainCompletionRemainsUncertainRegardlessOfLateRenewal(InboxMutationStatus renewalStatus)
    {
        var fixture = new Fixture();
        var processing = Signal<InboxNotificationProcessingResult>();
        var renewal = Signal<InboxMutationResult>();
        var completion = Signal<InboxMutationResult>();
        fixture.Processor.Process = _ => processing.Task;
        fixture.Store.Renew = _ => renewal.Task;
        fixture.Store.Complete = _ => completion.Task;
        var run = fixture.Run();
        (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        await fixture.Store.RenewEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        processing.SetResult(InboxNotificationProcessingResult.Succeeded);
        await fixture.Store.TerminalEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        completion.SetResult(new(InboxMutationStatus.OutcomeUnknown));
        await fixture.Time.NextTimerAsync(ShutdownTimeout);
        renewal.SetResult(new(renewalStatus));
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.OutcomeUnknown, result.Status);
        Assert.Equal(InboxCoordinationStage.Completion, result.Stage);
        Assert.Equal(1, fixture.Store.TerminalCalls);
        Assert.Equal(1, fixture.Processor.Calls);
    }

    [Fact]
    public async Task ConfirmedCompletionSurvivesShutdownDeadlineWithAnUnresponsiveRenewal()
    {
        var fixture = new Fixture();
        var processing = Signal<InboxNotificationProcessingResult>();
        var renewal = Signal<InboxMutationResult>();
        fixture.Processor.Process = _ => processing.Task;
        fixture.Store.Renew = _ => renewal.Task;
        var run = fixture.Run();
        (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        await fixture.Store.RenewEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        processing.SetResult(InboxNotificationProcessingResult.Succeeded);
        (await fixture.Time.NextTimerAsync(ShutdownTimeout)).Fire();
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.Completed, result.Status);
        Assert.True(result.ShutdownTimedOut);
        renewal.SetResult(new(InboxMutationStatus.LeaseLost));
        await result.Quiescence.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(InboxCoordinationStatus.Completed, result.Status);
        Assert.Equal(1, fixture.Store.TerminalCalls);
    }

    [Fact]
    public async Task RenewalIsNotStartedAfterFinalizationBegins()
    {
        var fixture = new Fixture();
        var completion = Signal<InboxMutationResult>();
        fixture.Store.Complete = _ => completion.Task;
        var run = fixture.Run();
        await fixture.Store.TerminalEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        completion.SetResult(Success());
        Assert.Equal(InboxCoordinationStatus.Completed, (await Finished(run)).Status);
        Assert.Equal(0, fixture.Store.RenewCalls);
    }

    [Fact]
    public async Task PreCancelledExecutionDoesNotAcquire()
    {
        var fixture = new Fixture();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var result = await Finished(fixture.Run(cancellation.Token));
        Assert.Equal(InboxCoordinationStatus.Cancelled, result.Status);
        Assert.Empty(fixture.Store.Calls);
    }

    [Fact]
    public async Task CooperativeShutdownCancelsAndWaitsWithoutReleasingTheLease()
    {
        var fixture = new Fixture();
        var cancelled = Signal<bool>();
        var processing = Signal<InboxNotificationProcessingResult>();
        fixture.Processor.Process = token =>
        {
            token.Register(() => cancelled.TrySetResult(true));
            return processing.Task;
        };
        using var cancellation = new CancellationTokenSource();
        var run = fixture.Run(cancellation.Token);
        cancellation.Cancel();
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(run.IsCompleted);
        Assert.Equal(0, fixture.Store.TerminalCalls);
        processing.SetResult(InboxNotificationProcessingResult.RetryAfter(TimeSpan.Zero));
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.Cancelled, result.Status);
        Assert.False(result.ShutdownTimedOut);
        Assert.Equal(0, fixture.Store.TerminalCalls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ShutdownKeepsRenewingSequentiallyWithAnIndependentStoreToken(bool renewalAlreadyInFlight)
    {
        var fixture = new Fixture(TimeSpan.FromSeconds(25));
        var processing = Signal<InboxNotificationProcessingResult>();
        var renewal = Signal<InboxMutationResult>();
        var processorCancelled = Signal<bool>();
        CancellationToken storeToken = default;
        fixture.Processor.Process = token =>
        {
            token.Register(() => processorCancelled.TrySetResult(true));
            return processing.Task;
        };
        fixture.Store.Renew = token => { storeToken = token; return renewal.Task; };
        using var cancellation = new CancellationTokenSource();
        var run = fixture.Run(cancellation.Token);
        // CancelAsync may still be disposing the completed acquisition's timer on another task.
        await (await fixture.Time.NextRegisteredTimerAsync(OperationTimeout)).Disposed.WaitAsync(TimeSpan.FromSeconds(10));
        var firstInterval = await fixture.Time.NextTimerAsync(RenewalInterval);
        ControlledTimeProvider.ControlledTimer? operationDeadline = null;
        if (renewalAlreadyInFlight)
        {
            firstInterval.Fire();
            await fixture.Store.RenewEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            operationDeadline = await fixture.Time.NextTimerAsync(OperationTimeout);
        }
        cancellation.Cancel();
        await processorCancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var shutdown = await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(25));
        if (!renewalAlreadyInFlight)
        {
            Assert.True(firstInterval.Active); // Shutdown does not reset the existing interval.
            firstInterval.Fire();
            await fixture.Store.RenewEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            operationDeadline = await fixture.Time.NextTimerAsync(OperationTimeout);
        }
        Assert.True(operationDeadline!.Active); // An in-flight store budget is retained too.
        Assert.True(fixture.Processor.Token.IsCancellationRequested);
        Assert.False(storeToken.IsCancellationRequested);
        Assert.False(run.IsCompleted);
        Assert.Equal(1, fixture.Store.RenewCalls);

        fixture.Store.Renew = token =>
        {
            Assert.False(token.IsCancellationRequested);
            return Task.FromResult(Success());
        };
        renewal.SetResult(Success());
        var secondInterval = await fixture.Time.NextTimerAsync(RenewalInterval);
        await operationDeadline.Disposed.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(operationDeadline.Active);
        Assert.Equal(1, fixture.Store.RenewCalls); // No second call until the next interval fires.
        secondInterval.Fire();
        var thirdInterval = await fixture.Time.NextTimerAsync(RenewalInterval);
        Assert.Equal(2, fixture.Store.RenewCalls);
        processing.SetResult(InboxNotificationProcessingResult.Succeeded);
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.Cancelled, result.Status);
        Assert.False(result.ShutdownTimedOut);
        Assert.True(result.Quiescence.IsCompletedSuccessfully);
        Assert.False(shutdown.Active);
        Assert.False(thirdInterval.Active);
        Assert.Equal(new[] { "acquire", "renew", "renew" }, fixture.Store.Calls);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ProcessorCancellationCallbackCannotBlockLeaseMaintenanceOrEscapeObservation(bool blocks)
    {
        var fixture = new Fixture(TimeSpan.FromSeconds(25));
        var processing = Signal<InboxNotificationProcessingResult>();
        var entered = Signal<bool>();
        using var release = new ManualResetEventSlim();
        fixture.Processor.Process = token =>
        {
            token.Register(() =>
            {
                entered.TrySetResult(true);
                if (blocks) release.Wait();
                else throw new InvalidOperationException("cancellation callback failed");
            });
            return processing.Task;
        };
        fixture.Store.Renew = token =>
        {
            Assert.False(token.IsCancellationRequested);
            return Task.FromResult(Success());
        };
        using var cancellation = new CancellationTokenSource();
        try
        {
            var run = fixture.Run(cancellation.Token);
            cancellation.Cancel();
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var shutdown = await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(25));
            (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
            var nextInterval = await fixture.Time.NextTimerAsync(RenewalInterval);
            Assert.Equal(1, fixture.Store.RenewCalls);
            shutdown.Fire();
            var result = await Finished(run);
            Assert.True(result.ShutdownTimedOut);
            Assert.Equal(InboxCoordinationStatus.Cancelled, result.Status);
            Assert.False(nextInterval.Active);
            release.Set();
            processing.SetException(new OperationCanceledException(fixture.Processor.Token));
            await result.Quiescence.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(result.Quiescence.IsCompletedSuccessfully);
            Assert.Equal(new[] { "acquire", "renew" }, fixture.Store.Calls);
            Assert.Equal(0, fixture.Time.ActiveTimerCount);
        }
        finally { release.Set(); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ProcessorExitDuringShutdownStopsRenewalsAndOnlyObservesLateStoreOutcome(bool lateFault)
    {
        var fixture = new Fixture(TimeSpan.FromSeconds(25));
        var processing = Signal<InboxNotificationProcessingResult>();
        var renewal = Signal<InboxMutationResult>();
        var storeCancelled = Signal<bool>();
        fixture.Processor.Process = _ => processing.Task;
        fixture.Store.Renew = token =>
        {
            token.Register(() => storeCancelled.TrySetResult(true));
            return renewal.Task;
        };
        using var cancellation = new CancellationTokenSource();
        var run = fixture.Run(cancellation.Token);
        cancellation.Cancel();
        var shutdown = await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(25));
        (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        await fixture.Store.RenewEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        processing.SetResult(InboxNotificationProcessingResult.RetryAfter(TimeSpan.Zero));
        await storeCancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        shutdown.Fire(); // Final observation must reuse the original shutdown budget.
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.Cancelled, result.Status);
        Assert.True(result.ShutdownTimedOut);
        Assert.False(result.Quiescence.IsCompleted);
        if (lateFault) renewal.SetException(new IOException("late renewal fault"));
        else renewal.SetResult(Success());
        await result.Quiescence.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { "acquire", "renew" }, fixture.Store.Calls);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ShutdownDeadlineStopsRenewalWithOrWithoutAnInFlightCall(bool inFlight)
    {
        var fixture = new Fixture();
        var processing = Signal<InboxNotificationProcessingResult>();
        var renewal = Signal<InboxMutationResult>();
        CancellationToken storeToken = default;
        fixture.Processor.Process = _ => processing.Task;
        fixture.Store.Renew = token => { storeToken = token; return renewal.Task; };
        using var cancellation = new CancellationTokenSource();
        var run = fixture.Run(cancellation.Token);
        var interval = await fixture.Time.NextTimerAsync(RenewalInterval);
        if (inFlight)
        {
            interval.Fire();
            await fixture.Store.RenewEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }
        cancellation.Cancel();
        (await fixture.Time.NextTimerAsync(ShutdownTimeout)).Fire();
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.Cancelled, result.Status);
        Assert.True(result.ShutdownTimedOut);
        Assert.True(fixture.Processor.Token.IsCancellationRequested);
        if (inFlight) Assert.True(storeToken.IsCancellationRequested);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
        renewal.SetResult(Success());
        processing.SetResult(InboxNotificationProcessingResult.Succeeded);
        await result.Quiescence.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(inFlight ? new[] { "acquire", "renew" } : new[] { "acquire" }, fixture.Store.Calls);
    }

    [Theory]
    [InlineData(0, InboxCoordinationStatus.LeaseLost)]
    [InlineData(1, InboxCoordinationStatus.OutcomeUnknown)]
    [InlineData(2, InboxCoordinationStatus.StoreFailed)]
    [InlineData(3, InboxCoordinationStatus.OutcomeUnknown)]
    public async Task ShutdownAbandonsRenewalOnLostOrUncertainAuthority(int failure, InboxCoordinationStatus expected)
    {
        var fixture = new Fixture(TimeSpan.FromSeconds(25));
        var processing = Signal<InboxNotificationProcessingResult>();
        var renewal = Signal<InboxMutationResult>();
        var storeCancelled = Signal<bool>();
        fixture.Processor.Process = _ => processing.Task;
        fixture.Store.Renew = token =>
        {
            token.Register(() => storeCancelled.TrySetResult(true));
            return renewal.Task;
        };
        using var cancellation = new CancellationTokenSource();
        var run = fixture.Run(cancellation.Token);
        await (await fixture.Time.NextRegisteredTimerAsync(OperationTimeout)).Disposed.WaitAsync(TimeSpan.FromSeconds(10));
        cancellation.Cancel();
        var shutdown = await fixture.Time.NextTimerAsync(TimeSpan.FromSeconds(25));
        (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        await fixture.Store.RenewEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        if (failure == 3) (await fixture.Time.NextTimerAsync(OperationTimeout)).Fire();
        else if (failure == 2) renewal.SetException(new IOException("renewal failed"));
        else renewal.SetResult(new(failure == 0 ? InboxMutationStatus.LeaseLost : InboxMutationStatus.OutcomeUnknown));
        await storeCancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(run.IsCompleted);
        shutdown.Fire();
        var result = await Finished(run);
        Assert.Equal(expected, result.Status);
        Assert.Equal(InboxCoordinationStage.Renewal, result.Stage);
        if (failure == 3) Assert.IsType<TimeoutException>(result.Error);
        if (failure == 2) Assert.IsType<IOException>(result.Error);
        Assert.True(result.ShutdownTimedOut);
        if (failure == 3) renewal.SetResult(Success()); // Late confirmation cannot restore authority.
        processing.SetResult(InboxNotificationProcessingResult.Succeeded);
        await result.Quiescence.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { "acquire", "renew" }, fixture.Store.Calls);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NonCooperativeProcessorHasBoundedShutdownAndLateOutcomeIsOnlyObserved(bool lateFault)
    {
        var fixture = new Fixture();
        var processing = Signal<InboxNotificationProcessingResult>();
        fixture.Processor.Process = _ => processing.Task;
        using var cancellation = new CancellationTokenSource();
        var run = fixture.Run(cancellation.Token);
        cancellation.Cancel();
        (await fixture.Time.NextTimerAsync(ShutdownTimeout)).Fire();
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.Cancelled, result.Status);
        Assert.True(result.ShutdownTimedOut);
        Assert.False(result.Quiescence.IsCompleted);
        Assert.Equal(0, fixture.Store.TerminalCalls);
        Assert.Equal(0, fixture.Time.ActiveTimerCount);

        if (lateFault) processing.SetException(new InvalidOperationException("late"));
        else processing.SetResult(InboxNotificationProcessingResult.Succeeded);
        await result.Quiescence.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(0, fixture.Store.TerminalCalls);
        Assert.Equal(1, fixture.Processor.Calls);
    }

    [Fact]
    public async Task BlockingCancellationCallbackDoesNotMakeShutdownUnbounded()
    {
        var fixture = new Fixture();
        using var release = new ManualResetEventSlim();
        var entered = Signal<bool>();
        fixture.Processor.Process = token =>
        {
            token.Register(() => { entered.SetResult(true); release.Wait(); });
            return Task.FromResult(InboxNotificationProcessingResult.Succeeded);
        };
        try
        {
            var run = fixture.Run();
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            (await fixture.Time.NextTimerAsync(ShutdownTimeout)).Fire();
            var result = await Finished(run);
            Assert.Equal(InboxCoordinationStatus.Completed, result.Status);
            Assert.True(result.ShutdownTimedOut);
            Assert.False(result.Quiescence.IsCompleted);
            release.Set();
            await result.Quiescence.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally { release.Set(); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AbandonedAcquisitionCannotStartWorkAfterReturning(bool callerCancellation)
    {
        var fixture = new Fixture();
        var acquisition = Signal<InboxAcquisitionResult>();
        fixture.Store.Acquire = _ => acquisition.Task;
        using var cancellation = new CancellationTokenSource();
        var run = fixture.Run(cancellation.Token);
        if (callerCancellation) cancellation.Cancel();
        else (await fixture.Time.NextTimerAsync(OperationTimeout)).Fire();
        (await fixture.Time.NextTimerAsync(ShutdownTimeout)).Fire();
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.OutcomeUnknown, result.Status);
        Assert.Equal(InboxCoordinationStage.Acquisition, result.Stage);
        Assert.True(result.ShutdownTimedOut);
        acquisition.SetResult(new(InboxAcquisitionStatus.Acquired, fixture.Store.Work));
        await result.Quiescence.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(0, fixture.Processor.Calls);
        Assert.Equal(new[] { "acquire" }, fixture.Store.Calls);
    }

    [Fact]
    public async Task RenewalTimeoutAbandonsAuthorityAndDoesNotWaitForeverForTheStore()
    {
        var fixture = new Fixture();
        var renewal = Signal<InboxMutationResult>();
        fixture.Processor.Process = async token =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
            return InboxNotificationProcessingResult.Succeeded;
        };
        fixture.Store.Renew = _ => renewal.Task;
        var run = fixture.Run();
        await (await fixture.Time.NextRegisteredTimerAsync(OperationTimeout)).Disposed.WaitAsync(TimeSpan.FromSeconds(10));
        (await fixture.Time.NextTimerAsync(RenewalInterval)).Fire();
        await fixture.Store.RenewEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        (await fixture.Time.NextTimerAsync(OperationTimeout)).Fire();
        (await fixture.Time.NextTimerAsync(ShutdownTimeout)).Fire();
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.OutcomeUnknown, result.Status);
        Assert.Equal(InboxCoordinationStage.Renewal, result.Stage);
        Assert.IsType<TimeoutException>(result.Error);
        Assert.True(fixture.Processor.Token.IsCancellationRequested);
        Assert.True(result.ShutdownTimedOut);
        renewal.SetResult(Success());
        await result.Quiescence.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(0, fixture.Store.TerminalCalls);
        Assert.Equal(1, fixture.Store.RenewCalls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InFlightCompletionCancellationIsUncertainUnlessConfirmationArrivesDuringDrain(bool confirmed)
    {
        var fixture = new Fixture();
        var completion = Signal<InboxMutationResult>();
        fixture.Store.Complete = _ => completion.Task;
        using var cancellation = new CancellationTokenSource();
        var run = fixture.Run(cancellation.Token);
        await fixture.Store.TerminalEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        cancellation.Cancel();
        var deadline = await fixture.Time.NextTimerAsync(ShutdownTimeout);
        if (confirmed) completion.SetResult(Success());
        else deadline.Fire();
        var result = await Finished(run);
        Assert.Equal(confirmed ? InboxCoordinationStatus.Completed : InboxCoordinationStatus.OutcomeUnknown, result.Status);
        Assert.Equal(InboxCoordinationStage.Completion, result.Stage);
        Assert.Equal(!confirmed, result.ShutdownTimedOut);
        if (!confirmed)
        {
            completion.SetResult(Success());
            await result.Quiescence.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(InboxCoordinationStatus.OutcomeUnknown, result.Status);
        }
        Assert.Equal(1, fixture.Store.TerminalCalls);
        Assert.Equal(1, fixture.Processor.Calls);
    }

    [Fact]
    public async Task FinalizationTimeoutIsUncertainAndNeverRepeatsBusinessEffects()
    {
        var fixture = new Fixture();
        var completion = Signal<InboxMutationResult>();
        fixture.Store.Complete = _ => completion.Task;
        var run = fixture.Run();
        await (await fixture.Time.NextRegisteredTimerAsync(OperationTimeout)).Disposed.WaitAsync(TimeSpan.FromSeconds(10));
        await fixture.Store.TerminalEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        (await fixture.Time.NextTimerAsync(OperationTimeout)).Fire();
        (await fixture.Time.NextTimerAsync(ShutdownTimeout)).Fire();
        var result = await Finished(run);
        Assert.Equal(InboxCoordinationStatus.OutcomeUnknown, result.Status);
        Assert.Equal(InboxCoordinationStage.Completion, result.Stage);
        Assert.IsType<TimeoutException>(result.Error);
        completion.SetException(new IOException("late fault"));
        await result.Quiescence.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { "acquire", "complete" }, fixture.Store.Calls);
    }

    [Fact]
    public void ConfigurationRejectsUnsafeBudgetsAndInvalidRetryDelays()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new InboxProcessingOptions(TimeSpan.Zero, RenewalInterval, OperationTimeout, ShutdownTimeout));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InboxProcessingOptions(LeaseDuration, TimeSpan.Zero, OperationTimeout, ShutdownTimeout));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InboxProcessingOptions(LeaseDuration, TimeSpan.FromTicks(1), OperationTimeout, ShutdownTimeout));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InboxProcessingOptions(LeaseDuration, LeaseDuration, OperationTimeout, ShutdownTimeout));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InboxProcessingOptions(LeaseDuration, RenewalInterval, TimeSpan.Zero, ShutdownTimeout));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InboxProcessingOptions(LeaseDuration, RenewalInterval, TimeSpan.FromTicks(1), ShutdownTimeout));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InboxProcessingOptions(LeaseDuration, RenewalInterval, LeaseDuration - RenewalInterval, ShutdownTimeout));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InboxProcessingOptions(LeaseDuration, RenewalInterval, OperationTimeout, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InboxProcessingOptions(LeaseDuration, RenewalInterval, OperationTimeout, TimeSpan.FromTicks(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InboxProcessingOptions(LeaseDuration, RenewalInterval, OperationTimeout, TimeSpan.MaxValue));
        Assert.Throws<ArgumentOutOfRangeException>(() => InboxNotificationProcessingResult.RetryAfter(TimeSpan.FromTicks(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => InboxNotificationProcessingResult.RetryAfter(TimeSpan.FromDays(8)));
        Assert.Throws<ArgumentException>(() => { _ = new Fixture().Coordinator.RunOnceAsync(" "); });
    }

    private static TaskCompletionSource<T> Signal<T>() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    private static InboxMutationResult Success() => new(InboxMutationStatus.Succeeded);
    // This real-time bound is a deadlock guard only; it does not order any test events.
    private static Task<InboxCoordinationResult> Finished(Task<InboxCoordinationResult> task) => task.WaitAsync(TimeSpan.FromSeconds(10));

    private sealed class Fixture(TimeSpan? shutdownTimeout = null)
    {
        public ControlledTimeProvider Time { get; } = new();
        public SyntheticStore Store { get; } = new();
        public SyntheticProcessor Processor { get; } = new();
        public InboxProcessingCoordinator Coordinator => new(Store, Processor, shutdownTimeout is null
            ? Options : new(LeaseDuration, RenewalInterval, OperationTimeout, shutdownTimeout.Value), Time);
        public Task<InboxCoordinationResult> Run(CancellationToken token = default) => Coordinator.RunOnceAsync("execution", token);
    }

    private sealed class SyntheticProcessor : IInboxNotificationProcessor
    {
        public int Calls { get; private set; }
        public InboxNotification? Notification { get; private set; }
        public CancellationToken Token { get; private set; }
        public Func<CancellationToken, Task<InboxNotificationProcessingResult>> Process { get; set; } =
            _ => Task.FromResult(InboxNotificationProcessingResult.Succeeded);

        public Task<InboxNotificationProcessingResult> ProcessAsync(InboxNotification notification, CancellationToken cancellationToken)
        {
            Calls++;
            Notification = notification;
            Token = cancellationToken;
            return Process(cancellationToken);
        }
    }

    private sealed class SyntheticStore : IInboxProcessingStore
    {
        public AcquiredInboxNotification Work { get; } = new(new(Guid.NewGuid(), "execution", 19),
            new(new("partner", "scope", "event"), new("event", "order"), 1, [1]),
            DateTimeOffset.MinValue, DateTimeOffset.MinValue); // Deliberately not worker-clock authority.
        public List<string> Calls { get; } = [];
        public int RenewCalls => Calls.Count(call => call == "renew");
        public int TerminalCalls => Calls.Count(call => call is "complete" or "retry");
        public TimeSpan? RetryDelay { get; private set; }
        public TaskCompletionSource<bool> RenewEntered { get; } = Signal<bool>();
        public TaskCompletionSource<bool> TerminalEntered { get; } = Signal<bool>();
        public Func<CancellationToken, Task<InboxAcquisitionResult>>? Acquire { get; set; }
        public Func<CancellationToken, Task<InboxMutationResult>> Renew { get; set; } = _ => Task.FromResult(Success());
        public Func<CancellationToken, Task<InboxMutationResult>> Complete { get; set; } = _ => Task.FromResult(Success());
        public Func<CancellationToken, Task<InboxMutationResult>> Retry { get; set; } = _ => Task.FromResult(Success());

        public Task<InboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan leaseDuration, CancellationToken cancellationToken)
        {
            Assert.Equal("execution", owner);
            Assert.Equal(LeaseDuration, leaseDuration);
            Calls.Add("acquire");
            return Acquire?.Invoke(cancellationToken) ?? Task.FromResult(new InboxAcquisitionResult(InboxAcquisitionStatus.Acquired, Work));
        }

        public Task<InboxMutationResult> RenewAsync(InboxLease lease, TimeSpan leaseDuration, CancellationToken cancellationToken)
        {
            Assert.Same(Work.Lease, lease);
            Assert.Equal(LeaseDuration, leaseDuration);
            Calls.Add("renew");
            var operation = Renew(cancellationToken);
            // Publish entry only after the delegate has installed its token/callback signals.
            RenewEntered.TrySetResult(true);
            return operation;
        }

        public Task<InboxMutationResult> CompleteAsync(InboxLease lease, CancellationToken cancellationToken)
        {
            Assert.Same(Work.Lease, lease);
            Calls.Add("complete");
            TerminalEntered.TrySetResult(true);
            return Complete(cancellationToken);
        }

        public Task<InboxMutationResult> ScheduleRetryAsync(InboxLease lease, TimeSpan retryDelay, CancellationToken cancellationToken)
        {
            Assert.Same(Work.Lease, lease);
            Calls.Add("retry");
            RetryDelay = retryDelay;
            TerminalEntered.TrySetResult(true);
            return Retry(cancellationToken);
        }
    }
}
