namespace Ordering.Application.Inbox;

/// <summary>Supervises one acquisition, never polls or repeats business processing.</summary>
public sealed class InboxProcessingCoordinator
{
    private readonly IInboxProcessingStore _store;
    private readonly IInboxNotificationProcessor _processor;
    private readonly InboxProcessingOptions _options;
    private readonly TimeProvider _time;

    public InboxProcessingCoordinator(IInboxProcessingStore store, IInboxNotificationProcessor processor,
        InboxProcessingOptions options, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(processor);
        ArgumentNullException.ThrowIfNull(options);
        _store = store;
        _processor = processor;
        _options = options;
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Use a fresh owner for each worker execution. Returned store expirations are not local authority.</summary>
    public Task<InboxCoordinationResult> RunOnceAsync(string owner, CancellationToken cancellationToken = default)
    {
        InboxProcessingLimits.ValidateOwner(owner);
        return new Execution(_store, _processor, _options, _time).RunAsync(owner, cancellationToken);
    }

    // Only this supervisor makes decisions and starts calls. Completion can overlap a renewal
    // already in flight, but no subsequent renewal starts once finalization has begun.
    private sealed class Execution(IInboxProcessingStore store, IInboxNotificationProcessor processor,
        InboxProcessingOptions options, TimeProvider time)
    {
        private readonly CancellationTokenSource _calls = new();
        private readonly CancellationTokenSource _timers = new();
        private readonly List<Task> _started = [];
        private InboxLease? _lease;

        public async Task<InboxCoordinationResult> RunAsync(string owner, CancellationToken cancellationToken)
        {
            using var cancellationSignal = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var cancelled = Task.Delay(Timeout.InfiniteTimeSpan, cancellationSignal.Token);
            try
            {
                if (cancellationToken.IsCancellationRequested)
                    return await StopAsync(Result(InboxCoordinationStatus.Cancelled, InboxCoordinationStage.Acquisition));

                var (acquisition, acquisitionDeadline) = StartStore(() => store.TryAcquireAsync(owner, options.LeaseDuration, _calls.Token));
                await Task.WhenAny(acquisition, cancelled, acquisitionDeadline);
                if (!acquisition.IsCompleted)
                    return await StopAsync(Unknown(InboxCoordinationStage.Acquisition,
                        cancellationToken.IsCancellationRequested ? null : new TimeoutException("Acquisition confirmation timed out.")));

                var acquired = await acquisition;
                if (acquired.Error is not null)
                    return await StopAsync(Result(InboxCoordinationStatus.StoreFailed, InboxCoordinationStage.Acquisition, acquired.Error));
                if (acquired.Value!.Status != InboxAcquisitionStatus.Acquired)
                    return await StopAsync(Result(acquired.Value.Status == InboxAcquisitionStatus.NoEligibleWork
                        ? InboxCoordinationStatus.NoEligibleWork : InboxCoordinationStatus.OutcomeUnknown, InboxCoordinationStage.Acquisition));
                if (acquired.Value.Work is not { } work)
                    return await StopAsync(Result(InboxCoordinationStatus.StoreFailed, InboxCoordinationStage.Acquisition,
                        new InvalidOperationException("Confirmed acquisition did not contain work.")));

                _lease = work.Lease;
                if (cancellationToken.IsCancellationRequested)
                    return await StopAsync(Result(InboxCoordinationStatus.Cancelled, InboxCoordinationStage.Processing));

                var processing = Start(() => processor.ProcessAsync(work.Content, _calls.Token));
                var renewalDue = Delay(options.RenewalInterval);
                Task<Call<InboxMutationResult>>? renewal = null;
                Task? renewalDeadline = null;
                Task<Call<InboxMutationResult>>? terminal = null;
                Task? terminalDeadline = null;
                var terminalStage = InboxCoordinationStage.Completion;

                while (true)
                {
                    // A confirmed terminal write wins, even if the store rejected a late renewal
                    // because that very write cleared the lease. Never infer rollback from LeaseLost.
                    if (terminal?.IsCompleted == true)
                        return await FinishAsync(await terminal, terminalStage);

                    if (cancellationToken.IsCancellationRequested)
                        return await StopAsync(terminal is null
                            ? Result(InboxCoordinationStatus.Cancelled, InboxCoordinationStage.Processing)
                            : Unknown(terminalStage), terminal, terminalStage);

                    if (renewal?.IsCompleted == true)
                    {
                        var renewed = await renewal;
                        renewal = null;
                        renewalDeadline = null;
                        if (terminal is null)
                        {
                            if (renewed.Error is not null || renewed.Value!.Status != InboxMutationStatus.Succeeded)
                                return await StopAsync(MutationResult(renewed, InboxCoordinationStage.Renewal));
                            renewalDue = Delay(options.RenewalInterval);
                        }
                        // Finalization already started with confirmed acquisition/renewal evidence.
                        // Its atomic store validation decides the result; wait for its confirmation.
                    }

                    if (terminal is null && renewalDeadline?.IsCompleted == true)
                        return await StopAsync(Unknown(InboxCoordinationStage.Renewal,
                            new TimeoutException("Renewal confirmation timed out.")));

                    if (terminal is null && processing.IsCompleted)
                    {
                        var processed = await processing;
                        if (processed.Error is not null)
                            return await StopAsync(Result(InboxCoordinationStatus.ProcessingFailed, InboxCoordinationStage.Processing, processed.Error));
                        if (processed.Value is null || processed.Value.Status == InboxNotificationProcessingStatus.OutcomeUnknown)
                            return await StopAsync(Unknown(InboxCoordinationStage.Processing));

                        if (processed.Value.Status == InboxNotificationProcessingStatus.Succeeded)
                            (terminal, terminalDeadline) = StartStore(() => store.CompleteAsync(_lease, _calls.Token));
                        else
                        {
                            terminalStage = InboxCoordinationStage.Retry;
                            (terminal, terminalDeadline) = StartStore(() => store.ScheduleRetryAsync(_lease, processed.Value.RetryDelay!.Value, _calls.Token));
                        }
                        continue;
                    }

                    if (terminalDeadline?.IsCompleted == true)
                        return await StopAsync(Unknown(terminalStage,
                            new TimeoutException("Finalization confirmation timed out.")), terminal, terminalStage);

                    if (terminal is null && renewal is null && renewalDue.IsCompleted)
                    {
                        (renewal, renewalDeadline) = StartStore(() => store.RenewAsync(_lease, options.LeaseDuration, _calls.Token));
                        continue;
                    }

                    var signals = new List<Task> { cancelled };
                    if (terminal is not null)
                    {
                        signals.Add(terminal);
                        signals.Add(terminalDeadline!);
                    }
                    else
                    {
                        signals.Add(processing);
                        if (renewal is null) signals.Add(renewalDue);
                    }
                    if (renewal is not null)
                    {
                        signals.Add(renewal);
                        if (terminal is null) signals.Add(renewalDeadline!);
                    }
                    await Task.WhenAny(signals);
                }
            }
            finally
            {
                // Release the otherwise infinite caller-cancellation wait and all delay timers.
                await cancellationSignal.CancelAsync();
                await _timers.CancelAsync();
                _timers.Dispose();
            }
        }

        private Task Delay(TimeSpan duration) => Task.Delay(duration, time, _timers.Token);

        private (Task<Call<T>> Call, Task Deadline) StartStore<T>(Func<Task<T>> call)
        {
            var budget = CancellationTokenSource.CreateLinkedTokenSource(_timers.Token);
            var deadline = Task.Delay(options.StoreOperationTimeout, time, budget.Token);
            var operation = Start(call);
            Track(ObserveAsync(ReleaseBudgetAsync()));
            return (operation, deadline);

            async Task ReleaseBudgetAsync()
            {
                // Mark the call completed BEFORE cancelling its deadline: a cancelled delay is
                // also a completed Task and must never win the race against its own confirmed call.
                await operation;
                await budget.CancelAsync();
                budget.Dispose();
            }
        }

        private Task<Call<T>> Start<T>(Func<Task<T>> call)
        {
            var task = CaptureAsync(call);
            Track(task);
            return task;
        }

        private void Track(Task task)
        {
            // A long-running processor must not retain one task per historical renewal.
            _started.RemoveAll(started => started.IsCompleted);
            _started.Add(task);
        }

        private static async Task<Call<T>> CaptureAsync<T>(Func<Task<T>> call)
        {
            try { return new(await call(), null); }
            catch (Exception error) { return new(default, error); }
        }

        private InboxCoordinationResult Result(InboxCoordinationStatus status, InboxCoordinationStage stage, Exception? error = null) =>
            new(status, stage, _lease, error);

        private InboxCoordinationResult Unknown(InboxCoordinationStage stage, Exception? error = null) =>
            Result(InboxCoordinationStatus.OutcomeUnknown, stage, error);

        private InboxCoordinationResult MutationResult(Call<InboxMutationResult> call, InboxCoordinationStage stage)
        {
            if (call.Error is not null) return Result(InboxCoordinationStatus.StoreFailed, stage, call.Error);
            return Result(call.Value!.Status switch
            {
                InboxMutationStatus.Succeeded => stage == InboxCoordinationStage.Retry
                    ? InboxCoordinationStatus.RetryScheduled : InboxCoordinationStatus.Completed,
                InboxMutationStatus.LeaseLost => InboxCoordinationStatus.LeaseLost,
                _ => InboxCoordinationStatus.OutcomeUnknown
            }, stage);
        }

        private Task<InboxCoordinationResult> FinishAsync(Call<InboxMutationResult> call, InboxCoordinationStage stage) =>
            StopAsync(MutationResult(call, stage));

        private async Task<InboxCoordinationResult> StopAsync(InboxCoordinationResult result,
            Task<Call<InboxMutationResult>>? terminal = null, InboxCoordinationStage terminalStage = InboxCoordinationStage.Completion)
        {
            // CancelAsync also bounds shutdown when a participant blocks inside a cancellation callback.
            Track(ObserveAsync(_calls.CancelAsync()));
            var quiescence = SettleAsync();
            using var deadlineCancellation = new CancellationTokenSource();
            var deadline = Task.Delay(options.ShutdownTimeout, time, deadlineCancellation.Token);
            await Task.WhenAny(quiescence, deadline);
            await deadlineCancellation.CancelAsync();

            if (terminal?.IsCompleted == true && (await terminal).Value?.Status == InboxMutationStatus.Succeeded)
                result = MutationResult(await terminal, terminalStage);
            return result with { ShutdownTimedOut = !quiescence.IsCompleted, Quiescence = quiescence };
        }

        private async Task SettleAsync()
        {
            await Task.WhenAll(_started);
            _calls.Dispose();
        }

        private static async Task ObserveAsync(Task task)
        {
            try { await task; }
            catch (Exception) { /* Cancellation callback faults must not escape task supervision. */ }
        }

        private sealed record Call<T>(T? Value, Exception? Error);
    }
}
