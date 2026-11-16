using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Ordering.Application.Inbox;

namespace Ordering.Hosting;

/// <summary>
/// Polls sequentially with dedicated dependencies per attempt. ExecuteTask retains supervision
/// after StopAsync's deadline, but cannot retain singletons disposed by the root container.
/// </summary>
public sealed class InboxBackgroundService : BackgroundService
{
    private readonly Func<InboxWorkerExecution> _createExecution;
    private readonly InboxWorkerOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger<InboxBackgroundService> _logger;
    private int _stopRequested;
    private int _operationsCompleted = 1;
    private int _scopesDisposed = 1;
    private int _supervisionFailures;

    public InboxWorkerShutdownState ShutdownState => new(Volatile.Read(ref _stopRequested) != 0,
        Volatile.Read(ref _operationsCompleted) != 0, ExecuteTask is { IsCompleted: true },
        Volatile.Read(ref _scopesDisposed) != 0, (InboxSupervisionFailure)Volatile.Read(ref _supervisionFailures));

    /// <summary>Explicit evidence from a delegate/composition aware of work outside Quiescence.
    /// This is sticky and must be reported before the supervised execution completes.</summary>
    public void ReportUntrackedWork() => RecordFailure(InboxSupervisionFailure.KnownUntrackedWork);

    private void RecordFailure(InboxSupervisionFailure failure) => Interlocked.Or(ref _supervisionFailures, (int)failure);

    public InboxBackgroundService(Func<InboxWorkerExecution> createExecution, InboxWorkerOptions options,
        TimeProvider timeProvider, ILogger<InboxBackgroundService> logger)
    {
        ArgumentNullException.ThrowIfNull(createExecution);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);
        options.Validate();
        _createExecution = createExecution;
        _options = options;
        _time = timeProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var owner = Guid.NewGuid().ToString("N");
        var storeFailureDelay = TimeSpan.Zero;
        var immediateResults = 0;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var started = _time.GetTimestamp();
                InboxCoordinationResult result;
                Volatile.Write(ref _operationsCompleted, 0);
                var execution = _createExecution();
                Volatile.Write(ref _scopesDisposed, 0);
                try
                {
                    // Unexpected exceptions propagate to host supervision, without retry. Without
                    // a result's Quiescence, detached tasks from a faulty delegate cannot be tracked.
                    result = await execution.RunOnceAsync(owner, stoppingToken);
                    try
                    {
                        try
                        {
                            if (result.ShutdownTimedOut)
                                _logger.LogWarning("Inbox owner {Owner} exceeded coordinator shutdown at {Stage}. Waiting for quiescence.",
                                    owner, result.Stage);
                            if (result.Status is InboxCoordinationStatus.StoreFailed or InboxCoordinationStatus.ProcessingFailed
                                or InboxCoordinationStatus.OutcomeUnknown or InboxCoordinationStatus.Cancelled)
                                _logger.LogWarning(result.Error, "Inbox owner {Owner} returned {Status} at {Stage}.",
                                    owner, result.Status, result.Stage);
                        }
                        finally
                        {
                            // Uncancellable, including when a logger throws. Dispose only this
                            // attempt's dependencies after every supervised task has settled.
                            await result.Quiescence;
                            Volatile.Write(ref _operationsCompleted, 1);
                        }
                    }
                    catch (Exception error)
                    {
                        RecordFailure(InboxSupervisionFailure.ResultSupervision);
                        _logger.LogCritical(error, "Inbox owner {Owner} result supervision failed. Acquisitions stopped.", owner);
                        return;
                    }
                }
                catch (Exception)
                {
                    // Record before disposal, which can itself remain pending past the deadline.
                    RecordFailure(InboxSupervisionFailure.Execution);
                    throw;
                }
                finally
                {
                    try
                    {
                        await execution.OwnedDependencies.DisposeAsync();
                        Volatile.Write(ref _scopesDisposed, 1);
                    }
                    catch (Exception)
                    {
                        RecordFailure(InboxSupervisionFailure.ScopeDisposal);
                        throw;
                    }
                }

                if (stoppingToken.IsCancellationRequested) break;

                storeFailureDelay = result.Status == InboxCoordinationStatus.StoreFailed
                    ? NextStoreFailureDelay(storeFailureDelay) : TimeSpan.Zero;
                var delay = result.Status switch
                {
                    InboxCoordinationStatus.NoEligibleWork => _options.PollingDelay,
                    InboxCoordinationStatus.StoreFailed => storeFailureDelay,
                    InboxCoordinationStatus.ProcessingFailed => _options.ProcessingFailureDelay,
                    InboxCoordinationStatus.OutcomeUnknown => _options.UnknownOutcomeDelay,
                    // Unexpected cancellation without host shutdown must also avoid a hot loop.
                    InboxCoordinationStatus.Cancelled => _options.ProcessingFailureDelay,
                    InboxCoordinationStatus.Completed or InboxCoordinationStatus.RetryScheduled
                        or InboxCoordinationStatus.LeaseLost => TimeSpan.Zero,
                    _ => throw new InvalidOperationException("Unsupported Inbox coordination status.")
                };

                if (delay == TimeSpan.Zero && _time.GetElapsedTime(started) < _options.ImmediateResultDelay)
                    immediateResults++;
                else
                    immediateResults = 0;

                if (immediateResults >= _options.MaximumImmediateResults)
                {
                    delay = _options.ImmediateResultDelay;
                    immediateResults = 0;
                    _logger.LogWarning("Inbox owner {Owner} reached the immediate-result limit. Waiting {Delay}.", owner, delay);
                }

                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, _time, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            if (Volatile.Read(ref _operationsCompleted) == 0)
                RecordFailure(InboxSupervisionFailure.Execution);
        }
        catch (Exception)
        {
            RecordFailure(InboxSupervisionFailure.Execution);
            throw;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        Volatile.Write(ref _stopRequested, 1);
        await base.StopAsync(cancellationToken);
        if (ExecuteTask is { IsCompleted: false })
            _logger.LogCritical("Inbox host shutdown deadline elapsed. ExecuteTask still supervises pending work and owns attempt dependencies; root-owned singletons may be disposed by the host.");
    }

    private TimeSpan NextStoreFailureDelay(TimeSpan previous) => previous == TimeSpan.Zero
        ? _options.InitialStoreFailureDelay
        : TimeSpan.FromTicks(Math.Min(previous.Ticks * 2, _options.MaximumStoreFailureDelay.Ticks));
}
