using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Ordering.Application.Inbox;

namespace Ordering.Hosting;

/// <summary>
/// Polls sequentially. ExecuteTask retains supervision and dependency ownership after the host's
/// StopAsync deadline, until all local tasks settle. Process termination ends that supervision.
/// </summary>
public sealed class InboxBackgroundService : BackgroundService
{
    private readonly Func<InboxWorkerExecution> _createExecution;
    private readonly InboxWorkerOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger<InboxBackgroundService> _logger;

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
        var execution = _createExecution();
        await using var dependencies = execution.OwnedDependencies;
        var storeFailureDelay = TimeSpan.Zero;
        var immediateResults = 0;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var started = _time.GetTimestamp();
                var result = await execution.RunOnceAsync(owner, stoppingToken);
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
                        // Uncancellable, including when a logger throws. StopAsync is bounded by
                        // the host token while this task retains all dependencies until quiescence.
                        await result.Quiescence;
                    }
                }
                catch (Exception error)
                {
                    _logger.LogCritical(error, "Inbox owner {Owner} result supervision failed. Acquisitions stopped.", owner);
                    return;
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
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        if (ExecuteTask is { IsCompleted: false })
            _logger.LogCritical("Inbox host shutdown deadline elapsed. ExecuteTask still supervises pending work and owns its dependencies.");
    }

    private TimeSpan NextStoreFailureDelay(TimeSpan previous) => previous == TimeSpan.Zero
        ? _options.InitialStoreFailureDelay
        : TimeSpan.FromTicks(Math.Min(previous.Ticks * 2, _options.MaximumStoreFailureDelay.Ticks));
}
