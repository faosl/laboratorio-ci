using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Ordering.Application.Outbox;

namespace Ordering.Hosting;

/// <summary>Sequential background polling; the relay retains acquisition and publication policy.</summary>
public sealed class OutboxBackgroundService : BackgroundService
{
    private readonly Func<ValueTask<OutboxWorkerExecution>> _createExecution;
    private readonly OutboxWorkerOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger<OutboxBackgroundService> _logger;
    private int _stopRequested;
    private int _operationsCompleted = 1;
    private int _scopesDisposed = 1;
    private int _failures;

    public OutboxWorkerShutdownState ShutdownState => new(Volatile.Read(ref _stopRequested) != 0,
        Volatile.Read(ref _operationsCompleted) != 0, ExecuteTask is { IsCompleted: true },
        Volatile.Read(ref _scopesDisposed) != 0, (OutboxSupervisionFailure)Volatile.Read(ref _failures));

    public OutboxBackgroundService(Func<ValueTask<OutboxWorkerExecution>> createExecution,
        OutboxWorkerOptions options, TimeProvider timeProvider, ILogger<OutboxBackgroundService> logger)
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
        var failureDelay = TimeSpan.Zero;
        var immediateResults = 0;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                // Conservatively retain failure evidence if scope creation/resolution fails.
                Volatile.Write(ref _operationsCompleted, 0);
                Volatile.Write(ref _scopesDisposed, 0);
                var execution = await _createExecution();
                OutboxRelayResult result;
                try
                {
                    // In-flight operation supervision: cancellation is a request, never proof
                    // that Kafka retracted a send. No next attempt or disposal before completion.
                    result = await execution.RunOnceAsync(owner, stoppingToken);
                    Volatile.Write(ref _operationsCompleted, 1);
                }
                catch
                {
                    Interlocked.Or(ref _failures, (int)OutboxSupervisionFailure.Execution);
                    throw;
                }
                finally
                {
                    try
                    {
                        await execution.OwnedDependencies.DisposeAsync();
                        Volatile.Write(ref _scopesDisposed, 1);
                    }
                    catch
                    {
                        Interlocked.Or(ref _failures, (int)OutboxSupervisionFailure.ScopeDisposal);
                        throw;
                    }
                }

                if (result.Status == OutboxRelayStatus.UnsupportedEvent)
                {
                    _logger.LogCritical("Outbox owner {Owner} found UnsupportedEvent {EventId} at {Stage}. Acquisitions stopped; intervention required.",
                        owner, result.EventId, result.Stage);
                    return;
                }
                _logger.Log(result.Status is OutboxRelayStatus.Published or OutboxRelayStatus.NoEligibleWork
                        ? LogLevel.Debug : LogLevel.Warning,
                    "Outbox owner {Owner} returned {Status} at {Stage} for {EventId}; delivery {Delivery}, receipt {Receipt}, error code {ErrorCode}.",
                    owner, result.Status, result.Stage, result.EventId, result.Delivery, result.Receipt, result.ErrorCode);
                if (stoppingToken.IsCancellationRequested) break;

                // Backoff resets only on confirmed progress or a confirmed eligibility query.
                // Ambiguous outcome, retry, cancellation and lease loss do not prove recovery.
                // At-least-once publication remains the relay's contract: an unknown completion
                // starts no blind completion retry here, only a later fresh acquisition.
                if (result.Status is OutboxRelayStatus.Published or OutboxRelayStatus.NoEligibleWork)
                    failureDelay = TimeSpan.Zero;
                if (result.Status == OutboxRelayStatus.OperationalFailure)
                    failureDelay = failureDelay == TimeSpan.Zero ? _options.InitialOperationalFailureDelay
                        : TimeSpan.FromTicks(Math.Min(failureDelay.Ticks * 2, _options.MaximumOperationalFailureDelay.Ticks));

                var delay = result.Status switch
                {
                    OutboxRelayStatus.Published or OutboxRelayStatus.LeaseLost => TimeSpan.Zero,
                    OutboxRelayStatus.NoEligibleWork => _options.PollingDelay,
                    OutboxRelayStatus.RetryScheduled or OutboxRelayStatus.Cancelled => _options.PublicationFailureDelay,
                    OutboxRelayStatus.AcquisitionUnknown or OutboxRelayStatus.CompletionUnknown
                        or OutboxRelayStatus.RetryUnknown => _options.UnknownOutcomeDelay,
                    OutboxRelayStatus.OperationalFailure => failureDelay,
                    _ => throw new InvalidOperationException("Unsupported Outbox relay status.")
                };
                // Liveness must not become busy polling, even for synchronous successful relays.
                immediateResults = delay == TimeSpan.Zero ? immediateResults + 1 : 0;
                if (immediateResults >= _options.MaximumImmediateResults)
                {
                    delay = _options.ImmediateResultDelay;
                    immediateResults = 0;
                    _logger.LogWarning("Outbox owner {Owner} reached the immediate-result limit. Waiting {Delay}.", owner, delay);
                }
                if (delay > TimeSpan.Zero) await Task.Delay(delay, _time, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested
            && Volatile.Read(ref _operationsCompleted) != 0) { }
        catch
        {
            // Sticky evidence also covers resolution, logging and policy failures.
            if (Volatile.Read(ref _failures) == 0)
                Interlocked.Or(ref _failures, (int)OutboxSupervisionFailure.Execution);
            throw;
        }
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        Volatile.Write(ref _stopRequested, 1);
        return base.StopAsync(cancellationToken);
    }
}
