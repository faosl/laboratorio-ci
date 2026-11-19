using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Ordering.Hosting;

/// <summary>
/// Owns the isolated host's single StopAsync call. Call after StartAsync, without RunAsync or
/// another stop owner. The caller owns host disposal and must keep the root alive on failure.
/// Delegates and cancellation callbacks must return promptly for the deadline to be observable.
/// </summary>
public sealed class OutboxShutdownCoordinator
{
    private readonly object _gate = new();
    private readonly IHost _host;
    private readonly OutboxBackgroundService[] _workers;
    private readonly TimeSpan _drainTimeout;
    private readonly TimeProvider _time;
    private Task<OutboxShutdownResult>? _shutdown;
    private Task? _completion;
    private volatile bool _hostStopCompleted;
    private volatile bool _hostStopFailed;

    public OutboxShutdownCoordinator(IHost host, TimeSpan drainTimeout, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(host);
        ArgumentNullException.ThrowIfNull(timeProvider);
        if (drainTimeout < TimeSpan.FromMilliseconds(1) || drainTimeout.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(nameof(drainTimeout));

        var options = host.Services.GetRequiredService<IOptions<HostOptions>>().Value;
        if (!options.ServicesStopConcurrently || options.ShutdownTimeout != Timeout.InfiniteTimeSpan)
            throw new ArgumentException("The isolated host requires concurrent stop and an infinite host shutdown timeout. The coordinator supplies the global deadline.", nameof(host));

        _workers = host.Services.GetServices<IHostedService>().OfType<OutboxBackgroundService>().ToArray();
        if (_workers.Length == 0 || _workers.Distinct().Count() != _workers.Length)
            throw new ArgumentException("Register each participating Outbox worker exactly once as IHostedService.", nameof(host));
        _host = host;
        _drainTimeout = drainTimeout;
        _time = timeProvider;
    }

    /// <summary>Settles after host stop and all ExecuteTasks, including late completion after timeout.
    /// Completion alone does not authorize root disposal when supervision has failed.</summary>
    public Task Completion
    {
        get
        {
            lock (_gate) return _completion ?? throw new InvalidOperationException("Shutdown has not started.");
        }
    }

    /// <summary>Repeated or concurrent calls share the same shutdown and immutable result.</summary>
    public Task<OutboxShutdownResult> ShutdownAsync()
    {
        lock (_gate)
        {
            if (_workers.Any(worker => worker.ExecuteTask is null))
                throw new InvalidOperationException("Start all participating workers before coordinating shutdown.");
            return _shutdown ??= ShutdownCoreAsync();
        }
    }

    private async Task<OutboxShutdownResult> ShutdownCoreAsync()
    {
        var started = _time.GetTimestamp();
        using var timerCancellation = new CancellationTokenSource();
        var deadline = Task.Delay(_drainTimeout, _time, timerCancellation.Token);
        // HostOptions makes Host.StopAsync signal every worker before awaiting their drains.
        // No worker is independently stopped here, and the host wait remains alive on timeout.
        _completion = ObserveCompletionAsync();
        var finished = await Task.WhenAny(_completion, deadline).ConfigureAwait(false);
        var deadlineExceeded = finished == deadline || _time.GetElapsedTime(started) >= _drainTimeout;
        await timerCancellation.CancelAsync().ConfigureAwait(false);

        var states = Array.AsReadOnly(_workers.Select(worker => worker.ShutdownState).ToArray());
        var unverified = _hostStopFailed || states.Any(state => state.SupervisionFailures != OutboxSupervisionFailure.None);
        var drained = _hostStopCompleted && states.All(state => state.StopRequested && state.OperationsCompleted
            && state.ExecuteTaskCompleted && state.ScopesDisposed);
        var status = OutboxShutdownStatus.Unverified;
        if (!unverified && deadlineExceeded) status = OutboxShutdownStatus.DeadlineExceeded;
        if (!unverified && !deadlineExceeded && drained) status = OutboxShutdownStatus.Drained;
        return new(status, _hostStopCompleted, _hostStopFailed, states);
    }

    private async Task ObserveCompletionAsync()
    {
        // Observe failures here as well as in the worker, even when the budget expires first.
        await Task.WhenAll(_workers.Select(worker => ObserveExecutionAsync(worker.ExecuteTask!))
            .Append(StopHostAsync())).ConfigureAwait(false);
    }

    private async Task StopHostAsync()
    {
        try { await _host.StopAsync(CancellationToken.None).ConfigureAwait(false); }
        catch (Exception) { _hostStopFailed = true; }
        finally { _hostStopCompleted = true; }
    }

    private static async Task ObserveExecutionAsync(Task execution)
    {
        try { await execution.ConfigureAwait(false); }
        catch (Exception) { /* The worker's sticky evidence records the supervision failure. */ }
    }
}
