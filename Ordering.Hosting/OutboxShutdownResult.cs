namespace Ordering.Hosting;

public enum OutboxShutdownStatus { Drained, DeadlineExceeded, Unverified }

[Flags]
public enum OutboxSupervisionFailure { None = 0, Execution = 1, ScopeDisposal = 2 }

public sealed record OutboxWorkerShutdownState(bool StopRequested, bool OperationsCompleted,
    bool ExecuteTaskCompleted, bool ScopesDisposed, OutboxSupervisionFailure SupervisionFailures);

/// <summary>Historical snapshot. Late completion cannot change a missed graceful shutdown deadline.</summary>
public sealed record OutboxShutdownResult(OutboxShutdownStatus Status, bool HostStopCompleted,
    bool HostStopFailed, IReadOnlyList<OutboxWorkerShutdownState> Workers)
{
    public bool RootDisposalAuthorized => Status == OutboxShutdownStatus.Drained;
}
