namespace Ordering.Hosting;

public enum InboxShutdownStatus { Drained, DeadlineExceeded, Unverified }

[Flags]
public enum InboxSupervisionFailure
{
    None = 0,
    Execution = 1,
    ResultSupervision = 2,
    ScopeDisposal = 4,
    KnownUntrackedWork = 8
}

/// <summary>Evidence for one registered worker, without payloads or exception messages.</summary>
public sealed record InboxWorkerShutdownState(bool StopRequested, bool OperationsCompleted,
    bool ExecuteTaskCompleted, bool ScopesDisposed, InboxSupervisionFailure SupervisionFailures);

/// <summary>
/// A snapshot covering only the Inbox workers registered with the isolated host.
/// The owner must retain the root on DeadlineExceeded or Unverified.
/// </summary>
public sealed record InboxShutdownResult(InboxShutdownStatus Status, bool HostStopCompleted,
    bool HostStopFailed, IReadOnlyList<InboxWorkerShutdownState> Workers)
{
    public bool RootDisposalAuthorized => Status == InboxShutdownStatus.Drained;
}
