namespace Ordering.Application.Inbox;

public enum InboxCoordinationStatus
{
    NoEligibleWork, Completed, RetryScheduled, LeaseLost, OutcomeUnknown,
    ProcessingFailed, StoreFailed, Cancelled
}

public enum InboxCoordinationStage { Acquisition, Processing, Renewal, Completion, Retry }

/// <summary>
/// Cancelled never asserts rollback. ShutdownTimedOut means invoked code may still be running.
/// Quiescence observes all invoked Tasks and cancellation callbacks, including late faults; it does
/// not prove that a remote system stopped its effects. No further store calls start after return.
/// Late results after the shutdown deadline do not change this snapshot.
/// </summary>
public sealed record InboxCoordinationResult(InboxCoordinationStatus Status, InboxCoordinationStage Stage,
    InboxLease? Lease = null, Exception? Error = null)
{
    public bool ShutdownTimedOut { get; init; }
    public Task Quiescence { get; init; } = Task.CompletedTask;
}
