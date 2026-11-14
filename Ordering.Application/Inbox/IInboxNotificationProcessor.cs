namespace Ordering.Application.Inbox;

public enum InboxNotificationProcessingStatus { Succeeded, RetryRequested, OutcomeUnknown }

public sealed record InboxNotificationProcessingResult
{
    private InboxNotificationProcessingResult(InboxNotificationProcessingStatus status, TimeSpan? retryDelay = null)
    {
        Status = status;
        RetryDelay = retryDelay;
    }

    public InboxNotificationProcessingStatus Status { get; }
    public TimeSpan? RetryDelay { get; }
    public static InboxNotificationProcessingResult Succeeded { get; } = new(InboxNotificationProcessingStatus.Succeeded);
    public static InboxNotificationProcessingResult OutcomeUnknown { get; } = new(InboxNotificationProcessingStatus.OutcomeUnknown);

    /// <summary>The processor explicitly asserts that another attempt is safe, including any external effects.</summary>
    public static InboxNotificationProcessingResult RetryAfter(TimeSpan delay)
    {
        InboxProcessingLimits.ValidateRetryDelay(delay);
        return new(InboxNotificationProcessingStatus.RetryRequested, delay);
    }
}

/// <summary>
/// Processes one normalized envelope. Implementations must return their Task promptly and cooperate with
/// cancellation. Task completion must include all their work; cancellation cannot undo external effects.
/// Exceptions and uncertain effects must not be reported as a safe retry.
/// </summary>
public interface IInboxNotificationProcessor
{
    Task<InboxNotificationProcessingResult> ProcessAsync(InboxNotification notification, CancellationToken cancellationToken);
}
