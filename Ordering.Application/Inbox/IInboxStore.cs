namespace Ordering.Application.Inbox;

public enum InboxRegistrationStatus { Inserted, AlreadyExists, Conflict, OutcomeUnknown }

public sealed record InboxRegistrationResult(InboxRegistrationStatus Status, Guid? InboxId);

public sealed record InboxEntry(Guid InboxId, InboxNotification Content,
    DateTimeOffset ReceivedAtUtc, string State);

public interface IInboxStore
{
    /// <summary>
    /// Equivalent means identical original bytes, order/event identifiers and normalized version/content.
    /// Receipt time and generated id are not compared. No existing content/state is changed.
    /// OutcomeUnknown requires reconciliation; cancellation during commit does not imply rollback.
    /// Failures before commit are propagated. This port does not authenticate notifications.
    /// </summary>
    Task<InboxRegistrationResult> RegisterAsync(InboxNotification notification, CancellationToken cancellationToken);

    /// <summary>Reads the primary. Absence is not proof that an in-flight commit failed.</summary>
    Task<InboxEntry?> GetByKeyAsync(InboxKey key, CancellationToken cancellationToken);
}
