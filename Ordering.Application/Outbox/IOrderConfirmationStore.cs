namespace Ordering.Application.Outbox;

public enum OrderConfirmationStatus { Confirmed, NoEligibleTransition, OutcomeUnknown }

public sealed record OrderConfirmationResult(OrderConfirmationStatus Status, Guid? EventId);

/// <summary>Minimal laboratory event; independent of partner notification formats.</summary>
public sealed record OrderConfirmed(Guid EventId, string OrderId);

public interface IOrderConfirmationStore
{
    /// <summary>
    /// Atomically changes a laboratory order from Pending to Confirmed and persists OrderConfirmed.
    /// Confirmed exposes the persisted event id only after successful local commit confirmation.
    /// NoEligibleTransition means absent or incompatible state; it creates no event and is not
    /// proof that an earlier uncertain attempt rolled back. Existing events are never overwritten.
    /// Failures before commit propagate. Loss/cancellation of commit confirmation returns
    /// OutcomeUnknown with no confirmed event id; it must not be interpreted as rollback.
    /// There are no automatic retries or external publication effects.
    /// </summary>
    Task<OrderConfirmationResult> ConfirmAsync(string orderId, CancellationToken cancellationToken);
}
