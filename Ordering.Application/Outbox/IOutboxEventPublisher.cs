namespace Ordering.Application.Outbox;

public enum OutboxDeliveryStatus { NotAttempted, Acknowledged, Unconfirmed }

/// <summary>Broker diagnostics, not evidence of consumption by a recipient.</summary>
public sealed record OutboxDeliveryReceipt(string Topic, int Partition, long Offset);

public sealed record OutboxPublicationResult(OutboxDeliveryStatus Status,
    OutboxDeliveryReceipt? Receipt = null, string? ErrorCode = null);

public interface IOutboxEventPublisher
{
    TimeSpan DeliveryTimeout { get; }

    /// <summary>
    /// One logical send, awaiting delivery acknowledgement. Transport retries may occur within
    /// DeliveryTimeout. Once sending starts, cancellation must not abandon supervision of it;
    /// an unconfirmed result does not prove that the broker did not persist the event.
    /// </summary>
    Task<OutboxPublicationResult> PublishAsync(OutboxMessage message, CancellationToken cancellationToken);
}
