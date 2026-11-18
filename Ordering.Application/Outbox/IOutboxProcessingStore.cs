namespace Ordering.Application.Outbox;

public enum OutboxProcessingState { Pending, Processing, RetryScheduled, Published }
public enum OutboxAcquisitionStatus { Acquired, NoEligibleWork, OutcomeUnknown }
public enum OutboxMutationStatus { Succeeded, LeaseLost, OutcomeUnknown }

/// <summary>Identifies one acquisition. Owners identify worker executions, not stable host names.</summary>
public sealed record OutboxLease
{
    public OutboxLease(Guid eventId, string owner, long generation)
    {
        if (eventId == Guid.Empty) throw new ArgumentException("Event id must not be empty.", nameof(eventId));
        OutboxProcessingLimits.ValidateOwner(owner);
        if (generation <= 0) throw new ArgumentOutOfRangeException(nameof(generation));
        EventId = eventId;
        Owner = owner;
        Generation = generation;
    }

    public Guid EventId { get; }
    public string Owner { get; }
    public long Generation { get; }
}

/// <summary>Persisted event data, independent of partner formats and future broker envelopes.</summary>
public sealed record OutboxMessage(Guid EventId, string EventType, string AggregateId, string Payload,
    DateTimeOffset CreatedAtUtc);

public sealed record AcquiredOutboxMessage(OutboxLease Lease, OutboxMessage Content, DateTimeOffset ExpiresAtUtc);

/// <summary>Work is present only after confirmed commit. NoEligibleWork does not mean an empty Outbox.</summary>
public sealed record OutboxAcquisitionResult(OutboxAcquisitionStatus Status, AcquiredOutboxMessage? Work);

/// <summary>Expiration is returned only for confirmed renewal; a local clock grants no authority.</summary>
public sealed record OutboxMutationResult(OutboxMutationStatus Status, DateTimeOffset? ExpiresAtUtc = null);

/// <summary>Laboratory bounds: leases 1 second–1 hour; retry delays 0–7 days, at microsecond precision.</summary>
public static class OutboxProcessingLimits
{
    public static void ValidateOwner(string owner)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(owner);
        if (owner.Length > 200 || owner.Contains('\0'))
            throw new ArgumentException("Owner must have at most 200 characters and no NUL.", nameof(owner));
    }

    public static void ValidateLeaseDuration(TimeSpan leaseDuration)
    {
        if (leaseDuration < TimeSpan.FromSeconds(1) || leaseDuration > TimeSpan.FromHours(1)
            || leaseDuration.Ticks % 10 != 0)
            throw new ArgumentOutOfRangeException(nameof(leaseDuration));
    }

    public static void ValidateRetryDelay(TimeSpan retryDelay)
    {
        if (retryDelay < TimeSpan.Zero || retryDelay > TimeSpan.FromDays(7) || retryDelay.Ticks % 10 != 0)
            throw new ArgumentOutOfRangeException(nameof(retryDelay));
    }
}

/// <summary>
/// Coordinates persisted work in short transactions without external calls under row locks.
/// Mutations require an unexpired lease at the store's atomic database validation point,
/// not at the later response time. Expired leases cannot be renewed even without a successor.
/// Failures before commit propagate; lost/cancelled commit confirmation yields OutcomeUnknown.
/// Unknown acquisitions grant no work; unknown renewals do not confirm continued authority.
/// Fencing protects these persisted writes, not external effects. There are no hidden retries.
/// </summary>
public interface IOutboxProcessingStore
{
    Task<OutboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan leaseDuration, CancellationToken cancellationToken);
    Task<OutboxMutationResult> RenewAsync(OutboxLease lease, TimeSpan leaseDuration, CancellationToken cancellationToken);

    /// <summary>
    /// Records local publication completion only. A future caller must first obtain broker confirmation;
    /// this operation neither contacts a broker nor proves external delivery.
    /// </summary>
    Task<OutboxMutationResult> MarkPublishedAsync(OutboxLease lease, CancellationToken cancellationToken);
    Task<OutboxMutationResult> ScheduleRetryAsync(OutboxLease lease, TimeSpan retryDelay, CancellationToken cancellationToken);
}
