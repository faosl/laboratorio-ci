namespace Ordering.Application.Inbox;

public enum InboxProcessingState { Pending, Processing, RetryScheduled, Completed }
public enum InboxAcquisitionStatus { Acquired, NoEligibleWork, OutcomeUnknown }
public enum InboxMutationStatus { Succeeded, LeaseLost, OutcomeUnknown }

/// <summary>Identifies one acquisition. Owners identify worker executions, not stable host names.</summary>
public sealed record InboxLease
{
    public InboxLease(Guid inboxId, string owner, long generation)
    {
        if (inboxId == Guid.Empty) throw new ArgumentException("Inbox id must not be empty.", nameof(inboxId));
        InboxProcessingLimits.ValidateOwner(owner);
        if (generation <= 0) throw new ArgumentOutOfRangeException(nameof(generation));
        InboxId = inboxId;
        Owner = owner;
        Generation = generation;
    }

    public Guid InboxId { get; }
    public string Owner { get; }
    public long Generation { get; }
}

public sealed record AcquiredInboxNotification(InboxLease Lease, InboxNotification Content,
    DateTimeOffset ReceivedAtUtc, DateTimeOffset ExpiresAtUtc);

/// <summary>Work is present only for a confirmed acquisition. NoEligibleWork does not mean an empty Inbox.</summary>
public sealed record InboxAcquisitionResult(InboxAcquisitionStatus Status, AcquiredInboxNotification? Work);

/// <summary>Expiration is returned only for a confirmed renewal; it is not authority from a local clock.</summary>
public sealed record InboxMutationResult(InboxMutationStatus Status, DateTimeOffset? ExpiresAtUtc = null);

/// <summary>Laboratory bounds: leases 1 second–1 hour; retry delays 0–7 days, at microsecond precision.</summary>
public static class InboxProcessingLimits
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
/// Coordinates persisted work without keeping a transaction open during business processing.
/// Mutations are authorized while the current lease is unexpired at the store's atomic validation
/// point, not at the later response time. A lost lease cannot be renewed even without a successor.
/// Failures before commit propagate; cancellation/lost confirmation during commit may yield OutcomeUnknown.
/// Unknown acquisitions must not start processing; unknown renewals do not confirm continued authority.
/// Fencing protects Inbox writes, not exactly-once external effects. There are no hidden retries.
/// </summary>
public interface IInboxProcessingStore
{
    Task<InboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan leaseDuration, CancellationToken cancellationToken);
    Task<InboxMutationResult> RenewAsync(InboxLease lease, TimeSpan leaseDuration, CancellationToken cancellationToken);
    Task<InboxMutationResult> CompleteAsync(InboxLease lease, CancellationToken cancellationToken);
    Task<InboxMutationResult> ScheduleRetryAsync(InboxLease lease, TimeSpan retryDelay, CancellationToken cancellationToken);
}
