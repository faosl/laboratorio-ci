namespace Ordering.Application.Inbox;

public sealed record InboxProcessingOptions
{
    public InboxProcessingOptions(TimeSpan leaseDuration, TimeSpan renewalInterval,
        TimeSpan storeOperationTimeout, TimeSpan shutdownTimeout)
    {
        InboxProcessingLimits.ValidateLeaseDuration(leaseDuration);
        if (renewalInterval < TimeSpan.FromMilliseconds(1) || renewalInterval >= leaseDuration)
            throw new ArgumentOutOfRangeException(nameof(renewalInterval));
        // A local budget abandons unconfirmed authority; it never proves database ownership.
        if (storeOperationTimeout < TimeSpan.FromMilliseconds(1) || storeOperationTimeout >= leaseDuration - renewalInterval)
            throw new ArgumentOutOfRangeException(nameof(storeOperationTimeout));
        if (shutdownTimeout < TimeSpan.FromMilliseconds(1) || shutdownTimeout.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(nameof(shutdownTimeout));
        LeaseDuration = leaseDuration;
        RenewalInterval = renewalInterval;
        StoreOperationTimeout = storeOperationTimeout;
        ShutdownTimeout = shutdownTimeout;
    }

    public TimeSpan LeaseDuration { get; }
    public TimeSpan RenewalInterval { get; }
    public TimeSpan StoreOperationTimeout { get; }
    public TimeSpan ShutdownTimeout { get; }
}
