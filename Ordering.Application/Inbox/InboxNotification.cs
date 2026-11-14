using Ordering.Contracts.Partners;

namespace Ordering.Application.Inbox;

/// <summary>Caller must authenticate the original body before constructing this envelope.</summary>
public sealed class InboxNotification
{
    private readonly byte[] _originalPayload;

    public InboxNotification(InboxKey key, OrderNotification notification,
        int normalizedVersion, byte[] originalPayload)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(notification);
        ArgumentNullException.ThrowIfNull(originalPayload);
        if (key.EventId != notification.EventId)
            throw new ArgumentException("Event identifiers must match.", nameof(notification));
        if (notification.OrderId.Length > 200 || notification.OrderId.Contains('\0'))
            throw new ArgumentException("Order identifier exceeds storage limits.", nameof(notification));
        if (normalizedVersion != 1)
            throw new ArgumentOutOfRangeException(nameof(normalizedVersion), "Only normalized version 1 is supported.");
        if (originalPayload.Length is 0 or > 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(originalPayload));
        Key = key;
        Notification = notification;
        NormalizedVersion = normalizedVersion;
        _originalPayload = originalPayload.ToArray();
    }

    public InboxKey Key { get; }
    public OrderNotification Notification { get; }
    public int NormalizedVersion { get; }
    public byte[] OriginalPayload => _originalPayload.ToArray();
}
