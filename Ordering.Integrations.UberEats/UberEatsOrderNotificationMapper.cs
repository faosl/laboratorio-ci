using Ordering.Contracts.Partners;

namespace Ordering.Integrations.UberEats;

public sealed class UberEatsOrderNotificationMapper
{
    public OrderNotification Map(UberEatsOrderNotificationDto notification)
    {
        ArgumentNullException.ThrowIfNull(notification);

        // Event semantics belong to the partner adapter, before constructing the common contract.
        if (!string.Equals(notification.EventType, "orders.notification", StringComparison.Ordinal))
        {
            throw new ArgumentException("Only orders.notification events are supported.", nameof(notification));
        }

        if (notification.Meta is null)
        {
            throw new ArgumentException("The Uber Eats event must contain meta.", nameof(notification));
        }

        return new OrderNotification(notification.EventId!, notification.Meta.ResourceId!);
    }
}
