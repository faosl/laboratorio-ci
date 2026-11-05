namespace Ordering.Contracts.Partners;

public sealed class OrderNotification
{
    public OrderNotification(string eventId, string orderId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventId);
        ArgumentException.ThrowIfNullOrWhiteSpace(orderId);

        EventId = eventId;
        OrderId = orderId;
    }

    public string EventId { get; }

    public string OrderId { get; }
}
