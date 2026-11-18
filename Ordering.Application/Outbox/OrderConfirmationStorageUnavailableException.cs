namespace Ordering.Application.Outbox;

public sealed class OrderConfirmationStorageUnavailableException : Exception
{
    public OrderConfirmationStorageUnavailableException() : base("Order confirmation storage is temporarily unavailable.") { }
}
