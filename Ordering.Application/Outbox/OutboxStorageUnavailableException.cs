namespace Ordering.Application.Outbox;

public sealed class OutboxStorageUnavailableException : Exception
{
    public OutboxStorageUnavailableException() : base("Outbox storage is temporarily unavailable.") { }
}
