namespace Ordering.Application.Inbox;

/// <summary>A storage adapter identified a temporary availability failure before a confirmed result.</summary>
public sealed class InboxStorageUnavailableException : Exception
{
    public InboxStorageUnavailableException() : base("Inbox storage is temporarily unavailable.") { }
}
