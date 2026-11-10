namespace Ordering.Application.Inbox;

public sealed record InboxKey
{
    public InboxKey(string partnerId, string integrationScope, string eventId)
    {
        Validate(partnerId, nameof(partnerId));
        Validate(integrationScope, nameof(integrationScope));
        Validate(eventId, nameof(eventId));
        PartnerId = partnerId;
        IntegrationScope = integrationScope;
        EventId = eventId;
    }

    private static void Validate(string value, string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, name);
        if (value.Length > 200 || value.Contains('\0'))
            throw new ArgumentException("Inbox identifiers must have at most 200 characters and no NUL.", name);
    }

    public string PartnerId { get; }
    public string IntegrationScope { get; }
    public string EventId { get; }
}
