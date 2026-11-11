using Ordering.Application.Inbox;

namespace Ordering.API;

/// <summary>Trusted host configuration, never populated from webhook headers or payloads.</summary>
public sealed class UberEatsIntegrationIdentity
{
    public const string PartnerId = "uber-eats";
    public string IntegrationScope { get; }

    public UberEatsIntegrationIdentity(string integrationScope)
    {
        _ = new InboxKey(PartnerId, integrationScope, "configuration-validation");
        IntegrationScope = integrationScope;
    }
}
