using Ordering.Contracts.Partners;

namespace Ordering.Application.Inbox;

/// <summary>Receives a notification whose original bytes have already been authenticated.
/// The caller must supply the same unchanged bytes and a trusted host-configured identity.</summary>
public sealed class RegisterAuthenticatedOrderNotificationUseCase(IInboxStore store)
{
    public Task<InboxRegistrationResult> ExecuteAsync(OrderNotification notification,
        byte[] originalPayload, string partnerId, string integrationScope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(notification);
        var envelope = new InboxNotification(new InboxKey(partnerId, integrationScope, notification.EventId),
            notification, 1, originalPayload);
        return store.RegisterAsync(envelope, cancellationToken);
    }
}
