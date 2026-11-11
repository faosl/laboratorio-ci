using Ordering.Application.Inbox;
using Ordering.Contracts.Partners;

namespace Ordering.Application.Tests;

public sealed class RegisterAuthenticatedOrderNotificationTests
{
    [Theory]
    [InlineData(InboxRegistrationStatus.Inserted)]
    [InlineData(InboxRegistrationStatus.AlreadyExists)]
    [InlineData(InboxRegistrationStatus.Conflict)]
    [InlineData(InboxRegistrationStatus.OutcomeUnknown)]
    public async Task ComposesAuthenticatedEnvelopeAndReturnsUnchangedResult(InboxRegistrationStatus status)
    {
        var result = new InboxRegistrationResult(status, status == InboxRegistrationStatus.OutcomeUnknown ? null : Guid.NewGuid());
        var store = new CapturingStore(result);
        byte[] original = [123, 13, 10, 255];
        var normalized = new OrderNotification(" event ", " order ");
        using var cancellation = new CancellationTokenSource();
        Assert.Same(result, await new RegisterAuthenticatedOrderNotificationUseCase(store).ExecuteAsync(
            normalized, original, "uber-eats", "stable-integration", cancellation.Token));
        original[0] = 0;
        Assert.Equal(new InboxKey("uber-eats", "stable-integration", " event "), store.Content!.Key);
        Assert.Same(normalized, store.Content.Notification);
        Assert.Equal(1, store.Content.NormalizedVersion);
        Assert.Equal(123, store.Content.OriginalPayload[0]);
        Assert.Equal(cancellation.Token, store.Token);
    }

    private sealed class CapturingStore(InboxRegistrationResult result) : IInboxStore
    {
        public InboxNotification? Content { get; private set; }
        public CancellationToken Token { get; private set; }
        public Task<InboxRegistrationResult> RegisterAsync(InboxNotification content, CancellationToken token)
        { Content = content; Token = token; return Task.FromResult(result); }
        public Task<InboxEntry?> GetByKeyAsync(InboxKey key, CancellationToken token) => throw new NotSupportedException();
    }
}
