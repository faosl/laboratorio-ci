using Ordering.Application.Inbox;
using Ordering.Contracts.Partners;

namespace Ordering.Application.Tests;

public class InboxNotificationTests
{
    [Fact]
    public void Envelope_CopiesInputAndOutputBuffers()
    {
        byte[] bytes = [1, 2, 3];
        var envelope = new InboxNotification(new("partner", "scope", "event"), new("event", "order"), 1, bytes);
        bytes[0] = 99;
        var returned = envelope.OriginalPayload;
        returned[1] = 99;
        Assert.Equal(new byte[] { 1, 2, 3 }, envelope.OriginalPayload);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public void Envelope_RejectsUnsupportedNormalizedVersion(int version)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new InboxNotification(new("partner", "scope", "event"), new("event", "order"), version, [1]));
    }

    [Fact]
    public void Envelope_RejectsInconsistentEventIdentity()
    {
        Assert.Throws<ArgumentException>(() =>
            new InboxNotification(new("partner", "scope", "event"), new("other", "order"), 1, [1]));
    }
}
