using Ordering.Application.Inbox;

namespace Ordering.Application.Tests;

public sealed class InboxProcessingContractTests
{
    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\0")]
    public void LeaseRejectsInvalidOwner(string owner) =>
        Assert.ThrowsAny<ArgumentException>(() => new InboxLease(Guid.NewGuid(), owner, 1));

    [Fact]
    public void LeaseRejectsInvalidIdentityAndGeneration()
    {
        Assert.Throws<ArgumentException>(() => new InboxLease(Guid.Empty, "owner", 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InboxLease(Guid.NewGuid(), "owner", 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InboxLease(Guid.NewGuid(), "owner", -1));
        Assert.Throws<ArgumentException>(() => new InboxLease(Guid.NewGuid(), new string('x', 201), 1));
    }

    [Fact]
    public void DurationBoundsAndPrecisionAreExplicit()
    {
        InboxProcessingLimits.ValidateLeaseDuration(TimeSpan.FromSeconds(1));
        InboxProcessingLimits.ValidateLeaseDuration(TimeSpan.FromHours(1));
        InboxProcessingLimits.ValidateRetryDelay(TimeSpan.Zero);
        InboxProcessingLimits.ValidateRetryDelay(TimeSpan.FromDays(7));
        foreach (var duration in new[] { TimeSpan.Zero, TimeSpan.FromMilliseconds(999), TimeSpan.FromHours(1).Add(TimeSpan.FromTicks(10)), TimeSpan.FromSeconds(1).Add(TimeSpan.FromTicks(1)) })
            Assert.Throws<ArgumentOutOfRangeException>(() => InboxProcessingLimits.ValidateLeaseDuration(duration));
        foreach (var delay in new[] { TimeSpan.FromTicks(-10), TimeSpan.FromDays(7).Add(TimeSpan.FromTicks(10)), TimeSpan.FromTicks(1) })
            Assert.Throws<ArgumentOutOfRangeException>(() => InboxProcessingLimits.ValidateRetryDelay(delay));
    }
}
