using Ordering.Application.Outbox;

namespace Ordering.Application.Tests;

public sealed class OutboxProcessingContractTests
{
    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    [InlineData("\0")]
    public void LeaseRejectsInvalidOwner(string owner) =>
        Assert.ThrowsAny<ArgumentException>(() => new OutboxLease(Guid.NewGuid(), owner, 1));

    [Fact]
    public void LeaseRequiresEventIdentityAndPositiveGeneration()
    {
        Assert.Throws<ArgumentException>(() => new OutboxLease(Guid.Empty, "owner", 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new OutboxLease(Guid.NewGuid(), "owner", 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new OutboxLease(Guid.NewGuid(), "owner", -1));
        Assert.Throws<ArgumentException>(() => new OutboxLease(Guid.NewGuid(), new string('x', 201), 1));
        var lease = new OutboxLease(Guid.NewGuid(), new string('x', 200), long.MaxValue);
        Assert.Equal(long.MaxValue, lease.Generation);
    }

    [Fact]
    public void DurationBoundsAndMicrosecondPrecisionAreExplicit()
    {
        OutboxProcessingLimits.ValidateLeaseDuration(TimeSpan.FromSeconds(1));
        OutboxProcessingLimits.ValidateLeaseDuration(TimeSpan.FromHours(1));
        OutboxProcessingLimits.ValidateRetryDelay(TimeSpan.Zero);
        OutboxProcessingLimits.ValidateRetryDelay(TimeSpan.FromDays(7));
        foreach (var duration in new[] { TimeSpan.Zero, TimeSpan.FromMilliseconds(999), TimeSpan.FromHours(1).Add(TimeSpan.FromTicks(10)), TimeSpan.FromSeconds(1).Add(TimeSpan.FromTicks(1)) })
            Assert.Throws<ArgumentOutOfRangeException>(() => OutboxProcessingLimits.ValidateLeaseDuration(duration));
        foreach (var delay in new[] { TimeSpan.FromTicks(-10), TimeSpan.FromDays(7).Add(TimeSpan.FromTicks(10)), TimeSpan.FromTicks(1) })
            Assert.Throws<ArgumentOutOfRangeException>(() => OutboxProcessingLimits.ValidateRetryDelay(delay));
    }
}
