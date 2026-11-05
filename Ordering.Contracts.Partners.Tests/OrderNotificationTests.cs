using Ordering.Contracts.Partners;

namespace Ordering.Contracts.Partners.Tests;

public class OrderNotificationTests
{
    [Theory]
    [InlineData("event-123", "order-456")]
    [InlineData(" event-123 ", " order-456 ")]
    public void Constructor_WithValidIdentifiers_PreservesIdentifiers(string eventId, string orderId)
    {
        var notification = new OrderNotification(eventId, orderId);

        Assert.Equal(eventId, notification.EventId);
        Assert.Equal(orderId, notification.OrderId);
    }

    [Fact]
    public void Constructor_WithNullEventId_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(
            () => new OrderNotification(null!, "order-456"));

        Assert.Equal("eventId", exception.ParamName);
    }

    [Fact]
    public void Constructor_WithNullOrderId_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(
            () => new OrderNotification("event-123", null!));

        Assert.Equal("orderId", exception.ParamName);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("   ")]
    [InlineData("\t")]
    [InlineData("\r\n")]
    [InlineData(" \t\r\n ")]
    public void Constructor_WithBlankEventId_ThrowsArgumentException(string eventId)
    {
        var exception = Assert.Throws<ArgumentException>(
            () => new OrderNotification(eventId, "order-456"));

        Assert.Equal("eventId", exception.ParamName);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("   ")]
    [InlineData("\t")]
    [InlineData("\r\n")]
    [InlineData(" \t\r\n ")]
    public void Constructor_WithBlankOrderId_ThrowsArgumentException(string orderId)
    {
        var exception = Assert.Throws<ArgumentException>(
            () => new OrderNotification("event-123", orderId));

        Assert.Equal("orderId", exception.ParamName);
    }
}
