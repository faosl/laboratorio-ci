namespace Ordering.Integrations.UberEats.Tests;

public class UberEatsOrderNotificationMapperTests
{
    private readonly UberEatsOrderNotificationMapper _mapper = new();

    [Theory]
    [InlineData("event-123", "order-456")]
    [InlineData(" event-123 ", " order-456 ")]
    public void Map_WithNotificationEvent_PreservesIdentifiers(string eventId, string orderId)
    {
        var result = _mapper.Map(CreateDto("orders.notification", eventId, orderId));

        Assert.Equal(eventId, result.EventId);
        Assert.Equal(orderId, result.OrderId);
    }

    [Theory]
    [InlineData("orders.cancel")]
    [InlineData("orders.release")]
    [InlineData("other.event")]
    [InlineData("ORDERS.NOTIFICATION")]
    [InlineData(" orders.notification ")]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(null)]
    public void Map_WithUnsupportedEventAndValidIdentifiers_ThrowsArgumentException(string? eventType)
    {
        var exception = Assert.Throws<ArgumentException>(
            () => _mapper.Map(CreateDto(eventType, "event-123", "order-456")));

        Assert.Equal("notification", exception.ParamName);
    }

    [Fact]
    public void Map_WithUnsupportedEventAndMissingData_RejectsEventTypeFirst()
    {
        var exception = Assert.Throws<ArgumentException>(() => _mapper.Map(
            new UberEatsOrderNotificationDto { EventType = "orders.cancel" }));

        Assert.StartsWith("Only orders.notification events are supported.", exception.Message);
    }

    [Fact]
    public void Map_WithNullDto_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => _mapper.Map(null!));
    }

    [Fact]
    public void Map_WithMissingMeta_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => _mapper.Map(new UberEatsOrderNotificationDto
        {
            EventType = "orders.notification",
            EventId = "event-123"
        }));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t\r\n")]
    public void Map_WithInvalidEventId_UsesCommonContractValidation(string? eventId)
    {
        var exception = Assert.ThrowsAny<ArgumentException>(
            () => _mapper.Map(CreateDto("orders.notification", eventId, "order-456")));

        Assert.Equal("eventId", exception.ParamName);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t\r\n")]
    public void Map_WithInvalidOrderId_UsesCommonContractValidation(string? orderId)
    {
        var exception = Assert.ThrowsAny<ArgumentException>(
            () => _mapper.Map(CreateDto("orders.notification", "event-123", orderId)));

        Assert.Equal("orderId", exception.ParamName);
    }

    private static UberEatsOrderNotificationDto CreateDto(string? eventType, string? eventId, string? orderId)
    {
        return new UberEatsOrderNotificationDto
        {
            EventType = eventType,
            EventId = eventId,
            Meta = new UberEatsOrderNotificationMetaDto { ResourceId = orderId }
        };
    }
}
