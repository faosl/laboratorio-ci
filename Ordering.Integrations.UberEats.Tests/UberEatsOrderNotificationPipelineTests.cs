using System.Text.Json;

namespace Ordering.Integrations.UberEats.Tests;

public class UberEatsOrderNotificationPipelineTests
{
    private readonly UberEatsOrderNotificationDeserializer _deserializer = new();
    private readonly UberEatsOrderNotificationMapper _mapper = new();

    [Fact]
    public void DeserializeThenMap_WithValidNotification_PreservesOriginalIdentifiers()
    {
        const string json = """
            {
              "extra_field": "ignored",
              "meta": {"resource_id": " order-456 ", "extra_meta": true},
              "event_id": " event-123 ",
              "event_type": "orders.notification"
            }
            """;

        var result = _mapper.Map(_deserializer.Deserialize(json));

        Assert.Equal(" event-123 ", result.EventId);
        Assert.Equal(" order-456 ", result.OrderId);
    }

    [Theory]
    [InlineData("orders.cancel")]
    [InlineData("orders.release")]
    [InlineData("other.event")]
    public void DeserializeThenMap_WithUnsupportedEvent_RejectsDespiteValidIdentifiers(string eventType)
    {
        var json = JsonSerializer.Serialize(new
        {
            event_type = eventType,
            event_id = "event-123",
            meta = new { resource_id = "order-456" }
        });

        Assert.Throws<ArgumentException>(() => _mapper.Map(_deserializer.Deserialize(json)));
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{\"event_id\":\"event-123\",\"meta\":{\"resource_id\":\"order-456\"}}")]
    [InlineData("{\"event_type\":null,\"event_id\":\"event-123\",\"meta\":{\"resource_id\":\"order-456\"}}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"meta\":{\"resource_id\":\"order-456\"}}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":null,\"meta\":{\"resource_id\":\"order-456\"}}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\"\",\"meta\":{\"resource_id\":\"order-456\"}}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\" \",\"meta\":{\"resource_id\":\"order-456\"}}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\"event-123\"}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\"event-123\",\"meta\":null}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\"event-123\",\"meta\":{}}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\"event-123\",\"meta\":{\"resource_id\":null}}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\"event-123\",\"meta\":{\"resource_id\":\"\"}}")]
    [InlineData("""{"event_type":"orders.notification","event_id":"event-123","meta":{"resource_id":" "}}""")]
    public void DeserializeThenMap_WithMissingOrInvalidRequiredFields_DoesNotProduceNotification(string json)
    {
        Assert.ThrowsAny<ArgumentException>(() => _mapper.Map(_deserializer.Deserialize(json)));
    }

    [Theory]
    [InlineData("{")]
    [InlineData("null")]
    [InlineData("[]")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":123,\"meta\":{\"resource_id\":\"order-456\"}}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\"event-123\",\"meta\":[]}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\"event-123\",\"meta\":{\"resource_id\":123}}")]
    public void DeserializeThenMap_WithMalformedOrIncompatibleJson_DoesNotProduceNotification(string json)
    {
        Assert.Throws<JsonException>(() => _mapper.Map(_deserializer.Deserialize(json)));
    }
}
