using System.Text.Json;

namespace Ordering.Integrations.UberEats.Tests;

public class UberEatsOrderNotificationDeserializerTests
{
    private readonly UberEatsOrderNotificationDeserializer _deserializer = new();

    [Fact]
    public void Deserialize_WithValidJson_ReadsPartnerFields()
    {
        var dto = _deserializer.Deserialize("""
            {"event_type":"orders.notification","event_id":"event-123","meta":{"resource_id":"order-456"}}
            """);

        Assert.Equal("orders.notification", dto.EventType);
        Assert.Equal("event-123", dto.EventId);
        Assert.Equal("order-456", dto.Meta?.ResourceId);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("{")]
    [InlineData("{\"event_id\":}")]
    [InlineData("null")]
    [InlineData("[]")]
    [InlineData("123")]
    [InlineData("true")]
    [InlineData("\"event\"")]
    [InlineData("{\"event_type\":123}")]
    [InlineData("{\"event_id\":123}")]
    [InlineData("{\"event_id\":{}}")]
    [InlineData("{\"meta\":[]}")]
    [InlineData("{\"meta\":\"order\"}")]
    [InlineData("{\"meta\":{\"resource_id\":123}}")]
    [InlineData("{\"meta\":{\"resource_id\":{}}}")]
    public void Deserialize_WithMalformedOrIncompatibleJson_ThrowsJsonException(string json)
    {
        Assert.Throws<JsonException>(() => _deserializer.Deserialize(json));
    }

    [Fact]
    public void Deserialize_WithNullInput_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => _deserializer.Deserialize(null!));
    }

    [Fact]
    public void Deserialize_WithUnsupportedEvent_LeavesSemanticValidationToMapper()
    {
        var dto = _deserializer.Deserialize("""{"event_type":"orders.cancel"}""");

        Assert.Equal("orders.cancel", dto.EventType);
        Assert.Null(dto.EventId);
        Assert.Null(dto.Meta);
    }
}
