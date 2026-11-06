using System.Text.Json.Serialization;

namespace Ordering.Integrations.UberEats;

public sealed class UberEatsOrderNotificationDto
{
    [JsonPropertyName("event_type")]
    public string? EventType { get; init; }

    [JsonPropertyName("event_id")]
    public string? EventId { get; init; }

    [JsonPropertyName("meta")]
    public UberEatsOrderNotificationMetaDto? Meta { get; init; }
}
