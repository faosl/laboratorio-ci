using System.Text.Json.Serialization;

namespace Ordering.Integrations.UberEats;

public sealed class UberEatsOrderNotificationMetaDto
{
    [JsonPropertyName("resource_id")]
    public string? ResourceId { get; init; }
}
