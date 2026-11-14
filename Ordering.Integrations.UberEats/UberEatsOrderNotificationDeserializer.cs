using System.Text.Json;

namespace Ordering.Integrations.UberEats;

public sealed class UberEatsOrderNotificationDeserializer
{
    public UberEatsOrderNotificationDto Deserialize(string json)
    {
        ArgumentNullException.ThrowIfNull(json);

        return JsonSerializer.Deserialize<UberEatsOrderNotificationDto>(json)
            ?? throw new JsonException("The Uber Eats event must be a JSON object.");
    }
}
