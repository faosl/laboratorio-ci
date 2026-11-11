using Ordering.Integrations.UberEats;

namespace Ordering.API;

public static class UberEatsServices
{
    public static IServiceCollection AddUberEatsOrchestrator(this IServiceCollection services)
    {
        services.AddSingleton<UberEatsSignatureVerifier>();
        services.AddSingleton<UberEatsOrderNotificationDeserializer>();
        services.AddSingleton<UberEatsOrderNotificationMapper>();
        services.AddSingleton<UberEatsOrderNotificationOrchestrator>();
        return services;
    }
}
