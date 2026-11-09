var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHealthChecks();
builder.Services.AddSingleton<Ordering.Integrations.UberEats.UberEatsSignatureVerifier>();
builder.Services.AddSingleton<Ordering.Integrations.UberEats.UberEatsOrderNotificationDeserializer>();
builder.Services.AddSingleton<Ordering.Integrations.UberEats.UberEatsOrderNotificationMapper>();
builder.Services.AddSingleton<Ordering.Integrations.UberEats.UberEatsOrderNotificationOrchestrator>();

var app = builder.Build();
app.MapGet("/", () => Results.Ok(new { service = "Ordering.API" }));
app.MapHealthChecks("/health");
// No production secret loading or operational webhook activation in this phase.
if (app.Environment.IsEnvironment("Laboratory"))
    Ordering.API.UberEatsOrderEndpoint.Map(app);
app.Run();

public partial class Program { }
