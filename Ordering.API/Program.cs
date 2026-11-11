var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHealthChecks();
Ordering.API.UberEatsServices.AddUberEatsOrchestrator(builder.Services);

var app = builder.Build();
app.MapGet("/", () => Results.Ok(new { service = "Ordering.API" }));
app.MapHealthChecks("/health");
// No production secret loading or operational webhook activation in this phase.
if (app.Environment.IsEnvironment("Laboratory"))
    Ordering.API.UberEatsOrderEndpoint.Map(app);
app.Run();

public partial class Program { }
