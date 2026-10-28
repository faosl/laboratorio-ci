var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHealthChecks();

var app = builder.Build();
app.MapGet("/", () => Results.Ok(new { service = "Ordering.API" }));
app.MapHealthChecks("/health");
app.Run();
