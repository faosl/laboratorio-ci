using System.Text;
using Npgsql;
using Ordering.API;
using Ordering.Application.Inbox;

namespace Ordering.API.LaboratoryHost;

public partial class LaboratoryProgram
{
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        // This executable is deliberately unusable outside the explicitly selected laboratory.
        if (!builder.Environment.IsEnvironment("Laboratory"))
            throw new InvalidOperationException("This host requires the Laboratory environment.");
        builder.WebHost.UseUrls("http://127.0.0.1:5088");
        LaboratoryComposition.Configure(builder.Services, builder.Configuration);
        var app = builder.Build();
        UberEatsOrderEndpoint.Map(app);
        app.Run();
    }
}

public static class LaboratoryComposition
{
    public static void Configure(IServiceCollection services, IConfiguration configuration)
    {
        string connectionString;
        UberEatsIntegrationIdentity identity;
        UberEatsSharedSecret secret;
        try
        {
            var connection = configuration["Inbox:ConnectionString"];
            var scope = configuration["UberEats:IntegrationScope"];
            var secretText = configuration["UberEats:SharedSecret"];
            if (string.IsNullOrWhiteSpace(connection) || string.IsNullOrWhiteSpace(scope)
                || string.IsNullOrWhiteSpace(secretText)) throw new ArgumentException();
            identity = new UberEatsIntegrationIdentity(scope);
            secret = new UberEatsSharedSecret(Encoding.UTF8.GetBytes(secretText));
            var settings = new NpgsqlConnectionStringBuilder(connection) { Enlist = false,
                IncludeErrorDetail = false, LogParameters = false };
            connectionString = settings.ConnectionString;
        }
        catch (Exception error) when (error is ArgumentException or FormatException)
        {
            // Never attach configuration values or provider exception details.
            throw new InvalidOperationException("Required laboratory Inbox/UberEats configuration is missing or invalid.");
        }
        services.AddSingleton(identity);
        services.AddSingleton(secret);
        // Factory registration makes the container responsible for disposing the data source.
        services.AddSingleton<NpgsqlDataSource>(_ => NpgsqlDataSource.Create(connectionString));
        services.AddSingleton<IInboxStore, Ordering.Infrastructure.PostgreSql.PostgresInboxStore>();
        services.AddSingleton<RegisterAuthenticatedOrderNotificationUseCase>();
        services.AddUberEatsOrchestrator();
    }
}
