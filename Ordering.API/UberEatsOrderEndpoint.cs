using System.Buffers;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Ordering.Integrations.UberEats;
using Ordering.Application.Inbox;
using Ordering.Contracts.Partners;

namespace Ordering.API;

public static class UberEatsOrderEndpoint
{
    public const int MaxBodyBytes = 1024 * 1024;
    public const string Route = "/webhooks/uber-eats/orders";

    public static void Map(WebApplication app)
    {
        if (app.Environment.IsEnvironment("Laboratory")) app.MapPost(Route, HandleAsync);
    }

    private static async Task<IResult> HandleAsync(HttpContext context,
        UberEatsOrderNotificationOrchestrator orchestrator)
    {
        var request = context.Request;
        try
        {
            if (!MediaTypeHeaderValue.TryParse(request.ContentType, out var contentType)
                || !string.Equals(contentType.MediaType, "application/json", StringComparison.OrdinalIgnoreCase)
                || (contentType.CharSet is { } charset
                    && !string.Equals(charset.Trim('"'), "utf-8", StringComparison.OrdinalIgnoreCase))
                || request.Headers.ContainsKey("Content-Encoding"))
                return Error(415);

            if (request.ContentLength > MaxBodyBytes) return Error(413);
            if (!request.Headers.TryGetValue("X-Uber-Signature", out var signatures)
                || signatures.Count != 1)
                return Error(403);

            using var body = new MemoryStream();
            var buffer = ArrayPool<byte>.Shared.Rent(8192);
            try
            {
                int read;
                while ((read = await request.Body.ReadAsync(buffer.AsMemory(0, 8192), context.RequestAborted)) != 0)
                {
                    if (body.Length + read > MaxBodyBytes) return Error(413);
                    await body.WriteAsync(buffer.AsMemory(0, read), context.RequestAborted);
                }
            }
            finally { ArrayPool<byte>.Shared.Return(buffer, clearArray: true); }

            var secret = context.RequestServices.GetService<UberEatsSharedSecret>();
            if (secret is null) return Error(503);

            var original = body.ToArray();
            OrderNotification notification;
            try
            {
                notification = orchestrator.Process(original, signatures[0], secret.Bytes);
            }
            catch (UberEatsInvalidSignatureException) { return Error(403); }
            catch (DecoderFallbackException) { return Error(400); }
            catch (JsonException) { return Error(400); }
            catch (ArgumentException exception) when (exception.ParamName is "notification" or "eventId" or "orderId")
            { return Error(422); }

            var identity = context.RequestServices.GetService<UberEatsIntegrationIdentity>();
            var useCase = context.RequestServices.GetService<RegisterAuthenticatedOrderNotificationUseCase>();
            if (identity is null || useCase is null) return Error(503);

            // Validate only client-supplied identifiers here; host configuration is validated separately.
            if (notification.EventId.Length > 200 || notification.EventId.Contains('\0')
                || notification.OrderId.Length > 200 || notification.OrderId.Contains('\0')) return Error(422);

            InboxRegistrationResult result;
            try
            {
                result = await useCase.ExecuteAsync(notification, original, UberEatsIntegrationIdentity.PartnerId,
                    identity.IntegrationScope, context.RequestAborted);
            }
            catch (InboxStorageUnavailableException) { return Error(503); }
            catch (IOException) { return Error(503); }
            catch (TimeoutException) { return Error(503); }

            return result.Status switch
            {
                InboxRegistrationStatus.Inserted or InboxRegistrationStatus.AlreadyExists
                    when result.InboxId is not null => Results.Ok(),
                InboxRegistrationStatus.Conflict => Error(409),
                InboxRegistrationStatus.OutcomeUnknown => Error(503),
                _ => Error(500)
            };
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested) { throw; }
        catch (OperationCanceledException) { return Error(503); }
        catch (BadHttpRequestException exception) when (exception.StatusCode is 400 or 413)
        { return Error(exception.StatusCode); }
        catch (Exception)
        {
            // Do not log exception messages, bodies, signatures or secret material.
            context.RequestServices.GetRequiredService<ILoggerFactory>()
                .CreateLogger("UberEatsOrderEndpoint").LogError("Webhook request failed unexpectedly.");
            return Error(500);
        }
    }

    private static IResult Error(int status) => Results.Problem(statusCode: status,
        title: "Webhook request could not be accepted.");
}
