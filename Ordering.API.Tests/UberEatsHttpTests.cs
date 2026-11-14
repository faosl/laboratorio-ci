using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Ordering.API;
using Ordering.Integrations.UberEats;

namespace Ordering.API.Tests;

public sealed class UberEatsHttpTests
{
    private static readonly byte[] Secret = Encoding.UTF8.GetBytes("synthetic-http-secret");
    private const string Valid = "{\"event_type\":\"orders.notification\",\"event_id\":\" event \",\"meta\":{\"resource_id\":\" order \"},\"extra\":\"café\"}\r\n";
    private static string Sign(byte[] body) => Convert.ToHexString(HMACSHA256.HashData(Secret, body)).ToLowerInvariant();

    private static WebApplicationFactory<Program> Host(string environment = "Laboratory", bool secret = true,
        string? failure = null) => new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
    {
        builder.UseEnvironment(environment);
        builder.ConfigureServices(services =>
        {
            if (secret) services.AddSingleton(new UberEatsSharedSecret(Secret));
            if (failure is not null) services.AddSingleton<IStartupFilter>(new FailureFilter(failure));
        });
    });

    private static HttpRequestMessage Request(byte[] body, string? signature, bool unknownLength = false)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, UberEatsOrderEndpoint.Route);
        request.Content = unknownLength ? new UnknownLengthContent(body) : new ByteArrayContent(body);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        if (signature is not null) request.Headers.TryAddWithoutValidation("X-Uber-Signature", signature);
        return request;
    }

    private static async Task AssertStatus(HttpClient client, HttpRequestMessage request, int status)
    {
        using var response = await client.SendAsync(request);
        Assert.Equal(status, (int)response.StatusCode);
        var text = await response.Content.ReadAsStringAsync();
        Assert.DoesNotContain("synthetic-http-secret", text);
        Assert.DoesNotContain(Valid, text);
        Assert.DoesNotContain("sensitive-failure", text);
        if (request.Headers.TryGetValues("X-Uber-Signature", out var values))
            foreach (var value in values.Where(value => value.Length >= 64)) Assert.DoesNotContain(value, text);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("abc")]
    [InlineData("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")]
    [InlineData("0000000000000000000000000000000000000000000000000000000000000000")]
    public async Task InvalidSignature_PrecedesInvalidUtf8(string? signature)
    {
        using var host = Host(); using var client = host.CreateClient();
        using var request = Request([0xff], signature);
        await AssertStatus(client, request, 403);
    }

    [Fact]
    public async Task MultipleSignatures_AreRejected()
    {
        using var host = Host(); using var client = host.CreateClient();
        var body = Encoding.UTF8.GetBytes(Valid);
        using var request = Request(body, Sign(body));
        request.Headers.TryAddWithoutValidation("X-Uber-Signature", Sign(body));
        await AssertStatus(client, request, 403);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ValidOriginalBytes_WithoutDurability_Return503(bool unknownLength)
    {
        using var host = Host(); using var client = host.CreateClient();
        var body = Encoding.UTF8.GetBytes(Valid);
        var notification = new UberEatsOrderNotificationOrchestrator(new(), new(), new()).Process(body, Sign(body), Secret);
        Assert.Equal(" event ", notification.EventId);
        Assert.Equal(" order ", notification.OrderId);
        using var request = Request(body, Sign(body), unknownLength);
        await AssertStatus(client, request, 503);
        using var repeated = Request(body, Sign(body), unknownLength);
        await AssertStatus(client, repeated, 503);
    }

    [Theory]
    [InlineData("\r\n", "\n")]
    [InlineData("café", "caf\\u00e9")]
    [InlineData("{\"event_type\"", "{ \"event_type\"")]
    public async Task EquivalentJson_WithChangedBytes_RejectsOriginalSignature(string original, string replacement)
    {
        using var host = Host(); using var client = host.CreateClient();
        using var request = Request(Encoding.UTF8.GetBytes(Valid.Replace(original, replacement)), Sign(Encoding.UTF8.GetBytes(Valid)));
        await AssertStatus(client, request, 403);
    }

    [Theory]
    [InlineData("")]
    [InlineData("{")]
    [InlineData("null")]
    [InlineData("[]")]
    [InlineData("{\"event_type\":123}")]
    public async Task SignedInvalidJson_Returns400(string json)
    {
        using var host = Host(); using var client = host.CreateClient();
        var body = Encoding.UTF8.GetBytes(json);
        using var request = Request(body, Sign(body));
        await AssertStatus(client, request, 400);
    }

    [Fact]
    public async Task SignedInvalidUtf8_Returns400()
    {
        using var host = Host(); using var client = host.CreateClient();
        byte[] body = [0xff]; using var request = Request(body, Sign(body));
        await AssertStatus(client, request, 400);
    }

    [Theory]
    [InlineData("{\"event_type\":\"orders.cancel\"}")]
    [InlineData("{\"event_type\":\"orders.notification\"}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"meta\":{\"resource_id\":\"o\"}}")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\"e\",\"meta\":{\"resource_id\":\" \"}}")]
    public async Task SignedInvalidSemantics_Returns422(string json)
    {
        using var host = Host(); using var client = host.CreateClient();
        var body = Encoding.UTF8.GetBytes(json); using var request = Request(body, Sign(body));
        await AssertStatus(client, request, 422);
    }

    [Theory]
    [InlineData("text/plain")]
    [InlineData("application/json; charset=iso-8859-1")]
    [InlineData(null)]
    public async Task UnsupportedContent_Returns415(string? contentType)
    {
        using var host = Host(); using var client = host.CreateClient();
        var body = Encoding.UTF8.GetBytes(Valid); using var request = Request(body, Sign(body));
        request.Content!.Headers.Remove("Content-Type");
        if (contentType is not null) request.Content.Headers.TryAddWithoutValidation("Content-Type", contentType);
        await AssertStatus(client, request, 415);
    }

    [Fact]
    public async Task EncodedContent_Returns415()
    {
        using var host = Host(); using var client = host.CreateClient();
        var body = Encoding.UTF8.GetBytes(Valid); using var request = Request(body, Sign(body));
        request.Content!.Headers.ContentEncoding.Add("gzip");
        await AssertStatus(client, request, 415);
    }

    [Fact]
    public async Task Utf8Charset_IsAcceptedWithoutTranscoding()
    {
        using var host = Host(); using var client = host.CreateClient();
        var body = Encoding.UTF8.GetBytes(Valid); using var request = Request(body, Sign(body));
        request.Content!.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json; charset=\"utf-8\"");
        await AssertStatus(client, request, 503);
    }

    [Fact]
    public async Task AbortedRequest_PropagatesCancellationWithoutAcknowledgement()
    {
        using var host = Host(failure: "abort"); using var client = host.CreateClient();
        var body = Encoding.UTF8.GetBytes(Valid); using var request = Request(body, Sign(body));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.SendAsync(request));
    }

    [Theory]
    [InlineData(false, 0, 503)]
    [InlineData(true, 0, 503)]
    [InlineData(false, 1, 413)]
    [InlineData(true, 1, 413)]
    public async Task BodyLimit_IsEnforced(bool unknownLength, int excess, int status)
    {
        using var host = Host(); using var client = host.CreateClient();
        var body = Encoding.UTF8.GetBytes(Valid.PadRight(UberEatsOrderEndpoint.MaxBodyBytes - 1 + excess));
        Assert.Equal(UberEatsOrderEndpoint.MaxBodyBytes + excess, body.Length); // café adds one UTF-8 byte
        using var request = Request(body, Sign(body), unknownLength);
        await AssertStatus(client, request, status);
    }

    [Theory]
    [InlineData("unexpected", 500)]
    [InlineData("timeout", 503)]
    public async Task ReadingFailure_IsSanitized(string failure, int status)
    {
        using var host = Host(failure: failure); using var client = host.CreateClient();
        var body = Encoding.UTF8.GetBytes(Valid); using var request = Request(body, Sign(body));
        await AssertStatus(client, request, status);
    }

    [Fact]
    public async Task MissingSecret_DoesNotAcknowledge()
    {
        using var host = Host(secret: false); using var client = host.CreateClient();
        var body = Encoding.UTF8.GetBytes(Valid); using var request = Request(body, Sign(body));
        await AssertStatus(client, request, 503);
    }

    [Theory]
    [InlineData("Production")]
    [InlineData("Development")]
    public async Task Route_IsAbsentOutsideLaboratory(string environment)
    {
        using var host = Host(environment); using var client = host.CreateClient();
        using var request = Request([], null);
        await AssertStatus(client, request, 404);
    }

    [Fact]
    public async Task OtherMethod_Returns405()
    {
        using var host = Host(); using var client = host.CreateClient();
        using var response = await client.GetAsync(UberEatsOrderEndpoint.Route);
        Assert.Equal(HttpStatusCode.MethodNotAllowed, response.StatusCode);
    }

    private sealed class UnknownLengthContent(byte[] bytes) : HttpContent
    {
        protected override bool TryComputeLength(out long length) { length = 0; return false; }
        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            for (var offset = 0; offset < bytes.Length; offset += 4096)
                await stream.WriteAsync(bytes.AsMemory(offset, Math.Min(4096, bytes.Length - offset)));
        }
    }

    private sealed class FailureFilter(string failure) : IStartupFilter
    {
        public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next) => app =>
        {
            app.Use(async (context, continuation) =>
            {
                if (failure == "abort") context.RequestAborted = new CancellationToken(true);
                context.Request.Body = new FailingStream(failure);
                await continuation(context);
            });
            next(app);
        };
    }

    private sealed class FailingStream(string failure) : MemoryStream
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => failure is "timeout" or "abort"
                ? ValueTask.FromException<int>(new OperationCanceledException("sensitive-failure"))
                : ValueTask.FromException<int>(new InvalidOperationException("sensitive-failure"));
    }
}
