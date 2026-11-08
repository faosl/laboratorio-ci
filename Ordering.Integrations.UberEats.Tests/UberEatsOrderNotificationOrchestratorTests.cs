using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Ordering.Integrations.UberEats.Tests;

public class UberEatsOrderNotificationOrchestratorTests
{
    private const string ValidJson = "{\"event_type\":\"orders.notification\",\"event_id\":\" event-123 \",\"meta\":{\"resource_id\":\" order-456 \"},\"extra\":\"caf\u00e9\"}\r\n";
    private static readonly byte[] Secret = Encoding.UTF8.GetBytes("test-secret");
    private readonly UberEatsOrderNotificationOrchestrator _orchestrator = new(
        new UberEatsSignatureVerifier(),
        new UberEatsOrderNotificationDeserializer(),
        new UberEatsOrderNotificationMapper());

    [Fact]
    public void Process_WithValidSignatureAndNotification_PreservesIdentifiers()
    {
        var body = Encoding.UTF8.GetBytes(ValidJson);

        var result = _orchestrator.Process(body, Sign(body), Secret);

        Assert.Equal(" event-123 ", result.EventId);
        Assert.Equal(" order-456 ", result.OrderId);
    }

    [Theory]
    [InlineData("{")]
    [InlineData("null")]
    [InlineData("{\"event_type\":\"orders.cancel\"}")]
    [InlineData("{\"event_type\":\"orders.notification\"}")]
    public void Process_WithInvalidSignature_RejectsBeforeJsonOrSemanticValidation(string json)
    {
        var body = Encoding.UTF8.GetBytes(json);

        Assert.Throws<UberEatsInvalidSignatureException>(
            () => _orchestrator.Process(body, new string('0', 64), Secret));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("abc")]
    [InlineData("gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg")]
    public void Process_WithMissingOrMalformedSignature_RejectsBeforeDeserialization(string? signature)
    {
        var body = Encoding.UTF8.GetBytes("{");

        Assert.Throws<UberEatsInvalidSignatureException>(
            () => _orchestrator.Process(body, signature, Secret));
    }

    [Fact]
    public void Process_WithUppercaseSignature_DoesNotNormalizeSignature()
    {
        var body = Encoding.UTF8.GetBytes(ValidJson);

        Assert.Throws<UberEatsInvalidSignatureException>(
            () => _orchestrator.Process(body, Sign(body).ToUpperInvariant(), Secret));
    }

    [Fact]
    public void Process_WithInvalidUtf8AndInvalidSignature_RejectsSignatureBeforeDecoding()
    {
        byte[] body = [0xff];

        Assert.Throws<UberEatsInvalidSignatureException>(
            () => _orchestrator.Process(body, new string('0', 64), Secret));
    }

    [Fact]
    public void Process_WithInvalidUtf8AndValidSignature_RejectsInvalidEncoding()
    {
        byte[] body = [0xff];

        Assert.Throws<DecoderFallbackException>(
            () => _orchestrator.Process(body, Sign(body), Secret));
    }

    [Theory]
    [InlineData("")]
    [InlineData("{")]
    [InlineData("null")]
    [InlineData("[]")]
    [InlineData("{\"event_type\":123}")]
    public void Process_WithValidSignatureAndInvalidJson_PropagatesJsonException(string json)
    {
        var body = Encoding.UTF8.GetBytes(json);

        Assert.Throws<JsonException>(() => _orchestrator.Process(body, Sign(body), Secret));
    }

    [Theory]
    [InlineData("{\"event_type\":\"orders.cancel\",\"event_id\":\"e\",\"meta\":{\"resource_id\":\"o\"}}", "notification")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\"e\"}", "notification")]
    [InlineData("{\"event_type\":\"orders.notification\",\"meta\":{\"resource_id\":\"o\"}}", "eventId")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\" \",\"meta\":{\"resource_id\":\"o\"}}", "eventId")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\"e\",\"meta\":{}}", "orderId")]
    [InlineData("{\"event_type\":\"orders.notification\",\"event_id\":\"e\",\"meta\":{\"resource_id\":\" \"}}", "orderId")]
    public void Process_WithValidSignatureAndInvalidSemantics_PreservesValidation(string json, string parameter)
    {
        var body = Encoding.UTF8.GetBytes(json);

        var exception = Assert.ThrowsAny<ArgumentException>(
            () => _orchestrator.Process(body, Sign(body), Secret));

        Assert.Equal(parameter, exception.ParamName);
    }

    [Theory]
    [InlineData("\r\n", "\n")]
    [InlineData("caf\u00e9", "caf\\u00e9")]
    [InlineData("{\"event_type\"", "{ \"event_type\"")]
    public void Process_WithEquivalentJsonButChangedBytes_RejectsOriginalSignature(string original, string replacement)
    {
        var body = Encoding.UTF8.GetBytes(ValidJson);
        var changedBody = Encoding.UTF8.GetBytes(ValidJson.Replace(original, replacement));

        Assert.Throws<UberEatsInvalidSignatureException>(
            () => _orchestrator.Process(changedBody, Sign(body), Secret));
    }

    [Fact]
    public void Process_WithDifferentSecret_RejectsSignature()
    {
        var body = Encoding.UTF8.GetBytes(ValidJson);
        var otherSecret = Encoding.UTF8.GetBytes("other-secret");

        Assert.Throws<UberEatsInvalidSignatureException>(
            () => _orchestrator.Process(body, Sign(body), otherSecret));
    }

    [Fact]
    public void Constructor_WithNullCollaborators_RejectsArguments()
    {
        var verifier = new UberEatsSignatureVerifier();
        var deserializer = new UberEatsOrderNotificationDeserializer();
        var mapper = new UberEatsOrderNotificationMapper();

        Assert.Throws<ArgumentNullException>(() => new UberEatsOrderNotificationOrchestrator(null!, deserializer, mapper));
        Assert.Throws<ArgumentNullException>(() => new UberEatsOrderNotificationOrchestrator(verifier, null!, mapper));
        Assert.Throws<ArgumentNullException>(() => new UberEatsOrderNotificationOrchestrator(verifier, deserializer, null!));
    }

    private static string Sign(byte[] body)
    {
        return Convert.ToHexString(HMACSHA256.HashData(Secret, body)).ToLowerInvariant();
    }
}
