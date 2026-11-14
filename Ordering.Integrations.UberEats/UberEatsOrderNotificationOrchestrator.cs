using System.Text;
using Ordering.Contracts.Partners;

namespace Ordering.Integrations.UberEats;

public sealed class UberEatsOrderNotificationOrchestrator
{
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);
    private readonly UberEatsSignatureVerifier _signatureVerifier;
    private readonly UberEatsOrderNotificationDeserializer _deserializer;
    private readonly UberEatsOrderNotificationMapper _mapper;

    public UberEatsOrderNotificationOrchestrator(
        UberEatsSignatureVerifier signatureVerifier,
        UberEatsOrderNotificationDeserializer deserializer,
        UberEatsOrderNotificationMapper mapper)
    {
        ArgumentNullException.ThrowIfNull(signatureVerifier);
        ArgumentNullException.ThrowIfNull(deserializer);
        ArgumentNullException.ThrowIfNull(mapper);

        _signatureVerifier = signatureVerifier;
        _deserializer = deserializer;
        _mapper = mapper;
    }

    /// <summary>
    /// Verifies the original body before decoding, deserializing and mapping it.
    /// The caller must keep the body and secret buffers unchanged during this call.
    /// Secret configuration validation belongs to application startup.
    /// </summary>
    public OrderNotification Process(
        ReadOnlySpan<byte> originalBody,
        string? signature,
        ReadOnlySpan<byte> sharedSecret)
    {
        if (!_signatureVerifier.Verify(originalBody, signature, sharedSecret))
        {
            throw new UberEatsInvalidSignatureException();
        }

        var json = StrictUtf8.GetString(originalBody);
        var notification = _deserializer.Deserialize(json);
        return _mapper.Map(notification);
    }
}
