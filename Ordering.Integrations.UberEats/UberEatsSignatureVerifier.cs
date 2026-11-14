using System.Security.Cryptography;

namespace Ordering.Integrations.UberEats;

public sealed class UberEatsSignatureVerifier
{
    /// <summary>
    /// Verifies X-Uber-Signature against the original HTTP body bytes, before JSON deserialization.
    /// </summary>
    /// <param name="originalBody">The unmodified bytes received in the HTTP request body.</param>
    /// <param name="signature">The header value, encoded as 64 lowercase hexadecimal characters.</param>
    /// <param name="sharedSecret">The previously configured secret bytes. Configuration validation belongs to application startup.</param>
    public bool Verify(ReadOnlySpan<byte> originalBody, string? signature, ReadOnlySpan<byte> sharedSecret)
    {
        if (signature is null || signature.Length != 64)
        {
            return false;
        }

        foreach (var character in signature)
        {
            if (character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f'))
            {
                return false;
            }
        }

        var receivedHash = Convert.FromHexString(signature);
        var expectedHash = HMACSHA256.HashData(sharedSecret, originalBody);

        return CryptographicOperations.FixedTimeEquals(expectedHash, receivedHash);
    }
}
