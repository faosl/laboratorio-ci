using System.Text;

namespace Ordering.Integrations.UberEats.Tests;

public class UberEatsSignatureVerifierTests
{
    // RFC 4231, section 4.3 (test case 2). Expected signatures are not computed by the tests.
    // https://www.rfc-editor.org/rfc/rfc4231.html#section-4.3
    private const string RfcSignature = "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843";
    private const string RfcBody = "what do ya want for nothing?";
    private const string RfcSecret = "Jefe";

    // Fixed supplementary fixtures for UTF-8/CRLF preservation and the empty body.
    private const string Utf8Body = "{\"name\":\"caf\u00e9\"}\r\n";
    private const string Utf8Signature = "ba9d821f3a3865a9e2490400f8150b295299384d920a752ae9c91b5896c3ad9f";
    private const string EmptyBodySignature = "a41bc6d81d6413576ae0994995e0ad89a416ec97389515c3604f47722122eeeb";

    private readonly UberEatsSignatureVerifier _verifier = new();

    [Fact]
    public void Verify_WithRfc4231TestCase2_ReturnsTrue()
    {
        Assert.True(_verifier.Verify(Encoding.UTF8.GetBytes(RfcBody), RfcSignature, Encoding.UTF8.GetBytes(RfcSecret)));
    }

    [Fact]
    public void Verify_WithRfc4231TestCase3_PreservesNonTextBytes()
    {
        // RFC 4231, section 4.4: 20 bytes of 0xaa as key, 50 bytes of 0xdd as data.
        var body = Enumerable.Repeat((byte)0xdd, 50).ToArray();
        var secret = Enumerable.Repeat((byte)0xaa, 20).ToArray();
        const string signature = "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe";

        Assert.True(_verifier.Verify(body, signature, secret));
    }

    [Fact]
    public void Verify_WithOriginalUtf8BodyAndCrLf_ReturnsTrue()
    {
        Assert.True(_verifier.Verify(Encoding.UTF8.GetBytes(Utf8Body), Utf8Signature, Encoding.UTF8.GetBytes("test-secret")));
    }

    [Fact]
    public void Verify_WithEmptyBodyAndMatchingSignature_ReturnsTrue()
    {
        // Cryptographic validity does not imply a valid JSON notification.
        Assert.True(_verifier.Verify(ReadOnlySpan<byte>.Empty, EmptyBodySignature, Encoding.UTF8.GetBytes("test-secret")));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(31)]
    [InlineData(63)]
    public void Verify_WithDifferentWellFormedSignature_ReturnsFalse(int position)
    {
        var signature = RfcSignature.ToCharArray();
        signature[position] = signature[position] == '0' ? '1' : '0';

        Assert.False(_verifier.Verify(Encoding.UTF8.GetBytes(RfcBody), new string(signature), Encoding.UTF8.GetBytes(RfcSecret)));
    }

    [Fact]
    public void Verify_WithOneBodyByteChanged_ReturnsFalse()
    {
        var body = Encoding.UTF8.GetBytes(RfcBody);
        body[0] ^= 1;

        Assert.False(_verifier.Verify(body, RfcSignature, Encoding.UTF8.GetBytes(RfcSecret)));
    }

    [Fact]
    public void Verify_WithDifferentSecret_ReturnsFalse()
    {
        Assert.False(_verifier.Verify(Encoding.UTF8.GetBytes(RfcBody), RfcSignature, Encoding.UTF8.GetBytes("Other secret")));
    }

    [Theory]
    [InlineData("{\"name\":\"caf\u00e9\"}")]
    [InlineData("{\"name\":\"caf\u00e9\"}\n")]
    [InlineData("{ \"name\": \"caf\u00e9\" }\r\n")]
    [InlineData("{\"name\":\"caf\\u00e9\"}\r\n")]
    public void Verify_WithEquivalentJsonButDifferentBytes_ReturnsFalse(string body)
    {
        Assert.False(_verifier.Verify(Encoding.UTF8.GetBytes(body), Utf8Signature, Encoding.UTF8.GetBytes("test-secret")));
    }

    [Fact]
    public void Verify_WithDifferentBodyEncoding_ReturnsFalse()
    {
        Assert.False(_verifier.Verify(Encoding.Unicode.GetBytes(Utf8Body), Utf8Signature, Encoding.UTF8.GetBytes("test-secret")));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec384")]
    [InlineData(RfcSignature + "0")]
    [InlineData("g" + "bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843")]
    [InlineData(" " + "bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843")]
    [InlineData("5BDCC146BF60754E6A042426089575C75A003F089D2739839DEC58B964EC3843")]
    [InlineData("5Bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843")]
    [InlineData(" " + RfcSignature)]
    [InlineData(RfcSignature + " ")]
    [InlineData(RfcSignature + "\r\n")]
    [InlineData("sha256=" + RfcSignature)]
    [InlineData(RfcSignature + "," + RfcSignature)]
    public void Verify_WithMissingOrMalformedSignature_ReturnsFalse(string? signature)
    {
        Assert.False(_verifier.Verify(Encoding.UTF8.GetBytes(RfcBody), signature, Encoding.UTF8.GetBytes(RfcSecret)));
    }
}
