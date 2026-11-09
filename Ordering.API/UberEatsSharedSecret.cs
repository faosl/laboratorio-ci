namespace Ordering.API;

// Supplied by the composing laboratory host; never loaded by the HTTP adapter.
public sealed class UberEatsSharedSecret
{
    private readonly byte[] _bytes;

    public UberEatsSharedSecret(ReadOnlySpan<byte> bytes) => _bytes = bytes.ToArray();

    public ReadOnlySpan<byte> Bytes => _bytes;
}
