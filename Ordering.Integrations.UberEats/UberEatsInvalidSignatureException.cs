namespace Ordering.Integrations.UberEats;

public sealed class UberEatsInvalidSignatureException : Exception
{
    public UberEatsInvalidSignatureException()
        : base("The Uber Eats notification signature is invalid.")
    {
    }
}
