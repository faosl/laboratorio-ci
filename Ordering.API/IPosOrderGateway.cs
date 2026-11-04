using Ordering.Dominio;

namespace Ordering.API;

public enum PosDispatchStatus
{
    ReceiptConfirmed,
    Rejected,
    OutcomeUnknown
}

public interface IPosOrderGateway
{
    Task<PosDispatchStatus> DispatchAsync(
        Order order,
        CancellationToken cancellationToken);
}
