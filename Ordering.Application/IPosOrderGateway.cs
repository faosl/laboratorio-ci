using Ordering.Dominio;

namespace Ordering.Application;

public interface IPosOrderGateway
{
    Task<PosDispatchStatus> DispatchAsync(
        Order order,
        CancellationToken cancellationToken);
}
