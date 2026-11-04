using Ordering.Dominio;

namespace Ordering.Application;

public sealed class DispatchOrderUseCase
{
    private readonly IPosOrderGateway _posOrderGateway;

    public DispatchOrderUseCase(IPosOrderGateway posOrderGateway)
    {
        _posOrderGateway = posOrderGateway;
    }

    public Task<PosDispatchStatus> ExecuteAsync(
        Order order,
        CancellationToken cancellationToken)
    {
        return _posOrderGateway.DispatchAsync(order, cancellationToken);
    }
}
