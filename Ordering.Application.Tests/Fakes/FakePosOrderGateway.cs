using Ordering.Application;
using Ordering.Dominio;

namespace Ordering.Application.Tests.Fakes;

internal sealed class FakePosOrderGateway : IPosOrderGateway
{
    public PosDispatchStatus Result { get; set; } = PosDispatchStatus.ReceiptConfirmed;

    public Order? ReceivedOrder { get; private set; }

    public CancellationToken ReceivedCancellationToken { get; private set; }

    public int CallCount { get; private set; }

    public Task<PosDispatchStatus> DispatchAsync(
        Order order,
        CancellationToken cancellationToken)
    {
        ReceivedOrder = order;
        ReceivedCancellationToken = cancellationToken;
        CallCount++;

        return Task.FromResult(Result);
    }
}
