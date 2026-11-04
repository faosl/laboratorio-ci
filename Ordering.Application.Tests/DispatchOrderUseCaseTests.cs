using Ordering.Dominio;

namespace Ordering.Application.Tests;

public class DispatchOrderUseCaseTests
{
    [Fact]
    public async Task ExecuteAsync_WhenGatewayRejectsOrder_ReturnsRejected()
    {
        var gateway = new RejectedPosOrderGatewayStub();
        var useCase = new DispatchOrderUseCase(gateway);
        var order = Order.Create(new[] { new OrderItem(1) });

        var result = await useCase.ExecuteAsync(order, CancellationToken.None);

        Assert.Equal(PosDispatchStatus.Rejected, result);
    }

    private sealed class RejectedPosOrderGatewayStub : IPosOrderGateway
    {
        public Task<PosDispatchStatus> DispatchAsync(
            Order order,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(PosDispatchStatus.Rejected);
        }
    }
}
