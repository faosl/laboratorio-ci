using Ordering.Application;
using Ordering.Application.Tests.Fakes;
using Ordering.Dominio;

namespace Ordering.Application.Tests;

public class DispatchOrderUseCaseTests
{
    [Theory]
    [InlineData(PosDispatchStatus.ReceiptConfirmed)]
    [InlineData(PosDispatchStatus.Rejected)]
    [InlineData(PosDispatchStatus.OutcomeUnknown)]
    public async Task ExecuteAsync_ForwardsOrderAndCancellationToken_AndReturnsGatewayResult(
        PosDispatchStatus status)
    {
        var gateway = new FakePosOrderGateway { Result = status };
        var useCase = new DispatchOrderUseCase(gateway);
        var order = Order.Create(new[] { new OrderItem(2) });
        using var cancellationTokenSource = new CancellationTokenSource();

        var result = await useCase.ExecuteAsync(order, cancellationTokenSource.Token);

        Assert.Equal(status, result);
        Assert.Same(order, gateway.ReceivedOrder);
        Assert.Equal(cancellationTokenSource.Token, gateway.ReceivedCancellationToken);
        Assert.Equal(1, gateway.CallCount);
    }

    [Fact]
    public void Constructor_WithNullGateway_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(
            () => new DispatchOrderUseCase(null!));

        Assert.Equal("posOrderGateway", exception.ParamName);
    }
}
