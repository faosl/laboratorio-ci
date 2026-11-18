namespace Ordering.Application.Outbox;

public sealed class ConfirmOrderUseCase
{
    private readonly IOrderConfirmationStore _store;

    public ConfirmOrderUseCase(IOrderConfirmationStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        _store = store;
    }

    public Task<OrderConfirmationResult> ExecuteAsync(string orderId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(orderId);
        return _store.ConfirmAsync(orderId, cancellationToken);
    }
}
