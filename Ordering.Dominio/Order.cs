namespace Ordering.Dominio;

public sealed class Order
{
    private Order(IReadOnlyList<OrderItem> items)
    {
        Items = items;
    }

    public IReadOnlyList<OrderItem> Items { get; }

    public static Order Create(IEnumerable<OrderItem> items)
    {
        ArgumentNullException.ThrowIfNull(items);

        var snapshot = items.ToArray();

        if (snapshot.Length == 0)
        {
            throw new ArgumentException("O pedido deve possuir pelo menos um item.", nameof(items));
        }

        if (snapshot.Any(item => item is null || item.Quantity <= 0))
        {
            throw new ArgumentException("Todos os itens devem possuir quantidade maior que zero.", nameof(items));
        }

        return new Order(Array.AsReadOnly(snapshot));
    }
}

public sealed class OrderItem
{
    public OrderItem(int quantity)
    {
        if (quantity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(quantity), quantity,
                "A quantidade deve ser maior que zero.");
        }

        Quantity = quantity;
    }

    public int Quantity { get; }
}
