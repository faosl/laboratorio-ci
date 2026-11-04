using Ordering.Dominio;

namespace Ordering.Dominio.Tests;

public class OrderTests
{
    [Fact]
    public void Create_WithValidItems_CreatesOrder()
    {
        var items = new[] { new OrderItem(2), new OrderItem(3) };

        var order = Order.Create(items);

        Assert.Collection(order.Items,
            item => Assert.Equal(2, item.Quantity),
            item => Assert.Equal(3, item.Quantity));
    }

    [Fact]
    public void Create_WithoutItems_ThrowsArgumentException()
    {
        var exception = Assert.Throws<ArgumentException>(
            () => Order.Create(Array.Empty<OrderItem>()));

        Assert.Equal("items", exception.ParamName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void OrderItem_WithNonPositiveQuantity_ThrowsArgumentOutOfRangeException(int quantity)
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(
            () => new OrderItem(quantity));

        Assert.Equal("quantity", exception.ParamName);
        Assert.Equal(quantity, exception.ActualValue);
    }

    [Fact]
    public void Create_WhenOriginalCollectionChanges_OrderItemsRemainUnchanged()
    {
        var items = new List<OrderItem> { new(2), new(3) };
        var order = Order.Create(items);

        items[0] = new OrderItem(10);
        items.Add(new OrderItem(4));
        items.Clear();

        Assert.Collection(order.Items,
            item => Assert.Equal(2, item.Quantity),
            item => Assert.Equal(3, item.Quantity));
    }

    [Fact]
    public void Items_WhenMutatedThroughListInterface_ThrowsNotSupportedException()
    {
        var order = Order.Create(new[] { new OrderItem(2), new OrderItem(3) });
        var items = Assert.IsAssignableFrom<IList<OrderItem>>(order.Items);

        Assert.True(items.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => items.Add(new OrderItem(4)));
        Assert.Throws<NotSupportedException>(() => items[0] = new OrderItem(10));
        Assert.Throws<NotSupportedException>(() => items.RemoveAt(0));
        Assert.Throws<NotSupportedException>(() => items.Clear());
        Assert.Collection(order.Items,
            item => Assert.Equal(2, item.Quantity),
            item => Assert.Equal(3, item.Quantity));
    }
}
