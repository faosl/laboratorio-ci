using Npgsql;
using Ordering.Application.Inbox;
using Ordering.Contracts.Partners;
using Ordering.Infrastructure.PostgreSql;

await using var source = NpgsqlDataSource.Create(Environment.GetEnvironmentVariable("INBOX_TEST_CONNECTION")
    ?? throw new InvalidOperationException("INBOX_TEST_CONNECTION required."));
if (args.Length == 1 && args[0] == "migrate")
{
    await InboxSchema.ApplyAsync(source);
    return 0;
}
if (args.Length != 2 || args[0] is not ("write" or "read")) return 1;
var store = new PostgresInboxStore(source);
var key = new InboxKey("restart-partner", "test", args[1]);
if (args[0] == "write")
{
    var result = await store.RegisterAsync(new(key, new OrderNotification(key.EventId, "restart-order"),
        1, new byte[] { 0, 255, 10 }), CancellationToken.None);
    if (result.Status != InboxRegistrationStatus.Inserted) return 2;
    Console.WriteLine(result.InboxId);
}
else
{
    var entry = await store.GetByKeyAsync(key, CancellationToken.None);
    if (entry is null || entry.State != "Pending" || entry.Content.NormalizedVersion != 1
        || entry.Content.Notification.OrderId != "restart-order"
        || !entry.Content.OriginalPayload.SequenceEqual(new byte[] { 0, 255, 10 })) return 3;
    Console.WriteLine(entry.InboxId);
}
return 0;
