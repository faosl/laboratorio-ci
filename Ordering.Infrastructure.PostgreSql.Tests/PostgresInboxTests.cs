using System.Diagnostics;
using Npgsql;
using Ordering.Application.Inbox;
using Ordering.Contracts.Partners;
using Ordering.Infrastructure.PostgreSql;

namespace Ordering.Infrastructure.PostgreSql.Tests;

public sealed class DatabaseFixture : IAsyncLifetime
{
    private readonly string _name = "inbox_test_" + Guid.NewGuid().ToString("N");
    private NpgsqlDataSource _admin = null!;
    public NpgsqlDataSource Source { get; private set; } = null!;
    public string ConnectionString { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var connection = Environment.GetEnvironmentVariable("INBOX_TEST_CONNECTION")
            ?? throw new InvalidOperationException("INBOX_TEST_CONNECTION must point to real PostgreSQL; tests never silently skip.");
        _admin = NpgsqlDataSource.Create(connection);
        await using var create = _admin.CreateCommand($"CREATE DATABASE {_name}");
        await create.ExecuteNonQueryAsync();
        ConnectionString = new NpgsqlConnectionStringBuilder(connection) { Database = _name, Enlist = false }.ConnectionString;
        Source = NpgsqlDataSource.Create(ConnectionString);
        await InboxSchema.ApplyAsync(Source);
        await InboxSchema.ApplyAsync(Source); // Versioned provisioning is repeatable.
    }

    public async Task DisposeAsync()
    {
        if (Source is not null) await Source.DisposeAsync();
        if (_admin is not null)
        {
            await using var drop = _admin.CreateCommand($"DROP DATABASE IF EXISTS {_name} WITH (FORCE)");
            await drop.ExecuteNonQueryAsync();
            await _admin.DisposeAsync();
        }
    }
}

public sealed class PostgresInboxTests(DatabaseFixture database) : IClassFixture<DatabaseFixture>
{
    private static readonly CancellationToken Token = CancellationToken.None;
    private static InboxNotification New(string eventId, string partner = "uber", string scope = "sandbox",
        string order = "order", byte[]? payload = null) => new(new(partner, scope, eventId),
            new OrderNotification(eventId, order),
            1, payload ?? [0, 255, 13, 10, 32, 123, 125]);

    private static InboxNotification New() => New(Guid.NewGuid().ToString());
    private PostgresInboxStore Store => new(database.Source);

    [Fact]
    public async Task Insert_RoundTripsExactBytesNormalizedVersionAndMetadata()
    {
        var input = New();
        var result = await Store.RegisterAsync(input, Token);
        var entry = await Store.GetByKeyAsync(input.Key, Token);
        Assert.Equal(InboxRegistrationStatus.Inserted, result.Status);
        Assert.NotNull(entry);
        Assert.Equal(result.InboxId, entry.InboxId);
        Assert.Equal(input.OriginalPayload, entry.Content.OriginalPayload);
        Assert.Equal(input.Notification.EventId, entry.Content.Notification.EventId);
        Assert.Equal(input.Notification.OrderId, entry.Content.Notification.OrderId);
        Assert.Equal(1, entry.Content.NormalizedVersion);
        Assert.Equal(TimeSpan.Zero, entry.ReceivedAtUtc.Offset);
        Assert.InRange(entry.ReceivedAtUtc, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddMinutes(1));
        Assert.Equal("Pending", entry.State);
    }

    [Fact]
    public async Task EquivalentRedelivery_DoesNotChangeContentIdTimeOrState()
    {
        var input = New();
        await Store.RegisterAsync(input, Token);
        var before = await Store.GetByKeyAsync(input.Key, Token);
        var result = await Store.RegisterAsync(input, Token);
        var after = await Store.GetByKeyAsync(input.Key, Token);
        Assert.Equal(InboxRegistrationStatus.AlreadyExists, result.Status);
        Assert.Equal(before!.InboxId, result.InboxId);
        Assert.Equal(before.ReceivedAtUtc, after!.ReceivedAtUtc);
        Assert.Equal(before.State, after.State);
        Assert.Equal(before.Content.OriginalPayload, after.Content.OriginalPayload);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task IncompatibleRedelivery_DoesNotOverwrite(bool changeOrder)
    {
        var input = New();
        await Store.RegisterAsync(input, Token);
        var changed = New(input.Key.EventId, order: changeOrder ? "other" : "order",
            payload: changeOrder ? input.OriginalPayload : [123, 32, 125]);
        var result = await Store.RegisterAsync(changed, Token);
        Assert.Equal(InboxRegistrationStatus.Conflict, result.Status);
        var entry = await Store.GetByKeyAsync(input.Key, Token);
        Assert.Equal(input.OriginalPayload, entry!.Content.OriginalPayload);
        Assert.Equal(input.Notification.OrderId, entry.Content.Notification.OrderId);
    }

    [Fact]
    public async Task ConcurrentIndependentInstances_CreateOneRow()
    {
        var input = New();
        var results = await Task.WhenAll(Enumerable.Range(0, 24).Select(async _ =>
        {
            await using var source = NpgsqlDataSource.Create(database.ConnectionString);
            return await new PostgresInboxStore(source).RegisterAsync(input, Token);
        }));
        Assert.Single(results, x => x.Status == InboxRegistrationStatus.Inserted);
        Assert.Equal(23, results.Count(x => x.Status == InboxRegistrationStatus.AlreadyExists));
        Assert.Single(results.Select(x => x.InboxId).Distinct());
        await using var count = database.Source.CreateCommand("SELECT count(*) FROM public.ordering_inbox WHERE event_id = $1");
        count.Parameters.AddWithValue(input.Key.EventId);
        Assert.Equal(1L, await count.ExecuteScalarAsync());
    }

    [Fact]
    public async Task PartnerScopeAndEvent_AreIndependent_OrderIsNotDeduplicationKey()
    {
        var id = Guid.NewGuid().ToString();
        var inputs = new[] { New(id), New(id, partner: "other"), New(id, scope: "production"), New(id + "-2") };
        foreach (var input in inputs)
            Assert.Equal(InboxRegistrationStatus.Inserted, (await Store.RegisterAsync(input, Token)).Status);
        foreach (var input in inputs) Assert.NotNull(await Store.GetByKeyAsync(input.Key, Token));
    }

    [Fact]
    public async Task FailureBeforeCommit_RollsBackWholeEnvelope()
    {
        var input = New();
        var store = new PostgresInboxStore(database.Source, new(BeforeCommit: (_, _, _) => throw new IOException("Synthetic failure")));
        await Assert.ThrowsAsync<IOException>(() => store.RegisterAsync(input, Token));
        Assert.Null(await Store.GetByKeyAsync(input.Key, Token));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LostCommitResponse_IsUnknown_ReconcilesOnlyWhenVisible(bool committed)
    {
        var input = New();
        var store = new PostgresInboxStore(database.Source, new(Commit: async (_, transaction, ct) =>
        {
            if (committed) await transaction.CommitAsync(ct);
            else await transaction.RollbackAsync(ct);
            throw new IOException("Synthetic lost response; client cannot know server outcome");
        }));
        var result = await store.RegisterAsync(input, Token);
        Assert.Equal(InboxRegistrationStatus.OutcomeUnknown, result.Status);
        Assert.Null(result.InboxId);
        var recovered = await Store.GetByKeyAsync(input.Key, Token);
        Assert.Equal(committed, recovered is not null);
        Assert.Equal(committed ? InboxRegistrationStatus.AlreadyExists : InboxRegistrationStatus.Inserted,
            (await Store.RegisterAsync(input, Token)).Status);
    }

    [Fact]
    public async Task PendingCommit_IsUnknown_AbsentReadDoesNotProveRollback()
    {
        var input = New();
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new PostgresInboxStore(database.Source, new(Commit: async (_, transaction, ct) =>
        {
            reached.SetResult();
            await release.Task;
            await transaction.CommitAsync(ct);
            throw new IOException("Lost response");
        }));
        var pending = store.RegisterAsync(input, Token);
        await reached.Task.WaitAsync(TimeSpan.FromSeconds(20));
        try { Assert.Null(await Store.GetByKeyAsync(input.Key, Token)); }
        finally { release.SetResult(); }
        Assert.Equal(InboxRegistrationStatus.OutcomeUnknown, (await pending).Status);
        Assert.NotNull(await Store.GetByKeyAsync(input.Key, Token));
    }

    [Fact]
    public async Task SeparateApplicationProcesses_RecoverCommittedEnvelope()
    {
        var eventId = Guid.NewGuid().ToString();
        var first = await RunHost("write", eventId);
        var second = await RunHost("read", eventId);
        Assert.Equal(first, second);
    }

    private async Task<string> RunHost(string operation, string eventId)
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root is not null && !Directory.Exists(Path.Combine(root.FullName, "Ordering.Inbox.TestHost"))) root = root.Parent;
        Assert.NotNull(root);
        var configuration = new DirectoryInfo(AppContext.BaseDirectory).Parent!.Name;
        var dll = Path.Combine(root.FullName, "Ordering.Inbox.TestHost", "bin", configuration, "net8.0", "Ordering.Inbox.TestHost.dll");
        var start = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        start.ArgumentList.Add(dll);
        start.ArgumentList.Add(operation);
        start.ArgumentList.Add(eventId);
        start.Environment["INBOX_TEST_CONNECTION"] = database.ConnectionString;
        using var process = Process.Start(start)!;
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try { await process.WaitForExitAsync(timeout.Token); }
        catch { process.Kill(entireProcessTree: true); throw; }
        Assert.True(process.ExitCode == 0, $"Host exited {process.ExitCode}: {await error}");
        return (await output).Trim();
    }

    [Fact]
    public async Task DatabaseConstraint_RejectsDuplicateWithoutUsingStore()
    {
        var input = New();
        await Store.RegisterAsync(input, Token);
        await using var duplicate = database.Source.CreateCommand("""
            INSERT INTO public.ordering_inbox
                (inbox_id, partner_id, integration_scope, event_id, order_id,
                 original_payload, normalized_payload, normalized_version)
            SELECT $1, partner_id, integration_scope, event_id, order_id,
                original_payload, normalized_payload, normalized_version
            FROM public.ordering_inbox WHERE event_id = $2
            """);
        duplicate.Parameters.AddWithValue(Guid.NewGuid());
        duplicate.Parameters.AddWithValue(input.Key.EventId);
        var error = await Assert.ThrowsAsync<PostgresException>(() => duplicate.ExecuteNonQueryAsync());
        Assert.Equal(PostgresErrorCodes.UniqueViolation, error.SqlState);
        Assert.Equal("ordering_inbox_event_unique", error.ConstraintName);
    }

    [Fact]
    public async Task Registration_OverridesAsynchronousSessionDefault()
    {
        var builder = new NpgsqlConnectionStringBuilder(database.ConnectionString) { Options = "-c synchronous_commit=off" };
        await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
        await using (var check = source.CreateCommand("SHOW synchronous_commit"))
            Assert.Equal("off", await check.ExecuteScalarAsync());
        var store = new PostgresInboxStore(source, new(BeforeCommit: async (connection, transaction, ct) =>
        {
            await using var check = new NpgsqlCommand("SHOW synchronous_commit", connection, transaction);
            Assert.Equal("on", await check.ExecuteScalarAsync(ct));
        }));
        Assert.Equal(InboxRegistrationStatus.Inserted, (await store.RegisterAsync(New(), Token)).Status);
    }

    [Fact]
    public async Task LocalDurabilitySettingsAndSchema_AreEffective()
    {
        await using var command = database.Source.CreateCommand("""
            SELECT current_setting('fsync') = 'on' AND current_setting('full_page_writes') = 'on'
              AND relpersistence = 'p' FROM pg_class WHERE oid = 'public.ordering_inbox'::regclass
            """);
        Assert.Equal(true, await command.ExecuteScalarAsync());
        var input = New();
        var store = new PostgresInboxStore(database.Source, new(BeforeCommit: async (connection, transaction, ct) =>
        {
            await using var check = new NpgsqlCommand("SHOW synchronous_commit", connection, transaction);
            Assert.Equal("on", await check.ExecuteScalarAsync(ct));
        }));
        Assert.Equal(InboxRegistrationStatus.Inserted, (await store.RegisterAsync(input, Token)).Status);
    }
}
