using System.Text.Json;
using Npgsql;
using Ordering.Application.Outbox;

namespace Ordering.Infrastructure.PostgreSql.Tests;

public sealed class PostgresOrderConfirmationTests(DatabaseFixture database) : IClassFixture<DatabaseFixture>
{
    private static readonly CancellationToken Token = CancellationToken.None;
    private PostgresOrderConfirmationStore Store => new(database.Source);

    [Fact]
    public async Task Confirm_PersistsTransitionAndOneCompleteEvent_RetryPreservesIt()
    {
        var orderId = await SeedAsync();
        var result = await new ConfirmOrderUseCase(Store).ExecuteAsync(orderId, Token);

        Assert.Equal(OrderConfirmationStatus.Confirmed, result.Status);
        Assert.Equal("Confirmed", await ReadStatusAsync(orderId));
        var message = Assert.Single(await ReadMessagesAsync(orderId));
        Assert.NotEqual(Guid.Empty, message.EventId);
        Assert.Equal(result.EventId, message.EventId);
        Assert.Equal("OrderConfirmed", message.EventType);
        Assert.Equal(orderId, message.AggregateId);
        Assert.Equal(new OrderConfirmed(message.EventId, orderId), JsonSerializer.Deserialize<OrderConfirmed>(message.Payload));
        Assert.Equal("Pending", message.PublicationStatus);
        Assert.Equal(TimeSpan.Zero, message.CreatedAtUtc.Offset);
        Assert.InRange(message.CreatedAtUtc, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddMinutes(1));

        var retry = await Store.ConfirmAsync(orderId, Token);
        Assert.Equal(OrderConfirmationStatus.NoEligibleTransition, retry.Status);
        Assert.Null(retry.EventId);
        Assert.Equal(message, Assert.Single(await ReadMessagesAsync(orderId)));
    }

    [Fact]
    public async Task FailureBeforeCommit_SeesBothWritesInsideTransaction_ThenRollsBackBoth()
    {
        var orderId = await SeedAsync();
        var store = new PostgresOrderConfirmationStore(database.Source, new(BeforeCommit: async (connection, transaction, ct) =>
        {
            await using var status = new NpgsqlCommand("SELECT status FROM public.ordering_lab_orders WHERE order_id = $1", connection, transaction);
            status.Parameters.AddWithValue(orderId);
            Assert.Equal("Confirmed", await status.ExecuteScalarAsync(ct));
            Assert.Single(await ReadMessagesAsync(orderId, connection, transaction));
            // Other connections see neither uncommitted change.
            Assert.Equal("Pending", await ReadStatusAsync(orderId));
            Assert.Empty(await ReadMessagesAsync(orderId));
            throw new IOException("Injected failure before commit");
        }));

        await Assert.ThrowsAsync<IOException>(() => store.ConfirmAsync(orderId, Token));
        Assert.Equal("Pending", await ReadStatusAsync(orderId));
        Assert.Empty(await ReadMessagesAsync(orderId));
        Assert.Equal(OrderConfirmationStatus.Confirmed, (await Store.ConfirmAsync(orderId, Token)).Status);
        Assert.Single(await ReadMessagesAsync(orderId));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AbsentOrIncompatibleOrder_CreatesNoEvent(bool exists)
    {
        var orderId = exists ? await SeedAsync("Confirmed") : Guid.NewGuid().ToString();
        var result = await Store.ConfirmAsync(orderId, Token);
        Assert.Equal(OrderConfirmationStatus.NoEligibleTransition, result.Status);
        Assert.Null(result.EventId);
        Assert.Equal(exists ? "Confirmed" : null, await ReadStatusAsync(orderId));
        Assert.Empty(await ReadMessagesAsync(orderId));
    }

    [Fact]
    public async Task ConcurrentConfirmations_WaitForSameRow_OnlyOneTransitionsAndCreatesEvent()
    {
        var orderId = await SeedAsync();
        var reached = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var winner = new PostgresOrderConfirmationStore(database.Source, new(BeforeCommit: async (connection, _, ct) =>
        {
            reached.SetResult(connection.ProcessID);
            await release.Task.WaitAsync(ct);
        }));
        var applicationName = "outbox_contender_" + Guid.NewGuid().ToString("N");
        var builder = new NpgsqlConnectionStringBuilder(database.ConnectionString) { ApplicationName = applicationName };
        await using var competingSource = NpgsqlDataSource.Create(builder.ConnectionString);
        var first = winner.ConfirmAsync(orderId, timeout.Token);
        Task<OrderConfirmationResult>? second = null;
        try
        {
            var blocker = await reached.Task.WaitAsync(timeout.Token);
            second = new PostgresOrderConfirmationStore(competingSource).ConfirmAsync(orderId, timeout.Token);
            await WaitForBlockedAsync(applicationName, blocker, timeout.Token);
            Assert.False(second.IsCompleted);
            Assert.Equal("Pending", await ReadStatusAsync(orderId));
            Assert.Empty(await ReadMessagesAsync(orderId));
        }
        finally
        {
            release.TrySetResult();
            await first;
            if (second is not null) await second;
        }

        var firstResult = await first;
        var secondResult = await second!;
        Assert.Equal(OrderConfirmationStatus.Confirmed, firstResult.Status);
        Assert.Equal(OrderConfirmationStatus.NoEligibleTransition, secondResult.Status);
        Assert.Null(secondResult.EventId);
        Assert.Equal("Confirmed", await ReadStatusAsync(orderId));
        Assert.Equal(firstResult.EventId, Assert.Single(await ReadMessagesAsync(orderId)).EventId);
    }

    [Theory]
    [InlineData(true, "io")]
    [InlineData(false, "io")]
    [InlineData(true, "timeout")]
    [InlineData(false, "timeout")]
    [InlineData(true, "cancellation")]
    [InlineData(false, "cancellation")]
    [InlineData(true, "provider")]
    [InlineData(false, "provider")]
    public async Task LostCommitResponse_IsUnknown_ForBothActualCommitAndRollback(bool committed, string failure)
    {
        var orderId = await SeedAsync();
        StoredMessage? attempted = null;
        var store = new PostgresOrderConfirmationStore(database.Source, new(
            BeforeCommit: async (connection, transaction, _) =>
                attempted = Assert.Single(await ReadMessagesAsync(orderId, connection, transaction)),
            Commit: async (_, transaction, ct) =>
            {
                if (committed) await transaction.CommitAsync(ct);
                else await transaction.RollbackAsync(ct);
                throw failure switch
                {
                    "timeout" => new TimeoutException("Lost commit response"),
                    "cancellation" => new OperationCanceledException("Cancelled while awaiting commit response"),
                    "provider" => new NpgsqlException("Lost commit response", new IOException()),
                    _ => new IOException("Lost commit response")
                };
            }));

        var result = await store.ConfirmAsync(orderId, Token);
        Assert.Equal(OrderConfirmationStatus.OutcomeUnknown, result.Status);
        Assert.Null(result.EventId);
        Assert.NotNull(attempted);
        Assert.Equal(committed ? "Confirmed" : "Pending", await ReadStatusAsync(orderId));
        if (committed) Assert.Equal(attempted, Assert.Single(await ReadMessagesAsync(orderId)));
        else Assert.Empty(await ReadMessagesAsync(orderId));

        var retry = await Store.ConfirmAsync(orderId, Token);
        Assert.Equal(committed ? OrderConfirmationStatus.NoEligibleTransition : OrderConfirmationStatus.Confirmed, retry.Status);
        var persisted = Assert.Single(await ReadMessagesAsync(orderId));
        if (committed) Assert.Equal(attempted, persisted);
        else
        {
            Assert.NotEqual(attempted.EventId, persisted.EventId);
            Assert.Equal(retry.EventId, persisted.EventId);
        }
    }

    [Fact]
    public async Task PendingCommit_InvisibleWritesDoNotProveRollback()
    {
        var orderId = await SeedAsync();
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var store = new PostgresOrderConfirmationStore(database.Source, new(Commit: async (_, transaction, ct) =>
        {
            reached.SetResult();
            await release.Task.WaitAsync(ct);
            await transaction.CommitAsync(ct);
            throw new IOException("Lost response after pending commit");
        }));
        var pending = store.ConfirmAsync(orderId, timeout.Token);
        try
        {
            await reached.Task.WaitAsync(timeout.Token);
            Assert.Equal("Pending", await ReadStatusAsync(orderId));
            Assert.Empty(await ReadMessagesAsync(orderId));
        }
        finally { release.TrySetResult(); }
        Assert.Equal(OrderConfirmationStatus.OutcomeUnknown, (await pending).Status);
        Assert.Equal("Confirmed", await ReadStatusAsync(orderId));
        Assert.Single(await ReadMessagesAsync(orderId));
    }

    [Fact]
    public async Task CancellationBeforeCommit_RollsBackBothWrites()
    {
        var orderId = await SeedAsync();
        using var cancellation = new CancellationTokenSource();
        var store = new PostgresOrderConfirmationStore(database.Source, new(BeforeCommit: async (connection, transaction, ct) =>
        {
            Assert.Single(await ReadMessagesAsync(orderId, connection, transaction));
            await cancellation.CancelAsync();
            ct.ThrowIfCancellationRequested();
        }));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.ConfirmAsync(orderId, cancellation.Token));
        Assert.Equal("Pending", await ReadStatusAsync(orderId));
        Assert.Empty(await ReadMessagesAsync(orderId));
    }

    [Fact]
    public async Task TransientProviderFailureBeforeCommit_UsesApplicationExceptionAndRollsBack()
    {
        var orderId = await SeedAsync();
        var store = new PostgresOrderConfirmationStore(database.Source, new(BeforeCommit: (_, _, _) =>
            throw new NpgsqlException("Synthetic connection failure", new IOException())));
        await Assert.ThrowsAsync<OrderConfirmationStorageUnavailableException>(() => store.ConfirmAsync(orderId, Token));
        Assert.Equal("Pending", await ReadStatusAsync(orderId));
        Assert.Empty(await ReadMessagesAsync(orderId));
    }

    [Fact]
    public async Task Confirmation_OverridesAsynchronousSessionDefault_UsesPersistentTables()
    {
        var orderId = await SeedAsync();
        var builder = new NpgsqlConnectionStringBuilder(database.ConnectionString) { Options = "-c synchronous_commit=off" };
        await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
        await using (var check = source.CreateCommand("SHOW synchronous_commit"))
            Assert.Equal("off", await check.ExecuteScalarAsync());
        var store = new PostgresOrderConfirmationStore(source, new(BeforeCommit: async (connection, transaction, ct) =>
        {
            await using var check = new NpgsqlCommand("SHOW synchronous_commit", connection, transaction);
            Assert.Equal("on", await check.ExecuteScalarAsync(ct));
        }));
        Assert.Equal(OrderConfirmationStatus.Confirmed, (await store.ConfirmAsync(orderId, Token)).Status);
        await using var tables = database.Source.CreateCommand("""
            SELECT count(*) FROM pg_class WHERE oid IN
                ('public.ordering_lab_orders'::regclass, 'public.ordering_outbox_messages'::regclass)
                AND relpersistence = 'p'
            """);
        Assert.Equal(2L, await tables.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ProvisioningRepeatedly_PreservesDataAndMigrationHistory()
    {
        var orderId = await SeedAsync();
        await Store.ConfirmAsync(orderId, Token);
        var before = Assert.Single(await ReadMessagesAsync(orderId));
        await using var history = database.Source.CreateCommand("SELECT array_agg(version ORDER BY version) FROM public.ordering_inbox_schema_version");
        Assert.Equal(new[] { 1, 2, 3, 4 }, (int[])(await history.ExecuteScalarAsync())!);
        await using var applied = database.Source.CreateCommand("SELECT applied_at_utc FROM public.ordering_inbox_schema_version WHERE version = 3");
        var appliedAt = await applied.ExecuteScalarAsync();

        await InboxSchema.ApplyAsync(database.Source);
        await InboxSchema.ApplyAsync(database.Source);

        Assert.Equal(appliedAt, await applied.ExecuteScalarAsync());
        Assert.Equal(new[] { 1, 2, 3, 4 }, (int[])(await history.ExecuteScalarAsync())!);
        Assert.Equal("Confirmed", await ReadStatusAsync(orderId));
        Assert.Equal(before, Assert.Single(await ReadMessagesAsync(orderId)));
    }

    [Fact]
    public async Task DatabaseConstraint_RejectsDuplicateEventId()
    {
        var orderId = await SeedAsync();
        await Store.ConfirmAsync(orderId, Token);
        await using var duplicate = database.Source.CreateCommand("""
            INSERT INTO public.ordering_outbox_messages (event_id, event_type, aggregate_id, payload)
            SELECT event_id, event_type, aggregate_id, payload
            FROM public.ordering_outbox_messages WHERE aggregate_id = $1
            """);
        duplicate.Parameters.AddWithValue(orderId);
        var error = await Assert.ThrowsAsync<PostgresException>(() => duplicate.ExecuteNonQueryAsync());
        Assert.Equal(PostgresErrorCodes.UniqueViolation, error.SqlState);
        Assert.Single(await ReadMessagesAsync(orderId));
    }

    [Fact]
    public async Task VersionTwoUpgrade_AddsLabTables_PreservesExistingInboxAndMigrationHistory()
    {
        var name = "outbox_upgrade_" + Guid.NewGuid().ToString("N");
        await using (var create = database.Source.CreateCommand($"CREATE DATABASE {name}"))
            await create.ExecuteNonQueryAsync();
        try
        {
            var builder = new NpgsqlConnectionStringBuilder(database.ConnectionString) { Database = name };
            await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
            foreach (var script in new[] { "001_inbox.sql", "002_inbox_processing.sql" })
            {
                using var stream = typeof(InboxSchema).Assembly.GetManifestResourceStream(
                    "Ordering.Infrastructure.PostgreSql.Migrations." + script)!;
                using var reader = new StreamReader(stream);
                await using var migration = source.CreateCommand(await reader.ReadToEndAsync());
                await migration.ExecuteNonQueryAsync();
            }
            await using (var setup = source.CreateCommand("""
                CREATE TABLE public.ordering_inbox_schema_version (version integer PRIMARY KEY,
                    applied_at_utc timestamptz NOT NULL DEFAULT clock_timestamp());
                INSERT INTO public.ordering_inbox_schema_version(version) VALUES (1), (2);
                INSERT INTO public.ordering_inbox
                    (inbox_id, partner_id, integration_scope, event_id, order_id,
                     original_payload, normalized_payload, normalized_version)
                VALUES (gen_random_uuid(), 'partner', 'scope', 'existing-event', 'existing-order',
                    decode('00ff0a', 'hex'), '{"EventId":"existing-event","OrderId":"existing-order"}', 1);
                """)) await setup.ExecuteNonQueryAsync();
            await using var inbox = source.CreateCommand("SELECT row_to_json(i)::text FROM public.ordering_inbox i");
            await using var history = source.CreateCommand("""
                SELECT json_agg(v ORDER BY version)::text
                FROM public.ordering_inbox_schema_version v WHERE version IN (1, 2)
                """);
            var before = await inbox.ExecuteScalarAsync();
            var previousHistory = await history.ExecuteScalarAsync();

            await InboxSchema.ApplyAsync(source);
            await InboxSchema.ApplyAsync(source);

            Assert.Equal(before, await inbox.ExecuteScalarAsync());
            Assert.Equal(previousHistory, await history.ExecuteScalarAsync());
            await using var seed = source.CreateCommand("INSERT INTO public.ordering_lab_orders(order_id) VALUES ('new-order')");
            await seed.ExecuteNonQueryAsync();
            Assert.Equal(OrderConfirmationStatus.Confirmed,
                (await new PostgresOrderConfirmationStore(source).ConfirmAsync("new-order", Token)).Status);
            await using var check = source.CreateCommand("""
                SELECT (SELECT array_agg(version ORDER BY version) FROM public.ordering_inbox_schema_version) = ARRAY[1, 2, 3, 4]
                    AND (SELECT count(*) FROM public.ordering_outbox_messages WHERE aggregate_id = 'new-order') = 1
                """);
            Assert.Equal(true, await check.ExecuteScalarAsync());
        }
        finally
        {
            await using var drop = database.Source.CreateCommand($"DROP DATABASE {name} WITH (FORCE)");
            await drop.ExecuteNonQueryAsync();
        }
    }

    private async Task<string> SeedAsync(string status = "Pending")
    {
        var orderId = Guid.NewGuid().ToString();
        await using var insert = database.Source.CreateCommand("INSERT INTO public.ordering_lab_orders (order_id, status) VALUES ($1, $2)");
        insert.Parameters.AddWithValue(orderId);
        insert.Parameters.AddWithValue(status);
        await insert.ExecuteNonQueryAsync();
        return orderId;
    }

    private async Task<string?> ReadStatusAsync(string orderId)
    {
        await using var command = database.Source.CreateCommand("SELECT status FROM public.ordering_lab_orders WHERE order_id = $1");
        command.Parameters.AddWithValue(orderId);
        return (string?)await command.ExecuteScalarAsync();
    }

    private async Task<List<StoredMessage>> ReadMessagesAsync(string orderId,
        NpgsqlConnection? connection = null, NpgsqlTransaction? transaction = null)
    {
        const string sql = """
            SELECT event_id, event_type, aggregate_id, payload::text, publication_status, created_at_utc
            FROM public.ordering_outbox_messages WHERE aggregate_id = $1
            """;
        await using var command = connection is null ? database.Source.CreateCommand(sql) : new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue(orderId);
        await using var reader = await command.ExecuteReaderAsync();
        var messages = new List<StoredMessage>();
        while (await reader.ReadAsync())
            messages.Add(new(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetString(4),
                new DateTimeOffset(reader.GetDateTime(5))));
        return messages;
    }

    private async Task WaitForBlockedAsync(string applicationName, int blocker, CancellationToken cancellationToken)
    {
        await using var command = database.Source.CreateCommand("""
            SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                WHERE application_name = $1 AND wait_event_type = 'Lock' AND $2 = ANY(pg_blocking_pids(pid)))
            """);
        command.Parameters.AddWithValue(applicationName);
        command.Parameters.AddWithValue(blocker);
        while (!(bool)(await command.ExecuteScalarAsync(cancellationToken))!)
            await Task.Delay(10, cancellationToken); // Poll actual lock evidence; elapsed time is never the synchronization condition.
    }

    private sealed record StoredMessage(Guid EventId, string EventType, string AggregateId, string Payload,
        string PublicationStatus, DateTimeOffset CreatedAtUtc);
}
