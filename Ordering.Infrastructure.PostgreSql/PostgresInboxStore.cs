using System.Data;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using Ordering.Application.Inbox;
using Ordering.Contracts.Partners;

namespace Ordering.Infrastructure.PostgreSql;

public sealed class PostgresInboxStore : IInboxStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly RegistrationFaults? _faults;

    public PostgresInboxStore(NpgsqlDataSource dataSource) : this(dataSource, null) { }

    internal PostgresInboxStore(NpgsqlDataSource dataSource, RegistrationFaults? faults)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
        _faults = faults;
    }

    public async Task<InboxRegistrationResult> RegisterAsync(InboxNotification notification, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(notification);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await RequireDurablePrimaryAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        await using (var settings = new NpgsqlCommand("SET LOCAL synchronous_commit = on", connection, transaction))
            await settings.ExecuteNonQueryAsync(cancellationToken);

        var id = Guid.NewGuid();
        var original = notification.OriginalPayload;
        var normalized = JsonSerializer.Serialize(notification.Notification);
        await using var insert = new NpgsqlCommand("""
            INSERT INTO public.ordering_inbox
                (inbox_id, partner_id, integration_scope, event_id, order_id,
                 original_payload, normalized_payload, normalized_version)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (partner_id, integration_scope, event_id) DO NOTHING
            RETURNING inbox_id
            """, connection, transaction);
        insert.Parameters.AddWithValue(id);
        AddKey(insert, notification.Key);
        insert.Parameters.AddWithValue(notification.Notification.OrderId);
        insert.Parameters.AddWithValue(original);
        insert.Parameters.AddWithValue(NpgsqlDbType.Jsonb, normalized);
        insert.Parameters.AddWithValue(notification.NormalizedVersion);
        var inserted = await insert.ExecuteScalarAsync(cancellationToken);
        var status = InboxRegistrationStatus.Inserted;
        if (inserted is null)
        {
            // A separate READ COMMITTED statement sees the winner after ON CONFLICT waited for it.
            await using var existing = new NpgsqlCommand("""
                SELECT inbox_id, order_id = $4 AND original_payload = $5
                    AND normalized_payload = $6 AND normalized_version = $7
                FROM public.ordering_inbox
                WHERE partner_id = $1 AND integration_scope = $2 AND event_id = $3
                FOR SHARE
                """, connection, transaction);
            AddKey(existing, notification.Key);
            existing.Parameters.AddWithValue(notification.Notification.OrderId);
            existing.Parameters.AddWithValue(original);
            existing.Parameters.AddWithValue(NpgsqlDbType.Jsonb, normalized);
            existing.Parameters.AddWithValue(notification.NormalizedVersion);
            await using var reader = await existing.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                return new(InboxRegistrationStatus.OutcomeUnknown, null);
            id = reader.GetGuid(0);
            status = reader.GetBoolean(1) ? InboxRegistrationStatus.AlreadyExists : InboxRegistrationStatus.Conflict;
        }

        if (_faults?.BeforeCommit is { } before)
            await before(connection, transaction, cancellationToken);
        try
        {
            // From this point a lost response/cancellation must never be interpreted as rollback.
            if (_faults?.Commit is { } commit)
                await commit(connection, transaction, cancellationToken);
            else
                await transaction.CommitAsync(cancellationToken);
        }
        catch (Exception error) when (error is NpgsqlException or IOException or TimeoutException or OperationCanceledException)
        {
            return new(InboxRegistrationStatus.OutcomeUnknown, null);
        }
        return new(status, id);
    }

    public async Task<InboxEntry?> GetByKeyAsync(InboxKey key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await RequireDurablePrimaryAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        await using (var settings = new NpgsqlCommand("SET LOCAL synchronous_commit = on", connection, transaction))
            await settings.ExecuteNonQueryAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            SELECT inbox_id, order_id, original_payload, normalized_payload::text,
                normalized_version, received_at_utc, state
            FROM public.ordering_inbox
            WHERE partner_id = $1 AND integration_scope = $2 AND event_id = $3
            FOR SHARE
            """, connection, transaction);
        AddKey(command, key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        var normalized = JsonSerializer.Deserialize<OrderNotification>(reader.GetString(3))
            ?? throw new InvalidDataException("Invalid normalized notification.");
        if (normalized.OrderId != reader.GetString(1)) throw new InvalidDataException("Inconsistent order identifier.");
        var entry = new InboxEntry(reader.GetGuid(0), new InboxNotification(key, normalized,
            reader.GetInt32(4), reader.GetFieldValue<byte[]>(2)),
            new DateTimeOffset(reader.GetDateTime(5)), reader.GetString(6));
        await reader.DisposeAsync();
        // A row lock assigns an xid and writes WAL. Its synchronous commit flushes the preceding
        // insertion's commit record too, even if that record became visible before its own flush.
        await transaction.CommitAsync(cancellationToken);
        return entry;
    }

    private static void AddKey(NpgsqlCommand command, InboxKey key)
    {
        command.Parameters.AddWithValue(key.PartnerId);
        command.Parameters.AddWithValue(key.IntegrationScope);
        command.Parameters.AddWithValue(key.EventId);
    }

    private static async Task RequireDurablePrimaryAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
            SELECT NOT pg_is_in_recovery() AND current_setting('fsync') = 'on'
                AND current_setting('full_page_writes') = 'on'
            """, connection);
        if (!(bool)(await command.ExecuteScalarAsync(cancellationToken))!)
            throw new InvalidOperationException("Inbox requires a primary with fsync and full_page_writes enabled.");
    }
}

// Internal deterministic fault boundary for real-database tests; not part of the Application port.
internal sealed record RegistrationFaults(
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? BeforeCommit = null,
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? Commit = null);
