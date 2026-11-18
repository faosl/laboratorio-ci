using System.Data;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using Ordering.Application.Outbox;

namespace Ordering.Infrastructure.PostgreSql;

public sealed class PostgresOrderConfirmationStore : IOrderConfirmationStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly OrderConfirmationFaults? _faults;

    public PostgresOrderConfirmationStore(NpgsqlDataSource dataSource) : this(dataSource, null) { }

    internal PostgresOrderConfirmationStore(NpgsqlDataSource dataSource, OrderConfirmationFaults? faults)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        _dataSource = dataSource;
        _faults = faults;
    }

    public async Task<OrderConfirmationResult> ConfirmAsync(string orderId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(orderId);
        try { return await ConfirmCoreAsync(orderId, cancellationToken); }
        catch (NpgsqlException error) when (error.IsTransient)
        {
            throw new OrderConfirmationStorageUnavailableException();
        }
    }

    private async Task<OrderConfirmationResult> ConfirmCoreAsync(string orderId, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using (var check = new NpgsqlCommand("""
            SELECT NOT pg_is_in_recovery() AND current_setting('fsync') = 'on'
                AND current_setting('full_page_writes') = 'on'
            """, connection))
        {
            if (!(bool)(await check.ExecuteScalarAsync(cancellationToken))!)
                throw new InvalidOperationException("Order confirmation requires a primary with fsync and full_page_writes enabled.");
        }
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
        await using (var settings = new NpgsqlCommand("SET LOCAL synchronous_commit = on", connection, transaction))
            await settings.ExecuteNonQueryAsync(cancellationToken);

        await using (var update = new NpgsqlCommand("""
            UPDATE public.ordering_lab_orders SET status = 'Confirmed'
            WHERE order_id = $1 AND status = 'Pending'
            RETURNING order_id
            """, connection, transaction))
        {
            update.Parameters.AddWithValue(orderId);
            // A competing updater waits on the row and rechecks Pending after the winner commits.
            if (await update.ExecuteScalarAsync(cancellationToken) is null)
                return new(OrderConfirmationStatus.NoEligibleTransition, null);
        }

        var confirmed = new OrderConfirmed(Guid.NewGuid(), orderId);
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO public.ordering_outbox_messages (event_id, event_type, aggregate_id, payload)
            VALUES ($1, 'OrderConfirmed', $2, $3)
            """, connection, transaction))
        {
            insert.Parameters.AddWithValue(confirmed.EventId);
            insert.Parameters.AddWithValue(orderId);
            insert.Parameters.AddWithValue(NpgsqlDbType.Jsonb, JsonSerializer.Serialize(confirmed));
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        if (_faults?.BeforeCommit is { } before)
            await before(connection, transaction, cancellationToken);
        try
        {
            // Once commit starts, a lost response is not evidence of rollback.
            if (_faults?.Commit is { } commit)
                await commit(connection, transaction, cancellationToken);
            else
                await transaction.CommitAsync(cancellationToken);
        }
        catch (Exception error) when (error is NpgsqlException or IOException or TimeoutException or OperationCanceledException)
        {
            return new(OrderConfirmationStatus.OutcomeUnknown, null);
        }
        return new(OrderConfirmationStatus.Confirmed, confirmed.EventId);
    }
}

// Deterministic real-database test boundaries, never exposed through the Application port.
internal sealed record OrderConfirmationFaults(
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? BeforeCommit = null,
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? Commit = null);
