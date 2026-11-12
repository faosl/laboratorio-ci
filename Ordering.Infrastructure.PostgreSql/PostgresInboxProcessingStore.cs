using System.Data;
using System.Text.Json;
using Npgsql;
using Ordering.Application.Inbox;
using Ordering.Contracts.Partners;

namespace Ordering.Infrastructure.PostgreSql;

/// <summary>Short, durable transactions coordinate Inbox writes; no external calls run under a lease lock.</summary>
public sealed class PostgresInboxProcessingStore : IInboxProcessingStore
{
    private readonly NpgsqlDataSource _source;
    private readonly ProcessingFaults? _faults;

    // Kept as one statement so the index-plan test exercises the actual selection query.
    internal const string AcquisitionSql = """
        SELECT inbox_id, partner_id, integration_scope, event_id, order_id,
            original_payload, normalized_payload::text, normalized_version, received_at_utc,
            lease_generation
        FROM public.ordering_inbox
        WHERE state IN ('Pending', 'RetryScheduled', 'Processing')
          AND (CASE state WHEN 'Pending' THEN received_at_utc
            WHEN 'RetryScheduled' THEN next_attempt_at_utc ELSE lease_expires_at_utc END)
            <= statement_timestamp()
        ORDER BY (CASE state WHEN 'Pending' THEN received_at_utc
            WHEN 'RetryScheduled' THEN next_attempt_at_utc ELSE lease_expires_at_utc END),
            received_at_utc, inbox_id
        LIMIT 1 FOR UPDATE SKIP LOCKED
        """;

    public PostgresInboxProcessingStore(NpgsqlDataSource source) : this(source, null) { }

    internal PostgresInboxProcessingStore(NpgsqlDataSource source, ProcessingFaults? faults)
    {
        ArgumentNullException.ThrowIfNull(source);
        _source = source;
        _faults = faults;
    }

    public async Task<InboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        InboxProcessingLimits.ValidateOwner(owner);
        InboxProcessingLimits.ValidateLeaseDuration(leaseDuration);
        return await TranslateAvailabilityAsync(async () =>
        {
            await using var connection = await _source.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
            await ConfigureAsync(connection, transaction, cancellationToken);
            if (_faults?.BeforeLock is { } beforeLock) await beforeLock(connection, transaction, cancellationToken);
            await using var select = Command(AcquisitionSql, connection, transaction);
            // Skipping locked rows reduces consumer contention, but provides neither strict FIFO
            // nor proof that the Inbox is empty. Table locks can still require waiting.
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                return new InboxAcquisitionResult(InboxAcquisitionStatus.NoEligibleWork, null);
            var id = reader.GetGuid(0);
            var key = new InboxKey(reader.GetString(1), reader.GetString(2), reader.GetString(3));
            var normalized = JsonSerializer.Deserialize<OrderNotification>(reader.GetString(6))
                ?? throw new InvalidDataException("Invalid normalized notification.");
            if (normalized.OrderId != reader.GetString(4)) throw new InvalidDataException("Inconsistent order identifier.");
            var content = new InboxNotification(key, normalized, reader.GetInt32(7), reader.GetFieldValue<byte[]>(5));
            var received = new DateTimeOffset(reader.GetDateTime(8));
            if (reader.GetInt64(9) == long.MaxValue)
                throw new InvalidOperationException("Inbox fencing generation exhausted; tokens must never be reused.");
            await reader.DisposeAsync();
            if (_faults?.AfterLock is { } afterLock) await afterLock(connection, transaction, cancellationToken);
            await using var update = Command("""
                WITH instant AS MATERIALIZED (SELECT clock_timestamp() AS now)
                UPDATE public.ordering_inbox SET state = 'Processing', lease_owner = $2,
                    lease_generation = lease_generation + 1,
                    lease_expires_at_utc = instant.now + $3, next_attempt_at_utc = NULL
                FROM instant WHERE inbox_id = $1 AND lease_generation < 9223372036854775807
                  AND (state = 'Pending'
                    OR (state = 'RetryScheduled' AND next_attempt_at_utc <= instant.now)
                    OR (state = 'Processing' AND lease_expires_at_utc <= instant.now))
                RETURNING lease_generation, lease_expires_at_utc
                """, connection, transaction);
            update.Parameters.AddWithValue(id);
            update.Parameters.AddWithValue(owner);
            update.Parameters.AddWithValue(leaseDuration);
            await using var updated = await update.ExecuteReaderAsync(cancellationToken);
            if (!await updated.ReadAsync(cancellationToken))
                return new InboxAcquisitionResult(InboxAcquisitionStatus.NoEligibleWork, null);
            var work = new AcquiredInboxNotification(new(id, owner, updated.GetInt64(0)), content,
                received, new DateTimeOffset(updated.GetDateTime(1)));
            await updated.DisposeAsync();
            // The generation advances with the acquisition commit. Never expose provisional work.
            return await CommitAsync(connection, transaction, cancellationToken)
                ? new(InboxAcquisitionStatus.Acquired, work)
                : new(InboxAcquisitionStatus.OutcomeUnknown, null);
        });
    }

    public Task<InboxMutationResult> RenewAsync(InboxLease lease, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        InboxProcessingLimits.ValidateLeaseDuration(leaseDuration);
        return MutateAsync(lease, Mutation.Renew, leaseDuration, cancellationToken);
    }

    public Task<InboxMutationResult> CompleteAsync(InboxLease lease, CancellationToken cancellationToken) =>
        MutateAsync(lease, Mutation.Complete, TimeSpan.Zero, cancellationToken);

    public Task<InboxMutationResult> ScheduleRetryAsync(InboxLease lease, TimeSpan retryDelay, CancellationToken cancellationToken)
    {
        InboxProcessingLimits.ValidateRetryDelay(retryDelay);
        return MutateAsync(lease, Mutation.Retry, retryDelay, cancellationToken);
    }

    private enum Mutation { Renew, Complete, Retry }

    private async Task<InboxMutationResult> MutateAsync(InboxLease lease, Mutation mutation, TimeSpan duration, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(lease);
        return await TranslateAvailabilityAsync(async () =>
        {
            await using var connection = await _source.OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
            await ConfigureAsync(connection, transaction, ct);
            if (_faults?.BeforeLock is { } beforeLock) await beforeLock(connection, transaction, ct);
            await using var rowLock = Command("SELECT inbox_id FROM public.ordering_inbox WHERE inbox_id = $1 FOR UPDATE", connection, transaction);
            rowLock.Parameters.AddWithValue(lease.InboxId);
            if (await rowLock.ExecuteScalarAsync(ct) is null) return new InboxMutationResult(InboxMutationStatus.LeaseLost);
            if (_faults?.AfterLock is { } afterLock) await afterLock(connection, transaction, ct);

            // This is a separate statement AFTER the lock wait. A transaction/statement start time
            // sampled before waiting could authorize an already expired worker. Validation and the
            // write share the lock, so no successor can intervene between them. Authorization is at
            // this database instant, not at commit completion or client response time.
            var assignment = mutation switch
            {
                Mutation.Renew => "lease_expires_at_utc = GREATEST(lease_expires_at_utc, instant.now + $4)",
                Mutation.Complete => "state = 'Completed', lease_owner = NULL, lease_expires_at_utc = NULL",
                _ => "state = 'RetryScheduled', next_attempt_at_utc = instant.now + $4, lease_owner = NULL, lease_expires_at_utc = NULL"
            };
            await using var update = Command($"""
                WITH instant AS MATERIALIZED (SELECT clock_timestamp() AS now)
                UPDATE public.ordering_inbox SET {assignment}
                FROM instant WHERE inbox_id = $1 AND state = 'Processing'
                    AND lease_owner = $2 AND lease_generation = $3 AND lease_expires_at_utc > instant.now
                RETURNING lease_expires_at_utc
                """, connection, transaction);
            update.Parameters.AddWithValue(lease.InboxId);
            update.Parameters.AddWithValue(lease.Owner);
            update.Parameters.AddWithValue(lease.Generation);
            if (mutation != Mutation.Complete) update.Parameters.AddWithValue(duration);
            await using var reader = await update.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) return new InboxMutationResult(InboxMutationStatus.LeaseLost);
            DateTimeOffset? expires = reader.IsDBNull(0) ? null : new DateTimeOffset(reader.GetDateTime(0));
            await reader.DisposeAsync();
            return await CommitAsync(connection, transaction, ct)
                ? new(InboxMutationStatus.Succeeded, expires)
                : new(InboxMutationStatus.OutcomeUnknown);
        });
    }

    private static NpgsqlCommand Command(string sql, NpgsqlConnection connection, NpgsqlTransaction transaction) =>
        new(sql, connection, transaction) { CommandTimeout = 10 };

    private static async Task ConfigureAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken ct)
    {
        await using var settings = Command("""
            SET LOCAL synchronous_commit = on;
            SET LOCAL lock_timeout = '2s';
            SET LOCAL statement_timeout = '5s';
            SET LOCAL idle_in_transaction_session_timeout = '10s';
            SELECT NOT pg_is_in_recovery() AND current_setting('fsync') = 'on'
                AND current_setting('full_page_writes') = 'on'
            """, connection, transaction);
        if (!(bool)(await settings.ExecuteScalarAsync(ct))!)
            throw new InvalidOperationException("Inbox requires a primary with fsync and full_page_writes enabled.");
    }

    private async Task<bool> CommitAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken ct)
    {
        if (_faults?.BeforeCommit is { } before) await before(connection, transaction, ct);
        try
        {
            if (_faults?.Commit is { } commit) await commit(connection, transaction, ct);
            else await transaction.CommitAsync(ct);
            return true;
        }
        catch (Exception error) when (error is NpgsqlException or IOException or TimeoutException or OperationCanceledException)
        {
            // A server commit may have succeeded before confirmation was lost. No rollback claim,
            // no automatic retry, and no usable acquisition/renewal evidence may escape this boundary.
            return false;
        }
    }

    private static async Task<T> TranslateAvailabilityAsync<T>(Func<Task<T>> operation)
    {
        try { return await operation(); }
        catch (NpgsqlException error) when (error.IsTransient) { throw new InboxStorageUnavailableException(); }
    }
}

// Deterministic real-database test boundaries, absent from the public Application contract.
internal sealed record ProcessingFaults(
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? BeforeLock = null,
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? AfterLock = null,
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? BeforeCommit = null,
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? Commit = null);
