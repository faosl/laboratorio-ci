using System.Data;
using Npgsql;
using Ordering.Application.Outbox;

namespace Ordering.Infrastructure.PostgreSql;

/// <summary>Durable local coordination only; no broker or executable Publisher.</summary>
public sealed class PostgresOutboxProcessingStore : IOutboxProcessingStore
{
    private readonly NpgsqlDataSource _source;
    private readonly OutboxProcessingFaults? _faults;

    // Kept as one statement so index-plan verification exercises the actual work claiming query.
    internal const string AcquisitionSql = """
        SELECT event_id, event_type, aggregate_id, payload::text, created_at_utc, lease_generation
        FROM public.ordering_outbox_messages
        WHERE publication_status IN ('Pending', 'RetryScheduled', 'Processing')
          AND (CASE publication_status WHEN 'Pending' THEN '-infinity'::timestamptz
            WHEN 'RetryScheduled' THEN next_attempt_at_utc ELSE lease_expires_at_utc END)
            <= statement_timestamp()
        ORDER BY (CASE publication_status WHEN 'Pending' THEN '-infinity'::timestamptz
            WHEN 'RetryScheduled' THEN next_attempt_at_utc ELSE lease_expires_at_utc END),
            created_at_utc, event_id
        LIMIT 1 FOR UPDATE SKIP LOCKED
        """;

    public PostgresOutboxProcessingStore(NpgsqlDataSource source) : this(source, null) { }

    internal PostgresOutboxProcessingStore(NpgsqlDataSource source, OutboxProcessingFaults? faults)
    {
        ArgumentNullException.ThrowIfNull(source);
        _source = source;
        _faults = faults;
    }

    public async Task<OutboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        OutboxProcessingLimits.ValidateOwner(owner);
        OutboxProcessingLimits.ValidateLeaseDuration(leaseDuration);
        return await TranslateAvailabilityAsync(async () =>
        {
            await using var connection = await _source.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
            await ConfigureAsync(connection, transaction, cancellationToken);
            if (_faults?.BeforeLock is { } beforeLock) await beforeLock(connection, transaction, cancellationToken);
            await using var select = Command(AcquisitionSql, connection, transaction);
            // Work claiming skips pessimistic row-level locks, allowing independent workers to
            // progress without strict FIFO. NoEligibleWork can mean all due rows are locked.
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                return new OutboxAcquisitionResult(OutboxAcquisitionStatus.NoEligibleWork, null);
            var content = new OutboxMessage(reader.GetGuid(0), reader.GetString(1), reader.GetString(2),
                reader.GetString(3), new DateTimeOffset(reader.GetDateTime(4)));
            if (reader.GetInt64(5) == long.MaxValue)
                throw new InvalidOperationException("Outbox fencing generation exhausted; tokens must never be reused.");
            await reader.DisposeAsync();
            if (_faults?.AfterLock is { } afterLock) await afterLock(connection, transaction, cancellationToken);
            // Lease-based recovery advances the fencing token under the selected row lock.
            // Revalidate against a fresh database instant rather than the selection timestamp.
            await using var update = Command("""
                WITH instant AS MATERIALIZED (SELECT clock_timestamp() AS now)
                UPDATE public.ordering_outbox_messages SET publication_status = 'Processing', lease_owner = $2,
                    lease_generation = lease_generation + 1,
                    lease_expires_at_utc = instant.now + $3, next_attempt_at_utc = NULL
                FROM instant WHERE event_id = $1 AND lease_generation < 9223372036854775807
                  AND (publication_status = 'Pending'
                    OR (publication_status = 'RetryScheduled' AND next_attempt_at_utc <= instant.now)
                    OR (publication_status = 'Processing' AND lease_expires_at_utc <= instant.now))
                RETURNING lease_generation, lease_expires_at_utc
                """, connection, transaction);
            update.Parameters.AddWithValue(content.EventId);
            update.Parameters.AddWithValue(owner);
            update.Parameters.AddWithValue(leaseDuration);
            await using var updated = await update.ExecuteReaderAsync(cancellationToken);
            if (!await updated.ReadAsync(cancellationToken))
                return new OutboxAcquisitionResult(OutboxAcquisitionStatus.NoEligibleWork, null);
            var work = new AcquiredOutboxMessage(new(content.EventId, owner, updated.GetInt64(0)), content,
                new DateTimeOffset(updated.GetDateTime(1)));
            await updated.DisposeAsync();
            // A short-lived transaction releases every lock before usable work reaches the caller.
            return await CommitAsync(connection, transaction, cancellationToken)
                ? new(OutboxAcquisitionStatus.Acquired, work)
                : new(OutboxAcquisitionStatus.OutcomeUnknown, null);
        });
    }

    public Task<OutboxMutationResult> RenewAsync(OutboxLease lease, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        OutboxProcessingLimits.ValidateLeaseDuration(leaseDuration);
        return MutateAsync(lease, Mutation.Renew, leaseDuration, cancellationToken);
    }

    public Task<OutboxMutationResult> MarkPublishedAsync(OutboxLease lease, CancellationToken cancellationToken) =>
        MutateAsync(lease, Mutation.Publish, TimeSpan.Zero, cancellationToken);

    public Task<OutboxMutationResult> ScheduleRetryAsync(OutboxLease lease, TimeSpan retryDelay, CancellationToken cancellationToken)
    {
        OutboxProcessingLimits.ValidateRetryDelay(retryDelay);
        return MutateAsync(lease, Mutation.Retry, retryDelay, cancellationToken);
    }

    private enum Mutation { Renew, Publish, Retry }

    private async Task<OutboxMutationResult> MutateAsync(OutboxLease lease, Mutation mutation, TimeSpan duration, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(lease);
        return await TranslateAvailabilityAsync(async () =>
        {
            await using var connection = await _source.OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
            await ConfigureAsync(connection, transaction, ct);
            if (_faults?.BeforeLock is { } beforeLock) await beforeLock(connection, transaction, ct);
            await using var rowLock = Command("SELECT event_id FROM public.ordering_outbox_messages WHERE event_id = $1 FOR UPDATE", connection, transaction);
            rowLock.Parameters.AddWithValue(lease.EventId);
            if (await rowLock.ExecuteScalarAsync(ct) is null) return new OutboxMutationResult(OutboxMutationStatus.LeaseLost);
            if (_faults?.AfterLock is { } afterLock) await afterLock(connection, transaction, ct);

            // Fencing token validation uses a separate statement AFTER the lock wait. Sampling
            // time before waiting could authorize an expired worker. The lock protects validation
            // and mutation together; authority is checked here, not at later commit/response time.
            var assignment = mutation switch
            {
                Mutation.Renew => "lease_expires_at_utc = GREATEST(lease_expires_at_utc, instant.now + $4)",
                Mutation.Publish => "publication_status = 'Published', published_at_utc = instant.now, lease_owner = NULL, lease_expires_at_utc = NULL",
                // Retry scheduling uses the database clock and releases authority in the same write.
                _ => "publication_status = 'RetryScheduled', next_attempt_at_utc = instant.now + $4, lease_owner = NULL, lease_expires_at_utc = NULL"
            };
            await using var update = Command($"""
                WITH instant AS MATERIALIZED (SELECT clock_timestamp() AS now)
                UPDATE public.ordering_outbox_messages SET {assignment}
                FROM instant WHERE event_id = $1 AND publication_status = 'Processing'
                    AND lease_owner = $2 AND lease_generation = $3 AND lease_expires_at_utc > instant.now
                RETURNING lease_expires_at_utc
                """, connection, transaction);
            update.Parameters.AddWithValue(lease.EventId);
            update.Parameters.AddWithValue(lease.Owner);
            update.Parameters.AddWithValue(lease.Generation);
            if (mutation != Mutation.Publish) update.Parameters.AddWithValue(duration);
            await using var reader = await update.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) return new OutboxMutationResult(OutboxMutationStatus.LeaseLost);
            DateTimeOffset? expires = reader.IsDBNull(0) ? null : new DateTimeOffset(reader.GetDateTime(0));
            await reader.DisposeAsync();
            return await CommitAsync(connection, transaction, ct)
                ? new(OutboxMutationStatus.Succeeded, expires)
                : new(OutboxMutationStatus.OutcomeUnknown);
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
            throw new InvalidOperationException("Outbox requires a primary with fsync and full_page_writes enabled.");
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
            // Ambiguous commit outcome: the server may already have committed. Do not assert
            // rollback, retry automatically, or expose usable acquisition/renewal evidence.
            return false;
        }
    }

    private static async Task<T> TranslateAvailabilityAsync<T>(Func<Task<T>> operation)
    {
        try { return await operation(); }
        catch (NpgsqlException error) when (error.IsTransient) { throw new OutboxStorageUnavailableException(); }
    }
}

// Deterministic real-database test boundaries, absent from the public Application contract.
internal sealed record OutboxProcessingFaults(
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? BeforeLock = null,
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? AfterLock = null,
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? BeforeCommit = null,
    Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task>? Commit = null);
