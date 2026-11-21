using Npgsql;
using Ordering.Infrastructure.PostgreSql;

namespace Ordering.Outbox.Benchmarks;

internal sealed record LockEvidence(int BlockerPid, int Requested, int Selected, int VisibleRows,
    int RowsLockableByOtherTransaction, string NowaitSqlState, bool GrantedTransactionLock);

internal sealed class CandidateLocks : IAsyncDisposable
{
    private readonly NpgsqlConnection _connection;
    private readonly NpgsqlTransaction _transaction;
    public Guid[] Ids { get; private set; } = [];
    public LockEvidence? Evidence { get; private set; }

    private CandidateLocks(NpgsqlConnection connection, NpgsqlTransaction transaction) =>
        (_connection, _transaction) = (connection, transaction);

    public static async Task<CandidateLocks> CreateAsync(NpgsqlDataSource source, int count, CancellationToken ct)
    {
        var connection = await source.OpenConnectionAsync(ct);
        NpgsqlTransaction tx;
        try { tx = await connection.BeginTransactionAsync(ct); }
        catch { await connection.DisposeAsync(); throw; }
        var holder = new CandidateLocks(connection, tx);
        try
        {
            // This intentionally long-lived auxiliary transaction must survive the measured batch.
            // Production and measured query transactions retain their existing 10-second limit.
            await using (var settings = new NpgsqlCommand("SET LOCAL idle_in_transaction_session_timeout = 0", connection, tx))
                await settings.ExecuteNonQueryAsync(ct);
            // Only the auxiliary blocker changes the LIMIT; timed SQL is the original constant.
            var sql = PostgresOutboxProcessingStore.AcquisitionSql.Replace(
                "LIMIT 1 FOR UPDATE SKIP LOCKED", "LIMIT $1 FOR UPDATE", StringComparison.Ordinal);
            await using (var command = new NpgsqlCommand(sql, connection, tx))
            {
                command.Parameters.AddWithValue(count);
                await using var reader = await command.ExecuteReaderAsync(ct);
                var ids = new List<Guid>();
                while (await reader.ReadAsync(ct)) ids.Add(reader.GetGuid(0));
                holder.Ids = ids.ToArray();
            }
            if (holder.Ids.Length != count) throw new InvalidOperationException("Not enough eligible candidates to lock.");
            holder.Evidence = await holder.ProveAsync(source, ct);
            return holder;
        }
        catch { await holder.DisposeAsync(); throw; }
    }

    public async Task<LockEvidence> ProveAsync(NpgsqlDataSource source, CancellationToken ct)
    {
        await using var probe = await source.OpenConnectionAsync(ct);
        await using var tx = await probe.BeginTransactionAsync(ct);
        async Task<int> Count(string sql)
        {
            await using var command = new NpgsqlCommand(sql, probe, tx);
            command.Parameters.AddWithValue(Ids);
            return (int)(await command.ExecuteScalarAsync(ct))!;
        }
        var visible = await Count("SELECT count(*)::int FROM public.ordering_outbox_messages WHERE event_id = ANY($1)");
        var lockable = await Count("""
            SELECT count(*)::int FROM (SELECT event_id FROM public.ordering_outbox_messages
                WHERE event_id = ANY($1) FOR UPDATE SKIP LOCKED) AS candidates
            """);
        await using var check = new NpgsqlCommand("""
            SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND locktype = 'transactionid'
                AND mode = 'ExclusiveLock' AND granted)
            """, probe, tx);
        check.Parameters.AddWithValue(_connection.ProcessID);
        var granted = (bool)(await check.ExecuteScalarAsync(ct))!;
        string state;
        try
        {
            await using var nowait = new NpgsqlCommand("SELECT event_id FROM public.ordering_outbox_messages WHERE event_id = $1 FOR UPDATE NOWAIT", probe, tx);
            nowait.Parameters.AddWithValue(Ids[0]);
            await nowait.ExecuteScalarAsync(ct);
            throw new InvalidOperationException("NOWAIT did not encounter the expected server lock.");
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable) { state = ex.SqlState; }
        finally { await tx.RollbackAsync(CancellationToken.None); }
        if (visible != Ids.Length || lockable != 0 || !granted)
            throw new InvalidOperationException("Server lock evidence is incomplete.");
        return new(_connection.ProcessID, Ids.Length, Ids.Length, visible, lockable, state, granted);
    }

    public async ValueTask DisposeAsync()
    {
        try { await _transaction.RollbackAsync(CancellationToken.None); }
        finally
        {
            try { await _transaction.DisposeAsync(); }
            finally { await _connection.DisposeAsync(); }
        }
    }
}
