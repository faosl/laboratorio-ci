using System.Diagnostics;
using System.Text.Json;
using Npgsql;
using Ordering.Application.Outbox;
using Ordering.Infrastructure.PostgreSql;

namespace Ordering.Outbox.Benchmarks;

internal sealed record Sample(int Worker, double Milliseconds, string Status, Guid? EventId = null,
    long? Generation = null, string? ErrorType = null, string? SqlState = null);

internal sealed record BatchResult(double WallSeconds, Sample[] Samples)
{
    public object Summary => new
    {
        Attempts = Samples.Length,
        Selected = Samples.Count(s => s.Status == "Selected"),
        SuccessfulAcquisitions = Samples.Count(s => s.Status == "Acquired"),
        NoEligibleWork = Samples.Count(s => s.Status == "NoEligibleWork"),
        OutcomeUnknown = Samples.Count(s => s.Status == "OutcomeUnknown"),
        Errors = Samples.Count(s => s.ErrorType is not null),
        KnownTimeouts = Samples.Count(s => s.Status == "Timeout"),
        // The production adapter deliberately removes provider details from transient errors.
        StorageUnavailableWithUnknownCause = Samples.Count(s => s.Status == "StorageUnavailable"),
        WallSeconds,
        AttemptsPerSecond = Samples.Length / WallSeconds,
        AcquisitionsPerSecond = Samples.Count(s => s.Status == "Acquired") / WallSeconds,
        SelectedPerSecond = Samples.Count(s => s.Status == "Selected") / WallSeconds,
        AllAttemptsLatencyMs = Percentiles(Samples),
        SuccessfulLatencyMs = Percentiles(Samples.Where(s => s.Status is "Selected" or "Acquired").ToArray()),
        StatusCounts = Samples.GroupBy(s => s.Status).ToDictionary(g => g.Key, g => g.Count())
    };

    public static object? Percentiles(Sample[] samples)
    {
        if (samples.Length == 0) return null;
        var ordered = samples.Select(s => s.Milliseconds).Order().ToArray();
        double P(double fraction) => ordered[(int)Math.Ceiling(ordered.Length * fraction) - 1];
        return new { P50 = P(.50), P95 = P(.95), P99 = P(.99) };
    }
}

internal static class Measurements
{
    public static async Task<JsonElement> ExplainAsync(NpgsqlDataSource source, CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);
        try
        {
            await ConfigureQueryAsync(connection, tx, ct);
            await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
                + PostgresOutboxProcessingStore.AcquisitionSql, connection, tx) { CommandTimeout = 10 };
            var json = (string)(await command.ExecuteScalarAsync(ct))!;
            using var document = JsonDocument.Parse(json);
            return document.RootElement.Clone();
        }
        // EXPLAIN ANALYZE executes FOR UPDATE: release its row lock before any next experiment.
        finally { await tx.RollbackAsync(CancellationToken.None); }
    }

    private static async Task ConfigureQueryAsync(NpgsqlConnection connection, NpgsqlTransaction tx, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("""
            SET LOCAL synchronous_commit = on;
            SET LOCAL lock_timeout = '2s';
            SET LOCAL statement_timeout = '5s';
            SET LOCAL idle_in_transaction_session_timeout = '10s'
            """, connection, tx) { CommandTimeout = 10 };
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task<BatchResult> RunAsync(NpgsqlDataSource source, bool queryOnly, int workers,
        int samples, CancellationToken ct)
    {
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = Enumerable.Range(0, workers).Select(_ => new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
        var tasks = Enumerable.Range(0, workers).Select(Worker).ToArray();
        try { await Task.WhenAll(ready.Select(r => r.Task)).WaitAsync(ct); }
        catch { start.TrySetCanceled(ct); try { await Task.WhenAll(tasks); } catch { } throw; }
        var wall = Stopwatch.StartNew();
        start.SetResult();
        var results = await Task.WhenAll(tasks);
        wall.Stop();
        return new(wall.Elapsed.TotalSeconds, results.SelectMany(s => s).ToArray());

        async Task<Sample[]> Worker(int worker)
        {
            NpgsqlConnection? queryConnection = null;
            try
            {
                // Query-only holds a connection and excludes open, BEGIN, SET and ROLLBACK from latency.
                if (queryOnly) queryConnection = await source.OpenConnectionAsync(ct);
                ready[worker].TrySetResult();
                await start.Task;
                var store = new PostgresOutboxProcessingStore(source);
                var results = new List<Sample>();
                for (var i = 0; i < samples / workers; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    results.Add(queryOnly ? await QueryAsync(queryConnection!, worker, ct)
                        : await AcquireAsync(store, worker, ct));
                }
                return results.ToArray();
            }
            catch (Exception ex) { ready[worker].TrySetException(ex); throw; }
            finally { if (queryConnection is not null) await queryConnection.DisposeAsync(); }
        }
    }

    private static async Task<Sample> QueryAsync(NpgsqlConnection connection, int worker, CancellationToken ct)
    {
        await using var tx = await connection.BeginTransactionAsync(ct);
        try
        {
            await ConfigureQueryAsync(connection, tx, ct);
            var timer = Stopwatch.StartNew();
            try
            {
                await using var command = new NpgsqlCommand(PostgresOutboxProcessingStore.AcquisitionSql, connection, tx)
                    { CommandTimeout = 10 };
                await using var reader = await command.ExecuteReaderAsync(ct);
                Sample result;
                if (await reader.ReadAsync(ct))
                {
                    var id = reader.GetGuid(0);
                    _ = reader.GetString(1); _ = reader.GetString(2); _ = reader.GetString(3); _ = reader.GetDateTime(4);
                    var generation = reader.GetInt64(5);
                    await reader.DisposeAsync();
                    result = new(worker, timer.Elapsed.TotalMilliseconds, "Selected", id, generation);
                }
                else result = new(worker, timer.Elapsed.TotalMilliseconds, "NoEligibleWork");
                return result;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested) { return Failure(ex, worker, timer.Elapsed.TotalMilliseconds); }
        }
        finally { await tx.RollbackAsync(CancellationToken.None); }
    }

    private static async Task<Sample> AcquireAsync(PostgresOutboxProcessingStore store, int worker, CancellationToken ct)
    {
        var timer = Stopwatch.StartNew();
        try
        {
            var result = await store.TryAcquireAsync("benchmark-worker-" + worker, TimeSpan.FromHours(1), ct);
            return new(worker, timer.Elapsed.TotalMilliseconds, result.Status.ToString(),
                result.Work?.Lease.EventId, result.Work?.Lease.Generation);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested) { return Failure(ex, worker, timer.Elapsed.TotalMilliseconds); }
    }

    private static Sample Failure(Exception ex, int worker, double ms)
    {
        var timeout = ex is TimeoutException || ex.InnerException is TimeoutException
            || ex is PostgresException { SqlState: PostgresErrorCodes.QueryCanceled or PostgresErrorCodes.LockNotAvailable };
        return new(worker, ms, timeout ? "Timeout" : ex is OutboxStorageUnavailableException ? "StorageUnavailable" : "Error",
            ErrorType: ex.GetType().Name, SqlState: (ex as PostgresException)?.SqlState);
    }
}
