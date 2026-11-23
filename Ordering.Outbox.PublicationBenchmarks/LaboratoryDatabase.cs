using Npgsql;
using Ordering.Application.Outbox;
using Ordering.Infrastructure.PostgreSql;

namespace Ordering.Outbox.PublicationBenchmarks;

// Each phase gets a fresh database. The supplied database is used only to CREATE/DROP our random name.
internal sealed class LaboratoryDatabase : IAsyncDisposable
{
    private readonly string _name = "outbox_pub_" + Guid.NewGuid().ToString("N");
    private readonly NpgsqlDataSource _admin;
    private readonly string _connection;
    private readonly PhaseReport _report;
    private bool _created;
    public NpgsqlDataSource Source { get; private set; } = null!;

    public LaboratoryDatabase(PhaseReport report)
    {
        _report = report;
        _report.Database = _name;
        _connection = Environment.GetEnvironmentVariable("INBOX_TEST_CONNECTION")
            ?? throw new InvalidOperationException("Laboratory connection required.");
        _admin = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(_connection)
            { Pooling = false, Enlist = false }.ConnectionString);
    }

    public async Task InitializeAsync(CancellationToken ct)
    {
        _report.DatabaseCleanup = "CreationOutcomeUnknown";
        await using (var command = _admin.CreateCommand($"CREATE DATABASE \"{_name}\" TEMPLATE template0"))
            await command.ExecuteNonQueryAsync(ct);
        _created = true;
        _report.DatabaseCleanup = "Pending";
        Source = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(_connection)
            { Database = _name, Enlist = false }.ConnectionString);
        await InboxSchema.ApplyAsync(Source, ct);
        await using var version = Source.CreateCommand("SHOW server_version");
        _report.PostgreSqlVersion = (string?)await version.ExecuteScalarAsync(ct);
    }

    public async Task SeedAsync(int count, CancellationToken ct)
    {
        var confirm = new ConfirmOrderUseCase(new PostgresOrderConfirmationStore(Source));
        for (var i = 0; i < count; i++)
        {
            var orderId = "publication-lab-" + Guid.NewGuid().ToString("N");
            await using (var insert = Source.CreateCommand("INSERT INTO public.ordering_lab_orders(order_id) VALUES ($1)"))
            {
                insert.Parameters.AddWithValue(orderId);
                await insert.ExecuteNonQueryAsync(ct);
            }
            var result = await confirm.ExecuteAsync(orderId, ct);
            if (result.Status != OrderConfirmationStatus.Confirmed || result.EventId is null)
                throw new InvalidOperationException("Order confirmation was not confirmed.");
            await using var read = Source.CreateCommand("""
                SELECT event_type, aggregate_id, payload::text, publication_status
                FROM public.ordering_outbox_messages WHERE event_id = $1
                """);
            read.Parameters.AddWithValue(result.EventId.Value);
            await using var reader = await read.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) throw new InvalidOperationException("Confirmed event missing.");
            _report.Expected.Add(new(result.EventId.Value, reader.GetString(1), reader.GetString(0), reader.GetString(2)));
            if (reader.GetString(3) != "Pending") throw new InvalidOperationException("Seed must be Pending.");
        }
        _report.PendingBefore.AddRange(await ReadAsync(ct));
        if (_report.PendingBefore.Count != count || _report.PendingBefore.Any(d => d.PublicationStatus != "Pending")
            || !_report.Expected.Select(e => e.EventId).ToHashSet().SetEquals(_report.PendingBefore.Select(d => d.EventId)))
            throw new InvalidOperationException("Pending seed identity mismatch.");
    }

    public async Task<List<DatabaseEvidence>> ReadAsync(CancellationToken ct)
    {
        // All rows, rather than an ID-filtered query, also reveal unexpected events in our empty-start database.
        await using var command = Source.CreateCommand("""
            SELECT event_id, publication_status, published_at_utc IS NOT NULL
            FROM public.ordering_outbox_messages ORDER BY event_id
            """);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<DatabaseEvidence>();
        while (await reader.ReadAsync(ct)) result.Add(new(reader.GetGuid(0), reader.GetString(1), reader.GetBoolean(2)));
        return result;
    }

    public async Task ExpireOwnedLeaseAsync(Guid eventId, CancellationToken ct)
    {
        if (!_created) throw new InvalidOperationException("Database not owned.");
        await using var command = Source.CreateCommand("""
            UPDATE public.ordering_outbox_messages
            SET lease_expires_at_utc = clock_timestamp() - interval '1 second'
            WHERE event_id = $1 AND publication_status = 'Processing'
            """);
        command.Parameters.AddWithValue(eventId);
        if (await command.ExecuteNonQueryAsync(ct) != 1) throw new InvalidOperationException("Recovery lease missing.");
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (Source is not null) await Source.DisposeAsync();
            if (_created)
            {
                await using var command = _admin.CreateCommand($"DROP DATABASE \"{_name}\" WITH (FORCE)");
                await command.ExecuteNonQueryAsync(CancellationToken.None);
                _report.DatabaseCleanup = "Dropped";
            }
        }
        catch (Exception error) { _report.DatabaseCleanup = "Failed:" + error.GetType().Name; }
        finally { await _admin.DisposeAsync(); }
    }
}
