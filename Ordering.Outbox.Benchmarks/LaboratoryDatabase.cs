using Npgsql;
using Ordering.Infrastructure.PostgreSql;

namespace Ordering.Outbox.Benchmarks;

// No user-supplied database identifier can reach CREATE, TRUNCATE or DROP.
internal sealed class LaboratoryDatabase : IAsyncDisposable
{
    private readonly string _name = "outbox_bench_" + Guid.NewGuid().ToString("N");
    private readonly NpgsqlDataSource _admin;
    private readonly string _connection;
    private bool _created;
    public NpgsqlDataSource Source { get; private set; } = null!;
    public string Name => _name;

    public LaboratoryDatabase()
    {
        _connection = Environment.GetEnvironmentVariable("INBOX_TEST_CONNECTION")
            ?? throw new InvalidOperationException("INBOX_TEST_CONNECTION must point exclusively to laboratory PostgreSQL with CREATEDB permission.");
        var admin = new NpgsqlConnectionStringBuilder(_connection) { Enlist = false, Pooling = false, Timeout = 10 };
        _admin = NpgsqlDataSource.Create(admin.ConnectionString);
    }

    public async Task InitializeAsync(CancellationToken ct)
    {
        await using var create = _admin.CreateCommand($"CREATE DATABASE \"{_name}\" TEMPLATE template0");
        await create.ExecuteNonQueryAsync(ct);
        _created = true;
        var builder = new NpgsqlConnectionStringBuilder(_connection)
        {
            Database = _name, Enlist = false, Pooling = true, MinPoolSize = 0, MaxPoolSize = 16,
            Timeout = 10, CommandTimeout = 60, ApplicationName = "Ordering.Outbox.Benchmarks",
            Multiplexing = false, MaxAutoPrepare = 0, NoResetOnClose = false
        };
        Source = NpgsqlDataSource.Create(builder.ConnectionString);
        await InboxSchema.ApplyAsync(Source, ct);
    }

    public async Task SeedAsync(BenchmarkOptions options, DateTime anchor, CancellationToken ct)
    {
        if (!_created || Source is null) throw new InvalidOperationException("Owned database was not created.");
        await using var connection = await Source.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var truncate = new NpgsqlCommand("TRUNCATE public.ordering_outbox_messages, public.ordering_lab_orders", connection, tx))
            await truncate.ExecuteNonQueryAsync(ct);
        await using (var orders = new NpgsqlCommand("""
            INSERT INTO public.ordering_lab_orders(order_id, status)
            SELECT 'benchmark-order-' || i, 'Confirmed' FROM generate_series(1, $1) AS i
            """, connection, tx) { CommandTimeout = 120 })
        {
            orders.Parameters.AddWithValue(options.Events);
            await orders.ExecuteNonQueryAsync(ct);
        }
        await using var command = new NpgsqlCommand("""
            INSERT INTO public.ordering_outbox_messages
                (event_id, event_type, aggregate_id, payload, publication_status, created_at_utc,
                 lease_owner, lease_expires_at_utc, lease_generation, next_attempt_at_utc, published_at_utc)
            SELECT md5(i::text)::uuid, 'OrderConfirmed', 'benchmark-order-' || i,
                jsonb_build_object('EventId', md5(i::text)::uuid, 'OrderId', 'benchmark-order-' || i),
                state, $2 - interval '2 days' + i * interval '1 microsecond',
                CASE WHEN state = 'Processing' THEN 'seed-owner' END,
                CASE WHEN state = 'Processing' THEN $2 + CASE WHEN i <= $1 * 7 / 20
                    THEN interval '-1 day' ELSE interval '7 days' END END,
                CASE WHEN state = 'Pending' THEN 0 ELSE 1 END,
                CASE WHEN state = 'RetryScheduled' THEN $2 + CASE WHEN i <= $1 * 3 / 20
                    THEN interval '-1 day' ELSE interval '7 days' END END,
                CASE WHEN state = 'Published' THEN $2 - interval '1 day' END
            FROM generate_series(1, $1) AS i
            CROSS JOIN LATERAL (SELECT CASE
                WHEN i <= $1 / 10 THEN 'Pending'
                WHEN i <= $1 * 3 / 10 THEN 'RetryScheduled'
                WHEN i <= $1 * 2 / 5 THEN 'Processing'
                ELSE 'Published' END AS state) AS s;
            """, connection, tx) { CommandTimeout = 120 };
        command.Parameters.AddWithValue(options.Events);
        command.Parameters.AddWithValue(anchor);
        await command.ExecuteNonQueryAsync(ct);
        await tx.CommitAsync(ct);
        // Same logical data and fresh heap/index before every warm-up and measured round.
        await using var analyze = Source.CreateCommand("ANALYZE public.ordering_lab_orders; ANALYZE public.ordering_outbox_messages");
        await analyze.ExecuteNonQueryAsync(ct);
        var actual = await CountsAsync(ct);
        if (options.Counts.Any(pair => actual.GetValueOrDefault(pair.Key) != pair.Value))
            throw new InvalidOperationException("Seed counts do not match the configured six-state distribution.");
    }

    public async Task<Dictionary<string, int>> CountsAsync(CancellationToken ct)
    {
        await using var command = Source.CreateCommand("""
            SELECT CASE publication_status
                WHEN 'RetryScheduled' THEN CASE WHEN next_attempt_at_utc <= statement_timestamp()
                    THEN 'RetryScheduledDue' ELSE 'RetryScheduledFuture' END
                WHEN 'Processing' THEN CASE WHEN lease_expires_at_utc <= statement_timestamp()
                    THEN 'ProcessingExpired' ELSE 'ProcessingActive' END
                ELSE publication_status END AS state, count(*)::int
            FROM public.ordering_outbox_messages GROUP BY 1
            """);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var counts = new Dictionary<string, int>();
        while (await reader.ReadAsync(ct)) counts.Add(reader.GetString(0), reader.GetInt32(1));
        return counts;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (Source is not null) await Source.DisposeAsync();
            if (_created)
            {
                await using var drop = _admin.CreateCommand($"DROP DATABASE \"{_name}\" WITH (FORCE)");
                await drop.ExecuteNonQueryAsync(CancellationToken.None);
                Console.WriteLine($"Removed owned database {_name}.");
            }
        }
        finally { await _admin.DisposeAsync(); }
    }
}
