using Npgsql;

namespace Ordering.Infrastructure.PostgreSql;

/// <summary>Explicit provisioning operation. Never called by the HTTP API.</summary>
public static class InboxSchema
{
    public static async Task ApplyAsync(NpgsqlDataSource dataSource, CancellationToken cancellationToken = default)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var setup = new NpgsqlCommand("""
            SET LOCAL synchronous_commit = on;
            SELECT pg_advisory_xact_lock(72492101);
            CREATE TABLE IF NOT EXISTS public.ordering_inbox_schema_version (
                version integer PRIMARY KEY, applied_at_utc timestamptz NOT NULL DEFAULT clock_timestamp());
            """, connection, transaction);
        await setup.ExecuteNonQueryAsync(cancellationToken);
        // Applied scripts are immutable; pending versions run in order under the same migration lock.
        foreach (var (number, name) in new[] { (1, "001_inbox.sql"), (2, "002_inbox_processing.sql") })
        {
            await using var version = new NpgsqlCommand(
                "SELECT EXISTS (SELECT 1 FROM public.ordering_inbox_schema_version WHERE version = $1)", connection, transaction);
            version.Parameters.AddWithValue(number);
            if ((bool)(await version.ExecuteScalarAsync(cancellationToken))!) continue;
            using var stream = typeof(InboxSchema).Assembly.GetManifestResourceStream(
                "Ordering.Infrastructure.PostgreSql.Migrations." + name)!;
            using var reader = new StreamReader(stream);
            await using var migration = new NpgsqlCommand(await reader.ReadToEndAsync(cancellationToken), connection, transaction);
            await migration.ExecuteNonQueryAsync(cancellationToken);
            await using var record = new NpgsqlCommand(
                "INSERT INTO public.ordering_inbox_schema_version(version) VALUES ($1)", connection, transaction);
            record.Parameters.AddWithValue(number);
            await record.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
    }
}
