using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using Ordering.Infrastructure.PostgreSql;
using Ordering.Outbox.Benchmarks;

if (args.Contains("--help"))
{
    Console.WriteLine("Explicit laboratory benchmark (.NET 8). INBOX_TEST_CONNECTION is required.\n"
        + "--events 100000 --blocked 500 --warmup 20 --samples 1000 --rounds 3 --output artifacts/outbox-benchmark.json\n"
        + "Workers: query=1, TryAcquireAsync=1,2,4. Samples and warmup are TOTAL attempts per round, divisible by 4.\n"
        + "Events must be divisible by 20; state proportions are 10/5/15/5/5/60 percent. See docs/outbox-acquisition-benchmark.md.");
    return 0;
}

using var cancellation = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cancellation.Cancel(); };
var jsonOptions = new JsonSerializerOptions { WriteIndented = true };
var experiments = new List<Experiment>();
var report = new Dictionary<string, object?> { ["StartedAtUtc"] = DateTimeOffset.UtcNow, ["Experiments"] = experiments };
string? output = null;
var ownsReport = false;
try
{
    var options = BenchmarkOptions.Parse(args);
    output = Path.GetFullPath(options.Output);
    Directory.CreateDirectory(Path.GetDirectoryName(output)!);
    // Reserve a new report before touching PostgreSQL; never overwrite a previous baseline.
    await using (var file = new FileStream(output, FileMode.CreateNew)) { }
    ownsReport = true;
    report["Options"] = options;
    report["StateCounts"] = options.Counts;
    report["Code"] = await CodeIdentityAsync();
    report["Client"] = new { Runtime = RuntimeInformation.FrameworkDescription, OS = RuntimeInformation.OSDescription,
        Architecture = RuntimeInformation.ProcessArchitecture.ToString(), Environment.ProcessorCount,
        Npgsql = typeof(NpgsqlConnection).Assembly.GetName().Version?.ToString() };
    report["Protocol"] = new
    {
        PoolMax = 16, ConnectionTimeoutSeconds = 10, QueryCommandTimeoutSeconds = 10,
        Multiplexing = false, MaxAutoPrepare = 0, NoResetOnClose = false,
        StatementTimeoutSeconds = 5, LockTimeoutSeconds = 2, IdleTransactionTimeoutSeconds = 10,
        LeaseSeconds = 3600, Isolation = "ReadCommitted", SynchronousCommit = "on",
        Percentile = "nearest-rank; all attempts and successful attempts separately",
        QueryLatency = "ExecuteReader + consume row; excludes open/BEGIN/SET/ROLLBACK; throughput includes transaction overhead",
        AcquisitionLatency = "complete unmodified TryAcquireAsync including pool checkout, settings, select, update, commit and disposal",
        Samples = "fixed total per round divided equally across synchronized workers; warmup excluded; no retries",
        Reset = "TRUNCATE owned tables + deterministic bulk seed + ANALYZE before warmup and again before measurement",
        Cache = "warm/mixed cache; no OS or PostgreSQL cache eviction; EXPLAIN executes before samples",
        Plan = "original AcquisitionSql, no planner GUC overrides; one EXPLAIN per measured round at initial state",
        TimeoutLimitation = "production transient failures lose provider cause; StorageUnavailable cannot be classified as exact timeout"
    };
    await using (var database = new LaboratoryDatabase())
    {
        await database.InitializeAsync(cancellation.Token);
        report["OwnedDatabase"] = database.Name;
        report["PostgreSql"] = await ServerSettingsAsync(database.Source, cancellation.Token);
        await using var clock = database.Source.CreateCommand("SELECT clock_timestamp()");
        var anchor = (DateTime)(await clock.ExecuteScalarAsync(cancellation.Token))!;
        report["SeedAnchorUtc"] = anchor;

        for (var round = 1; round <= options.Rounds; round++)
        {
            // Alternate order to expose/reduce a systematic always-unlocked-first cache bias.
            foreach (var scenario in round % 2 == 1 ? Scenarios(options.Blocked) : Scenarios(options.Blocked).Reverse())
            {
                Console.WriteLine($"Round {round}/{options.Rounds}: {scenario.Id} (reset + warmup + reset + EXPLAIN + samples)");
                var warmup = await RunPhaseAsync(database, options, anchor, scenario, options.Warmup, false, cancellation.Token);
                var measured = await RunPhaseAsync(database, options, anchor, scenario, options.Samples, true, cancellation.Token);
                experiments.Add(new(round, scenario.Id, scenario.QueryOnly, scenario.Workers, scenario.Blocked,
                    options.Counts, warmup, measured));
                await SaveAsync();
            }
        }
    }
    report["Cleanup"] = "Owned database dropped successfully";
    report["LockComparisons"] = experiments.Where(e => e.Blocked == 0).Select(unlocked =>
    {
        var locked = experiments.Single(e => e.Round == unlocked.Round && e.QueryOnly == unlocked.QueryOnly
            && e.Workers == unlocked.Workers && e.Blocked != 0);
        double P(BatchResult batch, double q)
        {
            var values = batch.Samples.Select(s => s.Milliseconds).Order().ToArray();
            return values[(int)Math.Ceiling(values.Length * q) - 1];
        }
        return new { unlocked.Round, unlocked.QueryOnly, unlocked.Workers,
            P50DeltaMs = P(locked.Measured.Batch, .50) - P(unlocked.Measured.Batch, .50),
            P95DeltaMs = P(locked.Measured.Batch, .95) - P(unlocked.Measured.Batch, .95),
            P99DeltaMs = P(locked.Measured.Batch, .99) - P(unlocked.Measured.Batch, .99),
            AttemptsPerSecondDelta = locked.Measured.Batch.Samples.Length / locked.Measured.Batch.WallSeconds
                - unlocked.Measured.Batch.Samples.Length / unlocked.Measured.Batch.WallSeconds,
            AcquisitionCountDelta = locked.Measured.Batch.Samples.Count(s => s.Status == "Acquired")
                - unlocked.Measured.Batch.Samples.Count(s => s.Status == "Acquired") };
    }).ToArray();
    report["CompletedAtUtc"] = DateTimeOffset.UtcNow;
    var valid = experiments.All(e => e.Warmup.Valid && e.Measured.Valid);
    report["Valid"] = valid;
    await SaveAsync();
    Console.WriteLine($"Report: {output}; valid={valid}. No performance improvement claim is made.");
    return valid ? 0 : 2;
}
catch (Exception ex)
{
    // Exception messages/connection strings may contain credentials; persist only type and SQLSTATE.
    report["Valid"] = false;
    report["Failure"] = new { Type = ex.GetType().Name, SqlState = (ex as PostgresException)?.SqlState,
        Cancelled = cancellation.IsCancellationRequested };
    Console.Error.WriteLine($"Benchmark failed: {ex.GetType().Name}, SQLSTATE={(ex as PostgresException)?.SqlState ?? "n/a"}. No complete baseline. See --help.");
    if (ownsReport) await SaveAsync();
    return 1;
}

async Task SaveAsync() => await File.WriteAllTextAsync(output!, JsonSerializer.Serialize(report, jsonOptions));

static IEnumerable<Scenario> Scenarios(int blocked)
{
    foreach (var locks in new[] { 0, blocked })
    {
        yield return new(true, 1, locks);
        foreach (var workers in new[] { 1, 2, 4 }) yield return new(false, workers, locks);
    }
}

static async Task<Phase> RunPhaseAsync(LaboratoryDatabase database, BenchmarkOptions options, DateTime anchor,
    Scenario scenario, int samples, bool explain, CancellationToken ct)
{
    await database.SeedAsync(options, anchor, ct);
    CandidateLocks? locks = null;
    try
    {
        if (scenario.Blocked != 0) locks = await CandidateLocks.CreateAsync(database.Source, scenario.Blocked, ct);
        JsonElement? plan = explain ? await Measurements.ExplainAsync(database.Source, ct) : null;
        var started = Stopwatch.StartNew();
        var batch = await Measurements.RunAsync(database.Source, scenario.QueryOnly, scenario.Workers, samples, ct);
        var locksAfter = locks is null ? null : await locks.ProveAsync(database.Source, ct);
        var countsAfter = await database.CountsAsync(ct);
        var successes = batch.Samples.Where(s => s.Status == "Acquired").ToArray();
        var duplicateIds = successes.Length - successes.Select(s => s.EventId).Distinct().Count();
        var lockedIdsReturned = batch.Samples.Count(s => s.EventId is { } id && (locks?.Ids.Contains(id) ?? false));
        var acquired = successes.Length;
        var expectedEligible = options.Events / 5 - acquired;
        var actualEligible = countsAfter.GetValueOrDefault("Pending") + countsAfter.GetValueOrDefault("RetryScheduledDue")
            + countsAfter.GetValueOrDefault("ProcessingExpired");
        var valid = batch.Samples.Length == samples && duplicateIds == 0 && lockedIdsReturned == 0
            && batch.Samples.All(s => s.Status == (scenario.QueryOnly ? "Selected" : "Acquired"))
            && actualEligible == expectedEligible && countsAfter.Values.Sum() == options.Events
            && started.Elapsed < TimeSpan.FromHours(1); // Leases must not expire during a round.
        return new(batch, plan, locks?.Evidence, locksAfter, countsAfter, duplicateIds, lockedIdsReturned, valid);
    }
    finally { if (locks is not null) await locks.DisposeAsync(); }
}

static async Task<object> ServerSettingsAsync(NpgsqlDataSource source, CancellationToken ct)
{
    await using var command = source.CreateCommand("""
        SELECT name, setting, unit FROM pg_settings WHERE name IN
            ('server_version', 'server_version_num', 'shared_buffers', 'effective_cache_size', 'work_mem',
             'maintenance_work_mem', 'random_page_cost', 'seq_page_cost', 'effective_io_concurrency',
             'max_connections', 'fsync', 'full_page_writes', 'synchronous_commit', 'jit', 'autovacuum',
             'track_io_timing', 'default_statistics_target', 'enable_seqscan', 'enable_indexscan',
             'enable_bitmapscan', 'max_parallel_workers_per_gather', 'block_size', 'data_checksums') ORDER BY name
        """);
    await using var reader = await command.ExecuteReaderAsync(ct);
    var settings = new Dictionary<string, object>();
    while (await reader.ReadAsync(ct)) settings.Add(reader.GetString(0), new { Value = reader.GetString(1),
        Unit = reader.IsDBNull(2) ? null : reader.GetString(2) });
    return settings;
}

static async Task<object> CodeIdentityAsync()
{
    async Task<string> Git(params string[] args)
    {
        var info = new ProcessStartInfo("git") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true };
        foreach (var arg in args) info.ArgumentList.Add(arg);
        using var process = Process.Start(info)!;
        var text = await process.StandardOutput.ReadToEndAsync();
        _ = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        if (process.ExitCode != 0) throw new InvalidOperationException("Run from the source repository so git SHA can be recorded.");
        return text.Trim();
    }
    static string Hash(string path) => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path)));
    return new { GitSha = await Git("rev-parse", "HEAD"), Dirty = (await Git("status", "--porcelain")).Length != 0,
        BenchmarkAssemblySha256 = Hash(Assembly.GetExecutingAssembly().Location),
        StoreAssemblySha256 = Hash(typeof(PostgresOutboxProcessingStore).Assembly.Location),
        AcquisitionSqlSha256 = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(PostgresOutboxProcessingStore.AcquisitionSql))) };
}

internal sealed record Scenario(bool QueryOnly, int Workers, int Blocked)
{
    public string Id => $"{(QueryOnly ? "query" : "acquire")}-workers-{Workers}-locks-{Blocked}";
}

internal sealed record Phase(BatchResult Batch, JsonElement? Explain, LockEvidence? LockEvidence, LockEvidence? LockEvidenceAfter,
    Dictionary<string, int> CountsAfter, int DuplicateAcquiredIds, int LockedIdsReturned, bool Valid);

internal sealed record Experiment(int Round, string Scenario, bool QueryOnly, int Workers, int Blocked,
    Dictionary<string, int> CountsBefore, Phase Warmup, Phase Measured);
