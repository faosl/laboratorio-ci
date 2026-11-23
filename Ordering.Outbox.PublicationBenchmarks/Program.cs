using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Npgsql;
using Ordering.Infrastructure.PostgreSql;
using Ordering.Outbox.PublicationBenchmarks;
using Testcontainers.Kafka;

if (args.Contains("--help"))
{
    Console.WriteLine("""
        Opt-in real publication laboratory (.NET 8). Run from the source checkout.
        Requires Docker and INBOX_TEST_CONNECTION pointing ONLY to laboratory PostgreSQL with CREATEDB.
        --smoke                      defaults: 20 events, 1 round (normal: 500 events, 3 rounds)
        --events N --rounds N --warmups N --warmup-events N
        --batch-timeout-seconds N     default 300; stop on first non-success, no benchmark retries
        --verification-timeout-seconds N  default 30
        --output new-file.json        default: unique file under artifacts/
        Always includes separate warm-up and injected-failure recovery phases; one sequential worker.
        See docs/outbox-publication-benchmark.md. No sustained-capacity or exactly-once claim.
        """);
    return 0;
}

var phases = new List<PhaseReport>();
var report = new Dictionary<string, object?>
{
    ["StartedAtUtc"] = DateTimeOffset.UtcNow, ["Status"] = "Incomplete", ["BaselineValid"] = false,
    ["Phases"] = phases, ["KafkaCleanup"] = "NotCreated",
    ["Protocol"] = new
    {
        Workers = 1, Partitions = 1, ReplicationFactor = 1, MinInSyncReplicas = 1,
        PublisherOptions = "Unmodified defaults: acks=all, idempotence enabled, delivery timeout 30s",
        RelayOptions = "Unmodified defaults: lease 60s, execution timeout 40s, safety margin 10s, retry delay 5s",
        Reset = "Fresh random database, migrated schema, new confirmed orders and exclusive topic for EVERY phase",
        Measurement = "Monotonic finite-batch wall time: immediately before first relay attempt through last result; preparation and verification excluded",
        Stages = "Existing MeterListener histograms: publication, completion, relay attempt (seconds converted to milliseconds); no acquisition histogram exists",
        Percentiles = "Nearest-rank from observed samples, independently per instrument; never sum stage percentiles",
        Limitations = "Single replica does not validate HA; finite batches do not measure sustained production capacity",
        Recovery = "Injected exception before PostgreSQL completion, manual expiry in owned database, fresh producer; not a real process crash; duplicates are possible",
        PartialResults = "Metrics on incomplete phases are diagnostic only; never a valid baseline",
        Confidentiality = "Synthetic laboratory identities only; no payloads, connection strings, passwords or provider error messages"
    }
};
using var cancellation = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cancellation.Cancel(); };
var json = new JsonSerializerOptions { WriteIndented = true, Converters = { new JsonStringEnumConverter() } };
FileStream? outputFile = null;
KafkaContainer? broker = null;
string? output = null;
var exitCode = 1;
try
{
    var options = BenchmarkOptions.Parse(args);
    output = Path.GetFullPath(options.Output);
    Directory.CreateDirectory(Path.GetDirectoryName(output)!);
    // Retain the CreateNew handle for the whole execution, including error and cleanup reporting.
    outputFile = new FileStream(output, FileMode.CreateNew, FileAccess.Write, FileShare.Read);
    report["Options"] = options;
    report["Code"] = await CodeIdentityAsync();
    report["Client"] = new
    {
        Runtime = RuntimeInformation.FrameworkDescription, OS = RuntimeInformation.OSDescription,
        Architecture = RuntimeInformation.ProcessArchitecture.ToString(), Environment.ProcessorCount,
        Npgsql = typeof(NpgsqlConnection).Assembly.GetName().Version?.ToString(),
        ConfluentKafka = typeof(ProducerConfig).Assembly.GetName().Version?.ToString(),
        Librdkafka = Library.VersionString, TestcontainersKafka = typeof(KafkaContainer).Assembly.GetName().Version?.ToString()
    };
    await SaveAsync();
    if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("INBOX_TEST_CONNECTION")))
        throw new InvalidOperationException("Laboratory connection required.");
    const string kafkaImage = "confluentinc/cp-kafka:7.6.1";
    var containerName = "outbox-publication-" + Guid.NewGuid().ToString("N");
    report["Kafka"] = new { Image = kafkaImage, ContainerName = containerName };
    report["KafkaCleanup"] = "Pending";
    await SaveAsync();
    broker = new KafkaBuilder().WithImage(kafkaImage).WithName(containerName).Build();
    await broker.StartAsync(cancellation.Token);
    var version = await broker.ExecAsync(["kafka-topics", "--version"], cancellation.Token);
    if (version.ExitCode != 0) throw new InvalidOperationException("Kafka version unavailable.");
    report["KafkaVersion"] = Regex.Match(version.Stdout, @"\b\d+\.\d+\.\d+[\w.-]*").Value;
    var bootstrap = broker.GetBootstrapAddress();
    for (var i = 1; i <= options.Warmups; i++)
        if (!await RunAsync("warmup", i, options.WarmupEvents)) throw new InvalidOperationException("Warm-up incomplete.");
    for (var i = 1; i <= options.Rounds; i++)
        if (!await RunAsync("benchmark", i, options.Events)) throw new InvalidOperationException("Round incomplete.");
    if (!await RunAsync("recovery", 1, 1)) throw new InvalidOperationException("Recovery incomplete.");
    report["Status"] = "Completed";
    report["BaselineValid"] = true;
    exitCode = 0;

    async Task<bool> RunAsync(string kind, int round, int events)
    {
        var phase = new PhaseReport(kind, round, events, "outbox-publication-" + Guid.NewGuid().ToString("N"));
        phases.Add(phase);
        await SaveAsync();
        Console.WriteLine($"{kind} {round}: {events} events; exclusive database and topic.");
        await PhaseRunner.RunAsync(phase, bootstrap, options, SaveAsync, cancellation.Token);
        await SaveAsync();
        Console.WriteLine($"{kind} {round}: complete={phase.Complete}; ACKs={phase.Validation?.KafkaAcks}; "
            + $"Kafka records={phase.Validation?.KafkaRecords}; distinct Published={phase.Validation?.PublishedDistinctIds}.");
        return phase.Complete && phase.DatabaseCleanup == "Dropped";
    }
}
catch (Exception error)
{
    report["Status"] = "Incomplete";
    report["BaselineValid"] = false;
    report["Failure"] = new { Type = error.GetType().Name, Cancelled = cancellation.IsCancellationRequested };
    Console.Error.WriteLine($"Incomplete run: {error.GetType().Name}. No valid baseline. See --help.");
}
finally
{
    if (broker is not null)
    {
        try { await broker.DisposeAsync(); report["KafkaCleanup"] = "Removed"; }
        catch (Exception error) { report["KafkaCleanup"] = "Failed:" + error.GetType().Name; }
    }
    if (report["KafkaCleanup"]?.ToString() is not ("Removed" or "NotCreated"))
    {
        report["Status"] = "Incomplete";
        report["BaselineValid"] = false;
        exitCode = 1;
    }
    report["FinishedAtUtc"] = DateTimeOffset.UtcNow;
    var measured = phases.Where(p => p.Kind == "benchmark").ToArray();
    report["MeasuredTotals"] = new
    {
        CompletedRounds = measured.Count(p => p.Complete),
        KafkaAcks = measured.Sum(p => p.Validation?.KafkaAcks ?? 0),
        KafkaRecords = measured.Sum(p => p.KafkaAfter.Count),
        KafkaDistinctIds = measured.SelectMany(p => p.KafkaAfter).Where(k => k.EventId.HasValue)
            .Select(k => k.EventId).Distinct().Count(),
        PublishedDistinctIds = measured.SelectMany(p => p.DatabaseAfter)
            .Where(d => d.PublicationStatus == "Published" && d.HasPublishedAt)
            .Select(d => d.EventId).Distinct().Count(),
        ExcludedPhases = "warmup and recovery; incomplete run totals are diagnostic only"
    };
    if (outputFile is not null)
    {
        try { await SaveAsync(); }
        finally { await outputFile.DisposeAsync(); }
        Console.WriteLine($"Report: {output}; status={report["Status"]}; baseline valid={report["BaselineValid"]}.");
    }
}
return exitCode;

async Task SaveAsync()
{
    var bytes = JsonSerializer.SerializeToUtf8Bytes(report, json);
    outputFile!.Position = 0;
    await outputFile.WriteAsync(bytes);
    outputFile.SetLength(outputFile.Position);
    await outputFile.FlushAsync();
}

static async Task<object> CodeIdentityAsync()
{
    async Task<string> Git(params string[] arguments)
    {
        var start = new ProcessStartInfo("git")
            { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
        foreach (var argument in arguments) start.ArgumentList.Add(argument);
        using var process = Process.Start(start)!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        await stderr;
        if (process.ExitCode != 0) throw new InvalidOperationException("Source checkout required.");
        return (await stdout).Trim();
    }
    static string Hash(Assembly assembly) => Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(assembly.Location)));
    return new
    {
        GitSha = await Git("rev-parse", "HEAD"), Dirty = (await Git("status", "--porcelain")).Length != 0,
        BenchmarkAssemblySha256 = Hash(Assembly.GetExecutingAssembly()),
        StoreAssemblySha256 = Hash(typeof(PostgresOutboxProcessingStore).Assembly),
        RelayAssemblySha256 = Hash(typeof(Ordering.Application.Outbox.OutboxRelay).Assembly),
        PublisherAssemblySha256 = Hash(typeof(Ordering.Infrastructure.Kafka.KafkaOutboxEventPublisher).Assembly)
    };
}
