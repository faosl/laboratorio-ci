using System.Globalization;

namespace Ordering.Outbox.PublicationBenchmarks;

internal sealed record BenchmarkOptions(bool Smoke, int Events, int Rounds, int Warmups, int WarmupEvents,
    int BatchTimeoutSeconds, int VerificationTimeoutSeconds, string Output, bool Scalability, int[] Workers,
    bool BalancedOrder = false)
{
    public IReadOnlyList<WorkerConfiguration> Configurations => !Scalability ? [new(1, PublisherTopology.Shared)]
        : Workers.SelectMany(w => w == 1
            ? new[] { new WorkerConfiguration(w, PublisherTopology.Shared) }
            : new[] { new WorkerConfiguration(w, PublisherTopology.Shared), new(w, PublisherTopology.PerWorker) }).ToArray();

    public IEnumerable<(string Kind, int Round, int Events, WorkerConfiguration Configuration)> Phases()
    {
        var configurations = Configurations;
        foreach (var config in configurations)
        {
            for (var i = 1; i <= Warmups; i++) yield return ("warmup", i, WarmupEvents, config);
            if (!BalancedOrder)
                for (var i = 1; i <= Rounds; i++) yield return ("benchmark", i, Events, config);
        }
        if (BalancedOrder)
        {
            for (var i = 1; i <= Rounds; i++)
                foreach (var pair in configurations.GroupBy(c => c.Workers))
                    foreach (var config in i % 2 == 0 ? pair.Reverse() : pair)
                        yield return ("benchmark", i, Events, config);
        }
        yield return ("recovery", 1, 1, new(1, PublisherTopology.Shared));
    }

    public static BenchmarkOptions Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        var smoke = false;
        var scalability = false;
        var balanced = false;
        for (var i = 0; i < args.Length; i++)
        {
            if (args[i] == "--smoke" && !smoke) { smoke = true; continue; }
            if (args[i] == "--scalability" && !scalability) { scalability = true; continue; }
            if (args[i] == "--balanced-order" && !balanced) { balanced = true; continue; }
            if (!new[] { "--events", "--rounds", "--warmups", "--warmup-events", "--batch-timeout-seconds",
                    "--verification-timeout-seconds", "--output", "--workers" }.Contains(args[i])
                || i + 1 == args.Length || !values.TryAdd(args[i], args[++i]))
                throw new ArgumentException("Invalid or duplicate option. See --help.");
        }
        int Number(string key, int fallback) => values.TryGetValue(key, out var value)
            ? int.Parse(value, CultureInfo.InvariantCulture) : fallback;
        var result = new BenchmarkOptions(smoke, Number("--events", smoke ? 20 : 500),
            Number("--rounds", smoke ? 1 : 3), Number("--warmups", 1), Number("--warmup-events", 20),
            Number("--batch-timeout-seconds", 300), Number("--verification-timeout-seconds", 30),
            values.GetValueOrDefault("--output", $"artifacts/outbox-publication-{Guid.NewGuid():N}.json"), scalability,
            values.GetValueOrDefault("--workers", smoke ? "1,2" : "1,2,4,8")
                .Split(',').Select(v => int.Parse(v, CultureInfo.InvariantCulture)).ToArray(), balanced);
        if (balanced && !scalability) throw new ArgumentException("--balanced-order requires --scalability.");
        if (result.Events < 1 || result.Rounds < 1 || result.Warmups < 1 || result.WarmupEvents < 1
            || result.BatchTimeoutSeconds < 1 || result.VerificationTimeoutSeconds < 1
            || string.IsNullOrWhiteSpace(result.Output))
            throw new ArgumentException("Counts and deadlines must be positive; output must be a new file.");
        if ((!scalability && values.ContainsKey("--workers")) || result.Workers.Any(w => w < 1)
            || result.Workers.Distinct().Count() != result.Workers.Length || result.Workers[0] != 1)
            throw new ArgumentException("--workers requires --scalability and distinct positive counts starting with 1 (baseline).");
        return result;
    }
}

internal enum PublisherTopology { Shared, PerWorker }
internal sealed record WorkerConfiguration(int Workers, PublisherTopology Topology)
{
    public string Id => $"{Workers}-{Topology}";
}
