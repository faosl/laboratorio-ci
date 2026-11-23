using System.Globalization;

namespace Ordering.Outbox.PublicationBenchmarks;

internal sealed record BenchmarkOptions(bool Smoke, int Events, int Rounds, int Warmups, int WarmupEvents,
    int BatchTimeoutSeconds, int VerificationTimeoutSeconds, string Output)
{
    public static BenchmarkOptions Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        var smoke = false;
        for (var i = 0; i < args.Length; i++)
        {
            if (args[i] == "--smoke" && !smoke) { smoke = true; continue; }
            if (!new[] { "--events", "--rounds", "--warmups", "--warmup-events", "--batch-timeout-seconds",
                    "--verification-timeout-seconds", "--output" }.Contains(args[i])
                || i + 1 == args.Length || !values.TryAdd(args[i], args[++i]))
                throw new ArgumentException("Invalid or duplicate option. See --help.");
        }
        int Number(string key, int fallback) => values.TryGetValue(key, out var value)
            ? int.Parse(value, CultureInfo.InvariantCulture) : fallback;
        var result = new BenchmarkOptions(smoke, Number("--events", smoke ? 20 : 500),
            Number("--rounds", smoke ? 1 : 3), Number("--warmups", 1), Number("--warmup-events", 20),
            Number("--batch-timeout-seconds", 300), Number("--verification-timeout-seconds", 30),
            values.GetValueOrDefault("--output", $"artifacts/outbox-publication-{Guid.NewGuid():N}.json"));
        if (result.Events < 1 || result.Rounds < 1 || result.Warmups < 1 || result.WarmupEvents < 1
            || result.BatchTimeoutSeconds < 1 || result.VerificationTimeoutSeconds < 1
            || string.IsNullOrWhiteSpace(result.Output))
            throw new ArgumentException("Counts and deadlines must be positive; output must be a new file.");
        return result;
    }
}
