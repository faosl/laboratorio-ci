namespace Ordering.Outbox.Benchmarks;

internal sealed record BenchmarkOptions(int Events, int Blocked, int Warmup, int Samples, int Rounds, string Output)
{
    public static BenchmarkOptions Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < args.Length; i += 2)
        {
            if (i + 1 == args.Length || !new[] { "--events", "--blocked", "--warmup", "--samples", "--rounds", "--output" }.Contains(args[i])
                || !values.TryAdd(args[i], args[i + 1]))
                throw new ArgumentException("Use --events N --blocked N --warmup N --samples N --rounds N --output path.json (no duplicate options).");
        }
        int Number(string key, int fallback) => values.TryGetValue(key, out var value)
            ? int.Parse(value, System.Globalization.CultureInfo.InvariantCulture) : fallback;
        var result = new BenchmarkOptions(Number("--events", 100_000), Number("--blocked", 500),
            Number("--warmup", 20), Number("--samples", 1000), Number("--rounds", 3),
            values.GetValueOrDefault("--output", "artifacts/outbox-benchmark.json"));
        if (result.Events < 20 || result.Events > int.MaxValue / 7 || result.Events % 20 != 0 || result.Blocked < 1
            || result.Warmup < 4 || result.Warmup % 4 != 0 || result.Samples < 4 || result.Samples % 4 != 0
            || result.Rounds < 1 || result.Blocked >= result.Events / 5
            || Math.Max(result.Warmup, result.Samples) > result.Events / 5 - result.Blocked)
            throw new ArgumentException("Events must be a positive multiple of 20; warmup/samples positive multiples of 4; rounds >= 1; blocked >= 1; warmup/samples <= eligible (20% of events) minus blocked.");
        return result;
    }

    public Dictionary<string, int> Counts => new()
    {
        ["Pending"] = Events / 10,
        ["RetryScheduledDue"] = Events / 20,
        ["RetryScheduledFuture"] = Events * 3 / 20,
        ["ProcessingExpired"] = Events / 20,
        ["ProcessingActive"] = Events / 20,
        ["Published"] = Events * 3 / 5
    };
}
