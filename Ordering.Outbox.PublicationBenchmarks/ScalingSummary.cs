namespace Ordering.Outbox.PublicationBenchmarks;

internal sealed record RoundComparison(int Round, bool BaselineEligible, double? DistinctPublishedPerSecond,
    double? Speedup, double? ScalingEfficiency);
internal sealed record ConfigurationSummary(WorkerConfiguration Configuration, bool BaselineEligible,
    RoundComparison[] Rounds, double? MedianDistinctPublishedPerSecond, double? Speedup, double? ScalingEfficiency);

internal static class ScalingSummary
{
    public static ConfigurationSummary[] Create(IReadOnlyList<PhaseReport> phases, bool validRun)
    {
        var measured = phases.Where(p => p.Kind == "benchmark").ToArray();
        var baseline = measured.Where(p => p.Configuration == new WorkerConfiguration(1, PublisherTopology.Shared)
            && p.BaselineEligible).ToDictionary(p => p.Round);
        static double? Throughput(PhaseReport p) => p.BatchMilliseconds is > 0
            ? p.Validation?.PublishedDistinctIds / (p.BatchMilliseconds.Value / 1000) : null;
        static double? Median(IEnumerable<double?> values)
        {
            var ordered = values.Where(v => v.HasValue).Select(v => v!.Value).Order().ToArray();
            return ordered.Length == 0 ? null : (ordered[(ordered.Length - 1) / 2] + ordered[ordered.Length / 2]) / 2;
        }
        var baselineMedian = validRun ? Median(baseline.Values.Select(Throughput)) : null;
        return measured.GroupBy(p => p.Configuration).Select(group =>
        {
            var eligible = validRun && baseline.Count > 0 && group.Count() == baseline.Count
                && group.All(p => p.BaselineEligible && baseline.ContainsKey(p.Round));
            var rounds = group.Select(p =>
            {
                var comparable = eligible && baseline.TryGetValue(p.Round, out _);
                // Speedup compares distinct durable publications/sec with the NEW 1-worker
                // run in the same experiment; efficiency divides that ratio by worker count.
                double? speedup = comparable && Throughput(baseline[p.Round]) is > 0
                    ? Throughput(p) / Throughput(baseline[p.Round]) : null;
                return new RoundComparison(p.Round, comparable, Throughput(p), speedup, speedup / group.Key.Workers);
            }).ToArray();
            var median = Median(group.Select(Throughput));
            var ratio = eligible && baselineMedian is > 0 ? median / baselineMedian : null;
            return new ConfigurationSummary(group.Key, eligible, rounds, median, ratio, ratio / group.Key.Workers);
        }).ToArray();
    }
}
