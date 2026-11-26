using Ordering.Application.Outbox;
using Ordering.Infrastructure.Kafka;

namespace Ordering.Outbox.PublicationBenchmarks.Tests;

public sealed class ScalingTests
{
    [Fact]
    public void SequentialProtocolAndFullAndReducedMatricesRemainExplicit()
    {
        Assert.Equal(new[] { new WorkerConfiguration(1, PublisherTopology.Shared) }, BenchmarkOptions.Parse([]).Configurations);
        var full = BenchmarkOptions.Parse(["--scalability"]);
        Assert.Equal(new[] { "1-Shared", "2-Shared", "2-PerWorker", "4-Shared", "4-PerWorker", "8-Shared", "8-PerWorker" },
            full.Configurations.Select(c => c.Id));
        Assert.Equal(500, full.Events);
        Assert.Equal(3, full.Rounds);
        Assert.Equal(3, BenchmarkOptions.Parse(["--scalability", "--smoke"]).Configurations.Count);
        var custom = BenchmarkOptions.Parse(["--scalability", "--workers", "1,3", "--events", "70", "--rounds", "2"]);
        Assert.Equal(3, custom.Configurations.Count);
        Assert.Equal(70, custom.Events);
        Assert.Equal(2, custom.Rounds);
    }

    [Theory]
    [InlineData("--workers", "1,2")]
    [InlineData("--scalability", "--scalability")]
    [InlineData("--balanced-order", "--balanced-order")]
    public void InvalidModeOptionsAreRejected(string a, string b) =>
        Assert.Throws<ArgumentException>(() => BenchmarkOptions.Parse([a, b]));

    [Theory]
    [InlineData("2,4")]
    [InlineData("1,0")]
    [InlineData("1,2,2")]
    [InlineData("1,-1")]
    public void WorkerListMustHaveBaselineAndUniquePositiveCounts(string value) =>
        Assert.Throws<ArgumentException>(() => BenchmarkOptions.Parse(["--scalability", "--workers", value]));

    [Fact]
    public void BalancedScheduleAlternatesTopologiesBetweenRoundsAndIsolatesWarmupsAndRecovery()
    {
        var options = BenchmarkOptions.Parse(["--scalability", "--workers", "1,8", "--balanced-order", "--rounds", "4"]);
        var phases = options.Phases().ToArray();
        Assert.All(phases[..3], p => Assert.Equal(("warmup", 20), (p.Kind, p.Events)));
        Assert.Equal(("recovery", 1, 1, options.Configurations[0]), phases[^1]);
        var measured = phases[3..^1];
        Assert.Equal(6000, measured.Sum(p => p.Events));
        for (var round = 1; round <= 4; round++)
        {
            var group = measured.Skip((round - 1) * 3).Take(3).ToArray();
            Assert.All(group, p => Assert.Equal(("benchmark", round, 500), (p.Kind, p.Round, p.Events)));
            Assert.Equal(round % 2 == 1 ? new[] { "1-Shared", "8-Shared", "8-PerWorker" }
                : new[] { "1-Shared", "8-PerWorker", "8-Shared" }, group.Select(p => p.Configuration.Id));
        }
        Assert.Throws<ArgumentException>(() => BenchmarkOptions.Parse(["--balanced-order"]));
    }

    [Fact]
    public void WaitSharesUseMatchedAttemptsAndWorkersWithPositiveDenominators()
    {
        StageSample[] samples = [new(1, KafkaOutboxDiagnostics.WaitName, "Acquired", 1),
            new(2, KafkaOutboxDiagnostics.WaitName, "Acquired", 8, 2),
            new(3, KafkaOutboxDiagnostics.WaitName, "Acquired", 0),
            new(3, OutboxRelayDiagnostics.PublicationDurationName, "Acknowledged", 0),
            new(2, OutboxRelayDiagnostics.PublicationDurationName, "Acknowledged", 10, 2),
            new(1, OutboxRelayDiagnostics.PublicationDurationName, "Acknowledged", 100),
            new(4, KafkaOutboxDiagnostics.WaitName, "Acquired", 1),
            new(4, OutboxRelayDiagnostics.PublicationDurationName, "Acknowledged", 2, 2)];
        Assert.Equal(new[] { .01, .8 }, TelemetryValidation.WaitShares(samples));
    }

    private static PhaseReport Phase(int workers, int round, double milliseconds, bool complete = true) => new("benchmark", round, 10, "topic")
    {
        Configuration = new(workers, PublisherTopology.Shared), BatchMilliseconds = milliseconds,
        Complete = complete, DatabaseCleanup = "Dropped", PublisherCleanup = "Disposed",
        Validation = new(10, 10, 10, 10, 0, [])
    };

    [Fact]
    public void SummaryPreservesIndividualRoundsAndComputesMedianSpeedupAndEfficiency()
    {
        var summary = ScalingSummary.Create([Phase(1, 1, 100), Phase(1, 2, 200), Phase(1, 3, 50),
            Phase(2, 1, 50), Phase(2, 2, 100), Phase(2, 3, 25)], true);
        Assert.Equal(100, summary[0].MedianDistinctPublishedPerSecond);
        Assert.Equal(200, summary[1].MedianDistinctPublishedPerSecond);
        Assert.Equal(new double?[] { 200, 100, 400 }, summary[1].Rounds.Select(r => r.DistinctPublishedPerSecond));
        Assert.Equal(2, summary[1].Speedup);
        Assert.Equal(1, summary[1].ScalingEfficiency);
        Assert.All(summary[1].Rounds, r => { Assert.Equal(2, r.Speedup); Assert.Equal(1, r.ScalingEfficiency); });
    }

    [Fact]
    public void IncompleteExecutionNeverProducesValidBaselineOrScalingClaims()
    {
        var phases = new[] { Phase(1, 1, 100, false), Phase(2, 1, 50) };
        foreach (var configuration in ScalingSummary.Create(phases, false))
        {
            Assert.False(configuration.BaselineEligible);
            Assert.Null(configuration.Speedup);
            Assert.Null(configuration.ScalingEfficiency);
            Assert.All(configuration.Rounds, r => Assert.Null(r.Speedup));
        }
    }

    [Fact]
    public void CorrelationValidationRejectsDuplicatesMissingSamplesAndWrongWorkerOrStatus()
    {
        var attempt = AttemptSample.Create(1, 5, new(OutboxRelayStatus.Published, OutboxRelayStage.Completion,
            Guid.NewGuid(), OutboxDeliveryStatus.Acknowledged, new("topic", 0, 0)), "topic", 2, "owner");
        StageSample[] samples = [new(1, StageCollector.AcquisitionName, "Acquired", 1, 2),
            new(1, KafkaOutboxDiagnostics.WaitName, "Acquired", .2, 2),
            new(1, KafkaOutboxDiagnostics.HoldName, "Released", .6, 2),
            new(1, KafkaOutboxDiagnostics.ProduceName, "Acknowledged", .4, 2),
            new(1, OutboxRelayDiagnostics.PublicationDurationName, "Acknowledged", 1, 2),
            new(1, OutboxRelayDiagnostics.CompletionDurationName, "Succeeded", 1, 2),
            new(1, OutboxRelayDiagnostics.AttemptDurationName, "Published", 4, 2)];
        WorkerIdentity[] owners = [new(2, "owner")];
        Assert.True(TelemetryValidation.Check([attempt], samples, owners));
        Assert.False(TelemetryValidation.Check([attempt], [.. samples, samples[0]], owners));
        Assert.False(TelemetryValidation.Check([attempt], samples.Skip(1).ToArray(), owners));
        Assert.False(TelemetryValidation.Check([attempt], [samples[0] with { Worker = 1 }, .. samples.Skip(1)], owners));
        Assert.False(TelemetryValidation.Check([attempt], [samples[0] with { Status = "Wrong" }, .. samples.Skip(1)], owners));
        Assert.False(TelemetryValidation.Check([attempt with { Owner = "other" }], samples, owners));
        Assert.False(TelemetryValidation.Check([attempt], [samples[0] with { Milliseconds = double.NaN }, .. samples.Skip(1)], owners));
        var cancelled = attempt with { Result = new(OutboxRelayStatus.Cancelled, OutboxRelayStage.Publication,
            Delivery: OutboxDeliveryStatus.Unconfirmed) };
        StageSample[] partial = [samples[0], samples[1] with { Status = "Cancelled" },
            samples[4] with { Status = "Cancelled" }, samples[6] with { Status = "Cancelled" }];
        Assert.True(TelemetryValidation.Check([cancelled], partial, owners));
        Assert.False(TelemetryValidation.Check([cancelled], [.. partial, samples[2]], owners));
    }
}
