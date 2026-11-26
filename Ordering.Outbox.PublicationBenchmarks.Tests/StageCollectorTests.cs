using Ordering.Application.Outbox;
using Ordering.Infrastructure.Kafka;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Ordering.Outbox.PublicationBenchmarks.Tests;

[Collection("Publisher telemetry")]
public sealed class StageCollectorTests
{
    [Fact]
    public async Task DisposedPhaseNeverReceivesSamplesFromLaterPhasesOrRecovery()
    {
        using var publisher = KafkaPublisherTests.Publisher(out _);
        var relay = new OutboxRelay(new Store(), publisher, new());
        var first = new StageCollector();
        using (first)
        using (first.BeginAttempt(1, 1)) await relay.RunOnceAsync("first");
        Assert.Equal(ExpectedStages(), first.Snapshot().Select(s => s.Instrument));
        using var second = new StageCollector();
        using (second.BeginAttempt(1, 2)) await relay.RunOnceAsync("second");
        await relay.RunOnceAsync("recovery");
        Assert.Equal(ExpectedStages(), first.Snapshot().Select(s => s.Instrument));
        Assert.Equal(ExpectedStages(), second.Snapshot().Select(s => s.Instrument));
        Assert.All(first.Snapshot(), s => { Assert.Equal(1, s.Attempt); Assert.Equal(1, s.Worker); });
        Assert.All(second.Snapshot(), s => { Assert.Equal(1, s.Attempt); Assert.Equal(2, s.Worker); });
    }

    private static IEnumerable<string> ExpectedStages() => new[] { StageCollector.AcquisitionName,
        KafkaOutboxDiagnostics.WaitName, KafkaOutboxDiagnostics.HoldName, KafkaOutboxDiagnostics.ProduceName,
        OutboxRelayDiagnostics.PublicationDurationName, OutboxRelayDiagnostics.CompletionDurationName,
        OutboxRelayDiagnostics.AttemptDurationName }.Order();

    [Fact]
    public async Task InterleavedAwaitedAttemptsKeepWorkerIdentityAndExactMetricValues()
    {
        using var collector = new StageCollector();
        using var meter = new Meter(OutboxRelayDiagnostics.SourceName);
        using var source = new ActivitySource(OutboxRelayDiagnostics.SourceName);
        var histogram = meter.CreateHistogram<double>(OutboxRelayDiagnostics.PublicationDurationName);
        using var kafka = new Meter(KafkaOutboxDiagnostics.MeterName);
        var kafkaStages = new[] { KafkaOutboxDiagnostics.WaitName, KafkaOutboxDiagnostics.HoldName,
            KafkaOutboxDiagnostics.ProduceName }.Select(name => kafka.CreateHistogram<double>(name, "s")).ToArray();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = Task.Run(async () =>
        {
            using var context = collector.BeginAttempt(11, 1);
            using var activity = source.StartActivity(StageCollector.AcquisitionName);
            histogram.Record(.011, new KeyValuePair<string, object?>("outbox.status", "first-before"));
            entered.SetResult();
            await resume.Task;
            foreach (var stage in kafkaStages) stage.Record(.011);
            histogram.Record(.012, new KeyValuePair<string, object?>("outbox.status", "first-after"));
        });
        await entered.Task;
        using (collector.BeginAttempt(22, 2))
        {
            await Task.Yield();
            using var activity = source.StartActivity(StageCollector.AcquisitionName);
            histogram.Record(.022, new KeyValuePair<string, object?>("outbox.status", "second"));
            foreach (var stage in kafkaStages) stage.Record(.022);
        }
        resume.SetResult();
        await first;
        var samples = collector.Snapshot();
        Assert.Equal(11, samples.Length);
        foreach (var stage in kafkaStages)
        {
            Assert.Single(samples, s => s.Instrument == stage.Name && s is { Attempt: 11, Worker: 1, Milliseconds: 11 });
            Assert.Single(samples, s => s.Instrument == stage.Name && s is { Attempt: 22, Worker: 2, Milliseconds: 22 });
        }
        Assert.Contains(samples, s => s is { Attempt: 11, Worker: 1, Status: "first-before", Milliseconds: 11 });
        Assert.Contains(samples, s => s is { Attempt: 11, Worker: 1, Status: "first-after", Milliseconds: 12 });
        Assert.Contains(samples, s => s is { Attempt: 22, Worker: 2, Status: "second", Milliseconds: 22 });
        Assert.Single(samples, s => s.Attempt == 11 && s.Instrument == StageCollector.AcquisitionName);
        Assert.Single(samples, s => s.Attempt == 22 && s.Instrument == StageCollector.AcquisitionName);
    }

    [Fact]
    public async Task ConcurrentCollectorsDoNotContaminateOverlappingPhasesAndLoseNoRelaySamples()
    {
        using var publisher = KafkaPublisherTests.Publisher(out _);
        var relay = new OutboxRelay(new Store(), publisher, new());
        using var first = new StageCollector();
        using var second = new StageCollector();
        async Task Run(StageCollector collector, int worker)
        {
            for (var i = 1; i <= 50; i++)
            {
                using var context = collector.BeginAttempt(i, worker);
                await Task.Yield();
                await relay.RunOnceAsync("worker-" + worker);
            }
        }
        await Task.WhenAll(Enumerable.Range(1, 8).Select(w => Task.Run(() => Run(w <= 4 ? first : second, w))));
        foreach (var collector in new[] { first, second })
        {
            var samples = collector.Snapshot();
            Assert.Equal(1400, samples.Length);
            Assert.Equal(1400, samples.Select(s => (s.Attempt, s.Worker, s.Instrument)).Distinct().Count());
            Assert.Equal(200, samples.Count(s => s.Instrument == StageCollector.AcquisitionName && s.Status == "Acquired"));
        }
        Assert.All(first.Snapshot(), s => Assert.InRange(s.Worker, 1, 4));
        Assert.All(second.Snapshot(), s => Assert.InRange(s.Worker, 5, 8));
    }

    private sealed class Store : IOutboxProcessingStore
    {
        public Task<OutboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan duration, CancellationToken ct)
        {
            var id = Guid.NewGuid();
            return Task.FromResult(new OutboxAcquisitionResult(OutboxAcquisitionStatus.Acquired,
                new(new(id, owner, 1), new(id, nameof(OrderConfirmed), "order", "{}", DateTimeOffset.UtcNow),
                    DateTimeOffset.UtcNow + duration)));
        }
        public Task<OutboxMutationResult> MarkPublishedAsync(OutboxLease lease, CancellationToken ct)
            => Task.FromResult(new OutboxMutationResult(OutboxMutationStatus.Succeeded));
        public Task<OutboxMutationResult> RenewAsync(OutboxLease lease, TimeSpan duration, CancellationToken ct)
            => throw new NotSupportedException();
        public Task<OutboxMutationResult> ScheduleRetryAsync(OutboxLease lease, TimeSpan delay, CancellationToken ct)
            => throw new NotSupportedException();
    }
}
