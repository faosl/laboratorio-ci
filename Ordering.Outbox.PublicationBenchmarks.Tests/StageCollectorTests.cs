using Ordering.Application.Outbox;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Ordering.Outbox.PublicationBenchmarks.Tests;

public sealed class StageCollectorTests
{
    [Fact]
    public async Task DisposedPhaseNeverReceivesSamplesFromLaterPhasesOrRecovery()
    {
        var first = new StageCollector();
        using (first)
        using (first.BeginAttempt(1, 1)) await Relay().RunOnceAsync("first");
        Assert.Equal(4, first.Snapshot().Length);
        using var second = new StageCollector();
        using (second.BeginAttempt(1, 2)) await Relay().RunOnceAsync("second");
        await Relay().RunOnceAsync("recovery");
        Assert.Equal(4, first.Snapshot().Length);
        Assert.Equal(4, second.Snapshot().Length);
        Assert.All(first.Snapshot(), s => { Assert.Equal(1, s.Attempt); Assert.Equal(1, s.Worker); });
        Assert.All(second.Snapshot(), s => { Assert.Equal(1, s.Attempt); Assert.Equal(2, s.Worker); });
    }

    [Fact]
    public async Task InterleavedAwaitedAttemptsKeepWorkerIdentityAndExactMetricValues()
    {
        using var collector = new StageCollector();
        using var meter = new Meter(OutboxRelayDiagnostics.SourceName);
        using var source = new ActivitySource(OutboxRelayDiagnostics.SourceName);
        var histogram = meter.CreateHistogram<double>(OutboxRelayDiagnostics.PublicationDurationName);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = Task.Run(async () =>
        {
            using var context = collector.BeginAttempt(11, 1);
            using var activity = source.StartActivity(StageCollector.AcquisitionName);
            histogram.Record(.011, new KeyValuePair<string, object?>("outbox.status", "first-before"));
            entered.SetResult();
            await resume.Task;
            histogram.Record(.012, new KeyValuePair<string, object?>("outbox.status", "first-after"));
        });
        await entered.Task;
        using (collector.BeginAttempt(22, 2))
        {
            await Task.Yield();
            using var activity = source.StartActivity(StageCollector.AcquisitionName);
            histogram.Record(.022, new KeyValuePair<string, object?>("outbox.status", "second"));
        }
        resume.SetResult();
        await first;
        var samples = collector.Snapshot();
        Assert.Equal(5, samples.Length);
        Assert.Contains(samples, s => s is { Attempt: 11, Worker: 1, Status: "first-before", Milliseconds: 11 });
        Assert.Contains(samples, s => s is { Attempt: 11, Worker: 1, Status: "first-after", Milliseconds: 12 });
        Assert.Contains(samples, s => s is { Attempt: 22, Worker: 2, Status: "second", Milliseconds: 22 });
        Assert.Single(samples, s => s.Attempt == 11 && s.Instrument == StageCollector.AcquisitionName);
        Assert.Single(samples, s => s.Attempt == 22 && s.Instrument == StageCollector.AcquisitionName);
    }

    [Fact]
    public async Task ConcurrentCollectorsDoNotContaminateOverlappingPhasesAndLoseNoRelaySamples()
    {
        using var first = new StageCollector();
        using var second = new StageCollector();
        async Task Run(StageCollector collector, int worker)
        {
            for (var i = 1; i <= 50; i++)
            {
                using var context = collector.BeginAttempt(i, worker);
                await Task.Yield();
                await Relay().RunOnceAsync("worker-" + worker);
            }
        }
        await Task.WhenAll(Enumerable.Range(1, 8).Select(w => Task.Run(() => Run(w <= 4 ? first : second, w))));
        foreach (var collector in new[] { first, second })
        {
            var samples = collector.Snapshot();
            Assert.Equal(800, samples.Length);
            Assert.Equal(800, samples.Select(s => (s.Attempt, s.Worker, s.Instrument)).Distinct().Count());
            Assert.Equal(200, samples.Count(s => s.Instrument == StageCollector.AcquisitionName && s.Status == "Acquired"));
        }
        Assert.All(first.Snapshot(), s => Assert.InRange(s.Worker, 1, 4));
        Assert.All(second.Snapshot(), s => Assert.InRange(s.Worker, 5, 8));
    }

    private static OutboxRelay Relay() => new(new Store(), new Publisher(), new());
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
    private sealed class Publisher : IOutboxEventPublisher
    {
        public TimeSpan DeliveryTimeout => TimeSpan.FromSeconds(30);
        public Task<OutboxPublicationResult> PublishAsync(OutboxMessage message, CancellationToken ct)
            => Task.FromResult(new OutboxPublicationResult(OutboxDeliveryStatus.Acknowledged, new("topic", 0, 0)));
    }
}
