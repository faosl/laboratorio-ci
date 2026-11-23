using Ordering.Application.Outbox;

namespace Ordering.Outbox.PublicationBenchmarks.Tests;

public sealed class StageCollectorTests
{
    [Fact]
    public async Task DisposedPhaseNeverReceivesSamplesFromLaterPhasesOrRecovery()
    {
        var first = new List<StageSample>();
        using (var collector = new StageCollector(first) { Attempt = 1 })
            await Relay().RunOnceAsync("first");
        Assert.Equal(3, first.Count);
        var second = new List<StageSample>();
        using (var collector = new StageCollector(second) { Attempt = 1 })
            await Relay().RunOnceAsync("second");
        await Relay().RunOnceAsync("recovery");
        Assert.Equal(3, first.Count);
        Assert.Equal(3, second.Count);
        Assert.All(first.Concat(second), s => Assert.Equal(1, s.Attempt));
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
