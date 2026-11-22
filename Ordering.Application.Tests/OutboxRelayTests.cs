using Ordering.Application.Outbox;

namespace Ordering.Application.Tests;

public sealed partial class OutboxRelayTests
{
    private sealed class Store : IOutboxProcessingStore
    {
        public AcquiredOutboxMessage Work { get; set; } = new(new(Guid.Parse("7721addd-2f0e-497f-b0f6-63a79652d4d2"), "worker", 1),
            new(Guid.Parse("7721addd-2f0e-497f-b0f6-63a79652d4d2"), nameof(OrderConfirmed), "order", "{}", DateTimeOffset.UtcNow),
            DateTimeOffset.UtcNow.AddMinutes(1));
        public OutboxAcquisitionStatus Acquisition { get; set; } = OutboxAcquisitionStatus.Acquired;
        public OutboxMutationStatus Mutation { get; set; } = OutboxMutationStatus.Succeeded;
        public Exception? Failure { get; set; }
        public Exception? MutationFailure { get; set; }
        public Action? DuringMutation { get; set; }
        public List<string> Calls { get; } = [];
        public OutboxLease? UsedLease { get; private set; }
        public Task<OutboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan duration, CancellationToken ct)
        {
            Calls.Add("acquire");
            if (Failure is not null) throw Failure;
            // Even a malformed unknown acquisition must not leak presumed work to the publisher.
            return Task.FromResult(new OutboxAcquisitionResult(Acquisition, Work));
        }
        public Task<OutboxMutationResult> MarkPublishedAsync(OutboxLease lease, CancellationToken ct) => Mutate("complete", lease);
        public Task<OutboxMutationResult> ScheduleRetryAsync(OutboxLease lease, TimeSpan delay, CancellationToken ct) => Mutate("retry", lease);
        private Task<OutboxMutationResult> Mutate(string call, OutboxLease lease)
        {
            Calls.Add(call);
            UsedLease = lease;
            DuringMutation?.Invoke();
            if (MutationFailure is not null) throw MutationFailure;
            return Task.FromResult(new OutboxMutationResult(Mutation));
        }
        public Task<OutboxMutationResult> RenewAsync(OutboxLease lease, TimeSpan duration, CancellationToken ct) =>
            throw new InvalidOperationException("One-shot relay must not renew.");
    }

    private sealed class Publisher : IOutboxEventPublisher
    {
        public TimeSpan DeliveryTimeout => TimeSpan.FromSeconds(2);
        public OutboxPublicationResult Result { get; set; } = new(OutboxDeliveryStatus.Acknowledged, new("orders", 0, 3));
        public int Calls { get; private set; }
        public Action? DuringSend { get; set; }
        public Exception? Failure { get; set; }
        public OutboxMessage? Sent { get; private set; }
        public Task<OutboxPublicationResult>? PendingSend { get; set; }
        public Task<OutboxPublicationResult> PublishAsync(OutboxMessage message, CancellationToken ct)
        {
            Calls++;
            Sent = message;
            DuringSend?.Invoke();
            if (Failure is not null) throw Failure;
            return PendingSend ?? Task.FromResult(Result);
        }
    }

    [Theory]
    [InlineData(OutboxAcquisitionStatus.NoEligibleWork, OutboxRelayStatus.NoEligibleWork)]
    [InlineData(OutboxAcquisitionStatus.OutcomeUnknown, OutboxRelayStatus.AcquisitionUnknown)]
    public async Task UnconfirmedAcquisition_NeverPublishesOrMutates(OutboxAcquisitionStatus acquisition, OutboxRelayStatus expected)
    {
        var store = new Store { Acquisition = acquisition };
        var publisher = new Publisher();
        var result = await new OutboxRelay(store, publisher, new()).RunOnceAsync("worker");
        Assert.Equal(expected, result.Status);
        Assert.Equal(OutboxDeliveryStatus.NotAttempted, result.Delivery);
        Assert.Null(result.EventId);
        Assert.Equal(0, publisher.Calls);
        Assert.Equal(new[] { "acquire" }, store.Calls);
    }

    [Theory]
    [InlineData(true, OutboxMutationStatus.Succeeded, OutboxRelayStatus.Published)]
    [InlineData(true, OutboxMutationStatus.LeaseLost, OutboxRelayStatus.LeaseLost)]
    [InlineData(true, OutboxMutationStatus.OutcomeUnknown, OutboxRelayStatus.CompletionUnknown)]
    [InlineData(false, OutboxMutationStatus.Succeeded, OutboxRelayStatus.RetryScheduled)]
    [InlineData(false, OutboxMutationStatus.LeaseLost, OutboxRelayStatus.LeaseLost)]
    [InlineData(false, OutboxMutationStatus.OutcomeUnknown, OutboxRelayStatus.RetryUnknown)]
    public async Task DeliveryAndDatabaseOutcomesRemainDistinct(bool ack, OutboxMutationStatus mutation, OutboxRelayStatus expected)
    {
        var store = new Store { Mutation = mutation };
        var publisher = new Publisher();
        if (!ack) publisher.Result = new(OutboxDeliveryStatus.Unconfirmed, ErrorCode: "timeout");
        var result = await new OutboxRelay(store, publisher, new()).RunOnceAsync("worker");
        Assert.Equal(expected, result.Status);
        Assert.Equal(ack ? OutboxDeliveryStatus.Acknowledged : OutboxDeliveryStatus.Unconfirmed, result.Delivery);
        Assert.Equal(new[] { "acquire", ack ? "complete" : "retry" }, store.Calls);
        Assert.Equal(1, publisher.Calls);
        Assert.Same(store.Work.Content, publisher.Sent);
        Assert.Same(store.Work.Lease, store.UsedLease);
        Assert.Equal(publisher.Result.Receipt, result.Receipt);
    }

    [Fact]
    public async Task UnsupportedEvent_DoesNotSendOrMarkPublished()
    {
        var store = new Store();
        store.Work = store.Work with { Content = store.Work.Content with { EventType = "unsupported" } };
        var publisher = new Publisher();
        var result = await new OutboxRelay(store, publisher, new()).RunOnceAsync("worker");
        Assert.Equal(OutboxRelayStatus.UnsupportedEvent, result.Status);
        Assert.Equal(0, publisher.Calls);
        Assert.Equal(new[] { "acquire" }, store.Calls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CancellationDuringSupervisedSend_PreservesDeliveryEvidenceAndLeavesLeaseForRecovery(bool ack)
    {
        var store = new Store();
        using var cancellation = new CancellationTokenSource();
        var publisher = new Publisher { DuringSend = cancellation.Cancel };
        if (!ack) publisher.Result = new(OutboxDeliveryStatus.Unconfirmed);
        var result = await new OutboxRelay(store, publisher, new()).RunOnceAsync("worker", cancellation.Token);
        Assert.Equal(OutboxRelayStatus.Cancelled, result.Status);
        Assert.Equal(publisher.Result.Status, result.Delivery);
        Assert.Equal(new[] { "acquire" }, store.Calls);
    }

    [Fact]
    public async Task CancellationBeforeAcquisition_DoesNotStartWork()
    {
        var store = new Store();
        var result = await new OutboxRelay(store, new Publisher(), new()).RunOnceAsync("worker", new(true));
        Assert.Equal(OutboxRelayStatus.Cancelled, result.Status);
        Assert.Empty(store.Calls);
    }

    [Fact]
    public async Task OperationalFailure_IsReportedWithoutExposingProviderMessage()
    {
        var store = new Store { Failure = new IOException("private connection details") };
        var result = await new OutboxRelay(store, new Publisher(), new()).RunOnceAsync("worker");
        Assert.Equal(OutboxRelayStatus.OperationalFailure, result.Status);
        Assert.Equal(OutboxRelayStage.Acquisition, result.Stage);
        Assert.DoesNotContain("private", result.ToString());
        Assert.Equal(new[] { "acquire" }, store.Calls);
    }

    [Theory]
    [InlineData(2, 10, 1)]
    [InlineData(10, 10, 1)]
    [InlineData(5, 5, 1)]
    [InlineData(5, 10, 0)]
    [InlineData(5, 10, -1)]
    public void IncompatibleExecutionLimitsFailAtConstruction(int execution, int lease, int margin)
    {
        Assert.Throws<ArgumentException>(() => new OutboxRelay(new Store(), new Publisher(), new()
        {
            ExecutionTimeout = TimeSpan.FromSeconds(execution), LeaseDuration = TimeSpan.FromSeconds(lease),
            LeaseSafetyMargin = TimeSpan.FromSeconds(margin)
        }));
    }
}
