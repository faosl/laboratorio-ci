using System.Diagnostics.Metrics;
using System.Reflection;
using Confluent.Kafka;
using Ordering.Application.Outbox;
using Ordering.Infrastructure.Kafka;

namespace Ordering.Outbox.PublicationBenchmarks.Tests;

// Tests install process-wide listeners, including one that deliberately throws.
[Collection("Publisher telemetry")]
public sealed class KafkaPublisherTests
{
    private static readonly OutboxMessage Message = new(Guid.NewGuid(), nameof(OrderConfirmed), "order", "{}", DateTimeOffset.UnixEpoch);
    private static DeliveryResult<string, byte[]> Delivery(PersistenceStatus status = PersistenceStatus.Persisted) =>
        new() { Status = status, Topic = "topic", Partition = 0, Offset = 1 };

    public class ProducerDouble : DispatchProxy
    {
        internal Func<Task<DeliveryResult<string, byte[]>>> Send = () => Task.FromResult(Delivery());
        internal int Calls;
        internal bool Disposed;

        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method?.Name == "Dispose") { Disposed = true; return null; }
            Assert.Equal("ProduceAsync", method?.Name);
            Assert.Equal(CancellationToken.None, args![2]);
            Calls++;
            return Send();
        }
    }

    internal static KafkaOutboxEventPublisher Publisher(out ProducerDouble producer)
    {
        var proxy = DispatchProxy.Create<IProducer<string, byte[]>, ProducerDouble>();
        producer = (ProducerDouble)proxy;
        return new(proxy, "topic", TimeSpan.FromSeconds(30));
    }

    [Theory]
    [InlineData("persisted", "Acknowledged")]
    [InlineData("possibly-persisted", "Unconfirmed")]
    [InlineData("kafka-error", "Exception")]
    [InlineData("io-error", "Exception")]
    [InlineData("cancelled", "Cancelled")]
    public async Task MeasuresTheActualCallAndReleasesOnEveryOutcome(string scenario, string status)
    {
        using var collector = new StageCollector();
        using var scope = collector.BeginAttempt(1, 1);
        using var publisher = Publisher(out var producer);
        var ioError = new IOException();
        producer.Send = () => scenario switch
        {
            "persisted" => Task.FromResult(Delivery()),
            "possibly-persisted" => Task.FromResult(Delivery(PersistenceStatus.PossiblyPersisted)),
            "kafka-error" => throw new KafkaException(new Error(ErrorCode.Local_MsgTimedOut)),
            "cancelled" => Task.FromCanceled<DeliveryResult<string, byte[]>>(new CancellationToken(true)),
            _ => Task.FromException<DeliveryResult<string, byte[]>>(ioError)
        };
        var task = publisher.PublishAsync(Message, default);
        if (scenario == "io-error") Assert.Same(ioError, await Assert.ThrowsAsync<IOException>(() => task));
        else if (scenario == "cancelled") await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        else Assert.Equal(status == "Acknowledged" ? OutboxDeliveryStatus.Acknowledged : OutboxDeliveryStatus.Unconfirmed,
            (await task).Status);
        var samples = collector.Snapshot();
        Assert.Equal(3, samples.Length);
        AssertStage(samples, "outbox.kafka.gate.wait.duration", "Acquired");
        var hold = AssertStage(samples, "outbox.kafka.gate.hold.duration", "Released");
        var send = AssertStage(samples, "outbox.kafka.produce.duration", status);
        Assert.True(send.Milliseconds <= hold.Milliseconds);
        producer.Send = () => Task.FromResult(Delivery());
        Assert.Equal(OutboxDeliveryStatus.Acknowledged, (await publisher.PublishAsync(Message, default)).Status);
    }

    private static StageSample AssertStage(StageSample[] samples, string name, string status) =>
        Assert.Single(samples, s => s.Instrument == name && s.Status == status && double.IsFinite(s.Milliseconds)
            && s.Milliseconds >= 0);

    [Fact]
    public async Task CancelledWaitDoesNotReleaseGateAndDisposalSupervisesOutstandingSend()
    {
        using var collector = new StageCollector();
        await using var publisher = Publisher(out var producer);
        using var caller = new CancellationTokenSource();
        var pending = new TaskCompletionSource<DeliveryResult<string, byte[]>>(TaskCreationOptions.RunContinuationsAsynchronously);
        producer.Send = () => pending.Task;
        var sending = publisher.PublishAsync(Message, caller.Token);
        using (collector.BeginAttempt(2, 2))
        {
            var waiting = publisher.PublishAsync(Message, caller.Token);
            caller.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        }
        Assert.Collection(collector.Snapshot(), s => Assert.Equal(("outbox.kafka.gate.wait.duration", "Cancelled"), (s.Instrument, s.Status)));
        var disposing = publisher.DisposeAsync().AsTask();
        Assert.False(disposing.IsCompleted);
        Assert.False(sending.IsCompleted);
        Assert.False(producer.Disposed);
        Assert.Equal(1, producer.Calls);
        pending.SetResult(Delivery());
        Assert.Equal(OutboxDeliveryStatus.Acknowledged, (await sending).Status);
        await disposing;
        Assert.True(producer.Disposed);
    }

    // Hold the waiting caller's continuation until the gate has granted ownership.
    private sealed class PausedContext : SynchronizationContext
    {
        public readonly TaskCompletionSource<Action> Posted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override void Post(SendOrPostCallback callback, object? state) => Posted.SetResult(() => callback(state));
    }

    [Fact]
    public async Task CancellationAfterGrantRecordsOnlyWaitAndHold()
    {
        using var collector = new StageCollector();
        await using var publisher = Publisher(out var producer);
        using var caller = new CancellationTokenSource();
        var pending = new TaskCompletionSource<DeliveryResult<string, byte[]>>(TaskCreationOptions.RunContinuationsAsynchronously);
        producer.Send = () => pending.Task;
        var first = publisher.PublishAsync(Message, default);
        using var scope = collector.BeginAttempt(2, 2);
        var paused = new PausedContext();
        var previous = SynchronizationContext.Current;
        Task<OutboxPublicationResult> second;
        try
        {
            SynchronizationContext.SetSynchronizationContext(paused);
            second = publisher.PublishAsync(Message, caller.Token);
        }
        finally { SynchronizationContext.SetSynchronizationContext(previous); }
        pending.SetResult(Delivery());
        await first;
        var resume = await paused.Posted.Task;
        caller.Cancel();
        resume();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second);
        Assert.Equal(1, producer.Calls);
        var samples = collector.Snapshot();
        Assert.Equal(2, samples.Length);
        AssertStage(samples, "outbox.kafka.gate.wait.duration", "Acquired");
        AssertStage(samples, "outbox.kafka.gate.hold.duration", "Released");
        Assert.Equal(OutboxDeliveryStatus.Acknowledged, (await publisher.PublishAsync(Message, default)).Status);
    }

    [Fact]
    public async Task ThrowingMetricListenerCannotMaskReceiptOrOriginalException()
    {
        using var publisher = Publisher(out var producer);
        using var listener = new MeterListener();
        var active = new AsyncLocal<bool> { Value = true };
        var callbacks = 0;
        listener.InstrumentPublished = (i, l) =>
        {
            if (i.Meter.Name == "Ordering.Infrastructure.Kafka.Outbox") l.EnableMeasurementEvents(i);
        };
        listener.SetMeasurementEventCallback<double>((i, _, tags, _) =>
        {
            if (!active.Value) return;
            if (i.Unit == "s" && tags.Length == 1 && tags[0].Key == "outbox.status") callbacks++;
            throw new InvalidOperationException("listener failure");
        });
        listener.Start();
        Assert.Equal(OutboxDeliveryStatus.Acknowledged, (await publisher.PublishAsync(Message, default)).Status);
        var original = new IOException();
        producer.Send = () => throw original;
        Assert.Same(original, await Assert.ThrowsAsync<IOException>(() => publisher.PublishAsync(Message, default)));
        Assert.Equal(6, callbacks);
    }
}
