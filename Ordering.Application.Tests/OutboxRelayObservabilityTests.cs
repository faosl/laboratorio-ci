using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Ordering.Application.Outbox;

namespace Ordering.Application.Tests;

[CollectionDefinition("Outbox diagnostics", DisableParallelization = true)]
public sealed class OutboxDiagnosticsCollection;

[Collection("Outbox diagnostics")]
public sealed partial class OutboxRelayTests
{
    private sealed record Measurement(Instrument Instrument, double Value, Dictionary<string, object?> Tags);

    private sealed class Telemetry : IDisposable
    {
        private readonly MeterListener _meters = new();
        private readonly ActivityListener _activities;
        private readonly ConcurrentQueue<Measurement> _measurements = new();
        public ConcurrentQueue<Activity> Spans { get; } = new();

        public Telemetry()
        {
            _meters.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == OutboxRelayDiagnostics.SourceName)
                    listener.EnableMeasurementEvents(instrument);
            };
            _meters.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
                _measurements.Enqueue(new(instrument, value, tags.ToArray().ToDictionary(t => t.Key, t => t.Value))));
            _meters.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
                _measurements.Enqueue(new(instrument, value, tags.ToArray().ToDictionary(t => t.Key, t => t.Value))));
            _meters.Start();
            _activities = new ActivityListener
            {
                ShouldListenTo = source => source.Name == OutboxRelayDiagnostics.SourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity => Spans.Enqueue(activity)
            };
            ActivitySource.AddActivityListener(_activities);
        }

        public Measurement Metric(string name, string? status = null)
        {
            var measurement = Assert.Single(_measurements, m => m.Instrument.Name == name);
            if (status is not null) Assert.Equal(status, measurement.Tags["outbox.status"]);
            if (measurement.Instrument is Counter<long>) Assert.Equal(1d, measurement.Value);
            else
            {
                Assert.IsType<Histogram<double>>(measurement.Instrument);
                Assert.Equal("s", measurement.Instrument.Unit);
                Assert.True(double.IsFinite(measurement.Value) && measurement.Value >= 0);
            }
            return measurement;
        }

        public void OnlyMetrics(params string[] names)
        {
            Assert.Equal(names.OrderBy(n => n), _measurements.Select(m => m.Instrument.Name).OrderBy(n => n));
            foreach (var name in names) Metric(name);
            // Exact allowlists also reject identity, owner, payload, offset and exception-message tags.
            foreach (var measurement in _measurements)
            {
                string[] expected = measurement.Instrument.Name switch
                {
                    OutboxRelayDiagnostics.KafkaAckCountName => [],
                    OutboxRelayDiagnostics.LeaseLostCountName => ["outbox.stage"],
                    OutboxRelayDiagnostics.AttemptDurationName => ["outbox.delivery", "outbox.stage", "outbox.status"],
                    _ => ["outbox.status"]
                };
                Assert.Equal(expected, measurement.Tags.Keys.OrderBy(k => k));
            }
        }

        public Activity Attempt(OutboxRelayResult result, AcquiredOutboxMessage? work, params string[] operations)
        {
            var root = Span("outbox.relay.attempt", result.Status.ToString());
            Assert.Equal(result.Stage.ToString(), root.GetTagItem("outbox.stage"));
            Assert.Equal(result.Delivery.ToString(), root.GetTagItem("outbox.delivery"));
            Assert.Equal(result.Status.ToString(), Metric(OutboxRelayDiagnostics.AttemptDurationName).Tags["outbox.status"]);
            Assert.Equal(result.Stage.ToString(), Metric(OutboxRelayDiagnostics.AttemptDurationName).Tags["outbox.stage"]);
            Assert.Equal(result.Delivery.ToString(), Metric(OutboxRelayDiagnostics.AttemptDurationName).Tags["outbox.delivery"]);
            Assert.Equal(operations.OrderBy(n => n), Spans.Where(s => s != root).Select(s => s.OperationName).OrderBy(n => n));
            foreach (var child in Spans.Where(s => s != root))
            {
                Assert.Equal(root.TraceId, child.TraceId);
                Assert.Equal(root.SpanId, child.ParentSpanId);
                Assert.True(child.IsStopped);
            }
            foreach (var span in Spans.Where(s => s == root || s.OperationName != "outbox.acquire"))
            {
                Assert.Equal(work?.Content.EventId.ToString(), span.GetTagItem("outbox.event_id"));
                Assert.Equal(work?.Lease.Generation, span.GetTagItem("outbox.lease_generation"));
            }
            Assert.All(Spans, span =>
            {
                Assert.Null(span.StatusDescription);
                Assert.All(span.TagObjects, tag => Assert.Contains(tag.Key, new[]
                {
                    "outbox.event_id", "outbox.lease_generation", "outbox.status", "outbox.stage", "outbox.delivery"
                }));
            });
            return root;
        }

        public Activity Span(string name, string status)
        {
            var span = Assert.Single(Spans, s => s.OperationName == name);
            Assert.Equal(status, span.GetTagItem("outbox.status"));
            return span;
        }

        public void Dispose()
        {
            _meters.Dispose();
            _activities.Dispose();
        }
    }

    [Theory]
    [InlineData(OutboxMutationStatus.Succeeded, OutboxRelayStatus.Published)]
    [InlineData(OutboxMutationStatus.LeaseLost, OutboxRelayStatus.LeaseLost)]
    [InlineData(OutboxMutationStatus.OutcomeUnknown, OutboxRelayStatus.CompletionUnknown)]
    public async Task Telemetry_AckAndActualCompletionResultRemainSeparate(OutboxMutationStatus mutation, OutboxRelayStatus expected)
    {
        using var telemetry = new Telemetry();
        var store = new Store { Mutation = mutation };
        var publisher = new Publisher();
        var result = await new OutboxRelay(store, publisher, new()).RunOnceAsync("worker");

        Assert.Equal(new(expected, OutboxRelayStage.Completion, store.Work.Content.EventId,
            publisher.Result.Status, publisher.Result.Receipt), result);
        Assert.Equal(new[] { "acquire", "complete" }, store.Calls);
        Assert.Equal(1, publisher.Calls);
        Assert.Same(store.Work.Lease, store.UsedLease);
        Assert.Same(store.Work.Content, publisher.Sent);
        var metrics = new List<string>
        {
            OutboxRelayDiagnostics.KafkaAckCountName, OutboxRelayDiagnostics.CompletionCountName,
            OutboxRelayDiagnostics.PublicationDurationName, OutboxRelayDiagnostics.CompletionDurationName,
            OutboxRelayDiagnostics.AttemptDurationName
        };
        if (mutation == OutboxMutationStatus.LeaseLost)
        {
            metrics.Add(OutboxRelayDiagnostics.LeaseLostCountName);
            Assert.Equal("Completion", telemetry.Metric(OutboxRelayDiagnostics.LeaseLostCountName).Tags["outbox.stage"]);
        }
        telemetry.OnlyMetrics(metrics.ToArray());
        telemetry.Metric(OutboxRelayDiagnostics.PublicationDurationName, "Acknowledged");
        telemetry.Metric(OutboxRelayDiagnostics.CompletionDurationName, mutation.ToString());
        telemetry.Metric(OutboxRelayDiagnostics.CompletionCountName, mutation.ToString());
        var root = telemetry.Attempt(result, store.Work, "outbox.acquire", "outbox.publish", "outbox.complete");
        var expectedTraceStatus = mutation == OutboxMutationStatus.Succeeded ? ActivityStatusCode.Ok : ActivityStatusCode.Error;
        Assert.Equal(expectedTraceStatus, root.Status);
        Assert.Equal(expectedTraceStatus, telemetry.Span("outbox.complete", mutation.ToString()).Status);
        Assert.Single(telemetry.Span("outbox.publish", "Acknowledged").Events, e => e.Name == "outbox.kafka.ack");
    }

    [Theory]
    [InlineData(OutboxMutationStatus.Succeeded, OutboxRelayStatus.RetryScheduled)]
    [InlineData(OutboxMutationStatus.LeaseLost, OutboxRelayStatus.LeaseLost)]
    [InlineData(OutboxMutationStatus.OutcomeUnknown, OutboxRelayStatus.RetryUnknown)]
    public async Task Telemetry_UnconfirmedPublicationMeasuresSendAndRetryOnly(OutboxMutationStatus mutation, OutboxRelayStatus expected)
    {
        using var telemetry = new Telemetry();
        var store = new Store { Mutation = mutation };
        var publisher = new Publisher { Result = new(OutboxDeliveryStatus.Unconfirmed, ErrorCode: "timeout") };
        var result = await new OutboxRelay(store, publisher, new()).RunOnceAsync("worker");

        Assert.Equal(new(expected, OutboxRelayStage.Retry, store.Work.Content.EventId,
            OutboxDeliveryStatus.Unconfirmed, ErrorCode: "timeout"), result);
        Assert.Equal(new[] { "acquire", "retry" }, store.Calls);
        Assert.Equal(1, publisher.Calls);
        var metrics = new List<string> { OutboxRelayDiagnostics.PublicationDurationName, OutboxRelayDiagnostics.AttemptDurationName };
        if (mutation == OutboxMutationStatus.LeaseLost)
        {
            metrics.Add(OutboxRelayDiagnostics.LeaseLostCountName);
            Assert.Equal("Retry", telemetry.Metric(OutboxRelayDiagnostics.LeaseLostCountName).Tags["outbox.stage"]);
        }
        telemetry.OnlyMetrics(metrics.ToArray());
        telemetry.Metric(OutboxRelayDiagnostics.PublicationDurationName, "Unconfirmed");
        telemetry.Attempt(result, store.Work, "outbox.acquire", "outbox.publish", "outbox.retry");
        Assert.Equal(ActivityStatusCode.Error, telemetry.Span("outbox.publish", "Unconfirmed").Status);
        telemetry.Span("outbox.retry", mutation.ToString());
        Assert.Empty(telemetry.Spans.SelectMany(s => s.Events));
    }

    [Theory]
    [InlineData(OutboxAcquisitionStatus.NoEligibleWork, OutboxRelayStatus.NoEligibleWork)]
    [InlineData(OutboxAcquisitionStatus.OutcomeUnknown, OutboxRelayStatus.AcquisitionUnknown)]
    public async Task Telemetry_NoConfirmedAcquisitionMeasuresNoSendOrMutation(OutboxAcquisitionStatus acquisition, OutboxRelayStatus expected)
    {
        using var telemetry = new Telemetry();
        var store = new Store { Acquisition = acquisition };
        var publisher = new Publisher();
        var result = await new OutboxRelay(store, publisher, new()).RunOnceAsync("worker");

        Assert.Equal(new(expected, OutboxRelayStage.Acquisition), result);
        Assert.Equal(new[] { "acquire" }, store.Calls);
        Assert.Equal(0, publisher.Calls);
        telemetry.OnlyMetrics(OutboxRelayDiagnostics.AttemptDurationName);
        telemetry.Attempt(result, null, "outbox.acquire");
        Assert.Equal(acquisition == OutboxAcquisitionStatus.OutcomeUnknown ? ActivityStatusCode.Error : ActivityStatusCode.Ok,
            telemetry.Span("outbox.acquire", acquisition.ToString()).Status);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Telemetry_CancellationAfterSendPreservesAckWithoutInventingCompletion(bool ack)
    {
        using var telemetry = new Telemetry();
        using var cancellation = new CancellationTokenSource();
        var store = new Store();
        var publisher = new Publisher { DuringSend = cancellation.Cancel };
        if (!ack) publisher.Result = new(OutboxDeliveryStatus.Unconfirmed);
        var result = await new OutboxRelay(store, publisher, new()).RunOnceAsync("worker", cancellation.Token);

        Assert.Equal(new(OutboxRelayStatus.Cancelled, OutboxRelayStage.Publication, store.Work.Content.EventId,
            publisher.Result.Status, publisher.Result.Receipt), result);
        Assert.Equal(new[] { "acquire" }, store.Calls);
        Assert.Equal(1, publisher.Calls);
        telemetry.OnlyMetrics(ack
            ? [OutboxRelayDiagnostics.AttemptDurationName, OutboxRelayDiagnostics.PublicationDurationName, OutboxRelayDiagnostics.KafkaAckCountName]
            : [OutboxRelayDiagnostics.AttemptDurationName, OutboxRelayDiagnostics.PublicationDurationName]);
        Assert.Equal(ActivityStatusCode.Error, telemetry.Attempt(result, store.Work, "outbox.acquire", "outbox.publish").Status);
        telemetry.Metric(OutboxRelayDiagnostics.PublicationDurationName, publisher.Result.Status.ToString());
        Assert.Equal(ack ? 1 : 0, telemetry.Spans.SelectMany(s => s.Events).Count(e => e.Name == "outbox.kafka.ack"));
    }

    [Theory]
    [InlineData("Acquisition", false)]
    [InlineData("Publication", false)]
    [InlineData("Completion", false)]
    [InlineData("Completion", true)]
    [InlineData("Retry", false)]
    public async Task Telemetry_ThrownOperationsMeasureCallsWithoutCountingReturnedCompletion(string stage, bool cancelled)
    {
        using var telemetry = new Telemetry();
        using var cancellation = new CancellationTokenSource();
        Exception failure = cancelled ? new OperationCanceledException(cancellation.Token) : new IOException("private connection details");
        var store = new Store();
        var publisher = new Publisher();
        if (stage == "Acquisition") store.Failure = failure;
        if (stage == "Publication") publisher.Failure = failure;
        if (stage is "Completion" or "Retry") store.MutationFailure = failure;
        if (stage == "Retry") publisher.Result = new(OutboxDeliveryStatus.Unconfirmed);
        if (cancelled) store.DuringMutation = cancellation.Cancel;
        var result = await new OutboxRelay(store, publisher, new()).RunOnceAsync("worker", cancellation.Token);

        Assert.Equal(cancelled ? OutboxRelayStatus.Cancelled : OutboxRelayStatus.OperationalFailure, result.Status);
        Assert.Equal(Enum.Parse<OutboxRelayStage>(stage), result.Stage);
        Assert.Equal(cancelled ? null : nameof(IOException), result.ErrorCode);
        Assert.Equal(stage == "Acquisition" ? 0 : 1, publisher.Calls);
        Assert.Equal(stage is "Completion" or "Retry" ? new[] { "acquire", stage == "Completion" ? "complete" : "retry" }
            : new[] { "acquire" }, store.Calls);
        var metrics = new List<string> { OutboxRelayDiagnostics.AttemptDurationName };
        var operations = new List<string> { "outbox.acquire" };
        if (stage != "Acquisition")
        {
            metrics.Add(OutboxRelayDiagnostics.PublicationDurationName);
            operations.Add("outbox.publish");
            telemetry.Metric(OutboxRelayDiagnostics.PublicationDurationName,
                stage == "Publication" ? "Exception" : publisher.Result.Status.ToString());
        }
        if (stage == "Completion")
        {
            metrics.AddRange([OutboxRelayDiagnostics.KafkaAckCountName, OutboxRelayDiagnostics.CompletionDurationName]);
            operations.Add("outbox.complete");
            telemetry.Metric(OutboxRelayDiagnostics.CompletionDurationName, cancelled ? "Cancelled" : "Exception");
        }
        if (stage == "Retry") operations.Add("outbox.retry");
        telemetry.OnlyMetrics(metrics.ToArray());
        Assert.Equal(ActivityStatusCode.Error, telemetry.Attempt(result, stage == "Acquisition" ? null : store.Work, operations.ToArray()).Status);
        Assert.DoesNotContain("private", result.ToString());
    }

    [Fact]
    public async Task Telemetry_AcknowledgementWithoutReceiptIsNotCountedOrCompleted()
    {
        using var telemetry = new Telemetry();
        var store = new Store();
        var publisher = new Publisher { Result = new(OutboxDeliveryStatus.Acknowledged) };
        var result = await new OutboxRelay(store, publisher, new()).RunOnceAsync("worker");

        Assert.Equal(OutboxRelayStatus.OperationalFailure, result.Status);
        Assert.Equal(nameof(InvalidOperationException), result.ErrorCode);
        Assert.Equal(new[] { "acquire" }, store.Calls);
        Assert.Equal(1, publisher.Calls);
        telemetry.OnlyMetrics(OutboxRelayDiagnostics.PublicationDurationName, OutboxRelayDiagnostics.AttemptDurationName);
        telemetry.Metric(OutboxRelayDiagnostics.PublicationDurationName, "InvalidReceipt");
        telemetry.Attempt(result, store.Work, "outbox.acquire", "outbox.publish");
        Assert.Empty(telemetry.Spans.SelectMany(s => s.Events));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Telemetry_PrePublicationExitDoesNotInventOperations(bool alreadyCancelled)
    {
        using var telemetry = new Telemetry();
        var store = new Store();
        store.Work = store.Work with { Content = store.Work.Content with { EventType = "unsupported" } };
        var publisher = new Publisher();
        var result = await new OutboxRelay(store, publisher, new()).RunOnceAsync("worker", new(alreadyCancelled));

        Assert.Equal(alreadyCancelled ? OutboxRelayStatus.Cancelled : OutboxRelayStatus.UnsupportedEvent, result.Status);
        Assert.Equal(alreadyCancelled ? OutboxRelayStage.Acquisition : OutboxRelayStage.Validation, result.Stage);
        Assert.Equal(OutboxDeliveryStatus.NotAttempted, result.Delivery);
        Assert.Equal(alreadyCancelled ? Array.Empty<string>() : ["acquire"], store.Calls);
        Assert.Equal(0, publisher.Calls);
        telemetry.OnlyMetrics(OutboxRelayDiagnostics.AttemptDurationName);
        telemetry.Attempt(result, alreadyCancelled ? null : store.Work, alreadyCancelled ? [] : ["outbox.acquire"]);
    }

    [Fact]
    public async Task Telemetry_AsyncSendRetainsAttemptCorrelationAndRestoresParent()
    {
        using var parent = new Activity("test.request").Start();
        using var telemetry = new Telemetry();
        var store = new Store();
        var release = new TaskCompletionSource<OutboxPublicationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var publisher = new Publisher { PendingSend = release.Task };
        var pending = new OutboxRelay(store, publisher, new()).RunOnceAsync("worker");
        try
        {
            Assert.False(pending.IsCompleted);
            Assert.Equal(new[] { "acquire" }, store.Calls);
            Assert.Same(parent, Activity.Current);
            telemetry.OnlyMetrics();
        }
        finally { release.TrySetResult(publisher.Result); }
        var result = await pending;

        Assert.Equal(OutboxRelayStatus.Published, result.Status);
        Assert.Equal(new[] { "acquire", "complete" }, store.Calls);
        Assert.Equal(1, publisher.Calls);
        telemetry.OnlyMetrics(OutboxRelayDiagnostics.KafkaAckCountName, OutboxRelayDiagnostics.CompletionCountName,
            OutboxRelayDiagnostics.PublicationDurationName, OutboxRelayDiagnostics.CompletionDurationName,
            OutboxRelayDiagnostics.AttemptDurationName);
        var root = telemetry.Attempt(result, store.Work, "outbox.acquire", "outbox.publish", "outbox.complete");
        Assert.Equal(parent.TraceId, root.TraceId);
        Assert.Equal(parent.SpanId, root.ParentSpanId);
        Assert.Same(parent, Activity.Current);
    }

    [Fact]
    public async Task Telemetry_ReturnedCompletionSurvivesConcurrentCancellation()
    {
        using var telemetry = new Telemetry();
        using var cancellation = new CancellationTokenSource();
        var store = new Store { DuringMutation = cancellation.Cancel };
        var result = await new OutboxRelay(store, new Publisher(), new()).RunOnceAsync("worker", cancellation.Token);

        Assert.Equal(OutboxRelayStatus.Published, result.Status);
        Assert.Equal(new[] { "acquire", "complete" }, store.Calls);
        telemetry.OnlyMetrics(OutboxRelayDiagnostics.KafkaAckCountName, OutboxRelayDiagnostics.CompletionCountName,
            OutboxRelayDiagnostics.PublicationDurationName, OutboxRelayDiagnostics.CompletionDurationName,
            OutboxRelayDiagnostics.AttemptDurationName);
        telemetry.Metric(OutboxRelayDiagnostics.CompletionCountName, "Succeeded");
        telemetry.Metric(OutboxRelayDiagnostics.CompletionDurationName, "Succeeded");
        telemetry.Attempt(result, store.Work, "outbox.acquire", "outbox.publish", "outbox.complete");
    }
}
