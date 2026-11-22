using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Ordering.Application.Outbox;

/// <summary>Stable telemetry contract; measurement boundaries are documented in docs/outbox-observability.md.</summary>
public static class OutboxRelayDiagnostics
{
    public const string SourceName = "Ordering.Application.Outbox";
    public const string KafkaAckCountName = "outbox.kafka.ack.count";
    public const string CompletionCountName = "outbox.completion.count";
    public const string LeaseLostCountName = "outbox.lease_lost.count";
    public const string PublicationDurationName = "outbox.publication.duration";
    public const string CompletionDurationName = "outbox.completion.duration";
    public const string AttemptDurationName = "outbox.relay.attempt.duration";

    private static readonly Meter Meter = new(SourceName);
    private static readonly ActivitySource Activities = new(SourceName);
    private static readonly Counter<long> KafkaAcks = Meter.CreateCounter<long>(KafkaAckCountName,
        "{ack}", "Acknowledged publication results with a non-null broker receipt.");
    private static readonly Counter<long> Completions = Meter.CreateCounter<long>(CompletionCountName,
        "{result}", "Returned MarkPublishedAsync results, by mutation status.");
    private static readonly Counter<long> LeasesLost = Meter.CreateCounter<long>(LeaseLostCountName,
        "{loss}", "LeaseLost results observed in completion or retry mutations.");
    private static readonly Histogram<double> PublicationDuration = Meter.CreateHistogram<double>(PublicationDurationName,
        "s", "Elapsed PublishAsync call time, including exceptions and cancellation.");
    private static readonly Histogram<double> CompletionDuration = Meter.CreateHistogram<double>(CompletionDurationName,
        "s", "Elapsed MarkPublishedAsync call time, including exceptions and cancellation.");
    private static readonly Histogram<double> AttemptDuration = Meter.CreateHistogram<double>(AttemptDurationName,
        "s", "Elapsed RunOnceAsync attempt time.");

    internal static Attempt StartAttempt() => new();

    internal static string PublicationStatus(OutboxPublicationResult result) =>
        result.Status == OutboxDeliveryStatus.Acknowledged && result.Receipt is null
            ? "InvalidReceipt" : result.Status.ToString();

    internal static async Task<T> ObserveAsync<T>(OutboxRelayStage stage, Func<Task<T>> operation,
        Func<T, string> statusOf, CancellationToken ct, AcquiredOutboxMessage? work = null)
    {
        using var activity = Activities.StartActivity(stage switch
        {
            OutboxRelayStage.Acquisition => "outbox.acquire",
            OutboxRelayStage.Publication => "outbox.publish",
            OutboxRelayStage.Completion => "outbox.complete",
            _ => "outbox.retry"
        });
        SetWork(activity, work);
        var status = "Exception";
        var started = Stopwatch.GetTimestamp();
        double? elapsed = null;
        try
        {
            var result = await operation();
            elapsed = Stopwatch.GetElapsedTime(started).TotalSeconds;
            status = statusOf(result);
            if (stage == OutboxRelayStage.Publication && status == nameof(OutboxDeliveryStatus.Acknowledged))
            {
                // Observe the external ACK before the relay checks cancellation or attempts local completion.
                KafkaAcks.Add(1);
                activity?.AddEvent(new ActivityEvent("outbox.kafka.ack"));
            }
            if (stage == OutboxRelayStage.Completion)
                Completions.Add(1, new KeyValuePair<string, object?>("outbox.status", status));
            if (stage is OutboxRelayStage.Completion or OutboxRelayStage.Retry
                && status == nameof(OutboxMutationStatus.LeaseLost))
                LeasesLost.Add(1, new KeyValuePair<string, object?>("outbox.stage", stage.ToString()));
            return result;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            status = "Cancelled";
            throw;
        }
        finally
        {
            var duration = elapsed ?? Stopwatch.GetElapsedTime(started).TotalSeconds;
            var tag = new KeyValuePair<string, object?>("outbox.status", status);
            if (stage == OutboxRelayStage.Publication) PublicationDuration.Record(duration, tag);
            if (stage == OutboxRelayStage.Completion) CompletionDuration.Record(duration, tag);
            activity?.SetTag("outbox.status", status);
            activity?.SetStatus(status is "Acquired" or "NoEligibleWork" or "Acknowledged" or "Succeeded"
                ? ActivityStatusCode.Ok : ActivityStatusCode.Error);
        }
    }

    private static void SetWork(Activity? activity, AcquiredOutboxMessage? work)
    {
        if (work is null) return;
        activity?.SetTag("outbox.event_id", work.Content.EventId.ToString());
        activity?.SetTag("outbox.lease_generation", work.Lease.Generation);
    }

    internal sealed class Attempt : IDisposable
    {
        private readonly long _started = Stopwatch.GetTimestamp();
        private readonly Activity? _activity = Activities.StartActivity("outbox.relay.attempt");
        private string _status = "Exception";
        private string _stage = nameof(OutboxRelayStage.Acquisition);
        private string _delivery = nameof(OutboxDeliveryStatus.NotAttempted);

        internal void Acquired(AcquiredOutboxMessage work) => SetWork(_activity, work);

        internal void Complete(OutboxRelayResult result)
        {
            _status = result.Status.ToString();
            _stage = result.Stage.ToString();
            _delivery = result.Delivery.ToString();
        }

        public void Dispose()
        {
            var tags = new TagList
            {
                { "outbox.status", _status }, { "outbox.stage", _stage }, { "outbox.delivery", _delivery }
            };
            AttemptDuration.Record(Stopwatch.GetElapsedTime(_started).TotalSeconds, tags);
            _activity?.SetTag("outbox.status", _status);
            _activity?.SetTag("outbox.stage", _stage);
            _activity?.SetTag("outbox.delivery", _delivery);
            _activity?.SetStatus(_status is "Published" or "NoEligibleWork" or "RetryScheduled"
                ? ActivityStatusCode.Ok : ActivityStatusCode.Error);
            _activity?.Dispose();
        }
    }
}
