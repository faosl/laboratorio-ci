using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Ordering.Application.Outbox;
using Ordering.Infrastructure.Kafka;

namespace Ordering.Outbox.PublicationBenchmarks;

internal sealed class StageCollector : IDisposable
{
    public const string AcquisitionName = "outbox.acquire";
    private sealed record Context(StageCollector Collector, int Attempt, int Worker);
    private static readonly AsyncLocal<Context?> Current = new();
    private readonly ConcurrentQueue<StageSample> _samples = new();
    private readonly MeterListener _listener = new();
    private readonly ActivityListener _activities;
    private int _closed;

    public StageCollector()
    {
        _activities = new ActivityListener
        {
            ShouldListenTo = source => source.Name == OutboxRelayDiagnostics.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity =>
            {
                if (activity.OperationName == AcquisitionName)
                    Record(AcquisitionName, activity.GetTagItem("outbox.status")?.ToString() ?? "Unknown",
                        activity.Duration.TotalMilliseconds);
            }
        };
        ActivitySource.AddActivityListener(_activities);
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument is Histogram<double> && instrument.Meter.Name switch
                {
                    OutboxRelayDiagnostics.SourceName => instrument.Name is OutboxRelayDiagnostics.PublicationDurationName
                        or OutboxRelayDiagnostics.CompletionDurationName or OutboxRelayDiagnostics.AttemptDurationName,
                    KafkaOutboxDiagnostics.MeterName => instrument.Name is KafkaOutboxDiagnostics.WaitName
                        or KafkaOutboxDiagnostics.HoldName or KafkaOutboxDiagnostics.ProduceName,
                    _ => false
                })
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
        {
            var status = "Unknown";
            foreach (var tag in tags) if (tag.Key == "outbox.status") status = tag.Value?.ToString() ?? "Unknown";
            Record(instrument.Name, status, value * 1000);
        });
        _listener.Start();
    }

    // Async context propagation keeps correlation across awaits without sharing a mutable
    // current attempt between workers. Collector identity also excludes overlapping phases.
    public IDisposable BeginAttempt(int attempt, int worker)
    {
        var previous = Current.Value;
        Current.Value = new(this, attempt, worker);
        return new Scope(() => Current.Value = previous);
    }

    private void Record(string instrument, string status, double milliseconds)
    {
        if (Volatile.Read(ref _closed) != 0 || Current.Value is not { } context
            || !ReferenceEquals(context.Collector, this)) return;
        // Thread-safe metric collection; snapshots are consolidated only after quiescence.
        _samples.Enqueue(new(context.Attempt, instrument, status, milliseconds, context.Worker));
    }

    public StageSample[] Snapshot() => _samples.OrderBy(s => s.Attempt).ThenBy(s => s.Instrument).ToArray();

    public void Dispose()
    {
        Interlocked.Exchange(ref _closed, 1);
        _listener.Dispose();
        _activities.Dispose();
    }

    private sealed class Scope(Action restore) : IDisposable
    {
        public void Dispose() => restore();
    }
}
