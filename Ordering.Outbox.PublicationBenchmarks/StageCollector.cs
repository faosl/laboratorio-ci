using System.Diagnostics.Metrics;
using Ordering.Application.Outbox;

namespace Ordering.Outbox.PublicationBenchmarks;

internal sealed class StageCollector : IDisposable
{
    private readonly MeterListener _listener = new();
    private int _closed;
    public int Attempt { get; set; }

    public StageCollector(List<StageSample> samples)
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == OutboxRelayDiagnostics.SourceName
                && instrument.Name is OutboxRelayDiagnostics.PublicationDurationName
                    or OutboxRelayDiagnostics.CompletionDurationName or OutboxRelayDiagnostics.AttemptDurationName)
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
        {
            // Some runtime listener lifecycles can still invoke an old callback. Never let a
            // disposed phase collect a later round or the injected recovery experiment.
            if (Volatile.Read(ref _closed) != 0) return;
            var status = "Unknown";
            foreach (var tag in tags) if (tag.Key == "outbox.status") status = tag.Value?.ToString() ?? "Unknown";
            samples.Add(new(Attempt, instrument.Name, status, value * 1000));
        });
        _listener.Start();
    }

    public void Dispose()
    {
        Interlocked.Exchange(ref _closed, 1);
        _listener.Dispose();
    }
}
