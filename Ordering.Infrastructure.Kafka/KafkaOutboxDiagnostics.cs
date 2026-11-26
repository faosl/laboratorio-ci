using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Ordering.Infrastructure.Kafka;

/// <summary>Local telemetry contract for the publisher's mutual exclusion and producer call.</summary>
public static class KafkaOutboxDiagnostics
{
    public const string MeterName = "Ordering.Infrastructure.Kafka.Outbox";
    public const string WaitName = "outbox.kafka.gate.wait.duration";
    public const string HoldName = "outbox.kafka.gate.hold.duration";
    public const string ProduceName = "outbox.kafka.produce.duration";

    private static class Instruments
    {
        internal static readonly Meter Meter = new(MeterName);
        internal static readonly Histogram<double> Wait = Meter.CreateHistogram<double>(WaitName, "s");
        internal static readonly Histogram<double> Hold = Meter.CreateHistogram<double>(HoldName, "s");
        internal static readonly Histogram<double> Produce = Meter.CreateHistogram<double>(ProduceName, "s");
    }

    internal static void Record(string name, long start, long end, string status)
    {
        try
        {
            var histogram = name switch
            {
                WaitName => Instruments.Wait,
                HoldName => Instruments.Hold,
                _ => Instruments.Produce
            };
            histogram.Record(Stopwatch.GetElapsedTime(start, end).TotalSeconds,
                new KeyValuePair<string, object?>("outbox.status", status));
        }
        catch (Exception)
        {
            // MeterListener callbacks run inline. Observers cannot change delivery or ownership.
        }
    }
}
