using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Ordering.Outbox.PublicationBenchmarks;

internal static class KafkaLaboratory
{
    public static async Task CreateTopicAsync(string bootstrap, string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig
            { BootstrapServers = bootstrap, SocketTimeoutMs = 10000 }).SetLogHandler((_, _) => { }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification
        {
            Name = topic, NumPartitions = 1, ReplicationFactor = 1,
            Configs = new Dictionary<string, string> { ["min.insync.replicas"] = "1" }
        }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(20) });
    }

    public static bool EnvelopeMatches(Message<string, byte[]> message, ExpectedEvent expected)
    {
        var headers = message.Headers;
        return message.Key == expected.AggregateId && message.Value is not null
            && message.Value.AsSpan().SequenceEqual(Encoding.UTF8.GetBytes(expected.Payload))
            && headers is not null && headers.Count == 2
            && headers.Count(h => h.Key == "event-id") == 1 && headers.Count(h => h.Key == "event-type") == 1
            && headers.GetLastBytes("event-id") is { } eventId
            && eventId.AsSpan().SequenceEqual(Encoding.UTF8.GetBytes(expected.EventId.ToString("D")))
            && headers.GetLastBytes("event-type") is { } eventType
            && eventType.AsSpan().SequenceEqual(Encoding.UTF8.GetBytes(expected.EventType));
    }

    public static bool Read(string bootstrap, PhaseReport report, List<KafkaEvidence> target,
        int timeoutSeconds, CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrap, GroupId = Guid.NewGuid().ToString("N"), EnableAutoCommit = false,
            EnableAutoOffsetStore = false, AutoOffsetReset = AutoOffsetReset.Earliest, EnablePartitionEof = true,
            AllowAutoCreateTopics = false, SocketTimeoutMs = 10000
        }).SetLogHandler((_, _) => { }).Build();
        consumer.Assign(new TopicPartitionOffset(report.Topic, new Partition(0), Offset.Beginning));
        var elapsed = Stopwatch.StartNew();
        var expected = report.Expected.ToDictionary(e => e.EventId);
        // No producer is running now. Require EOF AND the final high watermark, not merely N expected records.
        var high = consumer.QueryWatermarkOffsets(new(report.Topic, 0), TimeSpan.FromSeconds(timeoutSeconds)).High.Value;
        var nextOffset = 0L;
        while (elapsed.Elapsed < TimeSpan.FromSeconds(timeoutSeconds))
        {
            ct.ThrowIfCancellationRequested();
            var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
            if (record is null) continue;
            if (record.IsPartitionEOF)
            {
                var finalHigh = consumer.QueryWatermarkOffsets(new(report.Topic, 0), TimeSpan.FromSeconds(timeoutSeconds)).High.Value;
                if (nextOffset >= high && record.Offset.Value >= finalHigh) return true;
                high = finalHigh;
                continue;
            }
            Guid? id = null;
            var bytes = record.Message.Headers?.LastOrDefault(h => h.Key == "event-id")?.GetValueBytes();
            if (bytes is not null && Guid.TryParse(Encoding.UTF8.GetString(bytes), out var parsed)) id = parsed;
            var matches = id is { } key && expected.TryGetValue(key, out var value) && EnvelopeMatches(record.Message, value);
            target.Add(new(id, record.Partition.Value, record.Offset.Value, matches));
            nextOffset = record.Offset.Value + 1;
        }
        return false;
    }
}
