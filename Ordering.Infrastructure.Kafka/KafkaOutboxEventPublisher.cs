using System.Text;
using Confluent.Kafka;
using Ordering.Application.Outbox;

namespace Ordering.Infrastructure.Kafka;

public sealed record KafkaOutboxPublisherOptions
{
    public required string BootstrapServers { get; init; }
    public required string Topic { get; init; }
    public TimeSpan DeliveryTimeout { get; init; } = TimeSpan.FromSeconds(30);

    internal ProducerConfig CreateConfig()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(BootstrapServers);
        ArgumentException.ThrowIfNullOrWhiteSpace(Topic);
        if (Topic.Length > 249 || Topic is "." or ".."
            || Topic.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not '.' and not '_' and not '-'))
            throw new ArgumentException("Invalid Kafka topic.", nameof(Topic));
        if (DeliveryTimeout < TimeSpan.FromSeconds(1) || DeliveryTimeout > TimeSpan.FromMinutes(5)
            || DeliveryTimeout.Ticks % TimeSpan.TicksPerMillisecond != 0)
            throw new ArgumentOutOfRangeException(nameof(DeliveryTimeout));
        // Idempotent producer deduplicates transport retries within this producer session.
        // It cannot deduplicate a later Outbox acquisition or a restarted producer.
        return new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = (int)DeliveryTimeout.TotalMilliseconds,
            RequestTimeoutMs = (int)Math.Min(5000, DeliveryTimeout.TotalMilliseconds / 2),
            MessageSendMaxRetries = int.MaxValue,
            MaxInFlight = 5,
            RetryBackoffMs = 100,
            LingerMs = 0,
            AllowAutoCreateTopics = false
        };
    }
}

/// <summary>Owns and supervises the producer; dispose only after the in-flight send settles.</summary>
public sealed class KafkaOutboxEventPublisher : IOutboxEventPublisher, IDisposable, IAsyncDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly string _topic;
    private bool _disposed;
    public TimeSpan DeliveryTimeout { get; }

    public KafkaOutboxEventPublisher(KafkaOutboxPublisherOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var config = options.CreateConfig();
        _topic = options.Topic;
        DeliveryTimeout = options.DeliveryTimeout;
        _producer = new ProducerBuilder<string, byte[]>(config)
            .SetKeySerializer(Serializers.Utf8)
            .SetValueSerializer(Serializers.ByteArray)
            // Avoid the default native logger emitting broker connection details.
            .SetLogHandler((_, _) => { })
            .Build();
    }

    public async Task<OutboxPublicationResult> PublishAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (message.EventType != nameof(OrderConfirmed) || message.EventId == Guid.Empty)
            throw new ArgumentException("Only persisted OrderConfirmed events are supported.", nameof(message));
        await _gate.WaitAsync(cancellationToken);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                // Delivery acknowledgement comes from the delivery report, not local enqueue.
                // Do not cancel this Task after enqueue: message.timeout.ms bounds native retries
                // and we keep supervising the send even when the caller requests shutdown.
                var delivery = await _producer.ProduceAsync(_topic, new Message<string, byte[]>
                {
                    Key = message.AggregateId,
                    Value = Encoding.UTF8.GetBytes(message.Payload),
                    Headers = new Headers
                    {
                        { "event-id", Encoding.UTF8.GetBytes(message.EventId.ToString("D")) },
                        { "event-type", Encoding.UTF8.GetBytes(message.EventType) }
                    }
                }, CancellationToken.None);
                return delivery.Status == PersistenceStatus.Persisted
                    ? new(OutboxDeliveryStatus.Acknowledged,
                        new(delivery.Topic, delivery.Partition.Value, delivery.Offset.Value))
                    : new(OutboxDeliveryStatus.Unconfirmed, ErrorCode: delivery.Status.ToString());
            }
            catch (KafkaException error)
            {
                return new(OutboxDeliveryStatus.Unconfirmed, ErrorCode: error.Error.Code.ToString());
            }
        }
        finally { _gate.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync();
        try { DisposeProducer(); }
        finally { _gate.Release(); }
    }

    public void Dispose()
    {
        _gate.Wait();
        try { DisposeProducer(); }
        finally { _gate.Release(); }
    }

    private void DisposeProducer()
    {
        if (_disposed) return;
        _disposed = true;
        _producer.Dispose();
    }
}
