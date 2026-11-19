using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Npgsql;
using Ordering.Application.Outbox;
using Ordering.Infrastructure.Kafka;
using Testcontainers.Kafka;
using Xunit.Abstractions;

namespace Ordering.Infrastructure.PostgreSql.Tests;

public sealed class KafkaFixture : IAsyncLifetime
{
    private readonly KafkaContainer _broker = new KafkaBuilder()
        .WithImage("confluentinc/cp-kafka:7.6.1").Build();
    public string BootstrapServers => _broker.GetBootstrapAddress();
    public Task InitializeAsync() => _broker.StartAsync(); // Missing Docker/broker fails, never skips.
    public Task DisposeAsync() => _broker.DisposeAsync().AsTask();
}

public sealed partial class KafkaOutboxRelayTests(DatabaseFixture database, KafkaFixture kafka, ITestOutputHelper output)
    : IClassFixture<DatabaseFixture>, IClassFixture<KafkaFixture>, IAsyncLifetime
{
    private readonly string _topic = "order-confirmed-" + Guid.NewGuid().ToString("N");
    private PostgresOutboxProcessingStore Store => new(database.Source);
    private static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(30);
    private static OutboxRelayOptions RelayOptions => new()
    {
        LeaseDuration = LeaseDuration, ExecutionTimeout = TimeSpan.FromSeconds(20),
        LeaseSafetyMargin = TimeSpan.FromSeconds(5), RetryDelay = TimeSpan.FromHours(1)
    };

    public async Task InitializeAsync()
    {
        await Sql("TRUNCATE public.ordering_outbox_messages, public.ordering_lab_orders");
        using var admin = Admin();
        await admin.CreateTopicsAsync([new TopicSpecification
        {
            Name = _topic, NumPartitions = 1, ReplicationFactor = 1,
            Configs = new Dictionary<string, string> { ["min.insync.replicas"] = "1" }
        }], new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(20) });
    }

    public async Task DisposeAsync()
    {
        using var admin = Admin();
        await admin.DeleteTopicsAsync([_topic]);
    }

    private IAdminClient Admin() => new AdminClientBuilder(new AdminClientConfig
    {
        BootstrapServers = kafka.BootstrapServers, SocketTimeoutMs = 10000
    }).Build();

    private KafkaOutboxEventPublisher Publisher(TimeSpan? timeout = null) => new(new()
    {
        BootstrapServers = kafka.BootstrapServers, Topic = _topic,
        DeliveryTimeout = timeout ?? TimeSpan.FromSeconds(10)
    });

    private async Task Sql(string sql, params object[] values)
    {
        await using var command = database.Source.CreateCommand(sql);
        foreach (var value in values) command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<OutboxMessage> Seed()
    {
        var order = "pedido-ação-" + Guid.NewGuid().ToString("N");
        await Sql("INSERT INTO public.ordering_lab_orders(order_id) VALUES ($1)", order);
        var confirmation = await new ConfirmOrderUseCase(new PostgresOrderConfirmationStore(database.Source))
            .ExecuteAsync(order, CancellationToken.None);
        Assert.Equal(OrderConfirmationStatus.Confirmed, confirmation.Status);
        await using var read = database.Source.CreateCommand("""
            SELECT event_id, event_type, aggregate_id, payload::text, created_at_utc
            FROM public.ordering_outbox_messages WHERE event_id = $1
            """);
        read.Parameters.AddWithValue(confirmation.EventId!.Value);
        await using var reader = await read.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return new(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
            new DateTimeOffset(reader.GetDateTime(4)));
    }

    private async Task AssertState(OutboxMessage message, string expected)
    {
        await using var query = database.Source.CreateCommand("""
            SELECT publication_status, published_at_utc IS NOT NULL, payload::text, event_type, aggregate_id
            FROM public.ordering_outbox_messages WHERE event_id = $1
            """);
        query.Parameters.AddWithValue(message.EventId);
        await using var reader = await query.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(expected, reader.GetString(0));
        Assert.Equal(expected == "Published", reader.GetBoolean(1));
        Assert.Equal(message.Payload, reader.GetString(2));
        Assert.Equal(message.EventType, reader.GetString(3));
        Assert.Equal(message.AggregateId, reader.GetString(4));
    }

    private Task Expire(OutboxMessage message) => Sql("""
        UPDATE public.ordering_outbox_messages
        SET lease_expires_at_utc = clock_timestamp() - interval '1 second' WHERE event_id = $1
        """, message.EventId);

    private IConsumer<string, byte[]> Consumer()
    {
        var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = kafka.BootstrapServers, GroupId = Guid.NewGuid().ToString("N"),
            EnableAutoCommit = false, EnableAutoOffsetStore = false, AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();
        consumer.Assign(new TopicPartitionOffset(_topic, new Partition(0), Offset.Beginning));
        return consumer;
    }

    private ConsumeResult<string, byte[]> Observe(IConsumer<string, byte[]> consumer, OutboxMessage expected)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var record = consumer.Consume(deadline.Token);
        Assert.Equal(expected.AggregateId, record.Message.Key);
        Assert.Equal(Encoding.UTF8.GetBytes(expected.Payload), record.Message.Value);
        Assert.Equal(expected.EventId.ToString("D"), Encoding.UTF8.GetString(record.Message.Headers.GetLastBytes("event-id")));
        Assert.Equal(nameof(OrderConfirmed), Encoding.UTF8.GetString(record.Message.Headers.GetLastBytes("event-type")));
        Assert.Equal(2, record.Message.Headers.Count);
        output.WriteLine($"Observed event-id={expected.EventId:D} topic={record.Topic} partition={record.Partition.Value} offset={record.Offset.Value}");
        return record;
    }

    [Fact]
    public async Task ConfirmedOrder_ReachesRealKafka_ThenPublishedIsPersisted()
    {
        var message = await Seed();
        await using var publisher = Publisher();
        var result = await new OutboxRelay(Store, publisher, RelayOptions).RunOnceAsync("success");
        Assert.Equal(OutboxRelayStatus.Published, result.Status);
        Assert.Equal(OutboxDeliveryStatus.Acknowledged, result.Delivery);
        using var consumer = Consumer();
        var record = Observe(consumer, message);
        Assert.Equal(new OutboxDeliveryReceipt(_topic, record.Partition.Value, record.Offset.Value), result.Receipt);
        await AssertState(message, "Published");
    }

    [Fact]
    public async Task NoEligibleWork_DoesNotCreateKafkaRecords()
    {
        await using var publisher = Publisher();
        var result = await new OutboxRelay(Store, publisher, RelayOptions).RunOnceAsync("empty");
        Assert.Equal(OutboxRelayStatus.NoEligibleWork, result.Status);
        Assert.Equal(OutboxDeliveryStatus.NotAttempted, result.Delivery);
        using var consumer = Consumer();
        Assert.Null(consumer.Consume(TimeSpan.FromSeconds(2)));
        Assert.Equal(0, consumer.QueryWatermarkOffsets(new(_topic, 0), TimeSpan.FromSeconds(10)).High.Value);
    }

    private async Task SetMinimumReplicas(string value)
    {
        using var admin = Admin();
        await admin.AlterConfigsAsync(new Dictionary<ConfigResource, List<ConfigEntry>>
        {
            [new ConfigResource { Type = ResourceType.Topic, Name = _topic }] =
                [new ConfigEntry { Name = "min.insync.replicas", Value = value }]
        });
    }

    [Fact]
    public async Task BrokerRejectsDelivery_RetryIsPersisted_ExplicitLaterRunRecovers()
    {
        var message = await Seed();
        // A real single-replica broker cannot acknowledge acks=all with min ISR=2.
        await SetMinimumReplicas("2");
        await using var publisher = Publisher(TimeSpan.FromSeconds(2));
        var relay = new OutboxRelay(Store, publisher, RelayOptions);
        var failed = await relay.RunOnceAsync("rejected");
        Assert.Equal(OutboxRelayStatus.RetryScheduled, failed.Status);
        Assert.Equal(OutboxDeliveryStatus.Unconfirmed, failed.Delivery);
        Assert.Null(failed.Receipt);
        Assert.NotNull(failed.ErrorCode);
        await AssertState(message, "RetryScheduled");
        Assert.Equal(OutboxRelayStatus.NoEligibleWork, (await relay.RunOnceAsync("not-due")).Status);
        await SetMinimumReplicas("1");
        await Sql("UPDATE public.ordering_outbox_messages SET next_attempt_at_utc = clock_timestamp() - interval '1 second' WHERE event_id = $1", message.EventId);
        Assert.Equal(OutboxRelayStatus.Published, (await relay.RunOnceAsync("recovered")).Status);
        using var consumer = Consumer();
        Observe(consumer, message);
        await AssertState(message, "Published");
    }

    // Wrap the real store to target only the boundary after Kafka acknowledgement.
    private sealed class CompletionBoundaryStore(IOutboxProcessingStore inner,
        Func<OutboxLease, CancellationToken, Task<OutboxMutationResult>> complete) : IOutboxProcessingStore
    {
        public int CompletionCalls { get; private set; }
        public int RetryCalls { get; private set; }
        public Task<OutboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan duration, CancellationToken ct) => inner.TryAcquireAsync(owner, duration, ct);
        public Task<OutboxMutationResult> RenewAsync(OutboxLease lease, TimeSpan duration, CancellationToken ct) => inner.RenewAsync(lease, duration, ct);
        public Task<OutboxMutationResult> MarkPublishedAsync(OutboxLease lease, CancellationToken ct)
        {
            CompletionCalls++;
            return complete(lease, ct);
        }
        public Task<OutboxMutationResult> ScheduleRetryAsync(OutboxLease lease, TimeSpan delay, CancellationToken ct)
        {
            RetryCalls++;
            return inner.ScheduleRetryAsync(lease, delay, ct);
        }
    }

    [Fact]
    public async Task AckThenLocalFailure_LeaseRecoveryCanPublishSameEventAgain()
    {
        var message = await Seed();
        var failing = new CompletionBoundaryStore(Store, (_, _) => throw new IOException("controlled before completion"));
        await using (var publisher = Publisher())
        {
            var first = await new OutboxRelay(failing, publisher, RelayOptions).RunOnceAsync("before-crash");
            Assert.Equal(OutboxRelayStatus.OperationalFailure, first.Status);
            Assert.Equal(OutboxRelayStage.Completion, first.Stage);
            Assert.Equal(OutboxDeliveryStatus.Acknowledged, first.Delivery);
        }
        using var consumer = Consumer();
        var original = Observe(consumer, message);
        await AssertState(message, "Processing");
        Assert.Equal(1, failing.CompletionCalls);
        Assert.Equal(0, failing.RetryCalls);
        await Expire(message);
        await using (var restartedProducer = Publisher())
            Assert.Equal(OutboxRelayStatus.Published,
                (await new OutboxRelay(Store, restartedProducer, RelayOptions).RunOnceAsync("after-crash")).Status);
        var duplicate = Observe(consumer, message);
        Assert.True(duplicate.Offset.Value > original.Offset.Value);
        // At-least-once publication: stable identity enables consumer deduplication; the
        // idempotent producer does not close the PostgreSQL/Kafka dual-write gap.
        await AssertState(message, "Published");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task KafkaAck_PostgresCommitResponseLost_IsDistinctFromUnconfirmedDelivery(bool committed)
    {
        var message = await Seed();
        var uncertain = new PostgresOutboxProcessingStore(database.Source, new(Commit: async (_, transaction, ct) =>
        {
            if (committed) await transaction.CommitAsync(ct);
            else await transaction.RollbackAsync(ct);
            throw new IOException("controlled commit response loss");
        }));
        var boundary = new CompletionBoundaryStore(Store, uncertain.MarkPublishedAsync);
        await using var publisher = Publisher();
        var result = await new OutboxRelay(boundary, publisher, RelayOptions).RunOnceAsync("unknown-commit");
        Assert.Equal(OutboxRelayStatus.CompletionUnknown, result.Status);
        Assert.Equal(OutboxDeliveryStatus.Acknowledged, result.Delivery);
        Assert.NotNull(result.Receipt);
        Assert.Equal(1, boundary.CompletionCalls);
        Assert.Equal(0, boundary.RetryCalls);
        using var consumer = Consumer();
        Observe(consumer, message);
        await AssertState(message, committed ? "Published" : "Processing");
    }

    [Fact]
    public async Task AckAfterSuccessorAcquisition_StaleLeaseCannotCompleteOrRetry()
    {
        var message = await Seed();
        AcquiredOutboxMessage? successor = null;
        var boundary = new CompletionBoundaryStore(Store, async (stale, ct) =>
        {
            await Expire(message);
            successor = (await Store.TryAcquireAsync("successor", LeaseDuration, ct)).Work;
            Assert.NotNull(successor);
            Assert.True(successor.Lease.Generation > stale.Generation);
            return await Store.MarkPublishedAsync(stale, ct);
        });
        await using var publisher = Publisher();
        var result = await new OutboxRelay(boundary, publisher, RelayOptions).RunOnceAsync("stale");
        Assert.Equal(OutboxRelayStatus.LeaseLost, result.Status);
        Assert.Equal(OutboxDeliveryStatus.Acknowledged, result.Delivery);
        Assert.Equal(1, boundary.CompletionCalls);
        Assert.Equal(0, boundary.RetryCalls);
        await AssertState(message, "Processing");
        using var consumer = Consumer();
        Observe(consumer, message); // Fencing did not retract the external publication.
        Assert.Equal(OutboxMutationStatus.Succeeded,
            (await Store.ScheduleRetryAsync(successor!.Lease, TimeSpan.Zero, CancellationToken.None)).Status);
    }

    private sealed class PausedPublisher(IOutboxEventPublisher inner) : IOutboxEventPublisher
    {
        public TimeSpan DeliveryTimeout => inner.DeliveryTimeout;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public async Task<OutboxPublicationResult> PublishAsync(OutboxMessage message, CancellationToken ct)
        {
            Entered.SetResult();
            await Release.Task.WaitAsync(ct);
            return await inner.PublishAsync(message, ct);
        }
    }

    [Fact]
    public async Task ConcurrentIndependentRelays_OnlyOneOwnsValidReservation_NoOpenTransactionDuringSend()
    {
        var message = await Seed();
        await using var firstPublisher = Publisher();
        await using var secondPublisher = Publisher();
        var paused = new PausedPublisher(firstPublisher);
        var first = new OutboxRelay(Store, paused, RelayOptions).RunOnceAsync("first");
        await paused.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        try
        {
            // NOWAIT succeeds while the publisher is paused: acquisition released its row lock.
            await using (var connection = await database.Source.OpenConnectionAsync())
            await using (var transaction = await connection.BeginTransactionAsync())
            {
                await using var query = new NpgsqlCommand("SELECT event_id FROM public.ordering_outbox_messages FOR UPDATE NOWAIT", connection, transaction);
                Assert.Equal(message.EventId, await query.ExecuteScalarAsync());
                await transaction.RollbackAsync();
            }
            await using var independent = NpgsqlDataSource.Create(database.ConnectionString);
            var second = await new OutboxRelay(new PostgresOutboxProcessingStore(independent), secondPublisher, RelayOptions)
                .RunOnceAsync("second");
            Assert.Equal(OutboxRelayStatus.NoEligibleWork, second.Status);
        }
        finally { paused.Release.TrySetResult(); }
        Assert.Equal(OutboxRelayStatus.Published, (await first).Status);
        using var consumer = Consumer();
        Observe(consumer, message);
        Assert.Equal(1, consumer.QueryWatermarkOffsets(new(_topic, 0), TimeSpan.FromSeconds(10)).High.Value);
        await AssertState(message, "Published");
    }

    [Fact]
    public async Task UnknownAcquisitionCommit_GrantsNoSend_EvenIfReservationWasPersisted()
    {
        var message = await Seed();
        var uncertain = new PostgresOutboxProcessingStore(database.Source, new(Commit: async (_, transaction, ct) =>
        {
            await transaction.CommitAsync(ct);
            throw new IOException("controlled acquisition response loss");
        }));
        await using var publisher = Publisher();
        var result = await new OutboxRelay(uncertain, publisher, RelayOptions).RunOnceAsync("unknown-acquire");
        Assert.Equal(OutboxRelayStatus.AcquisitionUnknown, result.Status);
        Assert.Equal(OutboxDeliveryStatus.NotAttempted, result.Delivery);
        await AssertState(message, "Processing");
        using var consumer = Consumer();
        Assert.Equal(0, consumer.QueryWatermarkOffsets(new(_topic, 0), TimeSpan.FromSeconds(10)).High.Value);
    }

    [Fact]
    public void ProducerGuaranteesAreExplicitAndCannotBeOverriddenByArbitraryConfig()
    {
        var config = new KafkaOutboxPublisherOptions
        {
            BootstrapServers = kafka.BootstrapServers, Topic = _topic, DeliveryTimeout = TimeSpan.FromSeconds(3)
        }.CreateConfig();
        Assert.Equal(Acks.All, config.Acks);
        Assert.True(config.EnableIdempotence);
        Assert.Equal(3000, config.MessageTimeoutMs);
        Assert.Equal(int.MaxValue, config.MessageSendMaxRetries);
        Assert.Equal(5, config.MaxInFlight);
        Assert.False(config.AllowAutoCreateTopics);
    }

    [Fact]
    public async Task CancellationAfterEnqueue_DoesNotAbandonDeliveryOrDisposeProducerEarly()
    {
        var message = await Seed();
        await SetMinimumReplicas("2");
        var publisher = Publisher(TimeSpan.FromSeconds(2));
        using var cancellation = new CancellationTokenSource();
        var send = publisher.PublishAsync(message, cancellation.Token);
        cancellation.Cancel();
        var disposing = publisher.DisposeAsync().AsTask();
        try
        {
            Assert.False(send.IsCompleted);
            Assert.False(disposing.IsCompleted);
            Assert.Equal(OutboxDeliveryStatus.Unconfirmed, (await send).Status);
            await AssertState(message, "Pending"); // Direct adapter invocation cannot mutate PostgreSQL.
        }
        finally { await disposing; }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(301000)]
    public void UnboundedOrInvalidDeliveryTimeoutIsRejected(int milliseconds)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new KafkaOutboxEventPublisher(new()
        {
            BootstrapServers = kafka.BootstrapServers, Topic = _topic,
            DeliveryTimeout = TimeSpan.FromMilliseconds(milliseconds)
        }));
    }
}
