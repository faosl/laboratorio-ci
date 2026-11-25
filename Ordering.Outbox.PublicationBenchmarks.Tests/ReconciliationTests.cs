using System.Text;
using Confluent.Kafka;
using Ordering.Application.Outbox;

namespace Ordering.Outbox.PublicationBenchmarks.Tests;

public sealed class ReconciliationTests
{
    private static readonly Guid Id = Guid.NewGuid();
    private static readonly ExpectedEvent Expected = new(Id, "order", "OrderConfirmed", "{}");
    private static AttemptSample Success(Guid? id = null, long offset = 0) => AttemptSample.Create(1, 1,
        new(OutboxRelayStatus.Published, OutboxRelayStage.Completion, id ?? Id, OutboxDeliveryStatus.Acknowledged,
            new("topic", 0, offset)), "topic");
    private static DatabaseEvidence Published => new(Id, "Published", true);
    private static KafkaEvidence Record(long offset = 0) => new(Id, 0, offset, true);

    [Fact]
    public void CountsDistinctPersistedIdentitiesAndReconcilesReceipts()
    {
        var result = Reconciler.Check([Expected], [Success()], [Published], [Record()], false);
        Assert.True(result.Valid);
        Assert.Equal(1, result.PublishedDistinctIds);
        Assert.Equal(1, result.KafkaAcks);
        Assert.Equal(1, result.KafkaRecords);
    }

    [Fact]
    public void ConcurrentCompletionOrderDoesNotChangeDistinctReconciliationAndDuplicateIdsRemainInvalid()
    {
        var events = Enumerable.Range(1, 4).Select(i => new ExpectedEvent(Guid.NewGuid(), "order-" + i,
            "OrderConfirmed", "{}")).ToArray();
        var attempts = events.Select((e, i) => AttemptSample.Create(i + 1, 1,
            new(OutboxRelayStatus.Published, OutboxRelayStage.Completion, e.EventId,
                OutboxDeliveryStatus.Acknowledged, new("topic", 0, i)), "topic", i + 1, "owner-" + i)).Reverse().ToArray();
        var database = events.Select(e => new DatabaseEvidence(e.EventId, "Published", true)).Reverse().ToArray();
        var kafka = events.Select((e, i) => new KafkaEvidence(e.EventId, 0, i, true)).ToArray();
        var result = Reconciler.Check(events, attempts, database, kafka, false);
        Assert.True(result.Valid);
        Assert.Equal(4, result.PublishedDistinctIds);
        Assert.Equal(4, result.KafkaDistinctIds);
        var duplicate = Reconciler.Check(events, attempts, database,
            [.. kafka, kafka[0] with { Offset = 4 }], false);
        Assert.False(duplicate.Valid);
        Assert.Equal(1, duplicate.DuplicateKafkaRecords);
        Assert.Equal(4, duplicate.PublishedDistinctIds);
    }

    [Fact]
    public void EqualAggregateCountsDoNotHideMissingOrUnexpectedIdentities()
    {
        var other = Guid.NewGuid();
        var result = Reconciler.Check([Expected], [Success()], [new(other, "Published", true)],
            [new(other, 0, 0, true)], false);
        Assert.False(result.Valid);
        Assert.Equal(0, result.PublishedDistinctIds);
        Assert.Contains("LocalCompletionNotDurablyConfirmed", result.Issues);
        Assert.Contains("MissingKafkaIdentity", result.Issues);
        Assert.Contains("UnexpectedDatabaseIdentity", result.Issues);
    }

    [Fact]
    public void AckIsNotDurableLocalCompletion()
    {
        var result = Reconciler.Check([Expected], [Success()], [new(Id, "Processing", false)], [Record()], false);
        Assert.False(result.Valid);
        Assert.Equal(1, result.KafkaAcks);
        Assert.Equal(0, result.PublishedDistinctIds);
        Assert.Contains("AcknowledgementWithoutDurablePublication", result.Issues);
    }

    [Fact]
    public void RepeatedKafkaIdentityAndMismatchedReceiptAreDetected()
    {
        var result = Reconciler.Check([Expected], [Success(offset: 9)], [Published], [Record(), Record(1)], false);
        Assert.False(result.Valid);
        Assert.Equal(2, result.KafkaRecords);
        Assert.Equal(1, result.KafkaDistinctIds);
        Assert.Equal(1, result.DuplicateKafkaRecords);
        Assert.Contains("AcknowledgementNotObservedInKafka", result.Issues);
        Assert.Contains("KafkaRecordWithoutAcknowledgement", result.Issues);
    }

    [Fact]
    public void RecoveryAcceptsTwoAcknowledgedRecordsForOneDurablyPublishedEventOnlyInSeparateMode()
    {
        var first = AttemptSample.Create(1, 2, new(OutboxRelayStatus.OperationalFailure, OutboxRelayStage.Completion,
            Id, OutboxDeliveryStatus.Acknowledged, new("topic", 0, 0), "IOException"), "topic");
        var attempts = new[] { first, Success(offset: 1) };
        var result = Reconciler.Check([Expected], attempts, [Published], [Record(), Record(1)], true);
        Assert.True(result.Valid);
        Assert.Equal(2, result.KafkaAcks);
        Assert.Equal(1, result.PublishedDistinctIds);
        Assert.False(Reconciler.Check([Expected], attempts, [Published], [Record(), Record(1)], false).Valid);
    }

    [Theory]
    [InlineData(OutboxRelayStatus.CompletionUnknown, OutboxDeliveryStatus.Acknowledged)]
    [InlineData(OutboxRelayStatus.RetryScheduled, OutboxDeliveryStatus.Unconfirmed)]
    [InlineData(OutboxRelayStatus.AcquisitionUnknown, OutboxDeliveryStatus.NotAttempted)]
    public void UnknownOutcomesCannotBecomeSuccessfulBaseline(OutboxRelayStatus status, OutboxDeliveryStatus delivery)
    {
        var attempt = AttemptSample.Create(1, 1, new(status, OutboxRelayStage.Completion, Id, delivery), "topic");
        Assert.True(attempt.Uncertain);
        Assert.False(Reconciler.Check([Expected], [attempt], [Published], [Record()], false).Valid);
    }

    [Theory]
    [InlineData("other", 0, 0)]
    [InlineData("topic", -1, 0)]
    [InlineData("topic", 0, -1)]
    public void ReceiptMustIdentifyThisTopicAndRealPosition(string topic, int partition, long offset)
    {
        var attempt = AttemptSample.Create(1, 1, new(OutboxRelayStatus.Published, OutboxRelayStage.Completion,
            Id, OutboxDeliveryStatus.Acknowledged, new(topic, partition, offset)), "topic");
        Assert.False(attempt.ValidReceipt);
        var result = Reconciler.Check([Expected], [attempt], [Published], [Record()], false);
        Assert.Equal(0, result.KafkaAcks);
        Assert.Contains("InvalidAcknowledgementReceipt", result.Issues);
    }

    [Fact]
    public void EnvelopeChecksKeyBothHeadersAndExactPersistedPayload()
    {
        var message = new Message<string, byte[]>
        {
            Key = Expected.AggregateId, Value = Encoding.UTF8.GetBytes(Expected.Payload),
            Headers = new Headers
            {
                { "event-id", Encoding.UTF8.GetBytes(Id.ToString("D")) },
                { "event-type", Encoding.UTF8.GetBytes(Expected.EventType) }
            }
        };
        Assert.True(KafkaLaboratory.EnvelopeMatches(message, Expected));
        message.Key = "unexpected";
        Assert.False(KafkaLaboratory.EnvelopeMatches(message, Expected));
        message.Key = Expected.AggregateId;
        message.Value = Encoding.UTF8.GetBytes("{\"unexpected\":true}");
        Assert.False(KafkaLaboratory.EnvelopeMatches(message, Expected));
        message.Value = Encoding.UTF8.GetBytes(Expected.Payload);
        message.Headers.Add("event-id", Encoding.UTF8.GetBytes(Id.ToString("D")));
        Assert.False(KafkaLaboratory.EnvelopeMatches(message, Expected));
    }

    [Fact]
    public void PercentilesUseActualNearestRankSamples()
    {
        Assert.Null(Percentiles.From([]));
        var percentiles = Percentiles.From(Enumerable.Range(1, 100).Select(i => (double)i).Reverse());
        Assert.Equal(new Percentiles(50, 95, 99), percentiles);
    }

    [Fact]
    public void DefaultsAreExplicitAndOptionsRejectDuplicates()
    {
        var smoke = BenchmarkOptions.Parse(["--smoke"]);
        Assert.Equal(20, smoke.Events);
        Assert.Equal(1, smoke.Rounds);
        var full = BenchmarkOptions.Parse([]);
        Assert.Equal(500, full.Events);
        Assert.Equal(3, full.Rounds);
        Assert.Throws<ArgumentException>(() => BenchmarkOptions.Parse(["--events", "20", "--events", "30"]));
        Assert.Throws<ArgumentException>(() => BenchmarkOptions.Parse(["--warmups", "0"]));
    }
}
