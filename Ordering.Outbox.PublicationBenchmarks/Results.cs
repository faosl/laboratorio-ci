using System.Text.Json.Serialization;
using Ordering.Application.Outbox;
using Ordering.Infrastructure.Kafka;

namespace Ordering.Outbox.PublicationBenchmarks;

internal sealed record ExpectedEvent(Guid EventId, string AggregateId, string EventType,
    [property: JsonIgnore] string Payload);
internal sealed record DatabaseEvidence(Guid EventId, string PublicationStatus, bool HasPublishedAt);
internal sealed record KafkaEvidence(Guid? EventId, int Partition, long Offset, bool EnvelopeMatches);
internal sealed record StageSample(int Attempt, string Instrument, string Status, double Milliseconds, int Worker = 1);
internal sealed record AttemptSample(int Number, double Milliseconds, OutboxRelayResult Result, bool ValidReceipt,
    int Worker = 1, string Owner = "", double StartedAfterBatchMilliseconds = 0)
{
    public bool Uncertain => Result.Delivery == OutboxDeliveryStatus.Unconfirmed || Result.Status is
        OutboxRelayStatus.AcquisitionUnknown or OutboxRelayStatus.CompletionUnknown or OutboxRelayStatus.RetryUnknown;

    public static AttemptSample Create(int number, double milliseconds, OutboxRelayResult result, string topic,
        int worker = 1, string owner = "", double startedAfterBatchMilliseconds = 0) =>
        new(number, milliseconds, result, result.Receipt is { Partition: >= 0, Offset: >= 0 } receipt
            && receipt.Topic == topic, worker, owner, startedAfterBatchMilliseconds);
}
internal sealed record Percentiles(double P50, double P95, double P99)
{
    public static Percentiles? From(IEnumerable<double> samples)
    {
        var sorted = samples.Order().ToArray();
        if (sorted.Length == 0) return null;
        double P(double q) => sorted[(int)Math.Ceiling(q * sorted.Length) - 1];
        return new(P(.50), P(.95), P(.99));
    }
}
internal sealed record Reconciliation(int KafkaAcks, int KafkaRecords, int KafkaDistinctIds,
    int PublishedDistinctIds, int DuplicateKafkaRecords, string[] Issues)
{
    public bool Valid => Issues.Length == 0;
}
internal static class Reconciler
{
    public static Reconciliation Check(IReadOnlyList<ExpectedEvent> expected, IReadOnlyList<AttemptSample> attempts,
        IReadOnlyList<DatabaseEvidence> database, IReadOnlyList<KafkaEvidence> kafka, bool recovery)
    {
        var issues = new List<string>();
        var ids = expected.Select(e => e.EventId).ToHashSet();
        void Require(bool condition, string issue) { if (!condition) issues.Add(issue); }
        Require(ids.Count == expected.Count && !ids.Contains(Guid.Empty), "InvalidExpectedIdentities");
        Require(database.Select(d => d.EventId).Distinct().Count() == database.Count, "RepeatedDatabaseIdentity");
        Require(database.All(d => ids.Contains(d.EventId)), "UnexpectedDatabaseIdentity");
        Require(ids.All(id => database.Any(d => d.EventId == id)), "MissingDatabaseIdentity");
        var published = database.Where(d => ids.Contains(d.EventId) && d.PublicationStatus == "Published"
            && d.HasPublishedAt).Select(d => d.EventId).ToHashSet();
        Require(ids.SetEquals(published), "ExpectedIdentityNotDurablyPublished");
        Require(database.All(d => (d.PublicationStatus == "Published") == d.HasPublishedAt), "InvalidPublishedTimestamp");
        Require(kafka.All(k => k.EventId is { } id && ids.Contains(id)), "UnexpectedOrMissingKafkaIdentity");
        Require(kafka.All(k => k.EnvelopeMatches), "KafkaEnvelopeMismatch");
        Require(kafka.Select(k => (k.Partition, k.Offset)).Distinct().Count() == kafka.Count, "RepeatedKafkaPosition");
        var observed = kafka.Where(k => k.EventId.HasValue).Select(k => k.EventId!.Value).ToHashSet();
        Require(ids.IsSubsetOf(observed), "MissingKafkaIdentity");
        var duplicates = kafka.Count(k => k.EventId.HasValue) - observed.Count;
        Require(ids.All(id => kafka.Count(k => k.EventId == id) == (recovery ? 2 : 1)),
            recovery ? "RecoveryRequiresTwoRecordsPerIdentity" : "RepeatedOrMissingKafkaRecord");
        var reportedAcks = attempts.Where(a => a.Result.Delivery == OutboxDeliveryStatus.Acknowledged).ToArray();
        Require(reportedAcks.All(a => a.ValidReceipt), "InvalidAcknowledgementReceipt");
        var acks = reportedAcks.Where(a => a.ValidReceipt).ToArray();
        Require(acks.All(a => a.Result.EventId is { } id && ids.Contains(id)), "UnexpectedAcknowledgementIdentity");
        Require(acks.All(a => kafka.Any(k => k.EventId == a.Result.EventId
            && k.Partition == a.Result.Receipt?.Partition && k.Offset == a.Result.Receipt?.Offset)), "AcknowledgementNotObservedInKafka");
        Require(kafka.All(k => acks.Any(a => a.Result.EventId == k.EventId && a.ValidReceipt
            && a.Result.Receipt?.Partition == k.Partition && a.Result.Receipt?.Offset == k.Offset)), "KafkaRecordWithoutAcknowledgement");
        Require(acks.Select(a => (a.Result.Receipt?.Partition, a.Result.Receipt?.Offset)).Distinct().Count() == acks.Length,
            "RepeatedAcknowledgementReceipt");
        Require(acks.All(a => a.Result.EventId is { } id && published.Contains(id)), "AcknowledgementWithoutDurablePublication");
        var local = attempts.Where(a => a.Result.Status == OutboxRelayStatus.Published).ToArray();
        Require(local.All(a => a.Result.EventId is { } id && published.Contains(id)), "LocalCompletionNotDurablyConfirmed");
        Require(local.All(a => a.Result.Delivery == OutboxDeliveryStatus.Acknowledged && a.ValidReceipt), "LocalCompletionWithoutAcknowledgement");
        Require(ids.All(id => local.Count(a => a.Result.EventId == id) == 1), "MissingOrRepeatedLocalCompletion");
        if (recovery)
        {
            Require(expected.Count == 1 && attempts.Count == 2
                && attempts[0].Result.Status == OutboxRelayStatus.OperationalFailure
                && attempts[0].Result.Stage == OutboxRelayStage.Completion
                && attempts[0].Result.Delivery == OutboxDeliveryStatus.Acknowledged
                && attempts[1].Result.Status == OutboxRelayStatus.Published, "RecoverySequenceMismatch");
        }
        else Require(attempts.Count == expected.Count && attempts.All(a => a.Result.Status == OutboxRelayStatus.Published),
            "IncompleteOrUncertainAttempts");
        return new(acks.Length, kafka.Count, observed.Count, published.Count, duplicates, issues.ToArray());
    }
}

internal sealed class PhaseReport(string kind, int round, int requestedEvents, string topic)
{
    public WorkerConfiguration Configuration { get; init; } = new(1, PublisherTopology.Shared);
    public int ExecutionOrder { get; init; }
    public DateTimeOffset StartedAtUtc { get; set; }
    public DateTimeOffset? FinishedAtUtc { get; set; }
    public string Kind { get; } = kind;
    public int Round { get; } = round;
    public int RequestedEvents { get; } = requestedEvents;
    public string Topic { get; } = topic;
    public string? Database { get; set; }
    public string? PostgreSqlVersion { get; set; }
    public object? PostgreSqlSettings { get; set; }
    public object? PoolSettings { get; set; }
    public List<WorkerIdentity> Workers { get; } = [];
    public List<ExpectedEvent> Expected { get; } = [];
    public List<DatabaseEvidence> PendingBefore { get; } = [];
    public List<AttemptSample> Attempts { get; } = [];
    public List<StageSample> StageSamples { get; } = [];
    public List<DatabaseEvidence> DatabaseAfter { get; } = [];
    public List<KafkaEvidence> KafkaAfter { get; } = [];
    public List<DatabaseEvidence> RecoveryBeforeExpiration { get; } = [];
    public List<KafkaEvidence> RecoveryKafkaBeforeExpiration { get; } = [];
    public double? BatchMilliseconds { get; set; }
    public int TicketsIssued { get; set; }
    public string StopReason { get; set; } = "NotStarted";
    public string? FailureType { get; set; }
    public Reconciliation? Validation { get; set; }
    public bool VerificationReachedEnd { get; set; }
    public bool Complete { get; set; }
    public string DatabaseCleanup { get; set; } = "NotCreated";
    public string PublisherCleanup { get; set; } = "NotCreated";
    public bool BaselineEligible => Kind == "benchmark" && Complete && DatabaseCleanup == "Dropped"
        && PublisherCleanup == "Disposed";
    public bool TelemetryConsistent => Kind == "recovery" ? StageSamples.Count == 0
        : TelemetryValidation.Check(Attempts, StageSamples, Workers);
    public object? Metrics => Kind != "benchmark" || BatchMilliseconds is not > 0 ? null : new
    {
        BaselineEligible, BatchMilliseconds,
        AttemptsPerSecond = Attempts.Count / (BatchMilliseconds.Value / 1000),
        KafkaAcksPerSecond = Validation?.KafkaAcks / (BatchMilliseconds.Value / 1000),
        DistinctPublishedPerSecond = Validation?.PublishedDistinctIds / (BatchMilliseconds.Value / 1000),
        Uncertain = Attempts.Count(a => a.Uncertain),
        Failures = Attempts.Count(a => a.Result.Status != OutboxRelayStatus.Published),
        Duplicates = Validation?.DuplicateKafkaRecords,
        FirstAttempts = Attempts.OrderBy(a => a.StartedAfterBatchMilliseconds).Take(10).ToArray(),
        FirstAttemptPerWorker = Attempts.GroupBy(a => a.Worker).Select(g => g.OrderBy(a => a.Number).First()).ToArray(),
        WaitShareOfPublication = Percentiles.From(TelemetryValidation.WaitShares(StageSamples)),
        AttemptLatencyMs = Percentiles.From(Attempts.Select(a => a.Milliseconds)),
        Stages = StageSamples.GroupBy(s => s.Instrument).Select(g => new
        {
            Instrument = g.Key, Count = g.Count(), LatencyMs = Percentiles.From(g.Select(s => s.Milliseconds))
        }).ToArray()
    };
}

internal sealed record WorkerIdentity(int Worker, string Owner);

internal static class TelemetryValidation
{
    // A ratio belongs to one publication. Never divide independently ranked percentiles.
    public static IEnumerable<double> WaitShares(IEnumerable<StageSample> samples)
    {
        foreach (var group in samples.Where(s => s.Instrument is KafkaOutboxDiagnostics.WaitName
                     or OutboxRelayDiagnostics.PublicationDurationName).GroupBy(s => (s.Attempt, s.Worker)))
        {
            if (group.Count() != 2) continue;
            var wait = group.FirstOrDefault(s => s.Instrument == KafkaOutboxDiagnostics.WaitName);
            var publication = group.FirstOrDefault(s => s.Instrument == OutboxRelayDiagnostics.PublicationDurationName);
            if (wait is { Milliseconds: >= 0 } && publication is { Milliseconds: > 0 }
                && double.IsFinite(publication.Milliseconds) && wait.Milliseconds <= publication.Milliseconds)
                yield return wait.Milliseconds / publication.Milliseconds;
        }
    }

    public static bool Check(IReadOnlyList<AttemptSample> attempts, IReadOnlyList<StageSample> samples,
        IReadOnlyList<WorkerIdentity> workers)
    {
        if (!attempts.Select(a => a.Number).Order().SequenceEqual(Enumerable.Range(1, attempts.Count))
            || workers.DistinctBy(w => w.Worker).Count() != workers.Count
            || workers.DistinctBy(w => w.Owner).Count() != workers.Count)
            return false;
        var byAttempt = samples.ToLookup(s => (s.Attempt, s.Worker));
        var matched = 0;
        foreach (var attempt in attempts)
        {
            var stages = byAttempt[(attempt.Number, attempt.Worker)].ToArray();
            if (!workers.Contains(new(attempt.Worker, attempt.Owner))
                || !double.IsFinite(attempt.Milliseconds) || attempt.Milliseconds < 0
                || !ValidStages(attempt.Result, stages))
                return false;
            matched += stages.Length;
        }
        return matched == samples.Count;
    }

    private static bool ValidStages(OutboxRelayResult result, StageSample[] stages)
    {
        if (stages.Any(s => !double.IsFinite(s.Milliseconds) || s.Milliseconds < 0)
            || stages.DistinctBy(s => s.Instrument).Count() != stages.Length)
            return false;

        var remaining = stages.ToDictionary(s => s.Instrument);
        bool FitsWithin(string inner, string outer) =>
            !remaining.TryGetValue(inner, out var child) || (remaining.TryGetValue(outer, out var parent)
                && child.Milliseconds <= parent.Milliseconds);
        if (!FitsWithin(KafkaOutboxDiagnostics.WaitName, OutboxRelayDiagnostics.PublicationDurationName)
            || !FitsWithin(KafkaOutboxDiagnostics.HoldName, OutboxRelayDiagnostics.PublicationDurationName)
            || !FitsWithin(KafkaOutboxDiagnostics.ProduceName, KafkaOutboxDiagnostics.HoldName))
            return false;
        string? Take(string name) => remaining.Remove(name, out var sample) ? sample.Status : null;
        if (Take(OutboxRelayDiagnostics.AttemptDurationName) != result.Status.ToString()) return false;

        var acquisition = Take(StageCollector.AcquisitionName);
        if (result.Stage == OutboxRelayStage.Acquisition)
            return remaining.Count == 0 && result.Status switch
            {
                OutboxRelayStatus.NoEligibleWork => acquisition == "NoEligibleWork",
                OutboxRelayStatus.AcquisitionUnknown => acquisition == "OutcomeUnknown",
                OutboxRelayStatus.Cancelled => acquisition is null or "Cancelled",
                _ => acquisition is "Exception" or "Acquired"
            };
        if (acquisition != "Acquired") return false;
        if (result.Stage == OutboxRelayStage.Validation) return remaining.Count == 0;

        var publication = Take(OutboxRelayDiagnostics.PublicationDurationName);
        var wait = Take(KafkaOutboxDiagnostics.WaitName);
        var hold = Take(KafkaOutboxDiagnostics.HoldName);
        var produce = Take(KafkaOutboxDiagnostics.ProduceName);
        if (result.Stage == OutboxRelayStage.Completion)
        {
            if (publication != "Acknowledged") return false;
            var completion = Take(OutboxRelayDiagnostics.CompletionDurationName);
            var expected = result.Status switch
            {
                OutboxRelayStatus.Published => "Succeeded",
                OutboxRelayStatus.LeaseLost => "LeaseLost",
                OutboxRelayStatus.CompletionUnknown => "OutcomeUnknown",
                OutboxRelayStatus.Cancelled => "Cancelled",
                _ => "Exception"
            };
            if (completion != expected) return false;
        }
        if (result.Stage == OutboxRelayStage.Retry && publication != "Unconfirmed") return false;
        if (remaining.Count != 0) return false;
        // A failed wait owns nothing. A failed publication after entry can precede ProduceAsync.
        if (wait != "Acquired")
            return result.Stage == OutboxRelayStage.Publication && hold is null && produce is null
                && (wait, publication) is ("Cancelled", "Cancelled") or ("Exception", "Exception");
        if (hold != "Released") return false;
        return publication switch
        {
            "Acknowledged" => produce == "Acknowledged" && result.Delivery == OutboxDeliveryStatus.Acknowledged,
            "Unconfirmed" => produce is "Unconfirmed" or "Exception" && result.Delivery == OutboxDeliveryStatus.Unconfirmed,
            "Cancelled" => result.Stage == OutboxRelayStage.Publication && produce is null or "Cancelled",
            "Exception" => result.Stage == OutboxRelayStage.Publication && produce is null or "Exception" or "Cancelled",
            _ => false
        };
    }
}
