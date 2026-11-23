using System.Diagnostics;
using Ordering.Application.Outbox;
using Ordering.Infrastructure.Kafka;
using Ordering.Infrastructure.PostgreSql;

namespace Ordering.Outbox.PublicationBenchmarks;

internal static class PhaseRunner
{
    // Exactly the existing relay defaults and publisher defaults; no benchmark policy overrides.
    public static async Task RunAsync(PhaseReport report, string bootstrap, BenchmarkOptions options,
        Func<Task> checkpoint, CancellationToken ct)
    {
        try
        {
            await using var database = new LaboratoryDatabase(report);
            await checkpoint();
            await database.InitializeAsync(ct);
            await checkpoint();
            await KafkaLaboratory.CreateTopicAsync(bootstrap, report.Topic);
            await database.SeedAsync(report.RequestedEvents, ct);
            await checkpoint();
            var store = new PostgresOutboxProcessingStore(database.Source);
            if (report.Kind == "recovery") await RecoverAsync(database, store, report, bootstrap, options, ct);
            else await MeasureAsync(store, report, bootstrap, options, ct);
            // Consumer, durable state queries and reconciliation are strictly outside the batch timer.
            using var verification = CancellationTokenSource.CreateLinkedTokenSource(ct);
            verification.CancelAfter(TimeSpan.FromSeconds(options.VerificationTimeoutSeconds));
            report.DatabaseAfter.AddRange(await database.ReadAsync(verification.Token));
            report.VerificationReachedEnd = KafkaLaboratory.Read(bootstrap, report, report.KafkaAfter,
                options.VerificationTimeoutSeconds, verification.Token);
            report.Validation = Reconciler.Check(report.Expected, report.Attempts, report.DatabaseAfter, report.KafkaAfter,
                report.Kind == "recovery");
            report.Complete = report.VerificationReachedEnd && report.Validation.Valid && report.TelemetryConsistent
                && report.Expected.Count == report.RequestedEvents;
        }
        catch (Exception error)
        {
            report.FailureType = error.GetType().Name;
            report.Complete = false;
        }
    }

    private static KafkaOutboxEventPublisher Publisher(string bootstrap, string topic) => new(new()
        { BootstrapServers = bootstrap, Topic = topic });

    private static async Task MeasureAsync(IOutboxProcessingStore store, PhaseReport report, string bootstrap,
        BenchmarkOptions options, CancellationToken ct)
    {
        await using var publisher = Publisher(bootstrap, report.Topic);
        var relay = new OutboxRelay(store, publisher, new());
        using var listener = new StageCollector(report.StageSamples);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(TimeSpan.FromSeconds(options.BatchTimeoutSeconds));
        var completed = new HashSet<Guid>();
        var expected = report.Expected.Select(e => e.EventId).ToHashSet();
        var owner = "publication-benchmark-" + Guid.NewGuid().ToString("N");
        report.StopReason = "Interrupted";
        var started = Stopwatch.GetTimestamp();
        try
        {
            for (var i = 1; i <= report.RequestedEvents; i++)
            {
                listener.Attempt = i;
                var sample = await AttemptAsync(relay, owner, report, deadline.Token);
                if (sample.Result.Status != OutboxRelayStatus.Published || !sample.ValidReceipt
                    || sample.Result.EventId is not { } id || !expected.Contains(id) || !completed.Add(id))
                {
                    report.StopReason = deadline.IsCancellationRequested ? "DeadlineOrCancellation" : "NonSuccessfulOrUnexpectedResult";
                    return;
                }
                if (completed.Count == expected.Count) { report.StopReason = "ExpectedLocalCompletions"; return; }
            }
            report.StopReason = "AttemptLimit";
        }
        finally { report.BatchMilliseconds = Stopwatch.GetElapsedTime(started).TotalMilliseconds; }
    }

    private static async Task<AttemptSample> AttemptAsync(OutboxRelay relay, string owner, PhaseReport report, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        var result = await relay.RunOnceAsync(owner, ct);
        var sample = AttemptSample.Create(report.Attempts.Count + 1, Stopwatch.GetElapsedTime(started).TotalMilliseconds, result, report.Topic);
        report.Attempts.Add(sample);
        return sample;
    }

    private static async Task RecoverAsync(LaboratoryDatabase database, IOutboxProcessingStore store,
        PhaseReport report, string bootstrap, BenchmarkOptions options, CancellationToken ct)
    {
        // Injected pre-completion exception, NOT a process crash. Same boundary as the integration test.
        await using (var publisher = Publisher(bootstrap, report.Topic))
        {
            var relay = new OutboxRelay(new FailBeforeCompletionStore(store), publisher, new());
            await AttemptAsync(relay, "injected-failure", report, ct);
        }
        report.RecoveryBeforeExpiration.AddRange(await database.ReadAsync(ct));
        var reachedEnd = KafkaLaboratory.Read(bootstrap, report, report.RecoveryKafkaBeforeExpiration,
            options.VerificationTimeoutSeconds, ct);
        if (!reachedEnd || report.RecoveryBeforeExpiration is not [{ PublicationStatus: "Processing", HasPublishedAt: false }]
            || report.RecoveryKafkaBeforeExpiration is not [{ EnvelopeMatches: true }]
            || report.Attempts[0].Result is not { Status: OutboxRelayStatus.OperationalFailure,
                Stage: OutboxRelayStage.Completion, Delivery: OutboxDeliveryStatus.Acknowledged }
            || !report.Attempts[0].ValidReceipt)
            throw new InvalidOperationException("Injected boundary evidence mismatch.");
        await database.ExpireOwnedLeaseAsync(report.Expected.Single().EventId, ct);
        await using (var restartedPublisher = Publisher(bootstrap, report.Topic))
            await AttemptAsync(new OutboxRelay(store, restartedPublisher, new()), "lease-recovery", report, ct);
        report.StopReason = "RecoveryAttemptsFinished";
    }

    private sealed class FailBeforeCompletionStore(IOutboxProcessingStore inner) : IOutboxProcessingStore
    {
        public Task<OutboxAcquisitionResult> TryAcquireAsync(string owner, TimeSpan duration, CancellationToken ct)
            => inner.TryAcquireAsync(owner, duration, ct);
        public Task<OutboxMutationResult> RenewAsync(OutboxLease lease, TimeSpan duration, CancellationToken ct)
            => inner.RenewAsync(lease, duration, ct);
        public Task<OutboxMutationResult> MarkPublishedAsync(OutboxLease lease, CancellationToken ct)
            => throw new IOException("Controlled laboratory boundary.");
        public Task<OutboxMutationResult> ScheduleRetryAsync(OutboxLease lease, TimeSpan delay, CancellationToken ct)
            => inner.ScheduleRetryAsync(lease, delay, ct);
    }
}
