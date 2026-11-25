using System.Diagnostics;
using System.Collections.Concurrent;
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
        report.StartedAtUtc = DateTimeOffset.UtcNow;
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
                && report.Expected.Count == report.RequestedEvents && report.FailureType is null
                && (report.Kind == "recovery" || report.StopReason == "ExpectedLocalCompletions")
                && report.PublisherCleanup == "Disposed";
        }
        catch (Exception error)
        {
            report.FailureType = error.GetType().Name;
            report.Complete = false;
        }
        finally { report.FinishedAtUtc = DateTimeOffset.UtcNow; }
    }

    private static KafkaOutboxEventPublisher Publisher(string bootstrap, string topic) => new(new()
        { BootstrapServers = bootstrap, Topic = topic });

    private static async Task MeasureAsync(IOutboxProcessingStore store, PhaseReport report, string bootstrap,
        BenchmarkOptions options, CancellationToken ct)
    {
        // Resource ownership belongs to the phase, never to individual relays. Shared topology
        // retains the publisher's concurrency gate (access serialization through ACK); per-worker
        // topology changes both gate sharing AND Kafka session count, not just semaphore cost.
        var publishers = new List<KafkaOutboxEventPublisher>();
        var samples = new ConcurrentQueue<AttemptSample>();
        using var listener = new StageCollector();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var completed = new ConcurrentDictionary<Guid, byte>();
        var expected = report.Expected.Select(e => e.EventId).ToHashSet();
        report.StopReason = "Interrupted";
        try
        {
            report.PublisherCleanup = "Pending";
            for (var i = 0; i < (report.Configuration.Topology == PublisherTopology.Shared ? 1 : report.Configuration.Workers); i++)
                publishers.Add(Publisher(bootstrap, report.Topic));
            var relays = Enumerable.Range(1, report.Configuration.Workers).Select(worker =>
            {
                report.Workers.Add(new(worker, "publication-benchmark-" + Guid.NewGuid().ToString("N")));
                return new OutboxRelay(store,
                    publishers[report.Configuration.Topology == PublisherTopology.Shared ? 0 : worker - 1], new());
            }).ToArray();
            deadline.CancelAfter(TimeSpan.FromSeconds(options.BatchTimeoutSeconds));
            var outcome = await ConcurrentBatchRunner.RunAsync(report.Configuration.Workers, report.RequestedEvents,
                async (worker, ticket, offset, token) =>
                {
                    using var context = listener.BeginAttempt(ticket, worker);
                    var started = Stopwatch.GetTimestamp();
                    var owner = report.Workers[worker - 1].Owner;
                    var result = await relays[worker - 1].RunOnceAsync(owner, token);
                    var sample = AttemptSample.Create(ticket, Stopwatch.GetElapsedTime(started).TotalMilliseconds,
                        result, report.Topic, worker, owner, offset);
                    samples.Enqueue(sample);
                    return result.Status == OutboxRelayStatus.Published && sample.ValidReceipt
                        && result.EventId is { } id && expected.Contains(id) && completed.TryAdd(id, 0);
                }, deadline.Token);
            report.BatchMilliseconds = outcome.Milliseconds;
            report.TicketsIssued = outcome.TicketsIssued;
            report.StopReason = outcome.StopReason;
            report.FailureType = outcome.FailureType;
        }
        finally
        {
            // RunAsync has awaited every worker. Only now consolidate telemetry and dispose
            // producers; the enclosing database scope disposes the shared DataSource later.
            report.Attempts.AddRange(samples.OrderBy(a => a.Number));
            report.StageSamples.AddRange(listener.Snapshot());
            var disposed = true;
            foreach (var publisher in publishers)
            {
                try { await publisher.DisposeAsync(); }
                catch (Exception error) { disposed = false; report.PublisherCleanup = "Failed:" + error.GetType().Name; }
            }
            if (disposed) report.PublisherCleanup = "Disposed";
        }
    }

    private static async Task<AttemptSample> AttemptAsync(OutboxRelay relay, string owner, PhaseReport report, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        var result = await relay.RunOnceAsync(owner, ct);
        var sample = AttemptSample.Create(report.Attempts.Count + 1, Stopwatch.GetElapsedTime(started).TotalMilliseconds,
            result, report.Topic, 1, owner);
        report.Attempts.Add(sample);
        return sample;
    }

    private static async Task RecoverAsync(LaboratoryDatabase database, IOutboxProcessingStore store,
        PhaseReport report, string bootstrap, BenchmarkOptions options, CancellationToken ct)
    {
        // Injected pre-completion exception, NOT a process crash. Same boundary as the integration test.
        report.PublisherCleanup = "Pending";
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
        report.PublisherCleanup = "Disposed";
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
