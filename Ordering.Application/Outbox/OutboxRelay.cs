namespace Ordering.Application.Outbox;

public sealed record OutboxRelayOptions
{
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromSeconds(60);
    public TimeSpan ExecutionTimeout { get; init; } = TimeSpan.FromSeconds(40);
    public TimeSpan LeaseSafetyMargin { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan RetryDelay { get; init; } = TimeSpan.FromSeconds(5);

    internal void Validate(TimeSpan deliveryTimeout)
    {
        OutboxProcessingLimits.ValidateLeaseDuration(LeaseDuration);
        OutboxProcessingLimits.ValidateRetryDelay(RetryDelay);
        if (deliveryTimeout <= TimeSpan.Zero || ExecutionTimeout <= deliveryTimeout
            || LeaseSafetyMargin <= TimeSpan.Zero || LeaseSafetyMargin >= LeaseDuration
            || ExecutionTimeout > LeaseDuration - LeaseSafetyMargin)
            throw new ArgumentException("Require delivery timeout < execution timeout <= lease duration - positive safety margin.");
    }
}

public enum OutboxRelayStatus
{
    Published, NoEligibleWork, AcquisitionUnknown, RetryScheduled, LeaseLost,
    CompletionUnknown, RetryUnknown, UnsupportedEvent, Cancelled, OperationalFailure
}

public enum OutboxRelayStage { Acquisition, Validation, Publication, Completion, Retry }

public sealed record OutboxRelayResult(OutboxRelayStatus Status, OutboxRelayStage Stage,
    Guid? EventId = null, OutboxDeliveryStatus Delivery = OutboxDeliveryStatus.NotAttempted,
    OutboxDeliveryReceipt? Receipt = null, string? ErrorCode = null);

/// <summary>Explicit one-shot coordination; no polling, renewal loop or implicit second attempt.</summary>
public sealed class OutboxRelay
{
    private readonly IOutboxProcessingStore _store;
    private readonly IOutboxEventPublisher _publisher;
    private readonly OutboxRelayOptions _options;

    public OutboxRelay(IOutboxProcessingStore store, IOutboxEventPublisher publisher, OutboxRelayOptions options)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate(publisher.DeliveryTimeout);
        _store = store;
        _publisher = publisher;
        _options = options;
    }

    public async Task<OutboxRelayResult> RunOnceAsync(string owner, CancellationToken cancellationToken = default)
    {
        using var attempt = OutboxRelayDiagnostics.StartAttempt();
        OutboxProcessingLimits.ValidateOwner(owner);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(_options.ExecutionTimeout);
        var ct = deadline.Token;
        var stage = OutboxRelayStage.Acquisition;
        AcquiredOutboxMessage? work = null;
        var publication = new OutboxPublicationResult(OutboxDeliveryStatus.NotAttempted);
        OutboxRelayResult Result(OutboxRelayStatus status)
        {
            var result = new OutboxRelayResult(status, stage, work?.Content.EventId,
                publication.Status, publication.Receipt, publication.ErrorCode);
            attempt.Complete(result);
            return result;
        }
        try
        {
            ct.ThrowIfCancellationRequested();
            var acquisition = await OutboxRelayDiagnostics.ObserveAsync(stage,
                () => _store.TryAcquireAsync(owner, _options.LeaseDuration, ct), result => result.Status.ToString(), ct);
            if (acquisition.Status == OutboxAcquisitionStatus.NoEligibleWork)
                return Result(OutboxRelayStatus.NoEligibleWork);
            if (acquisition.Status == OutboxAcquisitionStatus.OutcomeUnknown)
                return Result(OutboxRelayStatus.AcquisitionUnknown);
            work = acquisition.Work ?? throw new InvalidOperationException("Confirmed acquisition requires work.");
            attempt.Acquired(work);
            stage = OutboxRelayStage.Validation;
            if (work.Content.EventType != nameof(OrderConfirmed))
                return Result(OutboxRelayStatus.UnsupportedEvent);
            if (work.Lease.EventId != work.Content.EventId)
                throw new InvalidOperationException("Lease and event identities must match.");
            ct.ThrowIfCancellationRequested();
            stage = OutboxRelayStage.Publication;
            // Ambiguous delivery outcome: a timeout/error can still leave a record in Kafka.
            // Retrying later preserves EventId, but can duplicate the external effect.
            publication = new(OutboxDeliveryStatus.Unconfirmed);
            publication = await OutboxRelayDiagnostics.ObserveAsync(stage,
                () => _publisher.PublishAsync(work.Content, ct), OutboxRelayDiagnostics.PublicationStatus, ct, work);
            ct.ThrowIfCancellationRequested();
            if (publication.Status == OutboxDeliveryStatus.Acknowledged)
            {
                if (publication.Receipt is null)
                    throw new InvalidOperationException("Acknowledgement requires broker diagnostics.");
                stage = OutboxRelayStage.Completion;
                // Fencing token validation remains atomic in PostgreSQL. It cannot cancel a
                // Kafka record already sent, even if this generation has lost its authority.
                var completion = await OutboxRelayDiagnostics.ObserveAsync(stage,
                    () => _store.MarkPublishedAsync(work.Lease, ct), result => result.Status.ToString(), ct, work);
                return Result(completion.Status switch
                {
                    OutboxMutationStatus.Succeeded => OutboxRelayStatus.Published,
                    OutboxMutationStatus.LeaseLost => OutboxRelayStatus.LeaseLost,
                    _ => OutboxRelayStatus.CompletionUnknown
                });
            }

            stage = OutboxRelayStage.Retry;
            // Lease-based recovery: the store alone validates current authority. Unknown
            // completion never reaches this path; it is not evidence of a rollback.
            var retry = await OutboxRelayDiagnostics.ObserveAsync(stage,
                () => _store.ScheduleRetryAsync(work.Lease, _options.RetryDelay, ct), result => result.Status.ToString(), ct, work);
            return Result(retry.Status switch
            {
                OutboxMutationStatus.Succeeded => OutboxRelayStatus.RetryScheduled,
                OutboxMutationStatus.LeaseLost => OutboxRelayStatus.LeaseLost,
                _ => OutboxRelayStatus.RetryUnknown
            });
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // The publisher supervises an already-started send before returning control.
            // Cancellation leaves recovery to persisted eligibility and lease expiration.
            return Result(OutboxRelayStatus.Cancelled);
        }
        catch (Exception error)
        {
            // No blind mutation after an operational failure, especially after a broker ACK.
            // Do not expose provider exception messages (they can contain connection details).
            return Result(OutboxRelayStatus.OperationalFailure) with
            {
                ErrorCode = publication.ErrorCode ?? error.GetType().Name
            };
        }
    }
}
