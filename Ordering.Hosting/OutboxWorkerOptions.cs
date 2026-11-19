namespace Ordering.Hosting;

/// <summary>Provisional laboratory values, not approved production parameters.</summary>
public sealed record OutboxWorkerOptions
{
    public TimeSpan PollingDelay { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan PublicationFailureDelay { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan UnknownOutcomeDelay { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan InitialOperationalFailureDelay { get; init; } = TimeSpan.FromSeconds(2);
    public TimeSpan MaximumOperationalFailureDelay { get; init; } = TimeSpan.FromSeconds(30);
    public int MaximumImmediateResults { get; init; } = 100;
    public TimeSpan ImmediateResultDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    internal void Validate()
    {
        ValidateDelay(PollingDelay, nameof(PollingDelay));
        ValidateDelay(PublicationFailureDelay, nameof(PublicationFailureDelay));
        ValidateDelay(UnknownOutcomeDelay, nameof(UnknownOutcomeDelay));
        ValidateDelay(InitialOperationalFailureDelay, nameof(InitialOperationalFailureDelay));
        ValidateDelay(MaximumOperationalFailureDelay, nameof(MaximumOperationalFailureDelay));
        ValidateDelay(ImmediateResultDelay, nameof(ImmediateResultDelay));
        if (MaximumOperationalFailureDelay < InitialOperationalFailureDelay)
            throw new ArgumentOutOfRangeException(nameof(MaximumOperationalFailureDelay));
        if (MaximumImmediateResults < 1)
            throw new ArgumentOutOfRangeException(nameof(MaximumImmediateResults));
    }

    private static void ValidateDelay(TimeSpan delay, string name)
    {
        if (delay < TimeSpan.FromMilliseconds(1) || delay.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(name);
    }
}
