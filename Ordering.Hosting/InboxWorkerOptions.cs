namespace Ordering.Hosting;

public sealed record InboxWorkerOptions
{
    public TimeSpan PollingDelay { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan InitialStoreFailureDelay { get; init; } = TimeSpan.FromSeconds(2);
    public TimeSpan MaximumStoreFailureDelay { get; init; } = TimeSpan.FromSeconds(30);
    public TimeSpan ProcessingFailureDelay { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan UnknownOutcomeDelay { get; init; } = TimeSpan.FromSeconds(5);
    public int MaximumImmediateResults { get; init; } = 100;
    public TimeSpan ImmediateResultDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    internal void Validate()
    {
        ValidateDelay(PollingDelay, nameof(PollingDelay));
        ValidateDelay(InitialStoreFailureDelay, nameof(InitialStoreFailureDelay));
        ValidateDelay(MaximumStoreFailureDelay, nameof(MaximumStoreFailureDelay));
        ValidateDelay(ProcessingFailureDelay, nameof(ProcessingFailureDelay));
        ValidateDelay(UnknownOutcomeDelay, nameof(UnknownOutcomeDelay));
        ValidateDelay(ImmediateResultDelay, nameof(ImmediateResultDelay));
        if (MaximumStoreFailureDelay < InitialStoreFailureDelay)
            throw new ArgumentOutOfRangeException(nameof(MaximumStoreFailureDelay));
        if (MaximumImmediateResults < 2)
            throw new ArgumentOutOfRangeException(nameof(MaximumImmediateResults));
    }

    private static void ValidateDelay(TimeSpan delay, string name)
    {
        if (delay < TimeSpan.FromMilliseconds(1) || delay.TotalMilliseconds > uint.MaxValue - 1)
            throw new ArgumentOutOfRangeException(name);
    }
}
