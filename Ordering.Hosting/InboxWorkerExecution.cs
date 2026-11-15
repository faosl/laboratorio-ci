using Ordering.Application.Inbox;

namespace Ordering.Hosting;

/// <summary>
/// Owns the complete dependency lifetime used by RunOnceAsync, including shared store/processor
/// resources. The factory must create dedicated resources whose disposal is transferred here.
/// Container-owned singletons must not be included in this ownership boundary.
/// </summary>
public sealed class InboxWorkerExecution
{
    internal Func<string, CancellationToken, Task<InboxCoordinationResult>> RunOnceAsync { get; }
    internal IAsyncDisposable OwnedDependencies { get; }

    public InboxWorkerExecution(Func<string, CancellationToken, Task<InboxCoordinationResult>> runOnceAsync,
        IAsyncDisposable ownedDependencies)
    {
        ArgumentNullException.ThrowIfNull(runOnceAsync);
        ArgumentNullException.ThrowIfNull(ownedDependencies);
        RunOnceAsync = runOnceAsync;
        OwnedDependencies = ownedDependencies;
    }
}
