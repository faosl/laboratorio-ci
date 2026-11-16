using Ordering.Application.Inbox;

namespace Ordering.Hosting;

/// <summary>
/// Owns the scoped dependency lifetime of one RunOnceAsync attempt through its Quiescence.
/// The factory must create a fresh asynchronous scope (or dedicated resources) for each attempt
/// and dispose it if resolution fails. Container-owned singletons stay outside this boundary;
/// retaining this scope cannot prevent their disposal by the root container during shutdown.
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
