using Microsoft.Extensions.DependencyInjection;
using Ordering.Application.Outbox;

namespace Ordering.Hosting;

/// <summary>
/// Dependency ownership covers only this attempt's asynchronous scope. Quiescence is the
/// completion of RunOnceAsync by adapter contract; external unknown tasks cannot be supervised.
/// </summary>
public sealed class OutboxWorkerExecution
{
    internal Func<string, CancellationToken, Task<OutboxRelayResult>> RunOnceAsync { get; }
    internal IAsyncDisposable OwnedDependencies { get; }

    public OutboxWorkerExecution(Func<string, CancellationToken, Task<OutboxRelayResult>> runOnceAsync,
        IAsyncDisposable ownedDependencies)
    {
        ArgumentNullException.ThrowIfNull(runOnceAsync);
        ArgumentNullException.ThrowIfNull(ownedDependencies);
        RunOnceAsync = runOnceAsync;
        OwnedDependencies = ownedDependencies;
    }

    public static async ValueTask<OutboxWorkerExecution> CreateScopedAsync(IServiceScopeFactory scopes)
    {
        var scope = scopes.CreateAsyncScope();
        try
        {
            var relay = scope.ServiceProvider.GetRequiredService<OutboxRelay>();
            return new(relay.RunOnceAsync, scope);
        }
        catch
        {
            await scope.DisposeAsync();
            throw;
        }
    }
}
