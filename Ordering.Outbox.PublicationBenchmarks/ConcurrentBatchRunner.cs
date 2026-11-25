using System.Diagnostics;

namespace Ordering.Outbox.PublicationBenchmarks;

internal sealed record BatchOutcome(double Milliseconds, int TicketsIssued, string StopReason, string? FailureType);

internal static class ConcurrentBatchRunner
{
    public static async Task<BatchOutcome> RunAsync(int workers, int tickets,
        Func<int, int, double, CancellationToken, Task<bool>> attempt, CancellationToken ct)
    {
        if (workers < 1 || tickets < 1) throw new ArgumentOutOfRangeException(nameof(workers));
        var admission = new object();
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var waiting = 0;
        var issued = 0;
        var stopped = false;
        string? failure = null;
        long started = 0;

        async Task Worker(int worker)
        {
            if (Interlocked.Increment(ref waiting) == workers) ready.SetResult();
            await start.Task;
            while (true)
            {
                int ticket;
                // Access serialization linearizes admission and failure. Tickets bound attempts,
                // never choose events: PostgreSQL still performs every real lease acquisition.
                lock (admission)
                {
                    if (stopped || ct.IsCancellationRequested || issued == tickets) return;
                    ticket = ++issued;
                }
                try
                {
                    // One outstanding operation per worker. Never cancel only the await:
                    // cancellation of an observer does not establish underlying quiescence.
                    if (await attempt(worker, ticket, Stopwatch.GetElapsedTime(started).TotalMilliseconds, ct)) continue;
                    lock (admission) stopped = true;
                }
                catch (Exception error)
                {
                    lock (admission) { stopped = true; failure ??= error.GetType().Name; }
                }
                return;
            }
        }

        var tasks = Enumerable.Range(1, workers).Select(w => Task.Run(() => Worker(w))).ToArray();
        await ready.Task;
        started = Stopwatch.GetTimestamp();
        start.SetResult();
        // Quiescence before disposal: WhenAll supervises all workers, including sends that
        // deliberately ignore cancellation after enqueue. Owners may dispose resources only now.
        await Task.WhenAll(tasks);
        return new(Stopwatch.GetElapsedTime(started).TotalMilliseconds, issued,
            ct.IsCancellationRequested ? "DeadlineOrCancellation" : stopped ? "NonSuccessfulOrUnexpectedResult"
                : "ExpectedLocalCompletions", failure);
    }
}
