using System.Collections.Concurrent;

namespace Ordering.Outbox.PublicationBenchmarks.Tests;

public sealed class ConcurrentBatchRunnerTests
{
    [Theory]
    [InlineData(1, 50)]
    [InlineData(2, 51)]
    [InlineData(8, 500)]
    [InlineData(8, 3)]
    public async Task TicketsAreAtomicBoundedAndEachWorkerAwaitsItsPreviousOperation(int workers, int count)
    {
        var seen = new ConcurrentBag<int>();
        var active = new int[workers];
        var outcome = await ConcurrentBatchRunner.RunAsync(workers, count, async (worker, ticket, _, ct) =>
        {
            Assert.Equal(1, Interlocked.Increment(ref active[worker - 1]));
            seen.Add(ticket);
            await Task.Yield();
            Assert.Equal(0, Interlocked.Decrement(ref active[worker - 1]));
            return true;
        }, CancellationToken.None);
        Assert.Null(outcome.FailureType);
        Assert.Equal("ExpectedLocalCompletions", outcome.StopReason);
        Assert.Equal(count, outcome.TicketsIssued);
        Assert.Equal(Enumerable.Range(1, count), seen.Order());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailureStopsNewAdmissionsWithoutHiddenRetry(bool throws)
    {
        var calls = 0;
        var outcome = await ConcurrentBatchRunner.RunAsync(1, 500, (_, _, _, _) =>
        {
            calls++;
            if (throws) throw new IOException();
            return Task.FromResult(false);
        }, CancellationToken.None);
        Assert.Equal(1, calls);
        Assert.Equal(1, outcome.TicketsIssued);
        Assert.Equal("NonSuccessfulOrUnexpectedResult", outcome.StopReason);
        Assert.Equal(throws ? "IOException" : null, outcome.FailureType);
    }

    [Fact]
    public async Task CancellationStopsTicketsButSupervisesOperationsIgnoringCancellationUntilTheySettle()
    {
        using var cancellation = new CancellationTokenSource();
        var bothEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = 0;
        var finished = 0;
        var batch = ConcurrentBatchRunner.RunAsync(2, 500, async (_, _, _, _) =>
        {
            if (Interlocked.Increment(ref entered) == 2) bothEntered.SetResult();
            await release.Task; // Model an already-enqueued Kafka send; observer cancellation cannot settle it.
            Interlocked.Increment(ref finished);
            return true;
        }, cancellation.Token);
        await bothEntered.Task;
        cancellation.Cancel();
        Assert.False(batch.IsCompleted);
        Assert.Equal(0, finished);
        release.SetResult();
        var outcome = await batch;
        Assert.Equal(2, finished);
        Assert.Equal(2, entered);
        Assert.Equal(2, outcome.TicketsIssued);
        Assert.Equal("DeadlineOrCancellation", outcome.StopReason);
    }

    [Fact]
    public async Task UnexpectedExceptionStillWaitsForOtherAdmittedOperation()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var throwing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = false;
        var batch = ConcurrentBatchRunner.RunAsync(2, 2, async (_, ticket, _, _) =>
        {
            if (ticket == 1)
            {
                await entered.Task;
                throwing.SetResult();
                throw new IOException();
            }
            entered.SetResult();
            await release.Task;
            finished = true;
            return true;
        }, CancellationToken.None);
        await throwing.Task;
        Assert.False(batch.IsCompleted);
        Assert.False(finished);
        release.SetResult();
        var outcome = await batch;
        Assert.True(finished);
        Assert.Equal("IOException", outcome.FailureType);
        Assert.Equal("NonSuccessfulOrUnexpectedResult", outcome.StopReason);
    }

    [Fact]
    public async Task AlreadyCancelledBatchStartsNoAttempts()
    {
        using var ct = new CancellationTokenSource();
        ct.Cancel();
        var outcome = await ConcurrentBatchRunner.RunAsync(8, 500,
            (_, _, _, _) => throw new InvalidOperationException(), ct.Token);
        Assert.Equal(0, outcome.TicketsIssued);
        Assert.Null(outcome.FailureType);
    }
}
