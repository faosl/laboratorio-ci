using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Ordering.Application.Outbox;
using Ordering.Application.Tests;

namespace Ordering.Hosting.Tests;

public sealed class OutboxBackgroundServiceTests
{
    [Fact]
    public async Task StartsAutomaticallyPollsAndCancelsWaitWithoutAnotherAttempt()
    {
        var f = new Fixture();
        using var worker = f.Create();
        await worker.StartAsync(default);
        (await f.Next()).Return(OutboxRelayStatus.NoEligibleWork);
        var timer = await f.Time.NextTimerAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, f.Calls);
        timer.Fire();
        (await f.Next()).Return(OutboxRelayStatus.NoEligibleWork);
        await f.Time.NextTimerAsync(TimeSpan.FromSeconds(5));
        await worker.StopAsync(default).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(2, f.Calls);
        Assert.Equal(2, f.Disposals);
        Assert.Equal(0, f.Time.ActiveTimerCount);
    }

    [Theory]
    [InlineData(OutboxRelayStatus.Published)]
    [InlineData(OutboxRelayStatus.LeaseLost)]
    public async Task ImmediateResultsHaveBoundedSequences(OutboxRelayStatus status)
    {
        var f = new Fixture();
        using var worker = f.Create(new() { MaximumImmediateResults = 3 });
        await worker.StartAsync(default);
        for (var i = 0; i < 3; i++) (await f.Next()).Return(status);
        var timer = await f.Time.NextTimerAsync(TimeSpan.FromMilliseconds(100));
        Assert.Equal(3, f.Calls);
        Assert.Equal(3, f.Disposals);
        timer.Fire();
        await Stop(worker, await f.Next());
    }

    [Theory]
    [InlineData(OutboxRelayStatus.RetryScheduled, 7)]
    [InlineData(OutboxRelayStatus.Cancelled, 7)]
    [InlineData(OutboxRelayStatus.AcquisitionUnknown, 9)]
    [InlineData(OutboxRelayStatus.CompletionUnknown, 9)]
    [InlineData(OutboxRelayStatus.RetryUnknown, 9)]
    public async Task FailureAndUncertaintyUseDedicatedDelays(OutboxRelayStatus status, int seconds)
    {
        var f = new Fixture();
        using var worker = f.Create(new()
        {
            PublicationFailureDelay = TimeSpan.FromSeconds(7), UnknownOutcomeDelay = TimeSpan.FromSeconds(9)
        });
        await worker.StartAsync(default);
        (await f.Next()).Return(status);
        var timer = await f.Time.NextTimerAsync(TimeSpan.FromSeconds(seconds));
        Assert.Contains(f.Log.Entries, entry => entry.Message.Contains(status.ToString()));
        Assert.Equal(1, f.Calls);
        timer.Fire();
        await Stop(worker, await f.Next());
    }

    [Theory]
    [InlineData(OutboxRelayStatus.Published)]
    [InlineData(OutboxRelayStatus.NoEligibleWork)]
    public async Task BackoffGrowsCapsSurvivesIntermediateResultsAndResetsOnRecovery(OutboxRelayStatus recovery)
    {
        var f = new Fixture();
        using var worker = f.Create();
        await worker.StartAsync(default);
        foreach (var seconds in new[] { 2, 4, 8, 16, 30, 30 })
        {
            (await f.Next()).Return(OutboxRelayStatus.OperationalFailure);
            (await f.Time.NextTimerAsync(TimeSpan.FromSeconds(seconds))).Fire();
        }
        foreach (var intermediate in new[] { OutboxRelayStatus.AcquisitionUnknown, OutboxRelayStatus.CompletionUnknown,
            OutboxRelayStatus.RetryUnknown, OutboxRelayStatus.RetryScheduled, OutboxRelayStatus.Cancelled, OutboxRelayStatus.LeaseLost })
        {
            (await f.Next()).Return(intermediate);
            if (intermediate != OutboxRelayStatus.LeaseLost)
                (await f.Time.NextTimerAsync(TimeSpan.FromSeconds(5))).Fire();
            (await f.Next()).Return(OutboxRelayStatus.OperationalFailure);
            (await f.Time.NextTimerAsync(TimeSpan.FromSeconds(30))).Fire();
        }
        (await f.Next()).Return(recovery);
        if (recovery == OutboxRelayStatus.NoEligibleWork)
            (await f.Time.NextTimerAsync(TimeSpan.FromSeconds(5))).Fire();
        (await f.Next()).Return(OutboxRelayStatus.OperationalFailure);
        (await f.Time.NextTimerAsync(TimeSpan.FromSeconds(2))).Fire();
        await Stop(worker, await f.Next());
    }

    [Fact]
    public async Task UnsupportedEventStopsAcquisitionsAndLogsCriticalEvidence()
    {
        var f = new Fixture();
        using var worker = f.Create();
        await worker.StartAsync(default);
        (await f.Next()).Return(OutboxRelayStatus.UnsupportedEvent);
        await worker.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(1, f.Calls);
        Assert.Equal(1, f.Disposals);
        Assert.Equal(0, f.Time.ActiveTimerCount);
        Assert.Contains(f.Log.Entries, e => e.Level == LogLevel.Critical && e.Message.Contains("intervention"));
    }

    [Fact]
    public async Task PendingAttemptRetainsScopeAfterCancellationUntilTaskActuallyCompletes()
    {
        var f = new Fixture();
        using var worker = f.Create();
        await worker.StartAsync(default);
        var call = await f.Next();
        var stopping = worker.StopAsync(default);
        Assert.True(call.Token.IsCancellationRequested);
        Assert.False(stopping.IsCompleted);
        Assert.Equal(0, f.Disposals);
        Assert.Equal(1, f.Calls);
        call.Return(OutboxRelayStatus.Cancelled);
        await stopping.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(worker.ShutdownState.OperationsCompleted);
        Assert.True(worker.ShutdownState.ScopesDisposed);
    }

    [Fact]
    public void InvalidOptionsAreRejectedBeforeAnyAttempt()
    {
        var f = new Fixture();
        foreach (var delay in new[] { TimeSpan.Zero, TimeSpan.FromTicks(-1), TimeSpan.MaxValue, TimeSpan.FromTicks(1) })
        foreach (var option in new[]
        {
            new OutboxWorkerOptions { PollingDelay = delay }, new() { PublicationFailureDelay = delay },
            new() { UnknownOutcomeDelay = delay }, new() { ImmediateResultDelay = delay },
            new() { InitialOperationalFailureDelay = delay }, new() { MaximumOperationalFailureDelay = delay }
        })
            Assert.Throws<ArgumentOutOfRangeException>(() => f.Create(option));
        Assert.Throws<ArgumentOutOfRangeException>(() => f.Create(new() { MaximumImmediateResults = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => f.Create(new() { MaximumOperationalFailureDelay = TimeSpan.FromSeconds(1) }));
        Assert.Equal(0, f.Calls);
    }

    private static async Task Stop(OutboxBackgroundService worker, Call call)
    {
        var stop = worker.StopAsync(default);
        call.Return(OutboxRelayStatus.Cancelled);
        await stop.WaitAsync(TimeSpan.FromSeconds(10));
    }

    private sealed record Call(CancellationToken Token)
    {
        public TaskCompletionSource<OutboxRelayResult> Result { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Return(OutboxRelayStatus status) => Result.SetResult(new(status, OutboxRelayStage.Acquisition));
    }

    private sealed class Fixture
    {
        private readonly Channel<Call> _calls = Channel.CreateUnbounded<Call>();
        public ControlledTimeProvider Time { get; } = new();
        public RecordingLogger Log { get; } = new();
        public int Calls;
        public int Disposals;
        public Task<Call> Next() => _calls.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        public OutboxBackgroundService Create(OutboxWorkerOptions? options = null) => new(() =>
            ValueTask.FromResult(new OutboxWorkerExecution(async (_, ct) =>
            {
                Assert.Equal(Calls, Disposals);
                Interlocked.Increment(ref Calls);
                var call = new Call(ct);
                _calls.Writer.TryWrite(call);
                return await call.Result.Task;
            }, new Disposal(() => Interlocked.Increment(ref Disposals)))), options ?? new(), Time, Log);
    }

    private sealed class Disposal(Action dispose) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() { dispose(); return ValueTask.CompletedTask; }
    }

    private sealed class RecordingLogger : ILogger<OutboxBackgroundService>
    {
        public ConcurrentQueue<(LogLevel Level, string Message)> Entries { get; } = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Entries.Enqueue((logLevel, formatter(state, exception)));
    }
}
