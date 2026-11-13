using System.Threading.Channels;

namespace Ordering.Application.Tests;

// Only one-shot timers are needed by Task.Delay. Tests release a registered timer explicitly,
// so races are synchronized by task/timer boundaries, never by sleeping or polling wall time.
internal sealed class ControlledTimeProvider : TimeProvider
{
    private readonly object _gate = new();
    private readonly Dictionary<TimeSpan, Channel<ControlledTimer>> _scheduled = [];
    private readonly List<ControlledTimer> _timers = [];

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new ControlledTimer(callback, state);
        Assert.Equal(Timeout.InfiniteTimeSpan, period);
        lock (_gate)
        {
            _timers.Add(timer);
            Queue(dueTime).Writer.TryWrite(timer);
        }
        return timer;
    }

    public async Task<ControlledTimer> NextTimerAsync(TimeSpan dueTime)
    {
        Channel<ControlledTimer> queue;
        lock (_gate) queue = Queue(dueTime);
        while (true)
        {
            var timer = await queue.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            if (timer.Active) return timer;
        }
    }

    public int ActiveTimerCount
    {
        get { lock (_gate) return _timers.Count(timer => timer.Active); }
    }

    private Channel<ControlledTimer> Queue(TimeSpan duration)
    {
        if (!_scheduled.TryGetValue(duration, out var queue))
            _scheduled[duration] = queue = Channel.CreateUnbounded<ControlledTimer>();
        return queue;
    }

    internal sealed class ControlledTimer(TimerCallback callback, object? state) : ITimer
    {
        private int _active = 1;
        public bool Active => Volatile.Read(ref _active) == 1;
        public void Fire()
        {
            Assert.Equal(1, Interlocked.Exchange(ref _active, 0));
            callback(state);
        }

        public bool Change(TimeSpan dueTime, TimeSpan period) => throw new NotSupportedException();
        public void Dispose() => Interlocked.Exchange(ref _active, 0);
        public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }
    }
}
