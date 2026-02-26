using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common.Buffers;

internal sealed class ChannelBasedBufferOptions
{
    /// <summary>Maximum number of items per batch.</summary>
    public int MaxBatchSize { get; init; } = 500;

    /// <summary>If no new items arrive for this duration, flush whatever is buffered.</summary>
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Bounded channel capacity (backpressure). If null, channel is unbounded.
    /// Recommendation: set to a multiple of MaxBatchSize, e.g. 5 * MaxBatchSize.
    /// </summary>
    public int? BoundedCapacity { get; init; } = null;

    /// <summary>
    /// Max number of concurrent flushes. Set to 1 if bulk sink is not thread-safe.
    /// </summary>
    public int MaxConcurrentFlushes { get; init; } = 1;

    /// <summary>
    /// Optional cap on number of batches that can be in-flight (queued or running).
    /// Prevents unlimited memory growth if bulk action is slower than producers.
    /// </summary>
    public int? MaxPendingFlushBatches { get; init; } = null;

    /// <summary>Enable automatic max-batch-size reduction heuristic on repeated idle flushes.</summary>
    public bool EnableAutoTuneMaxBatchSize { get; init; } = true;

    /// <summary>Number of idle flush samples to detect a repeated pattern.</summary>
    public int AutoTuneSampleSize { get; init; } = 3;

    /// <summary>Reset max batch size back to initial after this period.</summary>
    public TimeSpan AutoTuneResetAfter { get; init; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Heuristic: estimated ms needed to "refill" each item between idle flushes.
    /// Matches your original intent (20ms/item).
    /// </summary>
    public int AutoTunePopulateMsPerItem { get; init; } = 20;

    public void Validate()
    {
        if (MaxBatchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxBatchSize));
        if (IdleTimeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(IdleTimeout));
        if (MaxConcurrentFlushes <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxConcurrentFlushes));
        if (BoundedCapacity is { } cap && cap <= 0)
            throw new ArgumentOutOfRangeException(nameof(BoundedCapacity));
        if (MaxPendingFlushBatches is { } p && p <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxPendingFlushBatches));
        if (AutoTuneSampleSize <= 1)
            throw new ArgumentOutOfRangeException(nameof(AutoTuneSampleSize));
        if (AutoTuneResetAfter <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(AutoTuneResetAfter));
        if (AutoTunePopulateMsPerItem < 0)
            throw new ArgumentOutOfRangeException(nameof(AutoTunePopulateMsPerItem));
    }
}

internal interface IBufferMetrics
{
    void OnEnqueued(long totalEnqueued);
    void OnBackpressureWait(TimeSpan waited);
    void OnBatchScheduled(int batchSize, int maxBatchSize, int inFlightBatches);
    void OnBatchFlushed(int batchSize, TimeSpan duration, bool idleFlush, int maxBatchSize);
    void OnFlushFailed(int batchSize, TimeSpan duration, Exception ex, bool idleFlush);
}

internal sealed class NullBufferMetrics : IBufferMetrics
{
    public static readonly IBufferMetrics Instance = new NullBufferMetrics();
    private NullBufferMetrics() { }

    public void OnEnqueued(long totalEnqueued) { }
    public void OnBackpressureWait(TimeSpan waited) { }
    public void OnBatchScheduled(int batchSize, int maxBatchSize, int inFlightBatches) { }
    public void OnBatchFlushed(int batchSize, TimeSpan duration, bool idleFlush, int maxBatchSize) { }
    public void OnFlushFailed(int batchSize, TimeSpan duration, Exception ex, bool idleFlush) { }
}
/// <summary>
/// - AddAsync completes only after its item is flushed (or faults if flush fails).
/// - FlushAsync forces flushing of current buffered items and awaits completion.
/// - BoundedCapacity provides backpressure. MaxPendingFlushBatches prevents runaway in-flight flush growth.
/// - MaxConcurrentFlushes allows parallel flush if the sink supports it.
/// </summary>
internal sealed class ChannelBasedBuffer<T> : IDisposable, IAsyncDisposable, IBuffer<T>
{
    private readonly ChannelBasedBufferOptions _options;
    private readonly IBufferMetrics _metrics;
    private readonly Func<T[], CancellationToken, Task> _bulkActionAsync;
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;
    private readonly Channel<Message> _channel;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _consumerTask;

    private readonly SemaphoreSlim _flushConcurrency;
    private readonly SemaphoreSlim? _pendingFlushBatches;

    private volatile bool _disposed;

    private int _maxBatchSize;
    private readonly int _initialMaxBatchSize;

    private long _totalEnqueued;

    // ---- Auto-tune state ----
    private readonly object _tuneLock = new();
    private readonly List<(int itemCount, DateTime flushedAtUtc, TimeSpan flushDuration)> _idleFlushHistory;
    private DateTime _autoMaxSizeSetAtUtc;

    // ---- Message struct (low alloc) ----
    private readonly struct Message
    {
        public readonly MessageKind Kind;
        public readonly T Item;
        public readonly TaskCompletionSource? AddCompletion;
        public readonly TaskCompletionSource? FlushCompletion;

        private Message(MessageKind kind, T item, TaskCompletionSource? addCompletion, TaskCompletionSource? flushCompletion)
        {
            Kind = kind;
            Item = item;
            AddCompletion = addCompletion;
            FlushCompletion = flushCompletion;
        }

        public static Message Add(T item, TaskCompletionSource completion) =>
            new Message(MessageKind.Add, item, completion, null);

        public static Message Flush(TaskCompletionSource completion) =>
            new Message(MessageKind.Flush, default!, null, completion);
    }

    private enum MessageKind : byte
    {
        Add = 1,
        Flush = 2
    }

    private sealed class AddEntry
    {
        public T Item = default!;
        public TaskCompletionSource Completion = default!;
    }

    public ChannelBasedBuffer(
        ChannelBasedBufferOptions options,
        Func<T[], CancellationToken, Task> bulkActionAsync,
        IDateTimeOffsetProvider dateTimeOffsetProvider,
        IBufferMetrics? metrics = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _options.Validate();

        _bulkActionAsync = bulkActionAsync ?? throw new ArgumentNullException(nameof(bulkActionAsync));
        _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
        _metrics = metrics ?? NullBufferMetrics.Instance;

        _initialMaxBatchSize = _options.MaxBatchSize;
        _maxBatchSize = _options.MaxBatchSize;

        _idleFlushHistory = new List<(int, DateTime, TimeSpan)>(_options.AutoTuneSampleSize);

        _flushConcurrency = new SemaphoreSlim(_options.MaxConcurrentFlushes, _options.MaxConcurrentFlushes);

        if (_options.MaxPendingFlushBatches is { } pending)
            _pendingFlushBatches = new SemaphoreSlim(pending, pending);

        _channel = CreateChannel(_options);
        _consumerTask = Task.Run(ConsumeAsync);
    }

    private static Channel<Message> CreateChannel(ChannelBasedBufferOptions options)
    {
        if (options.BoundedCapacity is { } cap)
        {
            return Channel.CreateBounded<Message>(new BoundedChannelOptions(cap)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
        }

        return Channel.CreateUnbounded<Message>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
    }

    Task IBuffer<T>.Add(T item)
    {
        return AddAsync(item, default);
    }

    Task IBuffer<T>.Flush()
    {
        return FlushAsync(default);
    }

    /// <summary>Add item and complete only when that item is flushed.</summary>
    public async Task AddAsync(T item, CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(Buffer<T>));

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var msg = Message.Add(item, tcs);

        // Track producer-side backpressure time (bounded channel only).
        var sw = _options.BoundedCapacity is null ? null : Stopwatch.StartNew();

        // Best-effort cancellation: if cancelled before flush, caller observes cancel.
        using var reg = cancellationToken.CanBeCanceled
            ? cancellationToken.Register(static s => ((TaskCompletionSource)s!).TrySetCanceled(), tcs)
            : default;

        await _channel.Writer.WriteAsync(msg, cancellationToken).ConfigureAwait(false);

        if (sw is not null)
        {
            sw.Stop();
            if (sw.Elapsed > TimeSpan.Zero)
                _metrics.OnBackpressureWait(sw.Elapsed);
        }

        var total = Interlocked.Increment(ref _totalEnqueued);
        _metrics.OnEnqueued(total);

        await tcs.Task.ConfigureAwait(false);
    }

    /// <summary>Force flush now and wait until current buffered items are flushed.</summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(Buffer<T>));

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var msg = Message.Flush(tcs);

        var sw = _options.BoundedCapacity is null ? null : Stopwatch.StartNew();
        await _channel.Writer.WriteAsync(msg, cancellationToken).ConfigureAwait(false);

        if (sw is not null)
        {
            sw.Stop();
            if (sw.Elapsed > TimeSpan.Zero)
                _metrics.OnBackpressureWait(sw.Elapsed);
        }

        await tcs.Task.ConfigureAwait(false);
    }

    private async Task ConsumeAsync()
    {
        var reader = _channel.Reader;

        // Reused list to build batches on consumer thread
        var batch = new List<AddEntry>(_initialMaxBatchSize);

        DateTime lastItemAtUtc = DateTime.UtcNow;

        try
        {
            while (true)
            {
                // Wait until there is at least one message, or channel completes, or cancel
                var elapsed = DateTime.UtcNow - lastItemAtUtc;
                var timeToWait = GetTimeToWait(elapsed);
                var timeOutTask = Task.Delay(timeToWait);
                var readValueTask = reader.WaitToReadAsync(_cts.Token);
                var readTask = readValueTask.AsTask();
                var completedTask = await Task.WhenAny(readTask, timeOutTask);

                var isTimedOut = completedTask == timeOutTask;
                if (!isTimedOut && !await readTask)
                {
                    break;
                }

                // Drain currently available messages
                while (!isTimedOut && reader.TryRead(out var msg))
                {
                    if (msg.Kind == MessageKind.Add)
                    {
                        var entry = new AddEntry();
                        entry.Item = msg.Item;
                        entry.Completion = msg.AddCompletion!;

                        batch.Add(entry);
                        lastItemAtUtc = DateTime.UtcNow;

                        if (batch.Count >= _maxBatchSize)
                        {
                            _ = ScheduleFlush(batch, idleFlush: false);
                            batch = new List<AddEntry>(_maxBatchSize);
                            lastItemAtUtc = DateTime.UtcNow;
                        }
                    }
                    else // Flush
                    {
                        // Flush any buffered items before completing FlushAsync
                        if (batch.Count > 0)
                        {
                            var flushTask = ScheduleFlush(batch, idleFlush: false);
                            batch = new List<AddEntry>(_maxBatchSize);
                            await flushTask.ConfigureAwait(false); // FlushAsync must wait for actual flush
                        }

                        msg.FlushCompletion!.TrySetResult();
                    }
                }

                // Idle timeout flush: if we've buffered items and time elapsed, flush them.
                if (batch.Count > 0 && _options.IdleTimeout > TimeSpan.Zero && isTimedOut)
                {
                    _ = ScheduleFlush(batch, idleFlush: true);
                    batch = new List<AddEntry>(_maxBatchSize);
                    lastItemAtUtc = DateTime.UtcNow;
                }

                // If channel is completed and we still have buffered items, flush them before exiting.
                if (reader.Completion.IsCompleted)
                {
                    if (batch.Count > 0)
                    {
                        // best-effort final flush; await to ensure AddAsync completions are resolved.
                        var final = ScheduleFlush(batch, idleFlush: false);
                        batch = new List<AddEntry>(_maxBatchSize);
                        await final.ConfigureAwait(false);
                    }

                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // normal on dispose
        }
        catch (Exception ex)
        {
            // normal on dispose
        }
        finally
        {
            // Resolve any leftovers if we exit unexpectedly
            if (batch.Count > 0)
            {
                try
                {
                    var final = ScheduleFlush(batch, idleFlush: false);
                    await final.ConfigureAwait(false);
                }
                catch
                {
                    // individual AddAsync callers are completed by FlushWorker
                }
            }
        }

        TimeSpan GetTimeToWait(TimeSpan elapsed)
        {
            bool isTimeOutEnabled = _options.IdleTimeout > TimeSpan.Zero;
            if (!isTimeOutEnabled)
            {
                return Timeout.InfiniteTimeSpan;
            }

            return elapsed > _options.IdleTimeout ? TimeSpan.Zero : _options.IdleTimeout.Subtract(elapsed);
        }
    }

    private Task ScheduleFlush(List<AddEntry> batch, bool idleFlush)
    {
        // Backpressure on "pending flush batches" if configured.
        // This prevents infinite in-flight tasks if bulk is slow.
        if (_pendingFlushBatches is not null)
        {
            // Wait synchronously in consumer context is OK because consumer is already single-threaded.
            // But use WaitAsync to avoid blocking thread pool.
            return ScheduleFlushWithPendingGateAsync(batch, idleFlush);
        }

        return StartFlushWorkerAsync(batch, idleFlush);

        async Task ScheduleFlushWithPendingGateAsync(List<AddEntry> b, bool idle)
        {
            await _pendingFlushBatches!.WaitAsync(_cts.Token).ConfigureAwait(false);
            try
            {
                await StartFlushWorkerAsync(b, idle).ConfigureAwait(false);
            }
            finally
            {
                _pendingFlushBatches.Release();
            }
        }
    }

    private async Task StartFlushWorkerAsync(List<AddEntry> batch, bool idleFlush)
    {
        // Snapshot into array for the bulk action
        var count = batch.Count;
        if (count == 0)
            return;

        // Copy items
        var items = new T[count];
        for (int i = 0; i < count; i++)
            items[i] = batch[i].Item;

        // Record metrics: scheduled
        _metrics.OnBatchScheduled(count, _maxBatchSize, _pendingFlushBatches is null ? -1 : -1);

        await _flushConcurrency.WaitAsync(_cts.Token).ConfigureAwait(false);
        var started = DateTime.UtcNow;
        var sw = Stopwatch.StartNew();

        Exception? error = null;
        try
        {
            await _bulkActionAsync(items, _cts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            error = ex;
        }
        finally
        {
            sw.Stop();
            _flushConcurrency.Release();
        }

        // Complete AddAsync callers
        if (error is null)
        {
            for (int i = 0; i < count; i++)
                batch[i].Completion.TrySetResult();

            _metrics.OnBatchFlushed(count, sw.Elapsed, idleFlush, _maxBatchSize);
        }
        else
        {
            for (int i = 0; i < count; i++)
                batch[i].Completion.TrySetException(error);

            _metrics.OnFlushFailed(count, sw.Elapsed, error, idleFlush);
        }

        // Auto-tune max batch size based on repeated idle flush patterns
        if (_options.EnableAutoTuneMaxBatchSize)
            AutoTuneMaxBatchSize(idleFlush, count, started, sw.Elapsed);

        // Return entries to pool (reduce allocations)
        for (int i = 0; i < count; i++)
        {
            batch[i].Item = default!;
            batch[i].Completion = default!;
        }
    }

    private void AutoTuneMaxBatchSize(bool idleFlush, int flushedCount, DateTime flushedAtUtc, TimeSpan flushDuration)
    {
        lock (_tuneLock)
        {
            if (idleFlush)
            {
                _idleFlushHistory.Add((flushedCount, flushedAtUtc, flushDuration));
                if (_idleFlushHistory.Count > _options.AutoTuneSampleSize)
                    _idleFlushHistory.RemoveAt(0);

                if (_idleFlushHistory.Count == _options.AutoTuneSampleSize)
                {
                    var firstCount = _idleFlushHistory[0].itemCount;
                    bool sameCount = true;
                    for (int i = 1; i < _idleFlushHistory.Count; i++)
                    {
                        if (_idleFlushHistory[i].itemCount != firstCount)
                        {
                            sameCount = false;
                            break;
                        }
                    }

                    if (sameCount)
                    {
                        var spanMs =
                            (_idleFlushHistory[^1].flushedAtUtc - _idleFlushHistory[0].flushedAtUtc)
                            .TotalMilliseconds;

                        var expectedMinMs =
                            (_idleFlushHistory.Count - 1) * _options.IdleTimeout.TotalMilliseconds +
                            SumDurationsMs(_idleFlushHistory, _idleFlushHistory.Count - 1) +
                            firstCount * _options.AutoTunePopulateMsPerItem;

                        if (spanMs < expectedMinMs)
                        {
                            _maxBatchSize = Math.Max(1, firstCount);
                            _autoMaxSizeSetAtUtc = DateTime.UtcNow;
                        }
                    }
                }
            }

            if (_maxBatchSize != _initialMaxBatchSize &&
                (DateTime.UtcNow - _autoMaxSizeSetAtUtc) > _options.AutoTuneResetAfter)
            {
                _maxBatchSize = _initialMaxBatchSize;
            }
        }

        static double SumDurationsMs(List<(int itemCount, DateTime flushedAtUtc, TimeSpan flushDuration)> list, int take)
        {
            double sum = 0;
            for (int i = 0; i < take && i < list.Count; i++)
                sum += list[i].flushDuration.TotalMilliseconds;
            return sum;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        _channel.Writer.TryComplete();
        _cts.Cancel();

        try
        {
            await _consumerTask.ConfigureAwait(false);
        }
        finally
        {
            _cts.Dispose();
            _flushConcurrency.Dispose();
            _pendingFlushBatches?.Dispose();
        }
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}

internal sealed class ConsoleBufferMetrics : IBufferMetrics
{
    private long _batches;
    private long _items;
    private long _failures;

    public void OnEnqueued(long totalEnqueued) { }

    public void OnBackpressureWait(TimeSpan waited)
    {
        if (waited > TimeSpan.FromMilliseconds(5))
            Console.WriteLine($"Backpressure wait: {waited.TotalMilliseconds:N0}ms");
    }

    public void OnBatchScheduled(int batchSize, int maxBatchSize, int inFlightBatches) { }

    public void OnBatchFlushed(int batchSize, TimeSpan duration, bool idleFlush, int maxBatchSize)
    {
        Interlocked.Increment(ref _batches);
        Interlocked.Add(ref _items, batchSize);

        Console.WriteLine($"Flushed {batchSize} items in {duration.TotalMilliseconds:N0}ms (idle={idleFlush}) maxBatch={maxBatchSize}");
    }

    public void OnFlushFailed(int batchSize, TimeSpan duration, Exception ex, bool idleFlush)
    {
        Interlocked.Increment(ref _failures);
        Console.WriteLine($"FLUSH FAILED for {batchSize} items after {duration.TotalMilliseconds:N0}ms (idle={idleFlush}): {ex.Message}");
    }
}
