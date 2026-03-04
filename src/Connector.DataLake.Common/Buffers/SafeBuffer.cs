using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace CluedIn.Connector.DataLake.Common.Buffers;

internal sealed class SafeBuffer<TItem, TResult> : IDisposable, IBuffer<TItem>
{
    private const int IdlePollingIntervalMs = 100;

    private readonly int _maxSize;
    private readonly int _timeoutMs;
    private readonly Func<TItem[], Task<TResult[]>> _bulkAction;
    private readonly object _gate = new();

    private BatchContext _currentBatch;
    private Task _idleTask;
    private int _idleGeneration;

    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly Task _shutdownTask;
    private readonly ObjectDisposedException _cachedOde;
    private volatile bool _isDisposed;

    private readonly ConcurrentDictionary<BatchContext, byte> _inFlightBatches = new();
    private readonly SemaphoreSlim _globalLimitSemaphore;
    private readonly SemaphoreSlim _admissionSemaphore;

    public SafeBuffer(int maxSize, int timeoutMs, Func<TItem[], Task<TResult[]>> bulkAction)
    {
        _maxSize = maxSize;
        _timeoutMs = timeoutMs;
        _bulkAction = bulkAction ?? throw new ArgumentNullException(nameof(bulkAction));

        // Global limit allows pipelining (2x capacity) while admission limits lock contention (1x).
        _globalLimitSemaphore = new SemaphoreSlim(maxSize * 2, maxSize * 2);
        _admissionSemaphore = new SemaphoreSlim(maxSize, maxSize);
        _currentBatch = new BatchContext(maxSize);

        _shutdownTask = Task.Delay(Timeout.Infinite, _shutdownCts.Token);
        _cachedOde = new ObjectDisposedException(nameof(SafeBuffer<TItem, TResult>));
    }

    private sealed class BatchContext
    {
        public readonly TItem[] Items;
        public readonly TResult[] Results;
        public readonly TaskCompletionSource<bool> Tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal int Count;
        internal bool IsClosed;
        internal long LastAddedTimestamp;
        private int _executingFlag;

        public BatchContext(int maxSize)
        {
            Items = new TItem[maxSize];
            Results = new TResult[maxSize];
            LastAddedTimestamp = Stopwatch.GetTimestamp();
        }

        public bool TryBeginExecute() => Interlocked.CompareExchange(ref _executingFlag, 1, 0) == 0;
    }

    Task IBuffer<TItem>.Add(TItem item)
    {
        return Add(item, default);
    }

    async Task IBuffer<TItem>.Flush()
    {
        BatchContext batchToFlush = null;
        var shouldStartExecute = false;
        lock (_gate)
        {
            if (_isDisposed || _currentBatch.Count == 0)
            {
                return;
            }

            batchToFlush = _currentBatch;
            batchToFlush.IsClosed = true;
            shouldStartExecute = batchToFlush.TryBeginExecute();
        }

        if (shouldStartExecute)
        {
            await ExecuteBatchCore(batchToFlush);
        }
    }

    public async Task<TResult> Add(TItem item, CancellationToken ct = default)
    {
        while (true)
        {
            if (_isDisposed)
                throw new ObjectDisposedException(nameof(SafeBuffer<TItem, TResult>));

            var globalTaken = false;
            var admissionTaken = false;

            try
            {
                // PHASE 1: ADMISSION (Cancellable)
                await _globalLimitSemaphore.WaitAsync(ct).ConfigureAwait(false);
                globalTaken = true;
                if (_isDisposed)
                    throw _cachedOde;

                await _admissionSemaphore.WaitAsync(ct).ConfigureAwait(false);
                admissionTaken = true;
                if (_isDisposed)
                    throw _cachedOde;

                BatchContext batchToWait;
                var myIndex = -1;
                var shouldStartExecute = false;

                lock (_gate)
                {
                    if (_isDisposed)
                    {
                        // Explicitly release before throwing to prevent permit orphans in rare race
                        SafeRelease(_admissionSemaphore, ref admissionTaken);
                        SafeRelease(_globalLimitSemaphore, ref globalTaken);
                        throw _cachedOde;
                    }

                    if (_currentBatch.IsClosed || _currentBatch.Count >= _maxSize)
                    {
                        batchToWait = _currentBatch;
                        if (_currentBatch.Count >= _maxSize)
                            _currentBatch.IsClosed = true;

                        if (batchToWait.Count > 0 && !batchToWait.Tcs.Task.IsCompleted)
                            shouldStartExecute = batchToWait.TryBeginExecute();
                    }
                    else
                    {
                        // Ensure an idle task is monitoring the current batch
                        if (_idleTask == null || _idleTask.IsCompleted)
                        {
                            var gen = ++_idleGeneration;
                            _idleTask = Idle(gen);
                        }

                        myIndex = _currentBatch.Count;
                        _currentBatch.Items[myIndex] = item;
                        _currentBatch.Count++;
                        _currentBatch.LastAddedTimestamp = Stopwatch.GetTimestamp();
                        batchToWait = _currentBatch;

                        if (_currentBatch.Count == _maxSize)
                        {
                            _currentBatch.IsClosed = true;
                            shouldStartExecute = batchToWait.TryBeginExecute();
                        }
                    }
                }

                SafeRelease(_admissionSemaphore, ref admissionTaken);

                if (shouldStartExecute)
                    _ = ExecuteBatchCore(batchToWait);

                // REJECTION PATH: Batch was full/closed, wait for its completion then retry Add
                if (myIndex == -1)
                {
                    SafeRelease(_globalLimitSemaphore, ref globalTaken);
                    try
                    {
                        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _shutdownCts.Token);
                        await batchToWait.Tcs.Task.WaitAsync(linked.Token).ConfigureAwait(false);
                        await batchToWait.Tcs.Task.ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (_shutdownCts.IsCancellationRequested && !ct.IsCancellationRequested)
                    {
                        if (batchToWait.Tcs.Task.IsCompleted)
                            await batchToWait.Tcs.Task.ConfigureAwait(false);
                        throw _cachedOde;
                    }
                    continue;
                }

                // PHASE 2: COMMITTED WAIT (Shutdown-only unblocking)
                var completion = await Task.WhenAny(batchToWait.Tcs.Task, _shutdownTask).ConfigureAwait(false);
                if (completion == _shutdownTask)
                {
                    if (batchToWait.Tcs.Task.IsCompleted)
                        await batchToWait.Tcs.Task.ConfigureAwait(false);
                    throw _cachedOde;
                }

                await batchToWait.Tcs.Task.ConfigureAwait(false);
                return batchToWait.Results[myIndex];
            }
            finally
            {
                SafeRelease(_admissionSemaphore, ref admissionTaken);
                SafeRelease(_globalLimitSemaphore, ref globalTaken);
            }
        }
    }

    private async Task ExecuteBatchCore(BatchContext batch)
    {
        _inFlightBatches.TryAdd(batch, 0);
        try
        {
            if (_shutdownCts.IsCancellationRequested)
            { batch.Tcs.TrySetException(_cachedOde); return; }

            TItem[] itemsToProcess;
            int count;
            lock (_gate)
            {
                if (_isDisposed)
                {
                    batch.Tcs.TrySetException(_cachedOde);
                    return;
                }

                batch.IsClosed = true;
                count = batch.Count;
                if (count == 0)
                { batch.Tcs.TrySetResult(true); return; }

                itemsToProcess = new TItem[count];
                Array.Copy(batch.Items, itemsToProcess, count);

                if (ReferenceEquals(_currentBatch, batch))
                    _currentBatch = new BatchContext(_maxSize);
            }

            var results = await _bulkAction(itemsToProcess).ConfigureAwait(false);

            if (results == null || results.Length != count)
                throw new InvalidOperationException($"Bulk action result mismatch. Expected {count}, got {results?.Length ?? 0}");

            Array.Copy(results, batch.Results, count);
            batch.Tcs.TrySetResult(true);
        }
        catch (Exception ex)
        {
            batch.Tcs.TrySetException(ex is OperationCanceledException && _shutdownCts.IsCancellationRequested ? _cachedOde : ex);
        }
        finally { _inFlightBatches.TryRemove(batch, out _); }
    }

    private async Task Idle(int generation)
    {
        try
        {
            while (!_isDisposed)
            {
                try
                { await Task.Delay(IdlePollingIntervalMs, _shutdownCts.Token).ConfigureAwait(false); }
                catch (TaskCanceledException) { break; }

                BatchContext batchToFlush = null;
                var shouldStartExecute = false;

                lock (_gate)
                {
                    if (_isDisposed)
                        return;
                    if (_currentBatch.Count > 0)
                    {
                        // Manual calculation: (Current - Last) / Frequency
                        var elapsedMs = (Stopwatch.GetTimestamp() - _currentBatch.LastAddedTimestamp)
                            / (double)Stopwatch.Frequency * 1000;

                        if (elapsedMs >= _timeoutMs)
                        {
                            batchToFlush = _currentBatch;
                            batchToFlush.IsClosed = true;
                            shouldStartExecute = batchToFlush.TryBeginExecute();
                        }
                    }
                    else
                        return;
                }
                if (shouldStartExecute)
                    _ = ExecuteBatchCore(batchToFlush);
            }
        }
        finally
        {
            lock (_gate)
            { if (_idleGeneration == generation) _idleTask = null; }
        }
    }

    private static void SafeRelease(SemaphoreSlim s, ref bool taken)
    {
        if (!taken)
            return;
        try
        { s.Release(); }
        catch (ObjectDisposedException) { }
        catch (SemaphoreFullException) { Debug.Fail("Semaphore double-release detected."); }
        finally { taken = false; }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_isDisposed)
                return;
            _isDisposed = true;
            _currentBatch.Tcs.TrySetException(_cachedOde);
        }

        foreach (var b in _inFlightBatches.Keys)
            b.Tcs.TrySetException(_cachedOde);
        _shutdownCts.Cancel();

        _globalLimitSemaphore.Dispose();
        _admissionSemaphore.Dispose();
        _shutdownCts.Dispose();
        GC.SuppressFinalize(this);
    }
}
