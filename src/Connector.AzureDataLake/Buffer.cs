using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;

namespace CluedIn.Connector.AzureDataLake
{
    internal class PartitionedBuffer<TItem> : IDisposable
    {
        private readonly int _maxSize;
        private readonly int _timeout;
        private readonly Action<TItem[]> _bulkAction;
        private readonly Dictionary<object, Buffer<TItem>> _buffers;

        public PartitionedBuffer(int maxSize, int timeout, Action<TItem[]> bulkAction)
        {
            _maxSize = maxSize;
            _timeout = timeout;
            _bulkAction = bulkAction;
            _buffers = new Dictionary<object, Buffer<TItem>>();
        }

        public async Task Add(TItem item, object partition)
        {
            Buffer<TItem> buffer;
            lock (_buffers)
            {
                if (!_buffers.TryGetValue(partition, out buffer))
                {
                    _buffers.Add(partition, buffer = new Buffer<TItem>(_maxSize, _timeout, _bulkAction));
                }
            }

            await buffer.Add(item);
        }

        public void Dispose()
        {
            foreach (var buffer in _buffers)
            {
                buffer.Value.Dispose();
            }
        }

        public async Task Flush()
        {
            foreach (var buffer in _buffers)
            {
                await buffer.Value.Flush();
            }
        }
    }

    internal class Buffer<T> : IDisposable
    {
        private readonly int _initialMaxSize;

        private int _maxSize;

        private readonly int _timeout;

        private readonly Action<T[]> _bulkAction;

        private readonly T[] _items;

        private readonly SemaphoreSlim[] _addingTaskSemaphores;

        private readonly SemaphoreSlim _addingSemaphore;

        private readonly SemaphoreSlim _addingCompleteSemaphore;

        private int _currentCount;

        private Exception _bulkException;

        private Task _idleTask;

        private DateTime _lastAdded;

        private readonly CancellationTokenSource _idleCancellationTokenSource;

        private readonly int _maxSizeAutoDetectionSampleSize = 10;

        private readonly List<(int itemCount, DateTime flushedAt)> _idleFlushHistory = new List<(int, DateTime)>();

        private DateTime _autoMaxSizeSetAt;

        public Buffer(int maxSize, int timeout, Action<T[]> bulkAction)
        {
            _initialMaxSize = maxSize;
            _maxSize = maxSize;
            _timeout = timeout;
            _bulkAction = bulkAction;
            _items = new T[maxSize];
            _addingTaskSemaphores = new SemaphoreSlim[maxSize - 1];
            _addingSemaphore = new SemaphoreSlim(maxSize, maxSize);
            _addingCompleteSemaphore = new SemaphoreSlim(0, maxSize - 1);
            _idleCancellationTokenSource = new CancellationTokenSource();

            for (int i = 0; i < _addingTaskSemaphores.Length; i++)
            {
                _addingTaskSemaphores[i] = new SemaphoreSlim(0, 1);
            }
        }

        ~Buffer()
        {
            Dispose();
        }

        public void Dispose()
        {
            Flush().Wait();
        }

        private async Task Idle()
        {
            while (true)
            {
                await Task.Delay(100, _idleCancellationTokenSource.Token);

                if (!_idleCancellationTokenSource.IsCancellationRequested && DateTime.Now.Subtract(_lastAdded).TotalMilliseconds < _timeout)
                    continue;

                int acquiredCount = 0;

                try
                {
                    while (await _addingSemaphore.WaitAsync(10))  // stop other tasks from adding
                    {
                        acquiredCount++;
                    }

                    if (_currentCount == 0)
                        return;

                    await Flush(true);

                    return;
                }
                finally
                {
                    _idleTask = null;

                    for (int i = 0; i < acquiredCount; i++)
                    {
                        _addingSemaphore.Release();
                    }
                }
            }
        }

        public async Task Add(T item)
        {
            await _addingSemaphore.WaitAsync();

            int i;
            bool maxSizeReached;

            lock (this)
            {
                _idleTask ??= Idle();

                i = _currentCount;

                _currentCount++;

                _items[i] = item;

                _lastAdded = DateTime.Now;

                maxSizeReached = _currentCount == _maxSize;
            }

            if (maxSizeReached)
            {
                await Flush(false);
            }
            else
            {
                await _addingTaskSemaphores[i].WaitAsync();

                var e = _bulkException;

                _addingTaskSemaphores[i].Release();
                _addingCompleteSemaphore.Release();

                if (e != null)
                {
                    throw new AggregateException(e);
                }
            }
        }

        public async Task Flush()
        {
            var t = _idleTask;
            if (t != null)
            {
                _idleCancellationTokenSource.Cancel();

                await t;
            }
        }

        private async Task Flush(bool idle)
        {
            try
            {
                if (idle)
                {
                    _idleFlushHistory.Add((_currentCount, DateTime.Now));

                    if (_idleFlushHistory.Count > _maxSizeAutoDetectionSampleSize)
                    {
                        _idleFlushHistory.RemoveAt(0);
                    }

                    if (_idleFlushHistory.Count == _maxSizeAutoDetectionSampleSize &&
                        _idleFlushHistory.All(h => h.itemCount == _idleFlushHistory[0].itemCount))
                    {
                        var allIdleFlushesExecutedInMinimumTime =
                            _idleFlushHistory.Last().flushedAt.Subtract(_idleFlushHistory.First().flushedAt)
                                .TotalMilliseconds <
                            _idleFlushHistory.Count * _timeout +
                            100; // +100 as there will be random millisecond delays

                        if (allIdleFlushesExecutedInMinimumTime)
                        {
                            _maxSize = _idleFlushHistory[0].itemCount;
                            _autoMaxSizeSetAt = DateTime.Now;
                        }
                    }
                }

                // periodically reset maxSize back to initialMaxSize just in case the auto detection incorrectly reduced it
                if (_maxSize != _initialMaxSize && DateTime.Now.Subtract(_autoMaxSizeSetAt).TotalMinutes > 10)
                {
                    _maxSize = _initialMaxSize;
                }

                _bulkAction(_items.Take(_currentCount).ToArray());
            }
            catch (Exception ex)
            {
                _bulkException = ex;
            }
            finally
            {
                var c = idle ? _currentCount : _currentCount - 1;

                for (int idx = 0; idx < c; idx++)
                {
                    _addingTaskSemaphores[idx].Release();
                }

                for (int idx = 0; idx < c; idx++)
                {
                    await _addingCompleteSemaphore.WaitAsync();
                }

                for (int idx = 0; idx < c; idx++)
                {
                    await _addingTaskSemaphores[idx].WaitAsync();
                }

                _bulkException = null;
                var count = _currentCount;
                _currentCount = 0;
                _addingSemaphore.Release(count);
            }
        }
    }
}
