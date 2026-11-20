#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CluedIn.Connector.FileStorage.Common.Connector;

internal record struct OperationResult<TResult>(TResult? Result, Exception? Exception);

internal class OperationBatcher<TRequest, TResult> : IAsyncDisposable, IDisposable
{
    private static readonly TimeSpan DefaultBatchTimeOut = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan WaitSlice = TimeSpan.FromMilliseconds(100);

    private readonly TimeProvider _timeProvider;
    private readonly Channel<BatchingRequest> _inputChannel;
    private readonly Channel<IList<BatchingRequest>> _outputChannel;
    private readonly int _batchSize;
    private readonly int _inputProcessingThreadsCount;
    private readonly int _outputProcessingThreadsCount;
    private readonly Func<IList<TRequest>, Task<IList<OperationResult<TResult>>>> _processOutputBatchFunc;
    private readonly TimeSpan _batchTimeout;
    private readonly bool _shouldCollectBatchSizeMetrics;
    private readonly int[][] batchSizeStats;
    private readonly List<Task> _batcherTasks;
    private readonly List<Task> _processBatchTasks;
    private readonly bool _shouldUseBatchTimeout;
    private int _remainingBatchers;
    private bool _disposed;

    public OperationBatcher(
        int batchSize,
        int inputProcessingThreadsCount,
        int outputProcessingThreadsCount,
        Func<IList<TRequest>, Task<IList<OperationResult<TResult>>>> processOutputBatchFunc,
        TimeSpan? batchTimeout = null,
        TimeProvider? timeProvider = null,
        bool shouldCollectBatchSizeMetrics = false)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _batchSize = batchSize;
        _inputProcessingThreadsCount = inputProcessingThreadsCount;
        _outputProcessingThreadsCount = outputProcessingThreadsCount;
        _batchTimeout = batchTimeout ?? DefaultBatchTimeOut;
        _shouldUseBatchTimeout = batchTimeout.HasValue && batchTimeout.Value > TimeSpan.Zero;
        _inputChannel = Channel.CreateUnbounded<BatchingRequest>(new UnboundedChannelOptions
        {
            SingleReader = _inputProcessingThreadsCount == 1,
            SingleWriter = false,
        });
        _outputChannel = Channel.CreateUnbounded<IList<BatchingRequest>>(new UnboundedChannelOptions
        {
            SingleReader = _outputProcessingThreadsCount == 1,
            SingleWriter = false,
        });
        _processOutputBatchFunc = processOutputBatchFunc;

        _shouldCollectBatchSizeMetrics = shouldCollectBatchSizeMetrics;
        if (_shouldCollectBatchSizeMetrics)
        {
            batchSizeStats = new int[_inputProcessingThreadsCount][];
            for (var i = 0; i < _inputProcessingThreadsCount; ++i)
            {
                batchSizeStats[i] = new int[batchSize];
            }
        }
        else
        {
            batchSizeStats = Array.Empty<int[]>();
        }

        _batcherTasks = new List<Task>(_inputProcessingThreadsCount);
        _processBatchTasks = new List<Task>(_outputProcessingThreadsCount);
        _remainingBatchers = _inputProcessingThreadsCount;

        for (int i = 0; i < _inputProcessingThreadsCount; i++)
        {
            var consumerId = i;
            var task = Task.Run(() => ConsumeInputAsync(consumerId, _inputChannel.Reader));
            _batcherTasks.Add(task);
        }

        for (int i = 0; i < _outputProcessingThreadsCount; i++)
        {
            var consumerId = i;
            var task = Task.Run(() => ConsumeOutputAsync(consumerId, _outputChannel.Reader));
            _processBatchTasks.Add(task);
        }
    }


    public async Task<TResult> QueueAsync(TRequest request)
    {
        // The callers (message consumers that acknowledge afterwards) continue on the thread pool, not one after the other on the thread that completes the batch.
        var tcs = new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        await _inputChannel.Writer.WriteAsync(new BatchingRequest(request, tcs));
        return await tcs.Task;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _inputChannel.Writer.Complete();
        await Task.WhenAll(_batcherTasks);
        await Task.WhenAll(_processBatchTasks);

        _disposed = true;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _inputChannel.Writer.Complete();
        Task.WaitAll(_batcherTasks.ToArray());
        Task.WaitAll(_processBatchTasks.ToArray());

        _disposed = true;
    }

    // Task.Delay(TimeSpan, TimeProvider) is .NET 8 and later. Microsoft.Bcl.TimeProvider has it for the older frameworks
    // (the CluedIn 4.x lines are net6.0) as an extension of TimeProvider.
    private Task DelayAsync(TimeSpan delay)
    {
#if NET8_0_OR_GREATER
        return Task.Delay(delay, _timeProvider);
#else
        return TimeProviderTaskExtensions.Delay(_timeProvider, delay);
#endif
    }

    public ICollection<ICollection<int>> GetBatchSizeStats()
    {
        if(!_shouldCollectBatchSizeMetrics)
        {
            return Array.Empty<ICollection<int>>();
        }

        return batchSizeStats;
    }

    private async Task ConsumeInputAsync(int consumerId, ChannelReader<BatchingRequest> reader)
    {
        await foreach (var batch in ReadBatchAsync(reader, _batchSize))
        {
            try
            {
                if (_shouldCollectBatchSizeMetrics)
                {
                    batchSizeStats[consumerId][batch.Count - 1]++;
                }
                await _outputChannel.Writer.WriteAsync(batch);
            }
            catch (Exception e)
            {
                foreach (var (_, tcs) in batch)
                {
                    tcs.SetException(e);
                }
            }
        }

        // Only the last chunker task to finish should complete the shared chunked-request
        // channel - if every chunker task called Complete() independently, every call after
        // the first would throw ChannelClosedException.
        if (Interlocked.Decrement(ref _remainingBatchers) == 0)
        {
            _outputChannel.Writer.Complete();
        }
    }

    private async Task ConsumeOutputAsync(int consumerId, ChannelReader<IList<BatchingRequest>> reader)
    {
        // TryRead (rather than ReadAsync) avoids a race between multiple concurrent writer
        // tasks: WaitToReadAsync can return true and then another task drains the last item and
        // completes the channel before this task calls ReadAsync, which would otherwise throw
        // ChannelClosedException. TryRead just reports false instead of throwing.
        while (await reader.WaitToReadAsync())
        {
            while (reader.TryRead(out var batch))
            {
                try
                {
                    var results = await _processOutputBatchFunc(batch.Select(e => e.Request).ToList());
                    var i = 0;
                    foreach (var (_, tcs) in batch)
                    {
                        var currentResult = results[i];
                        if (currentResult.Exception == null)
                        {
                            tcs.SetResult(currentResult.Result!);
                        }
                        else
                        {
                            tcs.SetException(currentResult.Exception!);
                        }
                        i++;
                    }
                }
                catch (Exception ex)
                {
                    foreach (var (_, tcs) in batch)
                    {
                        tcs.SetException(ex);
                    }
                }
            }
        }
    }

    private async IAsyncEnumerable<List<T>> ReadBatchAsync<T>(
            ChannelReader<T> reader,
            int chunkSize,
            CancellationToken cancellationToken = default)
    {
        while (await reader.WaitToReadAsync())
        {
            var batch = new List<T>(chunkSize);
            while (batch.Count < chunkSize && reader.TryRead(out var item))
            {
                batch.Add(item);
            }

            if (_shouldUseBatchTimeout && batch.Count < chunkSize)
            {
                // Wait for more items to arrive or timeout, polling in WaitSlice increments.
                // Unlike OperationBatcherChunkedWait, the remaining budget is recalculated from
                // _timeProvider.GetUtcNow() against a fixed deadline every iteration, rather than
                // decremented by the nominal slice duration - so timer overshoot on any one delay
                // doesn't push the total wait past the configured timeout.
                if (_batchTimeout <= WaitSlice)
                {
                    await DelayAsync(WaitSlice);
                    while (batch.Count < chunkSize && reader.TryRead(out var item))
                    {
                        batch.Add(item);
                    }
                }
                else
                {
                    var deadline = _timeProvider.GetUtcNow().Add(_batchTimeout);
                    while (batch.Count < chunkSize)
                    {
                        var remaining = deadline - _timeProvider.GetUtcNow();
                        if (remaining <= TimeSpan.Zero)
                        {
                            break;
                        }

                        var delay = remaining < WaitSlice ? remaining : WaitSlice;
                        await DelayAsync(delay);
                        while (batch.Count < chunkSize && reader.TryRead(out var item))
                        {
                            batch.Add(item);
                        }
                    }
                }
            }

            if (batch.Count > 0)
            {
                yield return batch;
            }
        }
    }

    private record BatchingRequest(TRequest Request, TaskCompletionSource<TResult> CompletionSource);
}
