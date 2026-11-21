using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Connector;

using Microsoft.Extensions.Time.Testing;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Unit;

public class OperationBatcherTests
{
    [Fact]
    public async Task QueueAsync_ShouldReturnResultFromProcessBatchFunc()
    {
        // arrange
        await using var sut = new OperationBatcher<int, string>(
            batchSize: 1,
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: 1,
            processOutputBatchFunc: requests => Task.FromResult<IList<OperationResult<string>>>(
                requests.Select(r => new OperationResult<string>($"processed-{r}", null)).ToList()));

        // act
        var result = await sut.QueueAsync(42);

        // assert
        Assert.Equal("processed-42", result);
    }

    [Fact]
    public async Task QueueAsync_ShouldBatchRequestsTogether_WhenQueuedBeforeFirstChunkFlushes()
    {
        // arrange
        var receivedChunks = new ConcurrentBag<IList<int>>();

        await using var sut = new OperationBatcher<int, string>(
            batchSize: 3,
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: 1,
            batchTimeout: TimeSpan.FromMilliseconds(200),
            processOutputBatchFunc: requests =>
            {
                receivedChunks.Add(requests.ToList());
                return Task.FromResult<IList<OperationResult<string>>>(
                    requests.Select(r => new OperationResult<string>($"processed-{r}", null)).ToList());
            });

        // act
        var task1 = sut.QueueAsync(1);
        var task2 = sut.QueueAsync(2);
        var task3 = sut.QueueAsync(3);
        await Task.WhenAll(task1, task2, task3);

        // assert
        Assert.Single(receivedChunks, chunk => chunk.OrderBy(x => x).SequenceEqual(new[] { 1, 2, 3 }));
    }

    [Fact]
    public async Task QueueAsync_ShouldFlushPartialChunk_AfterBatchTimeoutElapses()
    {
        // arrange
        var receivedChunks = new ConcurrentBag<IList<int>>();

        await using var sut = new OperationBatcher<int, string>(
            batchSize: 5,
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: 1,
            batchTimeout: TimeSpan.FromMilliseconds(20),
            processOutputBatchFunc: requests =>
            {
                receivedChunks.Add(requests.ToList());
                return Task.FromResult<IList<OperationResult<string>>>(
                    requests.Select(r => new OperationResult<string>($"processed-{r}", null)).ToList());
            });

        // act
        var task1 = sut.QueueAsync(1);
        var task2 = sut.QueueAsync(2);
        await Task.WhenAll(task1, task2);

        // assert
        Assert.Single(receivedChunks, chunk => chunk.OrderBy(x => x).SequenceEqual(new[] { 1, 2 }));
    }

    [Fact]
    public async Task QueueAsync_ShouldReturnMatchingResultPerRequest_WhenChunkContainsMultipleItems()
    {
        // arrange
        await using var sut = new OperationBatcher<int, string>(
            batchSize: 3,
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: 1,
            batchTimeout: TimeSpan.FromMilliseconds(200),
            processOutputBatchFunc: requests => Task.FromResult<IList<OperationResult<string>>>(
                requests.Select(r => new OperationResult<string>($"processed-{r}", null)).ToList()));

        // act
        var task1 = sut.QueueAsync(1);
        var task2 = sut.QueueAsync(2);
        var task3 = sut.QueueAsync(3);
        var results = await Task.WhenAll(task1, task2, task3);

        // assert
        Assert.Equal(new[] { "processed-1", "processed-2", "processed-3" }, results);
    }

    [Fact]
    public async Task QueueAsync_ShouldOnlyFailItemsWithExceptions_WhenProcessBatchFuncReturnsMixedResults()
    {
        // arrange
        var expectedException = new InvalidOperationException("boom");

        await using var sut = new OperationBatcher<int, string>(
            batchSize: 3,
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: 1,
            batchTimeout: TimeSpan.FromMilliseconds(200),
            processOutputBatchFunc: requests => Task.FromResult<IList<OperationResult<string>>>(
                requests
                    .Select(r => r == 2
                        ? new OperationResult<string>(null, expectedException)
                        : new OperationResult<string>($"processed-{r}", null))
                    .ToList()));

        // act
        var task1 = sut.QueueAsync(1);
        var task2 = sut.QueueAsync(2);
        var task3 = sut.QueueAsync(3);
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => Task.WhenAll(task1, task2, task3));

        // assert
        Assert.Equal("boom", exception.Message);
        Assert.Equal("processed-1", await task1);
        Assert.Equal("processed-3", await task3);
    }

    [Fact]
    public async Task QueueAsync_ShouldThrow_WhenProcessBatchFuncThrows()
    {
        // arrange
        var expectedException = new InvalidOperationException("boom");

        await using var sut = new OperationBatcher<int, string>(
            batchSize: 1,
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: 1,
            processOutputBatchFunc: _ => throw expectedException);

        // act
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => sut.QueueAsync(1));

        // assert
        Assert.Equal("boom", exception.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task QueueAsync_ShouldNotWaitForMoreRequests_WhenBatchTimeoutIsNotPositive(int batchTimeoutMilliseconds)
    {
        // arrange
        var timeProvider = new FakeTimeProvider();
        var receivedChunks = new ConcurrentBag<IList<int>>();

        await using var sut = new OperationBatcher<int, string>(
            batchSize: 5,
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: 1,
            batchTimeout: TimeSpan.FromMilliseconds(batchTimeoutMilliseconds),
            timeProvider: timeProvider,
            processOutputBatchFunc: requests =>
            {
                receivedChunks.Add(requests.ToList());
                return Task.FromResult<IList<OperationResult<string>>>(
                    requests.Select(r => new OperationResult<string>($"processed-{r}", null)).ToList());
            });

        // act (the fake time never advances, so a request that waited for the timeout would never complete)
        var result = await sut.QueueAsync(1).WaitAsync(TimeSpan.FromSeconds(10));

        // assert
        Assert.Equal("processed-1", result);
        Assert.Single(receivedChunks, chunk => chunk.SequenceEqual(new[] { 1 }));
    }

    [Fact]
    public async Task QueueAsync_ShouldWaitForMoreRequests_WhenBatchTimeoutIsPositive()
    {
        // arrange
        var timeProvider = new FakeTimeProvider();

        await using var sut = new OperationBatcher<int, string>(
            batchSize: 5,
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: 1,
            batchTimeout: TimeSpan.FromMilliseconds(50),
            timeProvider: timeProvider,
            processOutputBatchFunc: requests => Task.FromResult<IList<OperationResult<string>>>(
                requests.Select(r => new OperationResult<string>($"processed-{r}", null)).ToList()));

        // act
        var task = sut.QueueAsync(1);
        await Task.Delay(TimeSpan.FromMilliseconds(200));
        var completedBeforeTimeout = task.IsCompleted;

        while (!task.IsCompleted)
        {
            timeProvider.Advance(TimeSpan.FromMilliseconds(100));
            await Task.Delay(10);
        }

        // assert
        Assert.False(completedBeforeTimeout);
        Assert.Equal("processed-1", await task);
    }

    [Fact]
    public async Task QueueAsync_ShouldMergeTheBatchesThatQueuedWhileTheOutputThreadWasBusy()
    {
        // arrange
        var receivedChunks = new ConcurrentQueue<IList<int>>();
        var release = new TaskCompletionSource();

        await using var sut = new OperationBatcher<int, string>(
            batchSize: 5,
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: 1,
            processOutputBatchFunc: async requests =>
            {
                receivedChunks.Enqueue(requests.ToList());
                if (receivedChunks.Count == 1)
                {
                    await release.Task;
                }

                return requests.Select(r => new OperationResult<string>($"processed-{r}", null)).ToList();
            });

        // act: the first request occupies the only output thread, the next ones are cut as batches of one meanwhile
        var tasks = new List<Task<string>> { sut.QueueAsync(1) };
        foreach (var request in new[] { 2, 3, 4 })
        {
            await Task.Delay(50);
            tasks.Add(sut.QueueAsync(request));
        }

        await Task.Delay(50);
        release.SetResult();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        // assert
        Assert.Equal(new[] { new[] { 1 }, new[] { 2, 3, 4 } }, receivedChunks.Select(chunk => chunk.ToArray()).ToArray());
    }

    [Fact]
    public async Task QueueAsync_ShouldNotMergeMoreThanTheBatchSize_AndKeepWhatDoesNotFit()
    {
        // arrange
        var receivedChunks = new ConcurrentQueue<IList<int>>();
        var release = new TaskCompletionSource();

        await using var sut = new OperationBatcher<int, string>(
            batchSize: 2,
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: 1,
            processOutputBatchFunc: async requests =>
            {
                receivedChunks.Enqueue(requests.ToList());
                if (receivedChunks.Count == 1)
                {
                    await release.Task;
                }

                return requests.Select(r => new OperationResult<string>($"processed-{r}", null)).ToList();
            });

        // act
        var tasks = new List<Task<string>> { sut.QueueAsync(1) };
        foreach (var request in new[] { 2, 3, 4, 5, 6 })
        {
            await Task.Delay(50);
            tasks.Add(sut.QueueAsync(request));
        }

        await Task.Delay(50);
        release.SetResult();
        var results = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        // assert
        Assert.Equal(new[] { "processed-1", "processed-2", "processed-3", "processed-4", "processed-5", "processed-6" }, results);
        Assert.Equal(new[] { new[] { 1 }, new[] { 2, 3 }, new[] { 4, 5 }, new[] { 6 } }, receivedChunks.Select(chunk => chunk.ToArray()).ToArray());
    }

    [Fact]
    public async Task DisposeAsync_ShouldCompleteWithoutHanging_WhenNoPendingRequests()
    {
        // arrange
        var sut = new OperationBatcher<int, string>(
            batchSize: 5,
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: 1,
            processOutputBatchFunc: requests => Task.FromResult<IList<OperationResult<string>>>(
                requests.Select(r => new OperationResult<string>($"processed-{r}", null)).ToList()));

        // act & assert
        await sut.DisposeAsync();
    }
}
