using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Xunit;

namespace CluedIn.Connector.DataLake.Common.Tests.Unit;

public abstract class BufferTestsBase
{
    [Fact]
    public async Task Add_ShouldIdleFlush()
    {
        // arrange
        var idleTimeout = 5000;
        var testTimeout = idleTimeout * 2;
        var maxBatchSize = 10;

        var actionHistory = new List<(DateTime actionAt, string[] items)>();
        using var buffer = CreateBuffer(idleTimeout: idleTimeout, maxBatchSize: maxBatchSize, actionHistory);

        // act
        // testTimeout to prevent waiting for a long time in case of failure.
        // In case of success, the tasks should complete when idleTimeout elapses and not when testTimeout elapses.
        await Task.WhenAny(
            Task.Delay(testTimeout),
            Task.WhenAll(
                buffer.Add("item1_1"),
                buffer.Add("item1_2"),
                buffer.Add("item1_3")));
        await Task.WhenAny(
            Task.Delay(testTimeout),
            Task.WhenAll(
                buffer.Add("item2_1")));
        await Task.WhenAny(
            Task.Delay(testTimeout),
            Task.WhenAll(
                buffer.Add("item3_1"),
                buffer.Add("item3_2"),
                buffer.Add("item3_3"),
                buffer.Add("item3_4")));

        // assert
        var tolerance = 500;

        for (var i = 1; i < actionHistory.Count; i++)
        {
            var observedTimeBetweenActions = actionHistory[i].actionAt.Subtract(actionHistory[i - 1].actionAt).TotalMilliseconds;
            Assert.True(observedTimeBetweenActions > idleTimeout - tolerance, $"Expected delay between actions {idleTimeout}ms but was {observedTimeBetweenActions}ms");
        }
    }

    [Fact]
    public async Task Flush_ClearsPendingItems()
    {
        // arrange
        var idleTimeout = 5000;
        var maxBatchSize = 10;

        var actionHistory = new List<(DateTime actionAt, string[] items)>();
        using var buffer = CreateBuffer(idleTimeout: idleTimeout, maxBatchSize: maxBatchSize, actionHistory);

        // act
        await Task.Run(async () => {
            _ = Task.WhenAll(
                buffer.Add("item1_1"),
                buffer.Add("item1_2"),
                buffer.Add("item1_3"));
            await Task.Delay(TimeSpan.FromMilliseconds(idleTimeout / 2));
            await buffer.Flush();
        });
        await Task.Run(async () => {

            _ = Task.WhenAll(
                buffer.Add("item2_1"));
            await Task.Delay(TimeSpan.FromMilliseconds(idleTimeout / 2));
            await buffer.Flush();
        });
        await Task.Run(async () => {
            _ = Task.WhenAll(
                buffer.Add("item3_1"),
                buffer.Add("item3_2"),
                buffer.Add("item3_3"),
                buffer.Add("item3_4"));
            await Task.Delay(TimeSpan.FromMilliseconds(idleTimeout / 2));
            await buffer.Flush();
        });

        // empty buffers should not trigger bulk action
        await buffer.Flush();

        // assert
        Assert.Equal(3, actionHistory.Count);
    }

    [Fact]
    public async Task Add_WhenReachMaxBatchSize_Flushes()
    {
        // arrange
        var idleTimeout = 5000;
        var testTimeout = idleTimeout * 2;
        var maxBatchSize = 10;

        var actionHistory = new List<(DateTime actionAt, string[] items)>();
        using var buffer = CreateBuffer(idleTimeout: idleTimeout, maxBatchSize: maxBatchSize, actionHistory);

        // act
        await Task.WhenAny(
            Task.Delay(testTimeout),
            Task.WhenAll(
                Enumerable
                .Range(0, maxBatchSize)
                .Select(x => buffer.Add($"item1_{x + 1}"))
            ));
        await Task.WhenAny(
            Task.Delay(testTimeout),
            Task.WhenAll(
                Enumerable
                .Range(0, maxBatchSize)
                .Select(x => buffer.Add($"item2_{x + 1}"))
            ));
        await Task.WhenAny(
            Task.Delay(testTimeout),
            Task.WhenAll(
                Enumerable
                .Range(0, maxBatchSize)
                .Select(x => buffer.Add($"item3_{x + 1}"))
            ));

        // assert
        Assert.Equal(3, actionHistory.Count);
        Assert.All(actionHistory, x => Assert.Equal(maxBatchSize, x.items.Length));
    }

    [Fact]
    public async Task Add_WhenIdleAfterInitialBatch_ShouldIdleFlush()
    {
        // arrange
        var idleTimeout = 5000;
        var testTimeout = idleTimeout * 2;
        var maxBatchSize = 10;
        var remainingItems = maxBatchSize / 2;

        var actionHistory = new List<(DateTime actionAt, string[] items)>();
        using var buffer = CreateBuffer(idleTimeout: idleTimeout, maxBatchSize: maxBatchSize, actionHistory);

        // act
        // testTimeout to prevent waiting for a long time in case of failure.
        // In case of success, the tasks should complete when idleTimeout elapses and not when testTimeout elapses.
        await Task.WhenAny(
            Task.Delay(testTimeout),
            Task.WhenAll(
                Enumerable
                .Range(0, maxBatchSize + remainingItems)
                .Select(x => buffer.Add($"item_{x + 1}"))
            ));

        // assert
        var tolerance = 500;

        Assert.Equal(2, actionHistory.Count);
        var observedTimeBetweenActions = actionHistory.Last().actionAt.Subtract(actionHistory.First().actionAt).TotalMilliseconds;
        Assert.True(observedTimeBetweenActions > idleTimeout - tolerance, $"Expected delay between actions {idleTimeout}ms but was {observedTimeBetweenActions}ms");

        Assert.Equal(maxBatchSize, actionHistory.First().items.Length);
        Assert.Equal(remainingItems, actionHistory.Last().items.Length);
    }

    [Fact]
    public async Task Add_AfterIdleFlush_ShouldFlushWhenReachMaxBatchSize()
    {
        // arrange
        var idleTimeout = 5000;
        var testTimeout = idleTimeout * 2;
        var maxBatchSize = 10;
        var initialItems = maxBatchSize / 2;

        var actionHistory = new List<(DateTime actionAt, string[] items)>();
        using var buffer = CreateBuffer(idleTimeout: idleTimeout, maxBatchSize: maxBatchSize, actionHistory);

        // act
        // testTimeout to prevent waiting for a long time in case of failure.
        // In case of success, the tasks should complete when idleTimeout elapses and not when testTimeout elapses.
        await Task.WhenAny(
            Task.Delay(testTimeout),
            Task.WhenAll(
                Enumerable
                .Range(0, initialItems)
                .Select(x => buffer.Add($"iteminitial_{x + 1}"))
            ));
        await Task.WhenAny(
            Task.Delay(testTimeout),
            Task.WhenAll(
                Enumerable
                .Range(0, maxBatchSize)
                .Select(x => buffer.Add($"itemnext_{x + 1}"))
            ));

        // assert
        var tolerance = 500;

        Assert.Equal(2, actionHistory.Count);
        var observedTimeBetweenActions = actionHistory.Last().actionAt.Subtract(actionHistory.First().actionAt).TotalMilliseconds;
        Assert.True(observedTimeBetweenActions < tolerance, $"Expected delay between actions {tolerance}ms but was {observedTimeBetweenActions}ms");

        Assert.Equal(initialItems, actionHistory.First().items.Length);
        Assert.Equal(maxBatchSize, actionHistory.Last().items.Length);
    }

    private protected abstract IBuffer<string> CreateBuffer(
        int idleTimeout,
        int maxBatchSize,
        List<(DateTime actionAt, string[] items)> actionHistory);
}
