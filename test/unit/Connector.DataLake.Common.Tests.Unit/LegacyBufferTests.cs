using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common.Buffers;

using Xunit;

namespace CluedIn.Connector.DataLake.Common.Tests.Unit;

public class LegacyBufferTests : BufferTestsBase
{
    private protected override IBuffer<string> CreateBuffer(
        int idleTimeout,
        int maxBatchSize,
        List<(DateTime actionAt, string[] items)> actionHistory)
    {
        return new Buffer<string>(maxBatchSize, idleTimeout, x =>
        {
            actionHistory.Add((DateTime.Now, x));
            return Task.CompletedTask;
        });
    }

    [Fact]
    public async Task GetStatus_WhenEmpty_ReturnsZeroPendingItems()
    {
        // arrange
        var idleTimeout = 5000;
        var maxBatchSize = 5;
        var actionHistory = new List<(DateTime actionAt, string[] items)>();
        using var buffer = CreateBuffer(idleTimeout, maxBatchSize, actionHistory);

        // act
        var status = await buffer.GetStatus();

        // assert
        Assert.Equal(0, status.TotalPendingItems);
        Assert.Equal(maxBatchSize, status.MaxPendingItems);
        Assert.Equal(idleTimeout, status.TimeOutMilliseconds);
        Assert.NotNull(status.AdditionalInformation);
    }

    [Fact]
    public async Task GetStatus_AfterAddingItems_ReflectsPendingCount()
    {
        // arrange
        var idleTimeout = 10000;
        var maxBatchSize = 10;
        var actionHistory = new List<(DateTime actionAt, string[] items)>();
        using var buffer = CreateBuffer(idleTimeout, maxBatchSize, actionHistory);

        // act
        _ = buffer.Add("item1");
        _ = buffer.Add("item2");
        _ = buffer.Add("item3");
        await Task.Delay(200); // allow adds to complete

        var status = await buffer.GetStatus();

        // assert
        Assert.Equal(3, status.TotalPendingItems);
        Assert.Equal(maxBatchSize, status.MaxPendingItems);
        Assert.Equal(idleTimeout, status.TimeOutMilliseconds);
    }

    [Fact]
    public async Task GetStatus_AfterFlush_ReturnsZeroPendingItems()
    {
        // arrange
        var idleTimeout = 10000;
        var maxBatchSize = 10;
        var actionHistory = new List<(DateTime actionAt, string[] items)>();
        using var buffer = CreateBuffer(idleTimeout, maxBatchSize, actionHistory);

        // act
        _ = buffer.Add("item1");
        _ = buffer.Add("item2");
        await Task.Delay(200);
        await buffer.Flush();

        var status = await buffer.GetStatus();

        // assert
        Assert.Equal(0, status.TotalPendingItems);
    }
}
