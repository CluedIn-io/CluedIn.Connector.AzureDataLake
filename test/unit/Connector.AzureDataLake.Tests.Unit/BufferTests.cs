using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CluedIn.Connector.DataLake.Common.Buffers;
using CluedIn.Core;

using Xunit;

namespace CluedIn.Connector.AzureDataLake.Tests.Unit;

public class BufferTests
{
    [Fact]
    public async Task LegacyBuffer_VerifyAddWaitsForFlush()
    {
        // arrange
        var idleTimeout = 5000;

        var actionHistory = new List<(DateTime actionAt, string[] items)>();
        var buffer = new Buffer<string>(10, idleTimeout, x =>
        {
            actionHistory.Add((DateTime.Now, x));
            return Task.CompletedTask;
        });

        // act
        await buffer.Add("item1");
        await buffer.Flush();
        await buffer.Add("item2");
        await buffer.Flush();
        await buffer.Add("item3");
        await buffer.Flush();

        // assert
        var tolerance = 500;

        for (var i = 1; i < actionHistory.Count; i++)
        {
            var observedTimeBetweenActions = actionHistory[i].actionAt.Subtract(actionHistory[i - 1].actionAt).TotalMilliseconds;
            Assert.True(observedTimeBetweenActions > idleTimeout - tolerance, $"Expected delay between actions {idleTimeout}ms but was {observedTimeBetweenActions}ms");
        }
    }

    [Fact]
    public async Task ChannelBasedBuffer_VerifyAddWaitsForFlush()
    {
        // arrange
        var idleTimeout = 5000;

        var actionHistory = new List<(DateTime actionAt, string[] items)>();
        var buffer = new ChannelBasedBuffer<string>(new ChannelBasedBufferOptions
        {
            MaxBatchSize = 10,
            IdleTimeout = TimeSpan.FromMilliseconds(idleTimeout),
            BoundedCapacity = 200,
            MaxConcurrentFlushes = 1,
            MaxPendingFlushBatches = 2,
            EnableAutoTuneMaxBatchSize = true,
        },
        (x,_) =>
        {
            actionHistory.Add((DateTime.Now, x));
            return Task.CompletedTask;
        },
        new TestDateTimeOffsetProvider());

        // act
        await buffer.AddAsync("item1");
        await buffer.FlushAsync();
        await buffer.AddAsync("item2");
        await buffer.FlushAsync();
        await buffer.AddAsync("item3");
        await buffer.FlushAsync();

        // assert
        var tolerance = 500;

        for (var i = 1; i < actionHistory.Count; i++)
        {
            var observedTimeBetweenActions = actionHistory[i].actionAt.Subtract(actionHistory[i - 1].actionAt).TotalMilliseconds;
            Assert.True(observedTimeBetweenActions > idleTimeout - tolerance, $"Expected delay between actions {idleTimeout}ms but was {observedTimeBetweenActions}ms");
        }
    }

    [Fact]
    public async Task SafeBuffer_VerifyAddWaitsForFlush()
    {
        // arrange
        var idleTimeout = 1000;

        var actionHistory = new List<(DateTime actionAt, string[] items)>();
        {
            using var buffer = new SafeBuffer<string, string>(10, idleTimeout, x =>
            {
                actionHistory.Add((DateTime.Now, x));
                return Task.FromResult(x);
            });

            // act
            await buffer.Add("item1");
            await Task.Delay(idleTimeout);
            await buffer.Add("item2");
            await Task.Delay(idleTimeout);
            await buffer.Add("item3");
            await Task.Delay(idleTimeout);
        }

        // assert
        var tolerance = 500;

        for (var i = 1; i < actionHistory.Count; i++)
        {
            var observedTimeBetweenActions = actionHistory[i].actionAt.Subtract(actionHistory[i - 1].actionAt).TotalMilliseconds;
            Assert.True(observedTimeBetweenActions > idleTimeout - tolerance, $"Expected delay between actions {idleTimeout}ms but was {observedTimeBetweenActions}ms");
        }
    }

    internal class TestDateTimeOffsetProvider : IDateTimeOffsetProvider
    {
        public DateTimeOffset GetCurrentTime()
        {
            return DateTimeOffset.Now;
        }

        public DateTimeOffset GetCurrentUtcTime()
        {
            return DateTimeOffset.UtcNow;
        }
    }
}
