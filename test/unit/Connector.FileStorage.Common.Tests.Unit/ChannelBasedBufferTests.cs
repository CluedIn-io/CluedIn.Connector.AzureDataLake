using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Buffers;
using CluedIn.Core;

namespace CluedIn.Connector.FileStorage.Common.Tests.Unit;

public class ChannelBasedBufferTests : BufferTestsBase
{
    private protected override IBuffer<string> CreateBuffer(
        int idleTimeout,
        int maxBatchSize,
        List<(DateTime actionAt, string[] items)> actionHistory)
    {
        return new ChannelBasedBuffer<string>(new ChannelBasedBufferOptions
        {
            MaxBatchSize = maxBatchSize,
            IdleTimeout = TimeSpan.FromMilliseconds(idleTimeout),
            BoundedCapacity = 200,
            MaxConcurrentFlushes = 1,
            MaxPendingFlushBatches = 2,
            EnableAutoTuneMaxBatchSize = true,
        },
        (x, _) =>
        {
            actionHistory.Add((DateTime.Now, x));
            return Task.CompletedTask;
        },
        new TestDateTimeOffsetProvider());
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
