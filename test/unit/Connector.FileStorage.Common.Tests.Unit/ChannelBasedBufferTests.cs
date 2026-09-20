using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Buffers;

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
        new TestTimeProvider());
    }

    internal class TestTimeProvider : ITimeProvider
    {
        public DateTimeOffset GetUtcNow()
        {
            return DateTimeOffset.UtcNow;
        }
    }
}
