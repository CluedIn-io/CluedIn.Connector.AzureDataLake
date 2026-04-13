using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Buffers;

namespace CluedIn.Connector.FileStorage.Common.Tests.Unit;

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
}
