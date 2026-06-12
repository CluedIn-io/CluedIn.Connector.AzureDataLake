using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common.Buffers;

namespace CluedIn.Connector.DataLake.Common.Tests.Unit;

public class SafeBufferTests : BufferTestsBase
{
    private protected override IBuffer<string> CreateBuffer(
        int idleTimeout,
        int maxBatchSize,
        List<(DateTime actionAt, string[] items)> actionHistory)
    {
        return new SafeBuffer<string, string>(maxBatchSize, idleTimeout, x =>
        {
            actionHistory.Add((DateTime.Now, x));
            return Task.FromResult(x);
        });
    }
}
