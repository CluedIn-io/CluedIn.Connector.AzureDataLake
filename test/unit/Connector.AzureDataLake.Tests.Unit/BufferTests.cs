using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CluedIn.Connector.DataLake.Common;
using Xunit;

namespace CluedIn.Connector.AzureDataLake.Tests.Unit
{
    public class BufferTests
    {
        [Fact]
        public async Task VerifyAddWaitsForFlush()
        {
            // arrange
            var idleTimeout = 5000;

            var actionHistory = new List<(DateTime actionAt, string[] items)>();
            var buffer = new Buffer<string>(10, idleTimeout, x =>
            {
                actionHistory.Add((DateTime.Now, x));
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
    }
}
