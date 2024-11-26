using System;
using System.Threading.Tasks;

namespace CluedIn.Connector.DataLake.Common;

internal interface IScheduler : IScheduledJobQueue
{
    void AddJobProducer(Func<IScheduledJobQueue, Task> jobProducer);
    Task RunAsync();
}
