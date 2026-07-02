using System;
using System.Threading.Tasks;

namespace CluedIn.Connector.FileStorage.Common;

internal interface IScheduler : IScheduledJobQueue
{
    void AddJobProducer(Func<IScheduledJobQueue, Task> jobProducer);
    Task RunAsync();
}
