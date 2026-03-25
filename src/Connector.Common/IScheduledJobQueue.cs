using System.Collections.Generic;

namespace CluedIn.Connector.DataLake.Common;

internal interface IScheduledJobQueue
{
    void AddOrUpdateJob(QueuedJob job);
    ICollection<QueuedJob> GetAllJobs();
    bool TryRemove(string key, out QueuedJob removeValue);
}
