using CluedIn.Core.Jobs;

namespace CluedIn.Connector.DataLake.Common;

internal class DataLakeJobArgs : JobArgs
{
    public DataLakeJobArgs() : base()
    {
    }

    public DataLakeJobArgs(JobArgs jobArgs, bool isTriggeredFromJobServer)
        : base(jobArgs)
    {
        IsTriggeredFromJobServer = isTriggeredFromJobServer;
    }

    public bool IsTriggeredFromJobServer { get; set; } = true;
}
