using System;

using CluedIn.Core.Jobs;

namespace CluedIn.Connector.DataLake.Common;

internal class DataLakeJobArgs : JobArgs, IDataLakeJobArgs
{
    public DataLakeJobArgs() : base()
    {
    }

    public DataLakeJobArgs(
        JobArgs jobArgs,
        bool isTriggeredFromJobServer,
        DateTimeOffset instanceTime)
        : base(jobArgs)
    {
        IsTriggeredFromJobServer = isTriggeredFromJobServer;
        InstanceTime = instanceTime;
    }

    public bool IsTriggeredFromJobServer { get; set; } = true;
    public DateTimeOffset InstanceTime { get; set; }
}
