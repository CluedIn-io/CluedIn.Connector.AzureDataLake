using System;

using CluedIn.Core.Jobs;

namespace CluedIn.Connector.FileStorage.Common;

internal class StorageJobArgs : JobArgs, IStorageJobArgs
{
    public StorageJobArgs() : base()
    {
    }

    public StorageJobArgs(
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
