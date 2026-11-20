using System;

namespace CluedIn.Connector.FileStorage.Common;

/// <summary>No longer derives from CluedIn.Core.Jobs.JobArgs (removed from CluedIn.Core by the Quartz job server migration) - self-contained instead.</summary>
internal class StorageJobArgs : IStorageJobArgs
{
    public StorageJobArgs()
    {
    }

    public StorageJobArgs(
        string message,
        string schedule,
        bool isTriggeredFromJobServer,
        DateTimeOffset instanceTime)
    {
        Message = message;
        Schedule = schedule;
        IsTriggeredFromJobServer = isTriggeredFromJobServer;
        InstanceTime = instanceTime;
    }

    public string Message { get; set; }
    public string Schedule { get; set; }
    public bool IsTriggeredFromJobServer { get; set; } = true;
    public DateTimeOffset InstanceTime { get; set; }
}
