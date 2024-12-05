using System;

using CluedIn.Core.Jobs;

namespace CluedIn.Connector.DataLake.Common;

public interface IDataLakeJobArgs : IJobArgs
{
    bool IsTriggeredFromJobServer { get; set; }
    DateTimeOffset InstanceTime { get; set; }
}
