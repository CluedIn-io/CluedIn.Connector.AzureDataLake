using System;

namespace CluedIn.Connector.FileStorage.Common;

/// <summary>
/// Self-contained - no longer extends CluedIn.Core.Jobs.IJobArgs (removed from CluedIn.Core by the Quartz
/// job server migration). Only Message and Schedule were ever read from the Core interface here, so those
/// two are declared directly instead of pulling in the rest of its (unused) surface.
/// </summary>
public interface IStorageJobArgs
{
    string Message { get; set; }
    string Schedule { get; set; }
    bool IsTriggeredFromJobServer { get; set; }
    DateTimeOffset InstanceTime { get; set; }
}
