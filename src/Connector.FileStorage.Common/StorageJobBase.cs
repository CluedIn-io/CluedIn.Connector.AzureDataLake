using System;
using System.Threading.Tasks;

using CluedIn.Core;

namespace CluedIn.Connector.FileStorage.Common;

/// <summary>
/// No longer derives from CluedIn.Core.Jobs.JobBase/ICustomScheduledJob (removed from CluedIn.Core by the
/// Quartz job server migration) - this family of jobs is never actually run through CluedIn.Core's job
/// server anyway (see UpdateStreamScheduleBase, which only ever unscheduled a legacy job-server registration
/// with CronSchedules.NeverCron); real scheduling and execution both go through this repo's own
/// Scheduler/IScheduledJobQueue, which calls DoRunAsync/CanRunAsync/HasMissed directly.
/// </summary>
internal abstract class StorageJobBase : IStorageJob
{
    private readonly ITimeProvider _timeProvider;

    protected StorageJobBase(ApplicationContext appContext, ITimeProvider timeProvider)
    {
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
    }

    public abstract Task DoRunAsync(ExecutionContext context, IStorageJobArgs args);

    public abstract Task<bool> HasMissed(ExecutionContext context, IStorageJobArgs args);

    public virtual Task<bool> CanRunAsync(ExecutionContext context, IStorageJobArgs args)
    {
        return Task.FromResult(true);
    }
}
