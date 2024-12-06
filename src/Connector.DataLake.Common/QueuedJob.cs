using System;

namespace CluedIn.Connector.DataLake.Common;

internal record QueuedJob(
    string Key,
    Guid OrganizationId,
    Type Type,
    Schedule Schedule,
    DateTimeOffset StartFromTime);


internal record Schedule(string CronSchedule)
{
    public bool IsNeverCron => CronSchedule == CronSchedules.NeverCron;
}
