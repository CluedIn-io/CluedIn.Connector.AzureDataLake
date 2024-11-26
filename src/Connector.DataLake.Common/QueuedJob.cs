using System;

namespace CluedIn.Connector.DataLake.Common;

internal record QueuedJob(
    string Key,
    Guid OrganizationId,
    Type Type,
    string CronSchedule,
    DateTimeOffset StartFromTime);
