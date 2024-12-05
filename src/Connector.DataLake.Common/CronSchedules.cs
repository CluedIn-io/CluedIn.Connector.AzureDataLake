using System;
using System.Collections.Generic;

using NCrontab;

namespace CluedIn.Connector.DataLake.Common;

internal static class CronSchedules
{
    private class JobScheduleNames
    {
        private static readonly HashSet<string> _supportedSchedules = new(StringComparer.OrdinalIgnoreCase)
        {
            Hourly,
            Daily,
            Weekly,
        };

        public const string Hourly = "Hourly";
        public const string Daily = "Daily";
        public const string Weekly = "Weekly";
        public const string Never = "Never";

        public static ICollection<string> SupportedSchedules => _supportedSchedules;
    }

    private static readonly Dictionary<string, string> _cronSchedules = new(StringComparer.OrdinalIgnoreCase)
    {
        [JobScheduleNames.Hourly] = "0 0/1 * * *",
        [JobScheduleNames.Daily] = "0 0 1-31 * *",
        [JobScheduleNames.Weekly] = "0 0 1-31 * 1",
        [JobScheduleNames.Never] = "0 5 31 2 *",
    };

    public static ICollection<string> SupportedCronScheduleNames = JobScheduleNames.SupportedSchedules;

    public static bool TryGetCronSchedule(string cronScheduleOrName, out string cronSchedule)
    {
        if (_cronSchedules.TryGetValue(cronScheduleOrName, out cronSchedule))
        {
            return true;
        }

        var schedule = CrontabSchedule.TryParse(cronScheduleOrName);
        if (schedule == null)
        {
            return false;
        }

        cronSchedule = cronScheduleOrName;
        return true;
    }

    public static bool IsSupportedScheduleName(string cronScheduleName) => _cronSchedules.ContainsKey(cronScheduleName);

    public static string NeverCron => _cronSchedules[JobScheduleNames.Never];

    public static DateTimeOffset GetNextOccurrence(string cronSchedule, DateTimeOffset baseTime)
    {
        var jobSchedule = CrontabSchedule.Parse(cronSchedule);
        var nextTime = jobSchedule.GetNextOccurrence(baseTime.DateTime);
        return new DateTimeOffset(nextTime, baseTime.Offset);
    }

    public static DateTimeOffset GetPreviousOccurrence(string cronSchedule, DateTimeOffset baseTime)
    {
        var crontabSchedule = CrontabSchedule.Parse(cronSchedule);
        var next = crontabSchedule.GetNextOccurrence(baseTime.DateTime);
        var nextNext = crontabSchedule.GetNextOccurrence(next.AddSeconds(1));
        var diff = nextNext - next;
        var previousOccurence = new DateTimeOffset(next, baseTime.Offset) - diff;
        return previousOccurence;
    }
}
