using System;
using System.Collections.Generic;
using System.Linq;

using CluedIn.Core;

using NCrontab;

namespace CluedIn.Connector.DataLake.Common;

internal static class CronSchedules
{
    public class JobScheduleNames
    {
        public const string Hourly = "Hourly";
        public const string Daily = "Daily";
        public const string Weekly = "Weekly";
        public const string Never = "Never";
    }

    private static readonly Dictionary<string, string> _cronSchedules = new(StringComparer.OrdinalIgnoreCase)
    {
        [JobScheduleNames.Hourly] = "0 0/1 * * *",
        [JobScheduleNames.Daily] = "0 0 1-31 * *",
        [JobScheduleNames.Weekly] = "0 0 1-31 * 1",
        [JobScheduleNames.Never] = "0 5 31 2 *",
    };

    public record CronScheduleName(string Name, string Description);

    public static IDictionary<string, CronScheduleName> SupportedCronScheduleNames = new Dictionary<string, CronScheduleName>(StringComparer.OrdinalIgnoreCase)
    {
        [JobScheduleNames.Hourly] = new (JobScheduleNames.Hourly, "Every hour,e.g: 1:00am, 2:00am, ..."),
        [JobScheduleNames.Daily] = new(JobScheduleNames.Daily, "Every day at 12:00am"),
        [JobScheduleNames.Weekly] = new(JobScheduleNames.Weekly, "Every Monday at 12:00am"),
    };

    public static bool TryGetCronSchedule(string cronScheduleOrName, out string cronSchedule)
    {
        if (SupportedCronScheduleNames.TryGetValue(cronScheduleOrName, out var _)
            && _cronSchedules.TryGetValue(cronScheduleOrName, out cronSchedule))
        {
            return true;
        }

        var schedule = CrontabSchedule.TryParse(cronScheduleOrName);
        if (schedule == null)
        {
            cronSchedule = null;
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

    public static DateTimeOffset? GetPreviousOccurrence(string cronSchedule, DateTimeOffset baseTime)
    {
        var crontabSchedule = CrontabSchedule.Parse(cronSchedule);

        // Progressively try to get last occurrence.
        // We cannot just get next two occurrences and find the difference and perform subtraction
        // That only works cron that has same size frequency like daily or hourly
        // It fails for cron like 0/5 11 * * *
        // We only try up to a month because it does not make sense to try to output file that should have been created more than a month ago
        var baseDateTime = baseTime.DateTime;
        var previousHour = baseDateTime.AddHours(-1);
        var previousHourOccurences = crontabSchedule.GetNextOccurrences(previousHour, baseDateTime);
        if (previousHourOccurences.Any())
        {
            return new DateTimeOffset(previousHourOccurences.Last(), baseTime.Offset);
        }

        var previousDay = baseDateTime.AddDays(-1);
        var previousDayOccurences = crontabSchedule.GetNextOccurrences(previousDay, previousHour);
        if (previousDayOccurences.Any())
        {
            return new DateTimeOffset(previousDayOccurences.Last(), baseTime.Offset);
        }

        var previousWeek = baseDateTime.AddDays(-7);
        var previousWeekOccurences = crontabSchedule.GetNextOccurrences(previousWeek, previousDay);
        if (previousWeekOccurences.Any())
        {
            return new DateTimeOffset(previousWeekOccurences.Last(), baseTime.Offset);
        }

        var previousMonth = baseDateTime.AddMonths(-1);
        var previousMonthOccurences = crontabSchedule.GetNextOccurrences(previousMonth, previousWeek);
        if (previousMonthOccurences.Any())
        {
            return new DateTimeOffset(previousMonthOccurences.Last(), baseTime.Offset);
        }

        return null;
    }
}
