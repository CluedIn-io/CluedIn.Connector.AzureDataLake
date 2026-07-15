using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.FileStorage.Common;

internal static class StorageFactoryExtensions
{
    public static async Task<Schedule> GetScheduleAsync(this IStorageFactory jobDataFactory, ExecutionContext context, StreamModel stream)
    {
        var configurations = await jobDataFactory.CreateStorageConfiguration(
            context,
            stream);

        if (configurations.IsStreamCacheEnabled
            && stream.Status == StreamStatus.Started
            && CronSchedules.TryGetCronSchedule(configurations.GetCronOrScheduleName(), out var retrievedSchedule))
        {
            return new Schedule(retrievedSchedule);
        }


        return new Schedule(CronSchedules.NeverCron);
    }

    public static string GetCronOrScheduleName(this IStorageConfiguration dataLakeJobData)
    {
        if (dataLakeJobData.Schedule == StorageConfigurationConstants.CustomCronScheduleName)
        {
            return dataLakeJobData.CustomCron;
        }

        return dataLakeJobData.Schedule;
    }
}
