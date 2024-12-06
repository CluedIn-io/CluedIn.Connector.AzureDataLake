using System;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.DataLake.Common;

internal static class DataLakeJobDataFactoryExtensions
{
    public static async Task<Schedule> GetScheduleAsync(this IDataLakeJobDataFactory jobDataFactory, ExecutionContext context, StreamModel stream)
    {
        var configurations = await jobDataFactory.GetConfiguration(
            context,
            stream.ConnectorProviderDefinitionId.Value,
            stream.ContainerName);

        if (configurations.IsStreamCacheEnabled
            && stream.Status == StreamStatus.Started
            && CronSchedules.TryGetCronSchedule(configurations.GetCronOrScheduleName(), out var retrievedSchedule))
        {
            return new Schedule(retrievedSchedule);
        }


        return new Schedule(CronSchedules.NeverCron);
    }

    public static string GetCronOrScheduleName(this IDataLakeJobData dataLakeJobData)
    {
        if (dataLakeJobData.Schedule == DataLakeConstants.CustomCronScheduleName)
        {
            return dataLakeJobData.CustomCron;
        }

        return dataLakeJobData.Schedule;
    }
}
