using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Core;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake;

internal abstract class UpdateStreamScheduleBase
{  
    private readonly ApplicationContext _applicationContext;

    protected ApplicationContext ApplicationContext => _applicationContext;

    protected UpdateStreamScheduleBase(ApplicationContext applicationContext)
    {
        _applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
    }

    protected async Task UpdateStreamSchedule(ExecutionContext executionContext, StreamModel stream)
    {
        var jobServerClient = ApplicationContext.Container.Resolve<IJobServerClient>();
        var exportJob = ApplicationContext.Container.Resolve<ExportEntitiesJob>();
        var cronSchedule = await GetCronSchedule(executionContext, stream);
        exportJob.Schedule(jobServerClient, cronSchedule, stream.Id.ToString());
    }

    private static async Task<string> GetCronSchedule(ExecutionContext context, StreamModel stream)
    {
        if (!stream.ConnectorProviderDefinitionId.HasValue)
        {
            return AzureDataLakeConstants.CronSchedules[AzureDataLakeConstants.JobScheduleNames.Never];
        }

        var containerName = stream.ContainerName;
        var providerDefinitionId = stream.ConnectorProviderDefinitionId.Value;
        var configurations = await AzureDataLakeConnectorJobData.Create(context, providerDefinitionId, containerName);
        if (configurations.IsStreamCacheEnabled
            && stream.Status == StreamStatus.Started
            && AzureDataLakeConstants.CronSchedules.TryGetValue(configurations.Schedule, out var retrievedSchedule))
        {
            context.Log.LogDebug("Enable export for stream {StreamId} using schedule '{Schedule}'.", stream.Id, retrievedSchedule);
            return retrievedSchedule;
        }

        context.Log.LogDebug("Disable export for stream {StreamId} that has schedule '{Schedule}'.", stream.Id, configurations.Schedule);

        return AzureDataLakeConstants.CronSchedules[AzureDataLakeConstants.JobScheduleNames.Never];
    }
}
