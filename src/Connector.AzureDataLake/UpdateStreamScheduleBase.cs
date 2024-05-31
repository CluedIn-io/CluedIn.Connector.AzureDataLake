using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Jobs;
using CluedIn.Core.Providers;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake;

internal abstract class UpdateStreamScheduleBase
{  
    private readonly ApplicationContext _applicationContext;
    private readonly Guid _providerId;

    protected ApplicationContext ApplicationContext => _applicationContext;

    protected UpdateStreamScheduleBase(ApplicationContext applicationContext, Guid providerId)
    {
        _applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
        _providerId = providerId;
    }

    protected async Task UpdateStreamSchedule(ExecutionContext executionContext, StreamModel stream)
    {
        var jobServerClient = ApplicationContext.Container.Resolve<IJobServerClient>();
        var exportJob = ApplicationContext.Container.Resolve<AzureDataLakeExportEntitiesJob>();
        var hasExistingJob = HasExistingJob(jobServerClient, stream);
        if (!await IsDataLakeProvider(executionContext, stream))
        {
            if (hasExistingJob)
            {
                var neverCron = AzureDataLakeConstants.CronSchedules[AzureDataLakeConstants.JobScheduleNames.Never];
                exportJob.Schedule(jobServerClient, neverCron, stream.Id.ToString());
            }
            return;
        }

        var cronSchedule = await GetCronSchedule(executionContext, stream);
        exportJob.Schedule(jobServerClient, cronSchedule, stream.Id.ToString());
    }

    private bool HasExistingJob(IJobServerClient jobServerClient, StreamModel stream)
    {
        var allJobs = jobServerClient.GetAllRecurringJobs();
        if (allJobs.Any(jobId => jobId == $"CustomScheduledJob|{typeof(AzureDataLakeExportEntitiesJob).FullName}|{stream.Id}"))
        {
            return true;
        }

        return false;
    }

    private async Task<bool> IsDataLakeProvider(ExecutionContext executionContext, StreamModel stream)
    {
        if (stream.ConnectorProviderDefinitionId == null)
        {
            return false;
        }


        var organizationProviderDataStore = executionContext.Organization.DataStores.GetDataStore<ProviderDefinition>();
        var provider = await organizationProviderDataStore.GetByIdAsync(executionContext, stream.ConnectorProviderDefinitionId.Value);

        if (provider == null)
        {
            return false;
        }

        return provider.ProviderId == _providerId;
    }

    private static async Task<string> GetCronSchedule(ExecutionContext context, StreamModel stream)
    {
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
