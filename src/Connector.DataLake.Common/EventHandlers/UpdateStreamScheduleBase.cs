using System;
using System.Linq;
using System.Threading.Tasks;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.EventHandlers;

internal abstract class UpdateStreamScheduleBase
{
    private readonly ApplicationContext _applicationContext;
    private readonly IDataLakeConstants _constants;
    private readonly IDataLakeJobDataFactory _jobDataFactory;
    private readonly Type _exportEntitiesJobType;

    protected ApplicationContext ApplicationContext => _applicationContext;

    protected Type ExportEntitiesJobType => _exportEntitiesJobType;

    protected UpdateStreamScheduleBase(
        ApplicationContext applicationContext,
        IDataLakeConstants dataLakeConstants,
        IDataLakeJobDataFactory jobDataFactory,
        Type exportEntitiesJobType)
    {
        _applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
        _constants = dataLakeConstants;
        _jobDataFactory = jobDataFactory ?? throw new ArgumentNullException(nameof(jobDataFactory));
        _exportEntitiesJobType = exportEntitiesJobType;
    }

    protected async Task UpdateStreamSchedule(ExecutionContext executionContext, StreamModel stream)
    {
        var jobServerClient = ApplicationContext.Container.Resolve<IJobServerClient>();
        var exportJob = ApplicationContext.Container.Resolve(ExportEntitiesJobType) as DataLakeExportEntitiesJobBase;
        var hasExistingJob = HasExistingJob(jobServerClient, stream);
        if (!await IsDataLakeProvider(executionContext, stream))
        {
            if (hasExistingJob)
            {
                var neverCron = DataLakeConstants.CronSchedules[DataLakeConstants.JobScheduleNames.Never];
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
        if (allJobs.Any(jobId => jobId == $"CustomScheduledJob|{ExportEntitiesJobType.FullName}|{stream.Id}"))
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

        return provider.ProviderId == _constants.ProviderId;
    }

    private async Task<string> GetCronSchedule(ExecutionContext context, StreamModel stream)
    {


        var containerName = stream.ContainerName;
        var providerDefinitionId = stream.ConnectorProviderDefinitionId.Value;
        var configurations = await _jobDataFactory.GetConfiguration(context, providerDefinitionId, containerName);
        if (configurations.IsStreamCacheEnabled
            && stream.Status == StreamStatus.Started
            && DataLakeConstants.CronSchedules.TryGetValue(configurations.Schedule, out var retrievedSchedule))
        {
            context.Log.LogDebug("Enable export for stream {StreamId} using schedule '{Schedule}'.", stream.Id, retrievedSchedule);
            return retrievedSchedule;
        }

        context.Log.LogDebug("Disable export for stream {StreamId} that has schedule '{Schedule}'.", stream.Id, configurations.Schedule);

        return DataLakeConstants.CronSchedules[DataLakeConstants.JobScheduleNames.Never];
    }
}
