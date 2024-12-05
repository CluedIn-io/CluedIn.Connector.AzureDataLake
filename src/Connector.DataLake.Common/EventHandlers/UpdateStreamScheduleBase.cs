using System;
using System.IO;
using System.Threading.Tasks;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Events;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json.Linq;

namespace CluedIn.Connector.DataLake.Common.EventHandlers;

internal abstract class UpdateStreamScheduleBase
{

    protected ApplicationContext ApplicationContext { get; }
    protected IDataLakeConstants Constants { get; }
    protected IDataLakeJobDataFactory JobDataFactory { get; }

    protected Type ExportEntitiesJobType { get; }
    protected IScheduledJobQueue JobQueue { get; }
    protected IDateTimeOffsetProvider DateTimeOffsetProvider { get; }

    protected UpdateStreamScheduleBase(
        ApplicationContext applicationContext,
        IDataLakeConstants constants,
        IDataLakeJobDataFactory jobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider,
        Type exportEntitiesJobType,
        IScheduledJobQueue jobQueue)
    {
        ApplicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
        Constants = constants ?? throw new ArgumentNullException(nameof(constants));
        JobDataFactory = jobDataFactory ?? throw new ArgumentNullException(nameof(jobDataFactory));
        DateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
        ExportEntitiesJobType = exportEntitiesJobType ?? throw new ArgumentNullException(nameof(exportEntitiesJobType));
        JobQueue = jobQueue ?? throw new ArgumentNullException(nameof(jobQueue));
    }

    protected static bool TryGetResourceInfo(RemoteEvent remoteEvent, string organizationIdKey, string resourceIdKey, out Guid organizationId, out Guid resourceId)
    {
        if (!(remoteEvent.EventData?.StartsWith("{") ?? false))
        {
            setResultToDefault(out organizationId, out resourceId);
            return false;
        }

        var eventObject = JsonUtility.Deserialize<JObject>(remoteEvent.EventData);
        if (!eventObject.TryGetValue(organizationIdKey, out var accountIdToken))
        {
            setResultToDefault(out organizationId, out resourceId);
            return false;
        }

        if (!eventObject.TryGetValue(resourceIdKey, out var resourceIdToken))
        {
            setResultToDefault(out organizationId, out resourceId);
            return false;
        }

        var accountId = accountIdToken.Value<string>();
        var resourceIdString = resourceIdToken.Value<string>();
        resourceId = new Guid(resourceIdString);
        organizationId = new Guid(accountId);

        return true;

        static void setResultToDefault(out Guid organizationId, out Guid resourceId)
        {
            organizationId = Guid.Empty;
            resourceId = Guid.Empty;
        }
    }

    protected async Task UpdateStreamScheduleFromStreamEvent(RemoteEvent remoteEvent)
    {
        if (!TryGetResourceInfo(remoteEvent, "AccountId", "StreamId", out var organizationId, out var streamId))
        {
            return;
        }

        var streamRepository = ApplicationContext.Container.Resolve<IStreamRepository>();
        var stream = await streamRepository.GetStream(streamId);
        if (stream == null)
        {
            return;
        }

        var executionContext = ApplicationContext.CreateExecutionContext(organizationId);
        await UpdateStreamSchedule(executionContext, stream);
    }

    protected async Task UpdateStreamSchedule(ExecutionContext executionContext, StreamModel stream)
    {
        if (!await IsDataLakeProvider(executionContext, stream))
        {
            return;
        }

        UpdateJobServerClientSchedule(stream);

        var cronSchedule = await GetCronScheduleAsync(executionContext, stream);
        var jobKey = stream.Id.ToString();
        if (cronSchedule == CronSchedules.NeverCron)
        {
            _ = JobQueue.TryRemove(jobKey, out var _);
        }
        else
        {
            var queuedJob = new QueuedJob(jobKey, executionContext.Organization.Id, ExportEntitiesJobType, cronSchedule, DateTimeOffsetProvider.GetCurrentUtcTime());
            JobQueue.AddOrUpdateJob(queuedJob);
        }
    }

    private void UpdateJobServerClientSchedule(StreamModel stream)
    {
        var jobServerClient = ApplicationContext.Container.Resolve<IJobServerClient>();
        var exportJob = ApplicationContext.Container.Resolve(ExportEntitiesJobType) as DataLakeExportEntitiesJobBase;
        exportJob.Schedule(jobServerClient, CronSchedules.NeverCron, stream.Id.ToString());
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

        return provider.ProviderId == Constants.ProviderId;
    }

    private async Task<string> GetCronScheduleAsync(ExecutionContext context, StreamModel stream)
    {
        var containerName = stream.ContainerName;
        var providerDefinitionId = stream.ConnectorProviderDefinitionId.Value;
        var configurations = await JobDataFactory.GetConfiguration(context, providerDefinitionId, containerName);
        if (configurations.IsStreamCacheEnabled
            && stream.Status == StreamStatus.Started
            && CronSchedules.TryGetCronSchedule(configurations.Schedule, out var retrievedSchedule))
        {
            context.Log.LogDebug("Enable export for stream {StreamId} using schedule '{Schedule}'.", stream.Id, retrievedSchedule);
            return retrievedSchedule;
        }

        context.Log.LogDebug("Disable export for stream {StreamId} that has schedule '{Schedule}'.", stream.Id, configurations.Schedule);

        return CronSchedules.NeverCron;
    }
}
