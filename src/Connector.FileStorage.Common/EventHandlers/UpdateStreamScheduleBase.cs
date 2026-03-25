using System;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Events;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;

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

    protected async Task UpdateStreamScheduleFromStreamEvent(RemoteEvent remoteEvent)
    {
        if (!remoteEvent.TryGetResourceInfo("AccountId", "StreamId", out var organizationId, out var streamId))
        {
            return;
        }

        var streamRepository = ApplicationContext.Container.Resolve<IStreamRepository>();
        var executionContext = ApplicationContext.CreateExecutionContext(organizationId);
        var stream = await streamRepository.GetStream(executionContext, streamId);
        if (stream == null)
        {
            return;
        }

        await UpdateStreamSchedule(executionContext, stream);
    }

    protected async Task UpdateStreamSchedule(ExecutionContext executionContext, StreamModel stream)
    {
        if (!await IsDataLakeProvider(executionContext, stream))
        {
            return;
        }

        UpdateJobServerClientSchedule(stream);

        var schedule = await JobDataFactory.GetScheduleAsync(executionContext, stream);
        var jobKey = stream.Id.ToString();
        if (schedule.IsNeverCron)
        {
            executionContext.Log.LogDebug("Disable export for stream {StreamId} that has schedule '{Schedule}'.", stream.Id, schedule.CronSchedule);
            _ = JobQueue.TryRemove(jobKey, out var _);
        }
        else
        {
            executionContext.Log.LogDebug("Enable export for stream {StreamId} using schedule '{Schedule}'.", stream.Id, schedule.CronSchedule);
            var queuedJob = new QueuedJob(jobKey, executionContext.Organization.Id, ExportEntitiesJobType, schedule, DateTimeOffsetProvider.GetCurrentUtcTime());
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
}
