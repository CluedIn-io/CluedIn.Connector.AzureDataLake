using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common;

internal class DataLakeScheduler : Scheduler
{
    private readonly IDataLakeConstants _dataLakeConstants;
    private readonly IDataLakeJobDataFactory _dataLakeJobDataFactory;
    private readonly Type _exportEntitiesJobType;

    public DataLakeScheduler(
        ILogger logger,
        string dataLakeComponentName,
        ApplicationContext applicationContext,
        IDateTimeOffsetProvider dateTimeOffsetProvider,
        IDataLakeConstants dataLakeConstants,
        IDataLakeJobDataFactory dataLakeJobDataFactory,
        Type exportEntitiesJobType) : base(logger, dataLakeComponentName, applicationContext, dateTimeOffsetProvider)
    {
        _dataLakeConstants = dataLakeConstants ?? throw new ArgumentNullException(nameof(dataLakeConstants));
        _dataLakeJobDataFactory = dataLakeJobDataFactory ?? throw new ArgumentNullException(nameof(dataLakeJobDataFactory));
        _exportEntitiesJobType = exportEntitiesJobType ?? throw new ArgumentNullException(nameof(exportEntitiesJobType));
    }

    public override Task RunAsync()
    {
        AddJobProducer(StreamJobProducerAsync);
        return base.RunAsync();
    }

    private async Task StreamJobProducerAsync(IScheduledJobQueue jobQueue)
    {
        var streamIdStrings = new HashSet<string>();
        await StreamsHelper.ForEachStreamAsync(_applicationContext, _dataLakeConstants, addStreamExportJobs);
        async Task addStreamExportJobs(ExecutionContext executionContext, ProviderDefinition definition, StreamModel stream)
        {
            streamIdStrings.Add(stream.Id.ToString());
            var jobKey = stream.Id.ToString();
            if (stream.Mode != StreamMode.Sync)
            {
                _logger.LogDebug("Stream {StreamId} is not in {Mode} mode. Removing it from scheduler '{SchedulerName}'.", stream.Id, StreamMode.Sync, _schedulerName);
                removeExportJob(jobKey);
                return;
            }

            var jobCronSchedule = await GetCronScheduleAsync(executionContext, stream);
            if (IsExportJobDisabled(jobCronSchedule))
            {
                _logger.LogDebug("Stream {StreamId} is disabled. Removing it from scheduler '{SchedulerName}'.", stream.Id, _schedulerName);
                removeExportJob(jobKey);
                return;
            }

            var streamStartFrom = stream.ModifiedAt ?? stream.CreatedAt;
            var startFrom = definition.CreatedDate.HasValue && definition.CreatedDate > streamStartFrom ? definition.CreatedDate.Value : streamStartFrom;
            var job = new QueuedJob(jobKey, stream.OrganizationId, _exportEntitiesJobType, jobCronSchedule, startFrom);
            jobQueue.AddOrUpdateJob(job);
        }

        var allJobKeys = jobQueue.GetAllJobs().Select(job => job.Key);
        var jobKeysToRemove = allJobKeys.Except(streamIdStrings);
        var removed = 0;
        foreach (var jobKey in jobKeysToRemove)
        {
            removeExportJob(jobKey);
            removed++;
        }

        if (removed > 0)
        {
            _logger.LogInformation("Total of obsolete {RemovedCount} stream jobs removed from scheduler '{SchedulerName}'.", removed, _schedulerName);
        }

        void removeExportJob(string jobKey)
        {
            if (jobQueue.TryRemove(jobKey, out var _))
            {
                _logger.LogDebug("Stream export for stream {StreamId} removed from scheduler '{SchedulerName}'.", jobKey, _schedulerName);
            }
            else
            {
                _logger.LogWarning("Failed to disable stream export for stream {StreamId} from scheduler '{SchedulerName}'..", jobKey, _schedulerName);
            }
        }
    }

    private static bool IsExportJobDisabled(string jobCronSchedule)
    {
        return jobCronSchedule == CronSchedules.NeverCron;
    }

    private async Task<string> GetCronScheduleAsync(ExecutionContext context, StreamModel stream)
    {
        var configurations = await _dataLakeJobDataFactory.GetConfiguration(
            context,
            stream.ConnectorProviderDefinitionId.Value,
            stream.ContainerName);

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
