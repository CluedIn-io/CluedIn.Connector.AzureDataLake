using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Data.Relational;
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
                removeExportJob(jobKey, warnIfNotRemoved: false);
                return;
            }

            var jobSchedule = await _dataLakeJobDataFactory.GetScheduleAsync(executionContext, stream);
            if (IsExportJobDisabled(jobSchedule))
            {
                _logger.LogDebug("Stream {StreamId} export is disabled. Removing it from scheduler '{SchedulerName}'.", stream.Id, _schedulerName);
                removeExportJob(jobKey, warnIfNotRemoved: false);
                return;
            }

            _logger.LogDebug("Enable export for stream {StreamId} using schedule '{Schedule}'.", stream.Id, jobSchedule.CronSchedule);
            var streamStartFrom = stream.ModifiedAt ?? stream.CreatedAt;
            var startFrom = definition.CreatedDate.HasValue && definition.CreatedDate > streamStartFrom ? definition.CreatedDate.Value : streamStartFrom;
            var job = new QueuedJob(jobKey, stream.OrganizationId, _exportEntitiesJobType, jobSchedule, startFrom);
            jobQueue.AddOrUpdateJob(job);
        }

        var allJobKeys = jobQueue.GetAllJobs().Select(job => job.Key);
        var jobKeysToRemove = allJobKeys.Except(streamIdStrings);
        var removed = 0;
        foreach (var jobKey in jobKeysToRemove)
        {
            removeExportJob(jobKey, warnIfNotRemoved: true);
            removed++;
        }

        if (removed > 0)
        {
            _logger.LogInformation("Total of obsolete {RemovedCount} stream jobs removed from scheduler '{SchedulerName}'.", removed, _schedulerName);
        }

        void removeExportJob(string jobKey, bool warnIfNotRemoved)
        {
            if (jobQueue.TryRemove(jobKey, out var _))
            {
                _logger.LogDebug("Stream export for stream {StreamId} removed from scheduler '{SchedulerName}'.", jobKey, _schedulerName);
            }
            else if (warnIfNotRemoved)
            {
                _logger.LogWarning("Failed to disable stream export for stream {StreamId} from scheduler '{SchedulerName}'.", jobKey, _schedulerName);
            }
        }
    }

    private static bool IsExportJobDisabled(Schedule schedule)
    {
        return schedule.IsNeverCron;
    }
}
