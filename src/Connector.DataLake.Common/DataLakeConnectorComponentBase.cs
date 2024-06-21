using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CluedIn.Connector.DataLake.Common.EventHandlers;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Jobs;
using CluedIn.Core.Server;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using ComponentHost;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common;

public abstract class DataLakeConnectorComponentBase : ServiceApplicationComponent<IServer>
{
    private UpdateExportTargetEventHandler _updateExportTargetHandler;
    private ChangeStreamStateEventHandler _changeStreamStateEvent;
    private UpdateStreamEventHandler _updateStreamEvent;

    private static readonly NCrontab.CrontabSchedule _schedulerCron = NCrontab.CrontabSchedule.Parse("* * * * *");
    private static readonly TimeSpan _initialSchedulerDelay = TimeSpan.FromSeconds(3);

    protected DataLakeConnectorComponentBase(ComponentInfo componentInfo) : base(componentInfo)
    {
    }

    protected void SubscribeToEvents(IDataLakeConstants constants, Type exportEntitiesJobType)
    {
        _updateExportTargetHandler = new(ApplicationContext, constants, exportEntitiesJobType);
        _changeStreamStateEvent = new(ApplicationContext, constants, exportEntitiesJobType);
        _updateStreamEvent = new(ApplicationContext, constants, exportEntitiesJobType);
    }

    protected async Task RunScheduler(IDataLakeConstants dataLakeConstants, IDataLakeJobDataFactory dataLakeJobDataFactory, Type jobType)
    {
        Log.LogDebug("Waiting for {InitialDelay} before starting scheduler.", _initialSchedulerDelay);
        await Task.Delay(_initialSchedulerDelay);

        var next = DateTime.UtcNow;

        var jobMap = new ConcurrentDictionary<Guid, JobData>();
        while (true)
        {
            Log.LogDebug($"Scheduler begin scheduling.");

            try
            {
                await Schedule(next, jobMap, dataLakeConstants, dataLakeJobDataFactory, jobType);
                next = _schedulerCron.GetNextOccurrence(DateTime.UtcNow.AddSeconds(1));
                Log.LogDebug("Scheduler completed scheduling. Next run at {NextRunTime}.", next);
                var delayToNextSchedule = next - DateTime.UtcNow;
                if (delayToNextSchedule.TotalSeconds > 0)
                {
                    await Task.Delay(delayToNextSchedule);
                }
            }
            catch(Exception ex)
            {
                Log.LogError(ex, "Error occurred when scheduling.");
            }
        }
    }

    protected async Task Schedule(DateTime next, ConcurrentDictionary<Guid, JobData> jobMap, IDataLakeConstants dataLakeConstants, IDataLakeJobDataFactory dataLakeJobDataFactory, Type jobType)
    {
        var streamRepository = Container.Resolve<IStreamRepository>();
        var streams = streamRepository.GetAllStreams().ToList();

        var organizationIds = streams.Select(s => s.OrganizationId).Distinct().ToArray();

        foreach (var orgId in organizationIds)
        {
            var org = new Organization(ApplicationContext, orgId);
            var executionContext = ApplicationContext.CreateExecutionContext(orgId);

            foreach (var provider in org.Providers.AllProviderDefinitions.Where(x =>
                         x.ProviderId == dataLakeConstants.ProviderId))
            {
                foreach (var stream in streams.Where(s => s.ConnectorProviderDefinitionId == provider.Id))
                {
                    if (stream.Mode != StreamMode.Sync)
                    {
                        Log.LogDebug("Stream is not in {Mode}. Skipping", StreamMode.Sync);
                        continue;
                    }

                    var jobCronSchedule = await GetCronSchedule(executionContext, stream, dataLakeJobDataFactory);
                    if (jobCronSchedule == DataLakeConstants.CronSchedules[DataLakeConstants.JobScheduleNames.Never])
                    {
                        Log.LogDebug("Stream export for stream {StreamId} is disabled.", stream.Id);
                        _ = jobMap.TryRemove(stream.Id, out _);
                        continue;
                    }

                    var jobSchedule = NCrontab.CrontabSchedule.Parse(jobCronSchedule);
                    var nextJobTime = jobSchedule.GetNextOccurrence(next.AddSeconds(1));
                    var job = new JobData(jobType, stream.OrganizationId, stream.Id, jobCronSchedule, nextJobTime);
                    jobMap.AddOrUpdate(stream.Id, job, (_, existingJob) =>
                    {
                        if (existingJob.CronSchedule == job.CronSchedule)
                        {
                            Log.LogDebug("Stream {StreamId} is has same cron schedule, not updating next run time", stream.Id);
                            return existingJob;
                        }

                        Log.LogDebug("Stream {StreamId} is has different cron schedule, updating next run time", stream.Id);
                        return job;
                    });
                }
            }
        }

        Log.LogDebug("Scheduler found {TotalJobs} jobs.", jobMap.Count);

        var executedJobData = new List<JobData>();
        foreach (var jobDataKvp in jobMap)
        {
            var jobData = jobDataKvp.Value;
            if (jobData.NextRunTime > next)
            {
                Log.LogDebug("Job '{JobType}' for Stream {StreamId} is not supposed to run now. Next run time at {JobNextRunTime} based on {CronSchedule} cron.",
                    jobData.Type,
                    jobData.StreamId,
                    jobData.NextRunTime,
                    jobData.CronSchedule);
                continue;
            }

            _ = Task.Run(async () =>
            {
                try
                {
                    var jobInstance = Container.Resolve(jobData.Type) as DataLakeJobBase;
                    if (jobInstance == null)
                    {
                        throw new ApplicationException($"Job {jobInstance.GetType()} is not of type {typeof(DataLakeJobBase)}.");
                    }
                    var executionContext = ApplicationContext.CreateExecutionContext(jobData.OrganizationId);
                    await jobInstance.DoRunAsync(executionContext, new JobArgs
                    {
                        Message = jobData.StreamId.ToString(),
                        Schedule = jobData.CronSchedule,
                    });
                }
                catch (Exception ex)
                {
                    Log.LogError(ex, "Error when executing job.");
                }
            });
            executedJobData.Add(jobData);
        }

        foreach (var jobData in executedJobData)
        {
            var jobCronSchedule = NCrontab.CrontabSchedule.Parse(jobData.CronSchedule);
            var nextJobTime = jobCronSchedule.GetNextOccurrence(next.AddSeconds(1));

            var newJobData = jobData with { NextRunTime = nextJobTime };
            var result = jobMap.AddOrUpdate(newJobData.StreamId, newJobData, (_, _) => newJobData);

            if (result != newJobData)
            {
                Log.LogWarning("Scheduler failed to update job next run time for stream {StreamId}.", jobData.StreamId);
            }
        }
    }

    private static async Task<string> GetCronSchedule(ExecutionContext context, StreamModel stream, IDataLakeJobDataFactory dataLakeJobDataFactory)
    {
        var containerName = stream.ContainerName;
        var providerDefinitionId = stream.ConnectorProviderDefinitionId.Value;
        var configurations = await dataLakeJobDataFactory.GetConfiguration(context, providerDefinitionId, containerName);
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

    protected record JobData(Type Type, Guid OrganizationId, Guid StreamId, string CronSchedule, DateTime NextRunTime);
}
