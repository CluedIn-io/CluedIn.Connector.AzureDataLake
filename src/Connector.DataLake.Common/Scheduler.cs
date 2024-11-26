using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common;

internal class Scheduler : IScheduledJobQueue, IScheduler
{
    protected readonly ILogger _logger;
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;
    private static readonly string _schedulerCron = "* * * * *";
    private static readonly TimeSpan _initialDelay = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan _errorDelay = TimeSpan.FromMinutes(1);

    protected readonly ApplicationContext _applicationContext;
    protected readonly string _schedulerName;

    private readonly ConcurrentDictionary<string, SchedulerQueuedJob> _jobMap = new ();
    private readonly List<Func<IScheduledJobQueue, Task>> _jobProducers;
    private DateTimeOffset _iterationTime;

    public Scheduler(
        ILogger logger,
        string componentName,
        ApplicationContext applicationContext,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
    {
        if (string.IsNullOrWhiteSpace(componentName))
        {
            throw new ArgumentException($"'{nameof(componentName)}' cannot be null or whitespace.", nameof(componentName));
        }

        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _schedulerName = componentName;
        _applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
        _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
        _jobProducers = new List<Func<IScheduledJobQueue, Task>>();
    }

    public virtual void AddJobProducer(Func<IScheduledJobQueue, Task> jobProducer)
    {
        if (jobProducer is null)
        {
            throw new ArgumentNullException(nameof(jobProducer));
        }

        _jobProducers.Add(jobProducer);
    }

    public async virtual Task RunAsync()
    {
        _logger.LogDebug("Waiting for {InitialDelay} before starting '{SchedulerName}' scheduler.", _initialDelay, _schedulerName);
        await Task.Delay(_initialDelay);

        _iterationTime = _dateTimeOffsetProvider.GetCurrentUtcTime();

        while (true)
        {
            _logger.LogDebug("Begin scheduling for '{SchedulerName}' scheduler.", _schedulerName);

            try
            {
                await RunSchedulerIterationAsync();
                _iterationTime = CronSchedules.GetNextOccurrence(_schedulerCron, _dateTimeOffsetProvider.GetCurrentUtcTime().AddSeconds(1));
                _logger.LogDebug("End scheduling for '{SchedulerName}' scheduler. Next run at {NextRunTime}", _schedulerName, _iterationTime);
                var delayToNextSchedule = _iterationTime - _dateTimeOffsetProvider.GetCurrentUtcTime();
                if (delayToNextSchedule.TotalSeconds > 0)
                {
                    await Task.Delay(delayToNextSchedule);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error scheduling for {SchedulerName}' scheduler. Waiting {DelayAfterError} before next scheduler run.", _schedulerName, _errorDelay);
                await Task.Delay(_errorDelay);
            }
        }
    }

    void IScheduledJobQueue.AddOrUpdateJob(QueuedJob job)
    {
        var jobNextRunTime = CronSchedules.GetNextOccurrence(job.CronSchedule, _iterationTime.AddSeconds(1));
        var schedulerJob = new SchedulerQueuedJob(
            job.Key,
            job.OrganizationId,
            job.Type,
            job.CronSchedule,
            job.StartFromTime,
            jobNextRunTime,
            null);
        _ = _jobMap.AddOrUpdate(job.Key, schedulerJob, (_, existingJob) =>
        {
            if (existingJob.CronSchedule == schedulerJob.CronSchedule)
            {
                _logger.LogDebug("Job {JobKey} of scheduler '{SchedulerName}' is has same cron schedule, not updating next run time.", job.Key, _schedulerName);
                var latestStartFromTime = job.StartFromTime > existingJob.StartFromTime ? job.StartFromTime : existingJob.StartFromTime;
                return existingJob with { StartFromTime = latestStartFromTime };
            }

            _logger.LogDebug("Job {JobKey} of scheduler '{SchedulerName}' is has different cron schedule, updating next run time.", job.Key, _schedulerName);
            return schedulerJob;
        });
    }

    bool IScheduledJobQueue.TryRemove(string key, out QueuedJob queuedJob)
    {
        var removed = _jobMap.TryRemove(key, out var jobData);
        queuedJob = jobData;
        return removed;
    }

    ICollection<QueuedJob> IScheduledJobQueue.GetAllJobs()
    {
        return _jobMap.Values.Select(job => job as QueuedJob).ToList();
    }

    protected async Task RunSchedulerIterationAsync()
    {
        foreach (var jobProducers in _jobProducers)
        {
            await jobProducers(this);
        }

        var executedJobData = await ExecuteJobsIfNeededAsync();
        UpdateNextJobTime(executedJobData);
    }

    private async Task<List<SchedulerQueuedJob>> ExecuteJobsIfNeededAsync()
    {
        _logger.LogDebug("Scheduler '{SchedulerName}' found {TotalJobs} jobs.", _schedulerName, _jobMap.Count);

        var executedJobData = new List<SchedulerQueuedJob>();
        foreach (var jobDataKvp in _jobMap)
        {
            var jobData = jobDataKvp.Value;
            var previousRunTime = CronSchedules.GetPreviousOccurrence(jobData.CronSchedule, jobData.NextRunTime.AddSeconds(-1));

            var executionContext = _applicationContext.CreateExecutionContext(jobData.OrganizationId);
            var jobInstance = CreateJobInstance(jobData.Type);

            var previousRunTimeArg = CreateDataLakeJobArgs(jobData, previousRunTime);

            // LastSuccessfulRunTime is only saved when HasMissed = false
            var shouldRerun = jobData.LastSuccessfulRunTime != previousRunTime && jobData.StartFromTime < previousRunTime
                ? await jobInstance.HasMissed(executionContext, previousRunTimeArg)
                : false;

            if (!shouldRerun)
            {
                saveLastSuccessfulRunTime(jobData, previousRunTime);
            }

            var nextRunTimeReached = jobData.NextRunTime <= _iterationTime;
            if (!nextRunTimeReached && !shouldRerun)
            {
                _logger.LogDebug("Job '{JobType}' with Key '{JobKey}' of scheduler '{SchedulerName}' is not supposed to run now. Next run time at {JobNextRunTime} based on {CronSchedule} cron.",
                    jobData.Type,
                    jobData.Key,
                    _schedulerName,
                    jobData.NextRunTime,
                    jobData.CronSchedule);
                continue;
            }

            _ = Task.Run(async () =>
            {
                try
                {
                    var executionContext = _applicationContext.CreateExecutionContext(jobData.OrganizationId);

                    if (shouldRerun)
                    {
                        _logger.LogInformation("Job '{JobType}' with Key '{JobKey}' of scheduler '{SchedulerName}' needs to be rerun for run time {JobPreviousRunTime} based on {CronSchedule} cron.",
                            jobData.Type,
                            jobData.Key,
                            _schedulerName,
                            previousRunTime,
                            jobData.CronSchedule);
                        await jobInstance.DoRunAsync(executionContext, previousRunTimeArg);
                    }

                    if (nextRunTimeReached)
                    {
                        await RunJobWithTimeAsync(executionContext, jobData, jobData.NextRunTime);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error when executing job {JobKey} of scheduler {SchedulerName}.", jobData.Key, _schedulerName);
                }
            });

            executedJobData.Add(jobData);
        }

        return executedJobData;

        void saveLastSuccessfulRunTime(SchedulerQueuedJob jobData, DateTimeOffset previousRunTime)
        {
            var newJobData = jobData with { LastSuccessfulRunTime = previousRunTime };
            var result = _jobMap.AddOrUpdate(newJobData.Key, newJobData, (_, _) => newJobData);

            if (result != newJobData)
            {
                _logger.LogWarning("Scheduler {SchedulerName} failed to update job next run time for job with key {JobKey}.", _schedulerName, jobData.Key);
            }
        }


    }

    private async Task RunJobWithTimeAsync(ExecutionContext executionContext, SchedulerQueuedJob job, DateTimeOffset time)
    {
        var jobArgs = CreateDataLakeJobArgs(job, time);
        var jobInstance = CreateJobInstance(job.Type);
        await jobInstance.DoRunAsync(executionContext, jobArgs);
    }

    private static DataLakeJobArgs CreateDataLakeJobArgs(SchedulerQueuedJob jobData, DateTimeOffset previousRunTime)
    {
        return new DataLakeJobArgs
        {
            Message = jobData.Key,
            Schedule = jobData.CronSchedule,
            IsTriggeredFromJobServer = false,
            InstanceTime = previousRunTime,
        };
    }

    private DataLakeJobBase CreateJobInstance(Type jobType)
    {
        if (_applicationContext.Container.Resolve(jobType) is not DataLakeJobBase jobInstance)
        {
            throw new ApplicationException($"Job {jobType} is not of type {typeof(DataLakeJobBase)}.");
        }

        return jobInstance;
    }

    private void UpdateNextJobTime(List<SchedulerQueuedJob> executedJobData)
    {
        foreach (var jobData in executedJobData)
        {
            var nextJobTime = CronSchedules.GetNextOccurrence(jobData.CronSchedule, _iterationTime.AddSeconds(1));

            var newJobData = jobData with { NextRunTime = nextJobTime };
            var result = _jobMap.AddOrUpdate(newJobData.Key, newJobData, (_, _) => newJobData);

            if (result != newJobData)
            {
                _logger.LogWarning("Scheduler failed to update job next run time for job with key {JobKey}.", jobData.Key);
            }
        }
    }

    protected record SchedulerQueuedJob(
        string Key,
        Guid OrganizationId,
        Type Type,
        string CronSchedule,
        DateTimeOffset StartFromTime,
        DateTimeOffset NextRunTime,
        DateTimeOffset? LastSuccessfulRunTime)
        : QueuedJob(
            Key,
            OrganizationId,
            Type,
            CronSchedule,
            StartFromTime);
}
