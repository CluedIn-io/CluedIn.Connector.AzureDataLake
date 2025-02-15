﻿using System;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Jobs;

namespace CluedIn.Connector.DataLake.Common;

internal abstract class DataLakeJobBase : JobBase, ICustomScheduledJob, IDataLakeJob
{
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;
    protected DataLakeJobBase(ApplicationContext appContext, IDateTimeOffsetProvider dateTimeOffsetProvider) : base(appContext, JobType.CustomScheduledJob)
    {
        _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
    }

    protected override ExecutionContext CreateExecutionContext(ExecutionContext systemContext, JobArgs args)
    {
        return systemContext;
    }

    protected override void DoRun(ExecutionContext context, JobArgs args)
    {
        var now = _dateTimeOffsetProvider.GetCurrentUtcTime();
        var asOfTime = args.Schedule == CronSchedules.NeverCron || !CronSchedules.TryGetCronSchedule(args.Schedule, out _)
            ? now
            : CronSchedules.GetPreviousOccurrence(args.Schedule, now.AddSeconds(-1));

        DoRunAsync(context, new DataLakeJobArgs(args, isTriggeredFromJobServer: true, asOfTime ?? now)).GetAwaiter().GetResult();
    }

    public abstract Task DoRunAsync(ExecutionContext context, IDataLakeJobArgs args);

    public abstract Task<bool> HasMissed(ExecutionContext context, IDataLakeJobArgs args);

    /// <summary>
    /// Register and schedule current job for recurrent run.
    /// </summary>
    /// <param name="jobServerClient">Instance of job scheduler</param>
    /// <param name="cronSchedule">Schedule in cron syntax. Use <see cref="Cron"/> helper for convenience.</param>
    /// <param name="jobInstanceId">Optional identifier of particular job instance. Should be used if you schedule more than one instance of job of the current type.</param>
    public void Schedule(IJobServerClient jobServerClient, string cronSchedule, string jobInstanceId = null)
    {
        Schedule(jobServerClient, new JobArgs { Schedule = cronSchedule }, jobInstanceId);
    }

    /// <summary>
    /// Register and schedule current job for recurrent run.
    /// </summary>
    /// <param name="jobServerClient">Instance of job scheduler</param>
    /// <param name="jobArgs">Job arguments passed back to job on job execution</param>
    /// <param name="jobInstanceId">Optional identifier of particular job instance. Should be used if you schedule more than one instance of job of the current type.</param>
    protected void Schedule(IJobServerClient jobServerClient, JobArgs jobArgs, string jobInstanceId = null)
    {
        jobArgs.Message = jobInstanceId;
        jobServerClient.Run(this, jobArgs);
    }
}
