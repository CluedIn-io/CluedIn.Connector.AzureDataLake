using CluedIn.Core;
using CluedIn.Core.Jobs;

using System.Threading.Tasks;

namespace CluedIn.Connector.AzureDataLake;

internal abstract class AzureDataLakeJobBase : JobBase, ICustomScheduledJob
{
    public AzureDataLakeJobBase(ApplicationContext appContext) : base(appContext, JobType.CustomScheduledJob)
    {
    }

    protected override ExecutionContext CreateExecutionContext(ExecutionContext systemContext, JobArgs args)
    {
        return systemContext;
    }

    protected override void DoRun(ExecutionContext context, JobArgs args)
    {
        DoRunAsync(context, args).GetAwaiter().GetResult();
    }

    protected abstract Task DoRunAsync(ExecutionContext context, JobArgs args);

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
