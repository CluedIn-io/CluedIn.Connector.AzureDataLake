using System;
using System.Threading.Tasks;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.DataLake.Common.EventHandlers;

internal abstract class UpdateStreamScheduleBase
{
    protected ApplicationContext ApplicationContext { get; }

    protected Type ExportEntitiesJobType { get; }

    protected UpdateStreamScheduleBase(
        ApplicationContext applicationContext,
        Type exportEntitiesJobType)
    {
        ApplicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
        ExportEntitiesJobType = exportEntitiesJobType;
    }

    protected Task UpdateStreamSchedule(ExecutionContext executionContext, StreamModel stream)
    {
        var jobServerClient = ApplicationContext.Container.Resolve<IJobServerClient>();
        var exportJob = ApplicationContext.Container.Resolve(ExportEntitiesJobType) as DataLakeExportEntitiesJobBase;
        var neverCron = DataLakeConstants.CronSchedules[DataLakeConstants.JobScheduleNames.Never];
        exportJob.Schedule(jobServerClient, neverCron, stream.Id.ToString());
        return Task.CompletedTask;
    }
}
