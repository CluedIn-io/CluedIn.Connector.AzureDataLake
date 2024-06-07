using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.AzureDataLake.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Jobs;
using CluedIn.Core.Providers;
using CluedIn.Core.Streams;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake;

internal abstract class UpdateStreamScheduleBase
{  
    private readonly ApplicationContext _applicationContext;
    private readonly Guid _providerId;

    protected ApplicationContext ApplicationContext => _applicationContext;

    protected UpdateStreamScheduleBase(ApplicationContext applicationContext, Guid providerId)
    {
        _applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
        _providerId = providerId;
    }

    protected async Task UpdateStreamSchedule(ExecutionContext executionContext, StreamModel stream)
    {
        var jobServerClient = ApplicationContext.Container.Resolve<IJobServerClient>();
        var exportJob = ApplicationContext.Container.Resolve<AzureDataLakeExportEntitiesJob>();
        var neverCron = AzureDataLakeConstants.CronSchedules[AzureDataLakeConstants.JobScheduleNames.Never];
        exportJob.Schedule(jobServerClient, neverCron, stream.Id.ToString());
    }
}
