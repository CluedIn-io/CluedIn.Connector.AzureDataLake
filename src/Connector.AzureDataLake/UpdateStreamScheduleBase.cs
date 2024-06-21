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
        if (!await IsDataLakeProvider(executionContext, stream))
        {
            return;
        }
        var jobServerClient = ApplicationContext.Container.Resolve<IJobServerClient>();
        var exportJob = ApplicationContext.Container.Resolve<AzureDataLakeExportEntitiesJob>();
        var neverCron = AzureDataLakeConstants.CronSchedules[AzureDataLakeConstants.JobScheduleNames.Never];
        exportJob.Schedule(jobServerClient, neverCron, stream.Id.ToString());
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

        return provider.ProviderId == _providerId;
    }
}
