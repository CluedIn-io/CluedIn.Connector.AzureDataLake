using System;
using System.Threading.Tasks;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Jobs;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.DataLake.Common.EventHandlers;

internal abstract class UpdateStreamScheduleBase
{
    protected ApplicationContext ApplicationContext { get; }
    protected IDataLakeConstants Constants { get; }

    protected Type ExportEntitiesJobType { get; }

    protected UpdateStreamScheduleBase(
        ApplicationContext applicationContext,
        IDataLakeConstants constants,
        Type exportEntitiesJobType)
    {
        ApplicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
        Constants = constants ?? throw new ArgumentNullException(nameof(constants));
        ExportEntitiesJobType = exportEntitiesJobType;
    }

    protected async Task UpdateStreamSchedule(ExecutionContext executionContext, StreamModel stream)
    {
        if (!await IsDataLakeProvider(executionContext, stream))
        {
            return;
        }
        var jobServerClient = ApplicationContext.Container.Resolve<IJobServerClient>();
        var exportJob = ApplicationContext.Container.Resolve(ExportEntitiesJobType) as DataLakeExportEntitiesJobBase;
        var neverCron = DataLakeConstants.CronSchedules[DataLakeConstants.JobScheduleNames.Never];
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

        return provider.ProviderId == Constants.ProviderId;
    }
}
