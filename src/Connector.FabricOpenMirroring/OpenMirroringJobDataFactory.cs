using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Connector.FabricOpenMirroring.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FabricOpenMirroring;

public class OpenMirroringJobDataFactory : DataLakeJobDataFactoryBase, IDataLakeJobDataFactory
{
    protected override Task<IDataLakeJobData> CreateJobData(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName)
    {
        return Task.FromResult<IDataLakeJobData>(new OpenMirroringConnectorJobData(authenticationDetails, containerName));
    }

    public override async Task<IDataLakeJobData> GetConfiguration(ExecutionContext executionContext, IDictionary<string, object> authenticationDetails, string containerName = null)
    {
        if (!authenticationDetails.TryGetValue(OpenMirroringConstants.MirroredDatabaseName, out var value)
            || string.IsNullOrWhiteSpace(value?.ToString()))
        {
            if (authenticationDetails.TryGetValue(DataLakeConstants.ProviderDefinitionIdKey, out var providerDefinitionId))
            {
                authenticationDetails[OpenMirroringConstants.MirroredDatabaseName] = $"CluedIn_ExportTarget_{providerDefinitionId:N}";
                //authenticationDetails[OpenMirroringConstants.ShouldCreateMirroredDatabase] = true;
            }
        }

        return await base.GetConfiguration(executionContext, authenticationDetails, containerName);
    }
    public override async Task<IDataLakeClient> CreateDataLakeClient(ExecutionContext executionContext, IDataLakeJobData jobData)
    {
        return await CreateDataLakeClient(executionContext, jobData as OpenMirroringConnectorJobData);
    }

    internal virtual Task<OpenMirroringClient> CreateDataLakeClient(ExecutionContext executionContext, OpenMirroringConnectorJobData jobData)
    {
        var logger = executionContext.ApplicationContext.Container.Resolve<ILogger<OpenMirroringClient>>();
        var dateTimeOffsetProvider = executionContext.ApplicationContext.Container.Resolve<IDateTimeOffsetProvider>();
        var client = new OpenMirroringClient(logger, dateTimeOffsetProvider, jobData);
        return Task.FromResult(client);
    }
}
