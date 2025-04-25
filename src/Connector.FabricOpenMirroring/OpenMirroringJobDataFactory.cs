using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;

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
}
