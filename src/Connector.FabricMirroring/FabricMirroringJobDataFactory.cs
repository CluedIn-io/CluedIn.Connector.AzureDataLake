using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;

namespace CluedIn.Connector.FabricMirroring;

public class FabricMirroringJobDataFactory : DataLakeJobDataFactoryBase, IDataLakeJobDataFactory
{
    protected override Task<IDataLakeJobData> CreateJobData(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName)
    {
        return Task.FromResult<IDataLakeJobData>(new FabricMirroringConnectorJobData(authenticationDetails, containerName));
    }

    public override async Task<IDataLakeJobData> GetConfiguration(ExecutionContext executionContext, Guid providerDefinitionId, string containerName)
    {
        var authenticationDetails = await GetAuthenticationDetails(executionContext, providerDefinitionId);
        var dictionary = authenticationDetails.Authentication.ToDictionary(detail => detail.Key, detail => detail.Value);

        if (!dictionary.TryGetValue(FabricMirroringConstants.ItemName, out var value)
            || string.IsNullOrWhiteSpace(value?.ToString()))
        {
            dictionary[FabricMirroringConstants.ItemName] = $"CluedIn_ExportTarget_{providerDefinitionId:N}";
            dictionary[FabricMirroringConstants.ShouldCreateMirroredDatabase] = true;
        }

        return await GetConfiguration(executionContext, dictionary, containerName);
    }
}
