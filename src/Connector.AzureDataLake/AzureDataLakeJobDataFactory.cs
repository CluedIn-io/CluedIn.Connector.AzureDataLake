using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;

namespace CluedIn.Connector.AzureDataLake;

public class AzureDataLakeJobDataFactory : DataLakeJobDataFactoryBase, IDataLakeJobDataFactory
{
    public async virtual Task<IDataLakeJobData> GetConfiguration(ExecutionContext executionContext, Guid providerDefinitionId, string containerName)
    {
        var authenticationDetails = await GetAuthenticationDetails(executionContext, providerDefinitionId);
        return await GetConfiguration(executionContext, authenticationDetails.Authentication.ToDictionary(detail => detail.Key, detail => detail.Value), containerName);
    }

    public virtual Task<IDataLakeJobData> GetConfiguration(ExecutionContext executionContext, IDictionary<string, object> authenticationDetails, string containerName = null)
    {
        UpdateAuthenticationDetails(executionContext, authenticationDetails);

        var configurations = new AzureDataLakeConnectorJobData(authenticationDetails, containerName);
        return Task.FromResult<IDataLakeJobData>(configurations);
    }
}
