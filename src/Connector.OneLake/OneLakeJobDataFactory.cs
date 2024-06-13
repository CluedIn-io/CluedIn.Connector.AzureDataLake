using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Core;

namespace CluedIn.Connector.OneLake;

public class OneLakeJobDataFactory : DataLakeJobDataFactoryBase, IDataLakeJobDataFactory
{
    public async Task<IDataLakeJobData> GetConfiguration(ExecutionContext executionContext, Guid providerDefinitionId, string containerName)
    {
        var authenticationDetails = await GetAuthenticationDetails(executionContext, providerDefinitionId);
        return await GetConfiguration(executionContext, authenticationDetails.Authentication.ToDictionary(detail => detail.Key, detail => detail.Value), containerName);
    }

    public Task<IDataLakeJobData> GetConfiguration(ExecutionContext executionContext, IDictionary<string, object> authenticationDetails, string containerName = null)
    {
        UpdateAuthenticationDetails(executionContext, authenticationDetails);

        var configurations = new OneLakeConnectorJobData(authenticationDetails, containerName);
        return Task.FromResult<IDataLakeJobData>(configurations);
    }
}
