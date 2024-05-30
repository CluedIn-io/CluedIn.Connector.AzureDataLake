using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Connectors;

namespace CluedIn.Connector.DataLake.Common;

public class DataLakeJobDataFactoryBase
{
    protected static async Task<IConnectorConnectionV2> GetAuthenticationDetails(
        ExecutionContext executionContext,
        Guid providerDefinitionId)
    {
        return await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, providerDefinitionId);
    }

    protected static void UpdateAuthenticationDetails(ExecutionContext executionContext, IDictionary<string, object> authenticationDetails)
    {
        var connectionStrings = executionContext.ApplicationContext.System.ConnectionStrings;
        var connectionStringKey = DataLakeConstants.StreamCacheConnectionStringKey;
        var configurationKey = DataLakeConstants.StreamCacheConnectionString;
        if (connectionStrings.ConnectionStringExists(connectionStringKey))
        {
            authenticationDetails[configurationKey] = connectionStrings.GetConnectionString(connectionStringKey);
        }
        else if (!authenticationDetails.ContainsKey(configurationKey))
        {
            authenticationDetails[configurationKey] = null;
        }
    }
}
