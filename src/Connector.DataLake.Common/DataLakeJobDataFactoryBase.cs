using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Connectors;

namespace CluedIn.Connector.DataLake.Common;

public abstract class DataLakeJobDataFactoryBase
{
    protected static async Task<IConnectorConnectionV2> GetAuthenticationDetails(
        ExecutionContext executionContext,
        Guid providerDefinitionId)
    {
        return await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, providerDefinitionId);
    }

    protected static void UpdateStreamCacheConnectionString(ExecutionContext executionContext, IDictionary<string, object> authenticationDetails)
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

    public virtual async Task<IDataLakeJobData> GetConfiguration(ExecutionContext executionContext, Guid providerDefinitionId, string containerName)
    {
        var authenticationDetails = await GetAuthenticationDetails(executionContext, providerDefinitionId);
        return await GetConfiguration(executionContext, authenticationDetails.Authentication.ToDictionary(detail => detail.Key, detail => detail.Value), containerName);
    }

    public virtual async Task<IDataLakeJobData> GetConfiguration(ExecutionContext executionContext, IDictionary<string, object> authenticationDetails, string containerName = null)
    {
        UpdateStreamCacheConnectionString(executionContext, authenticationDetails);

        return await CreateJobData(executionContext, authenticationDetails, containerName);
    }

    protected abstract Task<IDataLakeJobData> CreateJobData(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName);
}
