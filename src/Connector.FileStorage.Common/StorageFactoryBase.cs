using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Connectors;

namespace CluedIn.Connector.FileStorage.Common;

public abstract class StorageFactoryBase : IStorageFactory
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
        var connectionStringKey = StorageConfigurationConstants.StreamCacheConnectionStringKey;
        var configurationKey = StorageConfigurationConstants.StreamCacheConnectionString;
        if (connectionStrings.ConnectionStringExists(connectionStringKey))
        {
            authenticationDetails[configurationKey] = connectionStrings.GetConnectionString(connectionStringKey);
        }
        else if (!authenticationDetails.ContainsKey(configurationKey))
        {
            authenticationDetails[configurationKey] = null;
        }
    }

    public virtual async Task<IStorageConfiguration> CreateStorageConfiguration(ExecutionContext executionContext, Guid providerDefinitionId, string containerName)
    {
        var authenticationDetails = await GetAuthenticationDetails(executionContext, providerDefinitionId);
        return await CreateStorageConfiguration(executionContext, authenticationDetails.Authentication.ToDictionary(detail => detail.Key, detail => detail.Value), containerName);
    }

    public virtual async Task<IStorageConfiguration> CreateStorageConfiguration(ExecutionContext executionContext, IDictionary<string, object> authenticationDetails, string containerName = null)
    {
        UpdateStreamCacheConnectionString(executionContext, authenticationDetails);

        return await CreateStorageConfigurationInternal(executionContext, authenticationDetails, containerName);
    }

    protected abstract Task<IStorageConfiguration> CreateStorageConfigurationInternal(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName);

    public abstract Task<IStorageClient> CreateStorageClient(
        ExecutionContext executionContext,
        IStorageConfiguration jobData);
}
