using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;

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

    public virtual async Task<IStorageConfiguration> CreateStorageConfiguration(ExecutionContext executionContext, Guid providerDefinitionId)
    {
        var authenticationDetails = await GetAuthenticationDetails(executionContext, providerDefinitionId);
        return await CreateStorageConfiguration(
            executionContext,
            authenticationDetails.Authentication.ToDictionary(detail => detail.Key, detail => detail.Value),
            string.Empty);
    }

    public virtual async Task<IStorageConfiguration> CreateStorageConfiguration(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
    {
        var providerDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
        var containerName = streamModel.ContainerName;
        var authenticationDetails = await GetAuthenticationDetails(executionContext, providerDefinitionId);
        var authenticationDetailsDict = authenticationDetails.Authentication.ToDictionary(detail => detail.Key, detail => detail.Value);

        if (streamModel.ConnectorProperties != null)
        {
            foreach (var property in streamModel.ConnectorProperties!)
            {
                authenticationDetailsDict[property.Key] = property.Value;
            }
        }

        return await CreateStorageConfiguration(executionContext, authenticationDetailsDict, containerName);
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
        IStorageConfiguration configuration);
}
