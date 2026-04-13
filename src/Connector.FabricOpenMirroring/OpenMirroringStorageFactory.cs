using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.FabricOpenMirroring.Connector;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FabricOpenMirroring;

public class OpenMirroringStorageFactory : StorageFactoryBase, IStorageFactory
{
    protected override Task<IStorageConfiguration> CreateStorageConfigurationInternal(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName)
    {
        return Task.FromResult<IStorageConfiguration>(new OpenMirroringConnectorConfiguration(authenticationDetails, containerName));
    }

    public override async Task<IStorageConfiguration> CreateStorageConfiguration(ExecutionContext executionContext, IDictionary<string, object> authenticationDetails, string containerName = null)
    {
        if (!authenticationDetails.TryGetValue(OpenMirroringConfigurationConstants.MirroredDatabaseName, out var value)
            || string.IsNullOrWhiteSpace(value?.ToString()))
        {
            if (authenticationDetails.TryGetValue(StorageConfigurationConstants.ProviderDefinitionIdKey, out var providerDefinitionId))
            {
                authenticationDetails[OpenMirroringConfigurationConstants.MirroredDatabaseName] = $"CluedIn_ExportTarget_{providerDefinitionId:N}";
                //authenticationDetails[OpenMirroringConstants.ShouldCreateMirroredDatabase] = true;
            }
        }

        return await base.CreateStorageConfiguration(executionContext, authenticationDetails, containerName);
    }

    public override async Task<IStorageClient> CreateStorageClient(ExecutionContext executionContext, IStorageConfiguration storageConfiguration)
    {
        var logger = executionContext.ApplicationContext.Container.Resolve<ILogger<OpenMirroringStorageClient>>();
        var dateTimeOffsetProvider = executionContext.ApplicationContext.Container.Resolve<IDateTimeOffsetProvider>();
        var client = new OpenMirroringStorageClient(logger, dateTimeOffsetProvider, storageConfiguration as OpenMirroringConnectorConfiguration);
        return client;
    }
}
