using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.OneLake.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.OneLake;

public class OneLakeStorageFactory : StorageFactoryBase, IStorageFactory
{
    public override async Task<IStorageClient> CreateStorageClient(ExecutionContext executionContext, IStorageConfiguration configuration)
    {
        return await CreateDataLakeClient(executionContext, configuration as OneLakeConnectorConfiguration);
    }
    private Task<OneLakeStorageClient> CreateDataLakeClient(ExecutionContext executionContext, OneLakeConnectorConfiguration configuration)
    {
        var logger = executionContext.ApplicationContext.Container.Resolve<ILogger<OneLakeStorageClient>>();
        var dateTimeProvider = executionContext.ApplicationContext.Container.Resolve<IDateTimeOffsetProvider>();
        var client = new OneLakeStorageClient(logger, executionContext.ApplicationContext, dateTimeProvider, configuration);
        return Task.FromResult(client);
    }

    protected override Task<IStorageConfiguration> CreateStorageConfigurationInternal(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName)
    {
        return Task.FromResult<IStorageConfiguration>(new OneLakeConnectorConfiguration(authenticationDetails, containerName));
    }
}
