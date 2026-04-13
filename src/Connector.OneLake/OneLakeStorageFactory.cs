using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.OneLake.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.OneLake;

public class OneLakeStorageFactory : StorageFactoryBase, IStorageFactory
{
    public override async Task<IStorageClient> CreateStorageClient(ExecutionContext executionContext, IStorageConfiguration jobData)
    {
        return await CreateDataLakeClient(executionContext, jobData as OneLakeConnectorConfiguration);
    }
    private Task<OneLakeStorageClient> CreateDataLakeClient(ExecutionContext executionContext, OneLakeConnectorConfiguration jobData)
    {
        var logger = executionContext.ApplicationContext.Container.Resolve<ILogger<OneLakeStorageClient>>();
        var client = new OneLakeStorageClient(logger, jobData);
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
