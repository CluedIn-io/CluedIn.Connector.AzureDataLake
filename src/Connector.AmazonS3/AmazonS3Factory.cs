using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.AmazonS3.Connector;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AmazonS3;

public class AmazonS3Factory : StorageFactoryBase, IStorageFactory
{
    protected override Task<IStorageConfiguration> CreateStorageConfigurationInternal(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName)
    {
        return Task.FromResult<IStorageConfiguration>(new AmazonS3ConnectorConfiguration(authenticationDetails, containerName));
    }

    public override Task<IStorageClient> CreateStorageClient(ExecutionContext executionContext, IStorageConfiguration configuration)
    {
        var logger = executionContext.ApplicationContext.Container.Resolve<ILogger<AmazonS3StorageClient>>();
        var client = new AmazonS3StorageClient(logger, configuration as AmazonS3ConnectorConfiguration);
        return Task.FromResult<IStorageClient>(client);
    }
}
