using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureDataLake;

public class AzureDataLakeStorageFactory : StorageFactoryBase, IStorageFactory
{
    public override Task<IStorageClient> CreateStorageClient(ExecutionContext executionContext, IStorageConfiguration configuration)
    {
        if (configuration is not AzureDataLakeConnectorConfiguration castedConfiguration)
        {
            throw new ApplicationException($"Provided job data is not of expected type '{typeof(AzureDataLakeConnectorConfiguration)}'. It is '{configuration.GetType()}'.");
        }

        var logger = executionContext.ApplicationContext.Container.Resolve<ILogger<DataLakeStorageClient>>();
        return Task.FromResult<IStorageClient>(new DataLakeStorageClient(logger, castedConfiguration));
    }

    protected override Task<IStorageConfiguration> CreateStorageConfigurationInternal(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName)
    {
        return Task.FromResult<IStorageConfiguration>(new AzureDataLakeConnectorConfiguration(authenticationDetails, containerName));
    }
}
