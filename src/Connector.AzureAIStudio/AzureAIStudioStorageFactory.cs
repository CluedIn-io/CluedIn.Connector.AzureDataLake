using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AzureAIStudio;

public class AzureAIStudioStorageFactory : StorageFactoryBase, IStorageFactory
{
    public override Task<IStorageClient> CreateStorageClient(ExecutionContext executionContext, IStorageConfiguration jobData)
    {
        if (jobData is not AzureAIStudioConnectorConfiguration castedJobData)
        {
            throw new ApplicationException($"Provided job data is not of expected type '{typeof(AzureAIStudioConnectorConfiguration)}'. It is '{jobData.GetType()}'.");
        }

        var logger = executionContext.ApplicationContext.Container.Resolve<ILogger<DataLakeStorageClient>>();
        return Task.FromResult<IStorageClient>(new DataLakeStorageClient(logger, castedJobData));
    }

    protected override Task<IStorageConfiguration> CreateStorageConfigurationInternal(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName)
    {
        return Task.FromResult<IStorageConfiguration>(new AzureAIStudioConnectorConfiguration(authenticationDetails, containerName));
    }
}
