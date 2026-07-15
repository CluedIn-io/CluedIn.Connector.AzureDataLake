using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.AmazonS3.Connector;
using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.AmazonS3;

public class AmazonS3StorageFactory : StorageFactoryBase, IStorageFactory
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
        if (configuration == null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        if (configuration is not AmazonS3ConnectorConfiguration amazonS3Configuration)
        {
            throw new ArgumentException($"Configuration must be of type {nameof(AmazonS3ConnectorConfiguration)}.", nameof(configuration));
        }

        var loggerFactory = executionContext.ApplicationContext.Container.Resolve<ILoggerFactory>();
        var logger = loggerFactory.CreateLogger<AmazonS3StorageClient>();
        var client = new AmazonS3StorageClient(logger, loggerFactory, amazonS3Configuration);
        return Task.FromResult<IStorageClient>(client);
    }
}
