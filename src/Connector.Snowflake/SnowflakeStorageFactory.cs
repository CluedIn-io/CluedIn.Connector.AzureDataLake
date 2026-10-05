using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.Snowflake.Connector;
using CluedIn.Connector.Snowflake.Connector.Snowpipe;
using CluedIn.Core;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.Snowflake;

public class SnowflakeStorageFactory : StorageFactoryBase, IStorageFactory
{
    protected override Task<IStorageConfiguration> CreateStorageConfigurationInternal(
        ExecutionContext executionContext,
        IDictionary<string, object> authenticationDetails,
        string containerName)
    {
        return Task.FromResult<IStorageConfiguration>(new SnowflakeConnectorConfiguration(authenticationDetails, containerName));
    }

    public override Task<IStorageClient> CreateStorageClient(ExecutionContext executionContext, IStorageConfiguration configuration)
    {
        if (configuration == null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        if (configuration is not SnowflakeConnectorConfiguration snowflakeConfiguration)
        {
            throw new ArgumentException($"Configuration must be of type {nameof(SnowflakeConnectorConfiguration)}.", nameof(configuration));
        }

        var loggerFactory = executionContext.ApplicationContext.Container.Resolve<ILoggerFactory>();
        var logger = loggerFactory.CreateLogger<SnowflakeStorageClient>();
        var apiClient = new SnowflakeApiClient(SnowflakeConnectionSettings.FromConfiguration(snowflakeConfiguration));
        var client = new SnowflakeStorageClient(logger, loggerFactory, snowflakeConfiguration, apiClient);
        return Task.FromResult<IStorageClient>(client);
    }
}
