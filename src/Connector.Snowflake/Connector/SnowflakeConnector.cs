using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Connector.Snowflake.Connector.Snowpipe;
using CluedIn.Core;
using CluedIn.Core.Streams.Models;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.Snowflake.Connector;

public class SnowflakeConnector : StorageConnectorBase
{
    private readonly ILogger<SnowflakeConnector> _logger;

    internal const string InvalidAccountErrorMessage = "Account identifier cannot be empty.";
    internal const string InvalidUserErrorMessage = "User cannot be empty.";
    internal const string InvalidPrivateKeyErrorMessage = "Private key cannot be empty.";
    internal const string InvalidDatabaseErrorMessage = "Database cannot be empty.";
    internal const string InvalidSchemaErrorMessage = "Schema cannot be empty.";
    internal const string InvalidWarehouseErrorMessage = "Warehouse cannot be empty.";
    internal const string InvalidTableNameErrorMessage = "Table name cannot be empty.";
    internal const string InvalidCredentialsErrorMessage = "Unable to authenticate with Snowflake using the supplied account/user/private key.";

    public SnowflakeConnector(
        ILogger<SnowflakeConnector> logger,
        ApplicationContext applicationContext,
        ISnowflakeConfigurationConstants constants,
        SnowflakeStorageFactory storageFactory,
        ITimeProvider timeProvider)
        : base(logger, applicationContext, constants, storageFactory, timeProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override Type ExportJobType => typeof(SnowflakeExportEntitiesJob);

    public override IReadOnlyCollection<StreamMode> GetSupportedModes()
    {
        return [StreamMode.Sync];
    }

    protected override async Task<FileStorageConnectionVerificationResult> VerifyDataLakeConnection(ExecutionContext executionContext, IStorageConfiguration configuration, bool shouldLogException)
    {
        if (configuration is not SnowflakeConnectorConfiguration casted)
        {
            throw new ArgumentException($"Invalid configuration type: {configuration.GetType().Name}. Expected: {nameof(SnowflakeConnectorConfiguration)}.");
        }

        if (string.IsNullOrWhiteSpace(casted.Account))
        {
            return CreateFailedConnectionVerification(InvalidAccountErrorMessage);
        }

        if (string.IsNullOrWhiteSpace(casted.User))
        {
            return CreateFailedConnectionVerification(InvalidUserErrorMessage);
        }

        if (string.IsNullOrWhiteSpace(casted.PrivateKey))
        {
            return CreateFailedConnectionVerification(InvalidPrivateKeyErrorMessage);
        }

        if (string.IsNullOrWhiteSpace(casted.Database))
        {
            return CreateFailedConnectionVerification(InvalidDatabaseErrorMessage);
        }

        if (string.IsNullOrWhiteSpace(casted.Schema))
        {
            return CreateFailedConnectionVerification(InvalidSchemaErrorMessage);
        }

        if (string.IsNullOrWhiteSpace(casted.Warehouse))
        {
            return CreateFailedConnectionVerification(InvalidWarehouseErrorMessage);
        }

        if (string.IsNullOrWhiteSpace(casted.TableName))
        {
            return CreateFailedConnectionVerification(InvalidTableNameErrorMessage);
        }

        try
        {
            return SuccessfulConnectionVerification;
            //return await base.VerifyDataLakeConnection(executionContext, configuration, shouldLogException);
        }
        catch (SnowflakeApiException apiEx)
        {
            if (shouldLogException)
            {
                _logger.LogWarning(apiEx, "Snowflake API error when verifying connection.");
            }

            return CreateFailedConnectionVerification(InvalidCredentialsErrorMessage, hasException: true);
        }
        catch (Exception ex)
        {
            if (shouldLogException)
            {
                _logger.LogWarning(ex, "Error when verifying Snowflake connection.");
            }

            return CreateFailedConnectionVerification($"Failed to connect to Snowflake: {ex.Message}", hasException: true);
        }
    }
}
