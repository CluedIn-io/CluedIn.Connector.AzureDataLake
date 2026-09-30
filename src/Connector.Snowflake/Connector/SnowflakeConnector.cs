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
    // Referencing a warehouse (even just to list it via SHOW WAREHOUSES) can resume it, and
    // resuming/keeping it active on every health check tick is a real cost concern - so for
    // health checks (not interactive "test connection" calls), the warehouse check only runs
    // during this window at the start of each hour, rather than every tick.
    internal const int HealthCheckWarehouseCheckWindowMinutes = 5;

    private readonly ILogger<SnowflakeConnector> _logger;
    private readonly ITimeProvider _timeProvider;

    internal const string InvalidAccountErrorMessage = "Account identifier cannot be empty.";
    internal const string InvalidUserErrorMessage = "User cannot be empty.";
    internal const string InvalidPrivateKeyErrorMessage = "Private key cannot be empty.";
    internal const string InvalidDatabaseErrorMessage = "Database cannot be empty.";
    internal const string InvalidSchemaErrorMessage = "Schema cannot be empty.";
    internal const string InvalidWarehouseErrorMessage = "Warehouse cannot be empty.";
    internal const string InvalidTableNameErrorMessage = "Table name cannot be empty.";
    internal const string WarehouseNotAccessibleErrorMessageFormat = "Warehouse '{0}' does not exist, or role '{1}' does not have access to it.";
    internal const string DatabaseNotAccessibleErrorMessageFormat = "Database '{0}' does not exist, or role '{1}' does not have access to it.";
    internal const string SchemaNotAccessibleErrorMessageFormat = "Schema '{0}' does not exist, or role '{1}' does not have access to it.";

    public SnowflakeConnector(
        ILogger<SnowflakeConnector> logger,
        ApplicationContext applicationContext,
        ISnowflakeConfigurationConstants constants,
        SnowflakeStorageFactory storageFactory,
        ITimeProvider timeProvider)
        : base(logger, applicationContext, constants, storageFactory, timeProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
    }

    protected override Type ExportJobType => typeof(SnowflakeExportEntitiesJob);

    public override IReadOnlyCollection<StreamMode> GetSupportedModes()
    {
        return [StreamMode.Sync];
    }

    // Drops the transient table and pipe this connector created (see
    // SnowflakeExportEntitiesJob), rather than leaving them behind when a stream is
    // archived. The pipe is dropped before the transient table it's bound to, so nothing
    // is ever dropped while something else still references it.
    //
    // The target table is deliberately NOT dropped here: CREATE TABLE IF NOT EXISTS in
    // SnowflakeExportEntitiesJob means this connector can't tell whether it created that
    // table or the user pointed it at a table that already existed (with their own data),
    // and nothing prevents the same target table being configured across multiple streams.
    // Dropping it on archive could therefore destroy data this connector doesn't
    // exclusively own - the transient table and pipe are always connector-created/-owned
    // (their names are never user-facing), so only those are safe to clean up
    // unconditionally.
    public override async Task ArchiveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
    {
        var configuration = await StorageFactory.CreateStorageConfiguration(executionContext, streamModel);
        if (configuration is SnowflakeConnectorConfiguration snowflakeConfiguration)
        {
            var apiClient = new SnowflakeApiClient(SnowflakeConnectionSettings.FromConfiguration(snowflakeConfiguration));
            try
            {
                await apiClient.ExecuteStatementAsync(
                    SnowflakeSqlBuilder.DropPipeIfExists(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, snowflakeConfiguration.PipeName));
                await apiClient.ExecuteStatementAsync(
                    SnowflakeSqlBuilder.DropTableIfExists(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, snowflakeConfiguration.TransientTableName));
            }
            finally
            {
                (apiClient as IDisposable)?.Dispose();
            }
        }

        await base.ArchiveContainer(executionContext, streamModel);
    }

    // The base only calls VerifyDataLakeConnection, which can't tell a health check apart
    // from an interactive "test connection" call - isHealthCheck is only available here. So
    // the warehouse check (the one that can resume compute) lives here instead, gated to
    // HealthCheckWarehouseCheckWindowMinutes for health checks; database/schema checks stay
    // in VerifyDataLakeConnection and always run, since SHOW DATABASES/SHOW SCHEMAS don't
    // touch compute.
    protected override async Task<FileStorageConnectionVerificationResult> VerifyConnectionInternal(
        ExecutionContext executionContext, IStorageConfiguration configuration, bool shouldLogException, bool isHealthCheck)
    {
        var result = await base.VerifyConnectionInternal(executionContext, configuration, shouldLogException, isHealthCheck);
        if (!result.Success)
        {
            return result;
        }

        if (isHealthCheck && _timeProvider.GetUtcNow().Minute >= HealthCheckWarehouseCheckWindowMinutes)
        {
            return result;
        }

        if (configuration is not SnowflakeConnectorConfiguration casted)
        {
            return result;
        }

        var apiClient = new SnowflakeApiClient(SnowflakeConnectionSettings.FromConfiguration(casted));
        try
        {
            var warehouses = await apiClient.ExecuteStatementAsync(SnowflakeSqlBuilder.ShowWarehouses(casted.Warehouse), SnowflakeStatementScope.None);
            if (warehouses.Rows.Count == 0)
            {
                return CreateFailedConnectionVerification(WarehouseNotAccessibleErrorMessageFormat.FormatWith(casted.Warehouse, casted.Role));
            }

            return SuccessfulConnectionVerification;
        }
        catch (SnowflakeApiException apiEx)
        {
            if (shouldLogException)
            {
                _logger.LogWarning(apiEx, "Snowflake API error when verifying warehouse access.");
            }

            return CreateFailedConnectionVerification(apiEx.Message, hasException: true);
        }
        catch (Exception ex)
        {
            if (shouldLogException)
            {
                _logger.LogWarning(ex, "Error when verifying Snowflake warehouse access.");
            }

            return CreateFailedConnectionVerification($"Failed to connect to Snowflake: {ex.Message}", hasException: true);
        }
        finally
        {
            (apiClient as IDisposable)?.Dispose();
        }
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

        var apiClient = new SnowflakeApiClient(SnowflakeConnectionSettings.FromConfiguration(casted));
        try
        {
            // Checked one object at a time (rather than one combined query) so a failure
            // names exactly which object the role lacks access to, instead of an ambiguous
            // error that could be either. SHOW ... LIKE only returns a row for an object
            // that exists and the role has at least one privilege on, and - unlike the
            // warehouse check in VerifyConnectionInternal - neither of these touches
            // compute, so they always run regardless of health-check throttling.
            var databases = await apiClient.ExecuteStatementAsync(SnowflakeSqlBuilder.ShowDatabases(casted.Database), SnowflakeStatementScope.None);
            if (databases.Rows.Count == 0)
            {
                return CreateFailedConnectionVerification(DatabaseNotAccessibleErrorMessageFormat.FormatWith(casted.Database, casted.Role));
            }

            var schemas = await apiClient.ExecuteStatementAsync(SnowflakeSqlBuilder.ShowSchemasInDatabase(casted.Database, casted.Schema), SnowflakeStatementScope.None);
            if (schemas.Rows.Count == 0)
            {
                return CreateFailedConnectionVerification(SchemaNotAccessibleErrorMessageFormat.FormatWith(casted.Schema, casted.Role));
            }

            return SuccessfulConnectionVerification;
        }
        catch (SnowflakeApiException apiEx)
        {
            if (shouldLogException)
            {
                _logger.LogWarning(apiEx, "Snowflake API error when verifying connection.");
            }

            return CreateFailedConnectionVerification(apiEx.Message, hasException: true);
        }
        catch (Exception ex)
        {
            if (shouldLogException)
            {
                _logger.LogWarning(ex, "Error when verifying Snowflake connection.");
            }

            return CreateFailedConnectionVerification($"Failed to connect to Snowflake: {ex.Message}", hasException: true);
        }
        finally
        {
            (apiClient as IDisposable)?.Dispose();
        }
    }
}
