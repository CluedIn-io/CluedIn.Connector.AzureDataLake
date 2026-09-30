using System;
using System.Collections.Generic;
using System.Linq;
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
    internal const string TableNameDataTimeNotSupportedErrorMessage = "Table name cannot use the {DataTime} pattern variable - the resolved name is reused as the transient table/pipe name too, so it must stay the same across export runs. Supported variables are {StreamId}, {ContainerName} and {OutputFormat}.";
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
    // SnowflakeExportEntitiesJob) - they're always connector-created/-owned (their names
    // are never user-facing) - and, for the target table, renames it (rather than dropping
    // it) if this connector actually created it. Matches
    // StorageConnectorBase.RenameCacheTableIfExists's "_{yyyyMMddHHmmss}" suffix convention
    // for the SQL Server cache table: every other connector preserves exported data on
    // archive rather than deleting it, so the target table does too.
    //
    // The target table is only renamed if this connector actually created it -
    // CreateTargetTableIfNotExists stamps a COMMENT on the table that only takes effect
    // when CREATE TABLE IF NOT EXISTS genuinely creates it (never when it already existed),
    // so a matching comment here means this connector owns it; anything else (blank, or a
    // different comment - e.g. the user pointed the connector at a table that already
    // existed) is left alone.
    //
    // The whole Snowflake cleanup is best-effort (caught and logged, not propagated): if the
    // account is unreachable, the key has been rotated, or the role lacks the needed
    // privilege, base.ArchiveContainer (buffer flush + SQL Server cache table rename) must
    // still run rather than being skipped because this half failed first.
    public override async Task ArchiveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
    {
        var configuration = await StorageFactory.CreateStorageConfiguration(executionContext, streamModel);
        if (configuration is SnowflakeConnectorConfiguration snowflakeConfiguration)
        {
            try
            {
                await ArchiveSnowflakeObjectsAsync(executionContext, streamModel, snowflakeConfiguration);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to archive Snowflake objects for stream '{StreamId}' - continuing with the rest of archival.", streamModel.Id);
            }
        }

        await base.ArchiveContainer(executionContext, streamModel);
    }

    private async Task ArchiveSnowflakeObjectsAsync(ExecutionContext executionContext, IReadOnlyStreamModel streamModel, SnowflakeConnectorConfiguration snowflakeConfiguration)
    {
        // TableName can be a pattern (see SnowflakeConfigurationConstants) - resolve it the
        // same way SnowflakeExportEntitiesJob does, so the transient table/pipe/target this
        // touches are the same ones that export run created/used. {DataTime} isn't a
        // supported variable (rejected in VerifyDataLakeConnection), so - unlike a file name
        // pattern - this always resolves to the same name a given stream's export runs have
        // been using, regardless of when archive runs. Upper-cased for the same reason as
        // SnowflakeExportEntitiesJob - see there.
        var resolvedTableName = (await PatternHelper.ReplaceNameUsingPatternAsync(
            executionContext,
            snowflakeConfiguration.TableName,
            streamModel.Id,
            streamModel.ContainerName,
            _timeProvider.GetUtcNow(),
            snowflakeConfiguration.OutputFormat)).ToUpperInvariant();
        var transientTableName = SnowflakeConnectorConfiguration.GetTransientTableName(resolvedTableName);
        var pipeName = SnowflakeConnectorConfiguration.GetPipeName(resolvedTableName);

        var apiClient = new SnowflakeApiClient(SnowflakeConnectionSettings.FromConfiguration(snowflakeConfiguration));
        try
        {
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.DropPipeIfExists(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, pipeName));
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.DropTableIfExists(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, transientTableName));

            if (await OwnsTargetTableAsync(apiClient, snowflakeConfiguration, resolvedTableName))
            {
                var archivedTableName = $"{resolvedTableName}_{_timeProvider.GetUtcNow():yyyyMMddHHmmss}";
                await apiClient.ExecuteStatementAsync(
                    SnowflakeSqlBuilder.RenameTable(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, resolvedTableName, archivedTableName));
            }
            else
            {
                _logger.LogInformation(
                    "Not renaming Snowflake target table '{TableName}' on archive - it wasn't created by this connector (or ownership couldn't be confirmed).",
                    resolvedTableName);
            }
        }
        finally
        {
            (apiClient as IDisposable)?.Dispose();
        }
    }

    private static async Task<bool> OwnsTargetTableAsync(SnowflakeApiClient apiClient, SnowflakeConnectorConfiguration snowflakeConfiguration, string resolvedTableName)
    {
        var result = await apiClient.ExecuteStatementAsync(
            SnowflakeSqlBuilder.ShowTablesLikeInSchema(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, resolvedTableName),
            SnowflakeStatementScope.None);

        var commentIndex = result.ColumnNames
            .Select((name, index) => (name, index))
            .Where(pair => string.Equals(pair.name, "comment", StringComparison.OrdinalIgnoreCase))
            .Select(pair => (int?)pair.index)
            .FirstOrDefault();

        if (commentIndex == null)
        {
            return false;
        }

        var row = result.Rows.FirstOrDefault();
        return row != null && string.Equals(row[commentIndex.Value], SnowflakeSqlBuilder.OwnedTableComment, StringComparison.Ordinal);
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

        if (SnowflakeSqlBuilder.ContainsDataTimePatternVariable(casted.TableName))
        {
            return CreateFailedConnectionVerification(TableNameDataTimeNotSupportedErrorMessage);
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
