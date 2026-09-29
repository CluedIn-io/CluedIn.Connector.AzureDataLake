using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Transactions;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Connector.Snowflake.Connector.Snowpipe;
using CluedIn.Connector.Snowflake.Connector.SqlDataWriter;
using CluedIn.Core;
using CluedIn.Core.Streams;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.Snowflake.Connector;

internal class SnowflakeExportEntitiesJob : StorageExportEntitiesJobBase
{
    public SnowflakeExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        ISnowflakeConfigurationConstants configurationConstants,
        SnowflakeStorageFactory storageFactory,
        ITimeProvider timeProvider)
        : base(appContext, streamRepository, configurationConstants, storageFactory, timeProvider)
    {
    }

    // Overriding ExportDataAsync means the base's default file-writing implementation -
    // and, with it, the base's call to PostExportAsync - never runs. So this override does
    // the transient-table setup, streams rows via the Snowpipe writer, then does the
    // transient-to-target MERGE and cleanup itself, all in one place, instead of splitting
    // "post export" work into a PostExportAsync override that would silently never fire.
    protected override async Task<long> ExportDataAsync(
        ExecutionContext context,
        ExportJobData exportJobData,
        IStorageConfiguration configuration,
        IStorageClient storageClient,
        SqlDataReader reader,
        TransactionScope transactionScope,
        List<string> fieldNames,
        DateTimeOffset instanceTime)
    {
        var snowflakeConfiguration = GetSnowflakeConfiguration(exportJobData);
        var apiClient = CreateApiClient(snowflakeConfiguration);
        long totalProcessed;

        try
        {
            // The transient table (and its Snowpipe Streaming pipe) is stable/reused
            // across runs rather than recreated per run - see
            // SnowflakeConnectorConfiguration.TransientTableName/PipeName - so its column
            // shape must be able to grow as new CluedIn properties are seen over time.
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.CreateTransientTableIfNotExists(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, snowflakeConfiguration.TransientTableName, fieldNames));

            foreach (var alterStatement in SnowflakeSqlBuilder.GetAddMissingColumnsStatements(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, snowflakeConfiguration.TransientTableName, fieldNames))
            {
                await apiClient.ExecuteStatementAsync(alterStatement);
            }

            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.CreatePipeIfNotExists(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, snowflakeConfiguration.PipeName, snowflakeConfiguration.TransientTableName));

            // Clear out any rows left behind by a previous run that crashed before
            // reaching the truncate at the end of this method.
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.TruncateTransientTable(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, snowflakeConfiguration.TransientTableName));

            var writer = new SnowflakeSnowpipeSqlDataWriter();
            totalProcessed = await writer.WriteOutputAsync(context, configuration, null, fieldNames, exportJobData.IsInitialExport, reader);

            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.MergeTransientIntoTarget(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, snowflakeConfiguration.TransientTableName, snowflakeConfiguration.TableName, fieldNames));
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.TruncateTransientTable(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, snowflakeConfiguration.TransientTableName));
        }
        finally
        {
            (apiClient as IDisposable)?.Dispose();
        }

        return totalProcessed;
    }

    private static SnowflakeApiClient CreateApiClient(SnowflakeConnectorConfiguration configuration)
    {
        return new SnowflakeApiClient(SnowflakeConnectionSettings.FromConfiguration(configuration));
    }

    private static SnowflakeConnectorConfiguration GetSnowflakeConfiguration(ExportJobData exportJobData)
    {
        if (exportJobData.StorageConfiguration is not SnowflakeConnectorConfiguration configuration)
        {
            throw new ArgumentException($"Configuration must be of type {nameof(SnowflakeConnectorConfiguration)}.", nameof(exportJobData));
        }

        return configuration;
    }
}
