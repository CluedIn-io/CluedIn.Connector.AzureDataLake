using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Transactions;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Connector.FileStorage.Common.Connector.SqlDataWriter;
using CluedIn.Connector.Snowflake.Connector.Snowpipe;
using CluedIn.Connector.Snowflake.Connector.SqlDataWriter;
using CluedIn.Core;
using CluedIn.Core.Streams;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.Snowflake.Connector;

internal class SnowflakeExportEntitiesJob : StorageExportEntitiesJobBase
{
    private readonly SnowflakeStorageFactory _storageFactory;

    public SnowflakeExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        ISnowflakeConfigurationConstants configurationConstants,
        SnowflakeStorageFactory storageFactory,
        ITimeProvider timeProvider)
        : base(appContext, streamRepository, configurationConstants, storageFactory, timeProvider)
    {
        _storageFactory = storageFactory;
    }

    protected override Task<long> ExportDataAsync(
        ExecutionContext context,
        ExportJobData exportJobData,
        IStorageConfiguration configuration,
        IStorageClient storageClient,
        SqlDataReader reader,
        TransactionScope transactionScope,
        List<string> fieldNames,
        DateTimeOffset instanceTime)
    {
        var writer = new SnowflakeSnowpipeSqlDataWriter();
        return writer.WriteOutputAsync(context, configuration, null, fieldNames, exportJobData.IsInitialExport, reader);
    }

    // The Snowpipe Streaming pipe is bound to a fixed transient table (see
    // SnowflakeConnectorConfiguration.TransientTableName/PipeName), so it is
    // created once, idempotently, rather than fresh per run.
    protected override async Task InitializeOutputTargetAsync(
        ExecutionContext context,
        SqlConnection connection,
        ExportJobData exportJobData,
        IStorageClient client)
    {
        var configuration = GetSnowflakeConfiguration(exportJobData);
        var apiClient = CreateApiClient(configuration);
        try
        {
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.CreateTransientTableIfNotExists(configuration.Database, configuration.Schema, configuration.TransientTableName));
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.CreatePipeIfNotExists(configuration.Database, configuration.Schema, configuration.PipeName, configuration.TransientTableName));

            // Clear out any rows left behind by a previous run that crashed before its own
            // post-export truncate (see PostExportAsync) ran.
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.TruncateTransientTable(configuration.Database, configuration.Schema, configuration.TransientTableName));
        }
        finally
        {
            (apiClient as IDisposable)?.Dispose();
        }
    }

    private static SnowflakeApiClient CreateApiClient(SnowflakeConnectorConfiguration configuration)
    {
        return new SnowflakeApiClient(SnowflakeConnectionSettings.FromConfiguration(configuration));
    }

    private protected override async Task PostExportAsync(ExecutionContext context, ExportJobData exportJobData)
    {
        var configuration = GetSnowflakeConfiguration(exportJobData);
        var apiClient = CreateApiClient(configuration);
        try
        {
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.MergeTransientIntoTarget(configuration.Database, configuration.Schema, configuration.TransientTableName, configuration.TableName));
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.TruncateTransientTable(configuration.Database, configuration.Schema, configuration.TransientTableName));
        }
        finally
        {
            (apiClient as IDisposable)?.Dispose();
        }
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
