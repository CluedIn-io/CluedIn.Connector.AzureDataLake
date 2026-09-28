using System;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Connector.FileStorage.Common.Connector.SqlDataWriter;
using CluedIn.Connector.Snowflake.Connector.Snowpipe;
using CluedIn.Core;
using CluedIn.Core.Streams;

using SnowflakeSqlDataWriter = CluedIn.Connector.Snowflake.Connector.SqlDataWriter.SnowflakeSnowpipeSqlDataWriter;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.Snowflake.Connector;

internal class SnowflakeExportEntitiesJob : StorageExportEntitiesJobBase
{
    private readonly Func<SnowflakeConnectorConfiguration, ISnowflakeApiClient> _apiClientFactory;

    public SnowflakeExportEntitiesJob(
        ApplicationContext appContext,
        IStreamRepository streamRepository,
        ISnowflakeConfigurationConstants configurationConstants,
        SnowflakeStorageFactory storageFactory,
        ITimeProvider timeProvider,
        Func<SnowflakeConnectorConfiguration, ISnowflakeApiClient> apiClientFactory = null)
        : base(appContext, streamRepository, configurationConstants, storageFactory, timeProvider)
    {
        _apiClientFactory = apiClientFactory
            ?? (config => new SnowflakeApiClient(SnowflakeConnectionSettings.FromConfiguration(config)));
    }

    protected override ISqlDataWriter GetSqlDataWriter(string outputFormat)
    {
        return new SnowflakeSqlDataWriter(_apiClientFactory);
    }

    // The Snowpipe Streaming pipe is bound to a fixed transient table (see
    // SnowflakeConnectorConfiguration.TransientTableName/PipeName), so it is
    // created once, idempotently, rather than fresh per run.
    private protected override async Task InitializeBaseDirectoryAsync(
        ExecutionContext context,
        SqlConnection connection,
        ExportJobData exportJobData,
        IStorageClient client)
    {
        var configuration = GetSnowflakeConfiguration(exportJobData);
        var apiClient = _apiClientFactory(configuration);
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

    private protected override Task InitializeOutputDirectoryAsync(
        ExecutionContext context,
        SqlConnection connection,
        ExportJobData exportJobData,
        IStorageClient client)
    {
        return Task.CompletedTask;
    }

    private protected override async Task PostExportAsync(ExecutionContext context, ExportJobData exportJobData)
    {
        var configuration = GetSnowflakeConfiguration(exportJobData);
        var apiClient = _apiClientFactory(configuration);
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
