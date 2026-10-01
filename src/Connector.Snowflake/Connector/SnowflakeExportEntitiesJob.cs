using System;
using System.Collections.Generic;
using System.Linq;
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
    // The DDL/ALTER/truncate calls around it are fast and use SnowflakeApiClient's 60s
    // default, but a MERGE against a large target table (or on a small warehouse) can
    // legitimately take much longer - Snowflake cancels a statement's execution once its own
    // timeout elapses (not just how long the client waits for a response), so the MERGE
    // needs a timeout generous enough that a real one isn't mistaken for a stuck job.
    // async: true avoids holding one HTTP request open for however long that takes,
    // returning the statement handle immediately instead so SnowflakeApiClient's normal
    // polling loop can track progress.
    private const int MergeStatementTimeoutSeconds = 3600;

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
    // the transient- and target-table setup, streams rows via the Snowpipe writer, then
    // does the transient-to-target MERGE and cleanup itself, all in one place, instead of
    // splitting "post export" work into a PostExportAsync override that would silently
    // never fire.
    //
    // CreateContainer (the usual place a connector provisions its target) is a no-op for
    // every FileStorage.Common-based connector (see StorageConnectorBase.CreateContainer),
    // so the target table is created/grown here instead, on every export run.
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

        // SanitizeColumnName isn't injective (e.g. "user.email" and "user-email" both fold
        // to USER_EMAIL) - catch that up front with a clear error instead of silently
        // generating a table with duplicate columns and a writer that drops one value.
        SnowflakeSqlBuilder.EnsureNoColumnNameCollisions(fieldNames);

        // TableName can be a pattern (e.g. "{ContainerName}_Table") - same
        // {StreamId}/{OutputFormat}/{ContainerName} variables
        // OneLakeExportEntitiesJob.PostExportAsync resolves its own TableName pattern with
        // ({DataTime} isn't supported here - see VerifyDataLakeConnection - since, unlike a
        // file name, this resolved name is reused as the transient table/pipe name below
        // too, and is expected to stay the same across runs for a given stream). See
        // ResolveTableNameAsync for why it's also upper-cased.
        var resolvedTableName = await ResolveTableNameAsync(context, snowflakeConfiguration, exportJobData);
        var transientTableName = SnowflakeConnectorConfiguration.GetTransientTableName(resolvedTableName);
        var pipeName = SnowflakeConnectorConfiguration.GetPipeName(resolvedTableName);

        var apiClient = CreateApiClient(snowflakeConfiguration);
        long totalProcessed;

        try
        {
            // The transient table (and its Snowpipe Streaming pipe) is stable/reused
            // across runs rather than recreated per run - see
            // SnowflakeConnectorConfiguration.GetTransientTableName/GetPipeName - so its
            // column shape must be able to grow as new CluedIn properties are seen over time.
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.CreateTransientTableIfNotExists(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, transientTableName, fieldNames));

            foreach (var alterStatement in SnowflakeSqlBuilder.GetAddMissingColumnsStatements(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, transientTableName, fieldNames))
            {
                await apiClient.ExecuteStatementAsync(alterStatement);
            }

            // The target table is user-facing data, not a per-run landing area - create it
            // if this is the first export to it, and grow its columns the same way the
            // transient table's are grown, since it's never recreated afterwards.
            // __ChangeType__ is excluded here: it's only transient-side bookkeeping the
            // MERGE below reads to decide insert/update/delete, never a real CluedIn
            // property, so it has no business being a persisted column on the target table.
            var targetFieldNames = fieldNames
                .Where(fieldName => fieldName != StorageConfigurationConstants.ChangeTypeKey)
                .ToList();

            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.CreateTargetTableIfNotExists(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, resolvedTableName, targetFieldNames));

            foreach (var alterStatement in SnowflakeSqlBuilder.GetAddMissingColumnsStatements(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, resolvedTableName, targetFieldNames))
            {
                await apiClient.ExecuteStatementAsync(alterStatement);
            }

            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.CreatePipeIfNotExists(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, pipeName, transientTableName));

            // Clear out any rows left behind by a previous run that crashed before
            // reaching the truncate at the end of this method.
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.TruncateTransientTable(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, transientTableName));

            var writer = new SnowflakeSnowpipeSqlDataWriter(pipeName);
            totalProcessed = await writer.WriteOutputAsync(context, configuration, null, fieldNames, exportJobData.IsInitialExport, reader);

            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.MergeTransientIntoTarget(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, transientTableName, resolvedTableName, fieldNames),
                timeoutSeconds: MergeStatementTimeoutSeconds,
                async: true);
            await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.TruncateTransientTable(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, transientTableName));
        }
        finally
        {
            (apiClient as IDisposable)?.Dispose();
        }

        return totalProcessed;
    }

    // The base's default (lastExportedFile == null) relies on the SQL Server cache's
    // export history, which the archive-and-recreate flow leaves behind - rebuilding (not
    // truncating) a target table that was archived (renamed) or dropped out from under this
    // connector would otherwise be treated as a delta export against a table that doesn't
    // have the rows the delta assumes are already there. So, on top of the base check, an
    // absent target table is always treated as the initial export regardless of history.
    protected override async Task<bool> GetIsInitialExport(
        ExecutionContext context,
        ExportJobDataBase exportJobDataBase,
        IStorageClient storageClient,
        LastExportedFile? lastExportedFile,
        DirectoryPath outputDirectoryPath)
    {
        if (exportJobDataBase.StorageConfiguration is not SnowflakeConnectorConfiguration snowflakeConfiguration)
        {
            throw new ArgumentException($"Configuration must be of type {nameof(SnowflakeConnectorConfiguration)}.", nameof(exportJobDataBase));
        }

        var resolvedTableName = await ResolveTableNameAsync(context, snowflakeConfiguration, exportJobDataBase);

        var apiClient = CreateApiClient(snowflakeConfiguration);
        try
        {
            var result = await apiClient.ExecuteStatementAsync(
                SnowflakeSqlBuilder.ShowTablesLikeInSchema(snowflakeConfiguration.Database, snowflakeConfiguration.Schema, resolvedTableName),
                SnowflakeStatementScope.None);

            if (!result.HasExactNameMatch(resolvedTableName))
            {
                return true;
            }
        }
        finally
        {
            (apiClient as IDisposable)?.Dispose();
        }

        return await base.GetIsInitialExport(context, exportJobDataBase, storageClient, lastExportedFile, outputDirectoryPath);
    }

    private static Task<string> ResolveTableNameAsync(ExecutionContext context, SnowflakeConnectorConfiguration snowflakeConfiguration, ExportJobDataBase exportJobDataBase)
    {
        return ResolveTableNameAsync(context, snowflakeConfiguration, exportJobDataBase.StreamId, exportJobDataBase.ContainerName, exportJobDataBase.AsOfTime, exportJobDataBase.OutputFormat);
    }

    // Upper-cased afterwards (verified live): substituted variables like {ContainerName}
    // aren't necessarily upper-case, but the created pipe/table are referenced via
    // QualifiedName's double-quoted (case-preserving) identifiers for DDL/MERGE, while the
    // Snowpipe Streaming REST API's pipe lookup 404s unless given the canonical,
    // Snowflake-default (upper-case) form - so a mixed-case resolved name could create the
    // pipe successfully via the SQL API yet fail to open a channel on it via the streaming
    // API.
    private static async Task<string> ResolveTableNameAsync(ExecutionContext context, SnowflakeConnectorConfiguration snowflakeConfiguration, Guid streamId, string containerName, DateTimeOffset asOfTime, string outputFormat)
    {
        return (await PatternHelper.ReplaceNameUsingPatternAsync(
            context,
            snowflakeConfiguration.TableName,
            streamId,
            containerName,
            asOfTime,
            outputFormat)).ToUpperInvariant();
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
