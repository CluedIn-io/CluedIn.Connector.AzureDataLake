using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Events.Types;
using CluedIn.Core.Streams.Models;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Nest;

namespace CluedIn.Connector.FileStorage.Common.EventHandlers;

internal class RemoveStreamEventHandler : UpdateStreamScheduleBase, IDisposable
{
    private const int RemoveTableLockTimeoutInMilliseconds = 100;
    private readonly IDisposable _subscription;
    private bool _disposedValue;

    public RemoveStreamEventHandler(
        ApplicationContext applicationContext,
        IStorageConfigurationConstants constants,
        IStorageFactory storageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider,
        Type exportEntitiesJobType,
        IScheduledJobQueue jobQueue)
        : base(
            applicationContext,
            constants,
            storageFactory,
            dateTimeOffsetProvider,
            exportEntitiesJobType,
            jobQueue)
    {
        _subscription = ApplicationContext.System.Events.SubscribeAsync<RemoveStreamEvent>(ProcessEventAsync);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                _subscription.Dispose();
            }

            _disposedValue = true;
        }
    }

    private async Task ProcessEventAsync(RemoveStreamEvent eventData)
    {
        if (!eventData.TryGetResourceInfo("AccountId", "StreamId", out var organizationId, out var streamId))
        {
            return;
        }

        RemoveJobFromQueue(organizationId, streamId);
        await RemoveCacheTablesAsync(organizationId, streamId);
    }

    private async Task RemoveCacheTablesAsync(
        Guid organizationId,
        Guid streamId)
    {
        var logger = ApplicationContext.Container.Resolve<ILogger<RemoveStreamEventHandler>>();
        var connectionStrings = ApplicationContext.System.ConnectionStrings;
        var connectionStringKey = StorageConfigurationConstants.StreamCacheConnectionStringKey;
        var configurationKey = StorageConfigurationConstants.StreamCacheConnectionString;
        if (!connectionStrings.ConnectionStringExists(connectionStringKey))
        {
            logger.LogWarning("Unable to remove cache tables for stream {StreamId} in organization {OrganizationId} because connection string {ConnectionStringKey} is not configured.", streamId, organizationId, connectionStringKey);
            return;
        }

        var connectionString = connectionStrings.GetConnectionString(connectionStringKey);
        using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        if (!await DistributedLockHelper.TryAcquireExclusiveLock(connection, $"RemoveTable_{streamId}", RemoveTableLockTimeoutInMilliseconds))
        {
            logger.LogInformation("Unable to acquire lock to remove table for Stream '{StreamId}'. Skipping export.", streamId);
            return;
        }

        var tables = await GetTableNames();
        await DropStreamCacheTables();
        await DropExportHistoryTableAsync();
        transactionScope.Complete();

        async Task<List<string>> GetTableNames()
        {
            var list = new List<string>();
            var getTablesSql = $"""
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_NAME LIKE 'Stream_{streamId}%'
                AND TABLE_NAME NOT LIKE '%History%';
                """;
            var command = new SqlCommand(getTablesSql, connection)
            {
                CommandType = CommandType.Text
            };
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var tableName = reader.GetString(0);
                list.Add(tableName);
            }

            return list;
        }

        async Task DropStreamCacheTables()
        {
            foreach (var tableName in tables)
            {
                logger.LogInformation("Dropping table {TableName} for Stream '{StreamId}'.", tableName, streamId);
                try
                {
                    await DropCacheTableAsync(tableName);
                    logger.LogInformation("Dropped table {TableName} for Stream '{StreamId}'.", tableName, streamId);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to drop table {TableName} for Stream '{StreamId}'.", tableName, streamId);
                }
            }
        }

        async Task DropCacheTableAsync(string tableName)
        {
            var prefix = CacheTableHelper.GetCacheTableName(streamId);
            var suffix = tableName[prefix.Length..];
            var dropTableSql =
                $"""
                 ALTER TABLE [dbo].[{tableName}] SET ( SYSTEM_VERSIONING = OFF  );
                 IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{tableName}]') AND type in (N'U'))
                 DROP TABLE [dbo].[{tableName}];
                 IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{prefix}_History{suffix}]') AND type in (N'U'))
                 DROP TABLE [dbo].[{prefix}_History{suffix}];
                 """;
            var dropCommand = new SqlCommand(dropTableSql, connection)
            {
                CommandType = CommandType.Text
            };
            var result = await dropCommand.ExecuteNonQueryAsync();
            logger.LogInformation("Total of {Result} tables dropped for export history table {TableName} for Stream '{StreamId}'.", result, tableName, streamId);
        }

        async Task DropExportHistoryTableAsync()
        {
            var tableName = CacheTableHelper.GetExportHistoryTableName(streamId) + "_ExportHistory";
            var dropTableSql = $"""
                                IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{tableName}]') AND type in (N'U'))
                                DROP TABLE [dbo].[{tableName}];
                                """;
            var dropCommand = new SqlCommand(dropTableSql, connection)
            {
                CommandType = CommandType.Text
            };

            var result = await dropCommand.ExecuteNonQueryAsync();
            logger.LogInformation("Total of {Result} tables dropped for export history table {TableName} for Stream '{StreamId}'.", result, tableName, streamId);
        }
    }

    private void RemoveJobFromQueue(
        Guid organizationId,
        Guid streamId)
    {
        var executionContext = ApplicationContext.CreateExecutionContext(organizationId);
        if (JobQueue.TryRemove(streamId.ToString(), out _))
        {
            executionContext.Log.LogWarning("Removed job {StreamId} from queue.", streamId);
        }
        else
        {
            executionContext.Log.LogWarning("Failed to remove job {StreamId} from queue.", streamId);
        }
    }

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}
