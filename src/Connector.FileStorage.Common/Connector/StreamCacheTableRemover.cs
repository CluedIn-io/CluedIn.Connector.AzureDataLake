using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FileStorage.Common.Connector;

/// <summary>
/// Removes tables of streams from the stream cache database.
/// </summary>
internal class StreamCacheTableRemover
{
    private const int RemoveTableLockTimeoutInMilliseconds = 100;

    private readonly ILogger _logger;
    private readonly string _connectionString;

    public StreamCacheTableRemover(ILogger logger, string connectionString)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    /// <summary>
    /// Gets the names of the tables in the stream cache database, without the history tables.
    /// </summary>
    public async Task<IReadOnlyList<string>> GetTableNamesAsync()
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        return await GetTableNamesAsync(connection);
    }

    /// <summary>
    /// Removes all the tables of a stream, including its export history.
    /// </summary>
    public async Task RemoveAllTablesAsync(Guid streamId)
    {
        using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        if (!await TryAcquireLockAsync(connection, streamId))
        {
            return;
        }

        var tableNames = await GetTableNamesAsync(connection);
        var streamTables = StreamCacheTables.FromTableNames(tableNames).SingleOrDefault(tables => tables.StreamId == streamId);
        if (streamTables != null)
        {
            foreach (var tableName in streamTables.TableNames)
            {
                await TryDropCacheTableAsync(connection, streamId, tableName);
            }
        }

        await DropExportHistoryTableAsync(connection, streamId);
        transactionScope.Complete();
    }

    /// <summary>
    /// Removes the archived tables of a stream, except for the most recent one.
    /// </summary>
    public async Task RemoveObsoleteTablesAsync(StreamCacheTables streamTables)
    {
        foreach (var tableName in streamTables.ObsoleteTableNames)
        {
            try
            {
                // each table has its own transaction, for one that fails not to roll back the others
                using var transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
                await using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                if (!await TryAcquireLockAsync(connection, streamTables.StreamId))
                {
                    return;
                }

                await TryDropCacheTableAsync(connection, streamTables.StreamId, tableName);
                transactionScope.Complete();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to remove table {TableName} for Stream '{StreamId}'.", tableName, streamTables.StreamId);
            }
        }
    }

    private async Task<bool> TryAcquireLockAsync(SqlConnection connection, Guid streamId)
    {
        if (await DistributedLockHelper.TryAcquireExclusiveLock(connection, $"RemoveTable_{streamId}", RemoveTableLockTimeoutInMilliseconds))
        {
            return true;
        }

        _logger.LogInformation("Unable to acquire lock to remove table for Stream '{StreamId}'. Skipping removal.", streamId);
        return false;
    }

    private static async Task<IReadOnlyList<string>> GetTableNamesAsync(SqlConnection connection)
    {
        const string getTablesSql = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            AND TABLE_NAME NOT LIKE '%History%'
            """;
        var list = new List<string>();
        await using var command = new SqlCommand(getTablesSql, connection)
        {
            CommandType = CommandType.Text
        };
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(reader.GetString(0));
        }

        return list;
    }

    private async Task TryDropCacheTableAsync(SqlConnection connection, Guid streamId, string tableName)
    {
        _logger.LogInformation("Dropping table {TableName} for Stream '{StreamId}'.", tableName, streamId);
        try
        {
            await DropCacheTableAsync(connection, streamId, tableName);
            _logger.LogInformation("Dropped table {TableName} for Stream '{StreamId}'.", tableName, streamId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to drop table {TableName} for Stream '{StreamId}'.", tableName, streamId);
        }
    }

    private static async Task DropCacheTableAsync(SqlConnection connection, Guid streamId, string tableName)
    {
        var prefix = CacheTableHelper.GetCacheTableName(streamId);
        var suffix = tableName[prefix.Length..].Replace("]", "]]").Replace("'", "''");
        var dropTableSql =
            $"""
             IF OBJECTPROPERTY(OBJECT_ID(N'[dbo].[{prefix}{suffix}]'), 'TableTemporalType') = 2
             ALTER TABLE [dbo].[{prefix}{suffix}] SET ( SYSTEM_VERSIONING = OFF  );
             IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{prefix}{suffix}]') AND type in (N'U'))
             DROP TABLE [dbo].[{prefix}{suffix}];
             IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{prefix}_History{suffix}]') AND type in (N'U'))
             DROP TABLE [dbo].[{prefix}_History{suffix}];
             """;
        await using var dropCommand = new SqlCommand(dropTableSql, connection)
        {
            CommandType = CommandType.Text
        };
        _ = await dropCommand.ExecuteNonQueryAsync();
    }

    private async Task DropExportHistoryTableAsync(SqlConnection connection, Guid streamId)
    {
        var tableName = StorageExportEntitiesJobBase.GetExportHistoryTableName(streamId);
        var dropTableSql = $"""
                            IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{tableName}]') AND type in (N'U'))
                            DROP TABLE [dbo].[{tableName}];
                            """;
        await using var dropCommand = new SqlCommand(dropTableSql, connection)
        {
            CommandType = CommandType.Text
        };
        _ = await dropCommand.ExecuteNonQueryAsync();
        _logger.LogInformation("Dropped export history table {TableName} for Stream '{StreamId}' if it existed.", tableName, streamId);
    }
}
