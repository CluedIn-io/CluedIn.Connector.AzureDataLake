using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FileStorage.Common.Connector;

/// <summary>
/// Removes tables of streams from the stream cache database. The data of a table that cannot be removed is truncated.
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
    public Task RemoveAllTablesAsync(Guid streamId)
    {
        return RunWithLockAsync(streamId, async connection =>
        {
            var tableNames = await GetTableNamesAsync(connection);
            var streamTables = StreamCacheTables.FromTableNames(tableNames).SingleOrDefault(tables => tables.StreamId == streamId);
            if (streamTables != null)
            {
                foreach (var tableName in streamTables.TableNames)
                {
                    _ = await TryDropOrTruncateCacheTableAsync(connection, streamId, tableName);
                }
            }

            await DropExportHistoryTableAsync(connection, streamId);
        });
    }

    /// <summary>
    /// Removes the archived tables of a stream, except for the most recent one.
    /// </summary>
    /// <returns>The names of the tables that were removed.</returns>
    public async Task<IReadOnlyList<string>> RemoveObsoleteTablesAsync(StreamCacheTables streamTables)
    {
        var removedTableNames = new List<string>();
        var obsoleteTableNames = streamTables.ObsoleteTableNames;
        if (obsoleteTableNames.Count == 0)
        {
            return removedTableNames;
        }

        await RunWithLockAsync(streamTables.StreamId, async connection =>
        {
            foreach (var tableName in obsoleteTableNames)
            {
                if (await TryDropOrTruncateCacheTableAsync(connection, streamTables.StreamId, tableName))
                {
                    removedTableNames.Add(tableName);
                }
            }
        });
        return removedTableNames;
    }

    private async Task RunWithLockAsync(Guid streamId, Func<SqlConnection, Task> action)
    {
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        // a lock of the session, because each table is removed in a transaction of its own
        var lockName = $"RemoveTable_{streamId}";
        if (!await DistributedLockHelper.TryAcquireExclusiveSessionLock(connection, lockName, RemoveTableLockTimeoutInMilliseconds))
        {
            _logger.LogInformation("Unable to acquire lock to remove table for Stream '{StreamId}'. Skipping removal.", streamId);
            return;
        }

        try
        {
            await action(connection);
        }
        finally
        {
            // a connection that was closed by an error has lost its lock
            if (connection.State == ConnectionState.Open)
            {
                await DistributedLockHelper.ReleaseSessionLock(connection, lockName);
            }
        }
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

    private async Task<bool> TryDropOrTruncateCacheTableAsync(SqlConnection connection, Guid streamId, string tableName)
    {
        _logger.LogInformation("Dropping table {TableName} for Stream '{StreamId}'.", tableName, streamId);
        try
        {
            await DropCacheTableAsync(connection, streamId, tableName);
            _logger.LogInformation("Dropped table {TableName} for Stream '{StreamId}'.", tableName, streamId);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to drop table {TableName} for Stream '{StreamId}'. Truncating it instead.", tableName, streamId);
        }

        try
        {
            await TruncateCacheTableAsync(connection, streamId, tableName);
            _logger.LogWarning("Table {TableName} for Stream '{StreamId}' was truncated because it could not be dropped. Please remove it, and its history table, manually.", tableName, streamId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to truncate table {TableName} for Stream '{StreamId}'. Please remove it, and its history table, manually.", tableName, streamId);
        }

        return false;
    }

    private static Task DropCacheTableAsync(SqlConnection connection, Guid streamId, string tableName)
    {
        return ExecuteOnCacheTableAsync(connection, streamId, tableName, "DROP TABLE");
    }

    private static Task TruncateCacheTableAsync(SqlConnection connection, Guid streamId, string tableName)
    {
        return ExecuteOnCacheTableAsync(connection, streamId, tableName, "TRUNCATE TABLE");
    }

    /// <summary>
    /// Runs the statement on the table and its history table, both or none. Neither can be dropped or truncated while the table is
    /// system-versioned, so that is turned off.
    /// </summary>
    private static async Task ExecuteOnCacheTableAsync(SqlConnection connection, Guid streamId, string tableName, string statement)
    {
        var prefix = CacheTableHelper.GetCacheTableName(streamId);
        var suffix = tableName[prefix.Length..].Replace("]", "]]").Replace("'", "''");
        var sql =
            $"""
             IF OBJECTPROPERTY(OBJECT_ID(N'[dbo].[{prefix}{suffix}]'), 'TableTemporalType') = 2
             ALTER TABLE [dbo].[{prefix}{suffix}] SET ( SYSTEM_VERSIONING = OFF  );
             IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{prefix}{suffix}]') AND type in (N'U'))
             {statement} [dbo].[{prefix}{suffix}];
             IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[{prefix}_History{suffix}]') AND type in (N'U'))
             {statement} [dbo].[{prefix}_History{suffix}];
             """;
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
        await using var command = new SqlCommand(sql, connection, transaction)
        {
            CommandType = CommandType.Text
        };
        _ = await command.ExecuteNonQueryAsync();
        await transaction.CommitAsync();
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
