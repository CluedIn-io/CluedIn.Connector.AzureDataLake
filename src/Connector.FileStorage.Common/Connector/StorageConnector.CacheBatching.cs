using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

using CluedIn.Core.Data.Parts;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FileStorage.Common.Connector;

// Writes to the stream cache table in batches. Without this, every entity opens its own connection and
// transaction and runs its own statement, which is a lot of round trips and pooled connections when many
// entities are stored at the same time. With it, the entities that are stored concurrently are collected by
// the OperationBatcher and written with one connection, one transaction and a handful of set based statements.
//
// The set based statements only write what is certain to produce the same table as the single entity path:
// anything else (the entity is already up to date, the table does not exist yet, a statement failed, the same
// entity occurs twice in the batch) is written with the single entity path, which is also the path used when
// batching is switched off. So the batched path never decides a corner case on its own.
public abstract partial class StorageConnectorBase
{
    private const int MaximumCacheWriteOutputThreads = 4;

    // A set based write costs a fixed number of round trips (stage, copy, write, read back), which a
    // single entity does not need.
    private const int MinimumEntitiesForSetBasedWrite = 2;

    // SqlClient allows 2100 parameters in a command, and a VALUES clause 1000 rows
    private const int MaximumParametersPerCommand = 2000;
    private const int MaximumRowsPerInsert = 1000;

    private const string StageTableName = "#CacheWriteStage";
    private const string HandledTableName = "#CacheWriteHandled";

    // A set based update of a table locks its rows in the clustered key and in its [ValidFromValidTo] index (an update
    // moves the row in that one) in different orders, so two of them at the same time deadlock each other. The batches
    // of a table are therefore written one after the other, and the next one grows while the current one is written.
    private readonly ConcurrentDictionary<(string ConnectionString, string TableName), SemaphoreSlim> _cacheTableWriteGates = new();

    private enum CacheWriteKind
    {
        Upsert,
        HardDelete,
        SoftDelete,
    }

    private sealed record CacheWriteRequest(
        string ConnectionString,
        string TableName,
        bool UseSoftDelete,
        SyncItem Item);

    private sealed class CacheWriteEntry(CacheWriteRequest request, int index)
    {
        public CacheWriteRequest Request { get; } = request;

        /// <summary>Position of the request in the batch, which is where its result goes.</summary>
        public int Index { get; } = index;

        public Guid EntityId => Request.Item.EntityId;

        /// <summary>The values of the stage columns after the id, as the single entity path would send them.</summary>
        public object[] StageValues { get; set; }
    }

    private sealed record SetBasedWriteGroup(
        CacheWriteKind Kind,
        bool UseSoftDelete,
        bool IsChanged,
        IReadOnlyList<string> Columns,
        IReadOnlyList<SqlDbType> ColumnTypes,
        List<CacheWriteEntry> Entries);

    private OperationBatcher<CacheWriteRequest, bool> CreateCacheWriteBatcher(int batchSize, int batchTimeoutMilliseconds)
    {
        return new OperationBatcher<CacheWriteRequest, bool>(
            batchSize: Math.Max(1, batchSize),
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: Math.Clamp(Environment.ProcessorCount, 1, MaximumCacheWriteOutputThreads),
            processOutputBatchFunc: WriteCacheBatch,
            batchTimeout: batchTimeoutMilliseconds > 0 ? TimeSpan.FromMilliseconds(batchTimeoutMilliseconds) : null);
    }

    private async Task<IList<OperationResult<bool>>> WriteCacheBatch(IList<CacheWriteRequest> requests)
    {
        var results = new OperationResult<bool>[requests.Count];

        // The batcher's own tasks never run inside a caller's transaction, and the writes below manage theirs.
        using var suppressAmbientTransaction = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);

        var tableGroups = requests
            .Select((request, index) => new CacheWriteEntry(request, index))
            .GroupBy(entry => (entry.Request.ConnectionString, entry.Request.TableName));
        foreach (var tableGroup in tableGroups)
        {
            await WriteCacheTableEntries(tableGroup.ToList(), results);
        }

        return results;
    }

    private async Task WriteCacheTableEntries(List<CacheWriteEntry> entries, OperationResult<bool>[] results)
    {
        var connectionString = entries[0].Request.ConnectionString;
        var tableName = entries[0].Request.TableName;

        var gate = _cacheTableWriteGates.GetOrAdd((connectionString, tableName), _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync();
        try
        {
            await WriteCacheTableEntries(entries, connectionString, tableName, results);
        }
        finally
        {
            gate.Release();
        }
    }

    private async Task WriteCacheTableEntries(
        List<CacheWriteEntry> entries,
        string connectionString,
        string tableName,
        OperationResult<bool>[] results)
    {
        // Only the first occurrence of an entity is written set based. Later occurrences are written afterwards,
        // one by one and in order, so the last write of an entity still wins, as it does without batching.
        var seenEntityIds = new HashSet<Guid>();
        var firstOccurrences = new List<CacheWriteEntry>();
        var laterOccurrences = new List<CacheWriteEntry>();
        foreach (var entry in entries)
        {
            (seenEntityIds.Add(entry.EntityId) ? firstOccurrences : laterOccurrences).Add(entry);
        }

        var writeOneByOne = new List<CacheWriteEntry>();
        var setBasedGroups = new List<SetBasedWriteGroup>();
        foreach (var group in GroupForSetBasedWrite(firstOccurrences))
        {
            if (group.Entries.Count < MinimumEntitiesForSetBasedWrite)
            {
                writeOneByOne.AddRange(group.Entries);
            }
            else
            {
                setBasedGroups.Add(group);
            }
        }

        if (setBasedGroups.Count > 0)
        {
            try
            {
                var notWritten = await WriteSetBased(connectionString, tableName, setBasedGroups);
                var notWrittenEntries = notWritten.ToHashSet();
                foreach (var entry in setBasedGroups.SelectMany(group => group.Entries).Where(entry => !notWrittenEntries.Contains(entry)))
                {
                    results[entry.Index] = new OperationResult<bool>(true, null);
                }

                writeOneByOne.AddRange(notWritten);
            }
            catch (Exception ex)
            {
                // The transaction was rolled back, so nothing of this batch was written. Writing the entities one by one
                // gives each its own outcome, and creates the table when that is why this failed.
                if (ex is SqlException sqlException && sqlException.IsTableNotFoundException())
                {
                    _logger.LogDebug("Table {TableName} does not exist. Writing the batch one entity at a time.", tableName);
                }
                else
                {
                    _logger.LogWarning(
                        ex,
                        "Failed to write a batch of {Count} entities to cache table {TableName}. Writing them one entity at a time.",
                        setBasedGroups.Sum(group => group.Entries.Count),
                        tableName);
                }

                writeOneByOne.AddRange(setBasedGroups.SelectMany(group => group.Entries));
            }
        }

        foreach (var entry in writeOneByOne.Concat(laterOccurrences))
        {
            try
            {
                var request = entry.Request;
                await WriteToCacheTableUnbatched(request.ConnectionString, request.TableName, request.UseSoftDelete, request.Item);
                results[entry.Index] = new OperationResult<bool>(true, null);
            }
            catch (Exception ex)
            {
                results[entry.Index] = new OperationResult<bool>(false, ex);
            }
        }
    }

    /// <summary>
    /// Groups the entries that one set of statements can write: the same kind of write, with the same columns, of the types
    /// the single entity path would send them as (a column of the stage has one type, and the server converts it into the
    /// cache table's column like it does a parameter of that type).
    /// </summary>
    private static IEnumerable<SetBasedWriteGroup> GroupForSetBasedWrite(List<CacheWriteEntry> entries)
    {
        var groups = new Dictionary<(CacheWriteKind Kind, bool UseSoftDelete, bool IsChanged, string Signature), SetBasedWriteGroup>();
        foreach (var entry in entries)
        {
            var item = entry.Request.Item;
            var useSoftDelete = entry.Request.UseSoftDelete;
            var kind = item.ChangeType == VersionChangeType.Removed
                ? useSoftDelete ? CacheWriteKind.SoftDelete : CacheWriteKind.HardDelete
                : CacheWriteKind.Upsert;
            var isChanged = kind == CacheWriteKind.Upsert && useSoftDelete && item.ChangeType == VersionChangeType.Changed;

            // A delete only touches the id (and, for a soft delete, the change type, timestamp and epoch)
            var columns = kind switch
            {
                CacheWriteKind.Upsert => GetPropertyKeysWithoutId(item),
                CacheWriteKind.SoftDelete => [StorageConfigurationConstants.ChangeTypeKey, StorageConfigurationConstants.TimestampKey, StorageConfigurationConstants.EpochKey],
                _ => new List<string>(),
            };

            entry.StageValues = columns.Select(column => GetStageValue(item, kind, column)).ToArray();
            var columnTypes = columns
                .Select((column, index) => CacheTableParameterHelper.GetSqlDbType(entry.StageValues[index], GetStageColumnType(item, kind, column)))
                .ToList();

            var key = (kind, useSoftDelete, isChanged, string.Join('\u001f', columns.Zip(columnTypes, (column, type) => $"{column}:{type}")));
            if (!groups.TryGetValue(key, out var group))
            {
                group = new SetBasedWriteGroup(kind, useSoftDelete, isChanged, columns, columnTypes, new List<CacheWriteEntry>());
                groups.Add(key, group);
            }

            group.Entries.Add(entry);
        }

        return groups.Values;
    }

    // The single entity path sends the change type of the entity for a soft delete, not what the data holds
    private static object GetStageValue(SyncItem item, CacheWriteKind kind, string column)
    {
        return kind == CacheWriteKind.SoftDelete && column == StorageConfigurationConstants.ChangeTypeKey
            ? item.ChangeType.ToString()
            : GetDatabaseValue(item, column);
    }

    private static Type GetStageColumnType(SyncItem item, CacheWriteKind kind, string column)
    {
        return kind == CacheWriteKind.SoftDelete && column == StorageConfigurationConstants.ChangeTypeKey
            ? typeof(string)
            : item.DataValueTypes[column];
    }

    /// <summary>
    /// Writes the groups in one transaction and returns the entries it did not write: the ones that already exist
    /// and are not changed by the statements (e.g. an older version), which the single entity path judges.
    /// Throws, after rolling back, when anything fails.
    /// </summary>
    private static async Task<List<CacheWriteEntry>> WriteSetBased(
        string connectionString,
        string tableName,
        List<SetBasedWriteGroup> groups)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();

        var notWritten = new List<CacheWriteEntry>();
        foreach (var group in groups)
        {
            notWritten.AddRange(await WriteSetBasedGroup(connection, transaction, tableName, group));
        }

        await transaction.CommitAsync();
        return notWritten;
    }

    private static async Task<List<CacheWriteEntry>> WriteSetBasedGroup(
        SqlConnection connection,
        SqlTransaction transaction,
        string tableName,
        SetBasedWriteGroup group)
    {
        const string id = StorageConfigurationConstants.IdKey;

        var stageColumnDefinitions = group.Columns
            .Select((column, index) => $"[{column}] {CacheTableParameterHelper.GetColumnTypeDeclaration(group.ColumnTypes[index])}")
            .Prepend($"[{id}] UNIQUEIDENTIFIER NOT NULL PRIMARY KEY CLUSTERED");
        var stageColumns = string.Join(", ", group.Columns.Prepend(id).Select(column => $"[{column}]"));

        // The entities go to the stage as parameters, with the types the single entity path sends them as, so the server
        // converts them into the cache table's columns like it does for that path. The stage, the statements, what they
        // wrote and the clean up are the same command when the parameters fit in one, which is one round trip for the batch.
        var rowsPerCommand = Math.Clamp(MaximumParametersPerCommand / (group.Columns.Count + 1), 1, MaximumRowsPerInsert);
        var chunks = group.Entries.Chunk(rowsPerCommand).ToList();

        var handledEntityIds = new HashSet<Guid>();
        for (var i = 0; i < chunks.Count; i++)
        {
            var isFirst = i == 0;
            var isLast = i == chunks.Count - 1;

            var sql = new StringBuilder();
            if (isFirst)
            {
                sql.AppendLine($"CREATE TABLE {StageTableName} ({string.Join(", ", stageColumnDefinitions)});");
                sql.AppendLine($"CREATE TABLE {HandledTableName} ([{id}] UNIQUEIDENTIFIER NOT NULL);");
            }

            await using var command = new SqlCommand { Connection = connection, Transaction = transaction, CommandType = CommandType.Text };
            AppendStageRows(sql, command.Parameters, chunks[i], group, stageColumns);

            if (!isLast)
            {
                command.CommandText = sql.ToString();
                await command.ExecuteNonQueryAsync();
                continue;
            }

            sql.AppendLine(GetSetBasedWriteSql(group, tableName));
            sql.AppendLine($"SELECT [{id}] FROM {HandledTableName}; DROP TABLE {StageTableName}; DROP TABLE {HandledTableName};");
            command.CommandText = sql.ToString();
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                handledEntityIds.Add(reader.GetGuid(0));
            }
        }

        return group.Entries.Where(entry => !handledEntityIds.Contains(entry.EntityId)).ToList();
    }

    private static void AppendStageRows(
        StringBuilder sql,
        SqlParameterCollection parameters,
        CacheWriteEntry[] entries,
        SetBasedWriteGroup group,
        string stageColumns)
    {
        sql.Append($"INSERT INTO {StageTableName} ({stageColumns}) VALUES ");
        for (var row = 0; row < entries.Length; row++)
        {
            sql.Append(row == 0 ? "(" : ", (");
            sql.Append($"@i{row}");
            parameters.Add(new SqlParameter($"@i{row}", SqlDbType.UniqueIdentifier) { Value = entries[row].EntityId });

            for (var column = 0; column < group.Columns.Count; column++)
            {
                sql.Append($", @v{row}_{column}");
                parameters.Add(CacheTableParameterHelper.CreateParameter($"@v{row}_{column}", entries[row].StageValues[column], group.ColumnTypes[column]));
            }

            sql.Append(')');
        }

        sql.AppendLine(";");
    }

    private static string GetSetBasedWriteSql(SetBasedWriteGroup group, string tableName)
    {
        const string id = StorageConfigurationConstants.IdKey;
        var handled = $"OUTPUT inserted.[{id}] INTO {HandledTableName} ([{id}])";

        switch (group.Kind)
        {
            case CacheWriteKind.HardDelete:
                return $"""
                    DELETE t OUTPUT deleted.[{id}] INTO {HandledTableName} ([{id}])
                    FROM {StageTableName} AS s INNER LOOP JOIN [{tableName}] AS t ON t.[{id}] = s.[{id}];
                    """;

            case CacheWriteKind.SoftDelete:
                return $"""
                    UPDATE t SET
                        {string.Join(",\n    ", group.Columns.Select(column => $"t.[{column}] = s.[{column}]"))}
                    {handled}
                    FROM {StageTableName} AS s INNER LOOP JOIN [{tableName}] AS t ON t.[{id}] = s.[{id}];
                    """;

            default:
                // Same conditions as the single entity upsert: with soft delete an entity is only updated by a newer
                // persist version, and a removed one is only brought back by something other than a change.
                var versionCondition = string.Empty;
                if (group.UseSoftDelete)
                {
                    versionCondition = $"WHERE t.[{StorageConfigurationConstants.PersistVersionKey}] < s.[{StorageConfigurationConstants.PersistVersionKey}]";
                    if (group.IsChanged)
                    {
                        var changeType = $"t.[{StorageConfigurationConstants.ChangeTypeKey}]";
                        versionCondition += $" AND ({changeType} != '{nameof(VersionChangeType.Removed)}' OR {changeType} IS NULL)";
                    }
                }

                var allColumns = group.Columns.Prepend(id).ToList();
                return $"""
                    UPDATE t SET
                        {string.Join(",\n    ", group.Columns.Select(column => $"t.[{column}] = s.[{column}]"))}
                    {handled}
                    FROM {StageTableName} AS s INNER LOOP JOIN [{tableName}] AS t ON t.[{id}] = s.[{id}]
                    {versionCondition};

                    INSERT INTO [{tableName}] ({string.Join(", ", allColumns.Select(column => $"[{column}]"))})
                    {handled}
                    SELECT {string.Join(", ", allColumns.Select(column => $"s.[{column}]"))}
                    FROM {StageTableName} AS s
                    WHERE NOT EXISTS (SELECT 1 FROM [{tableName}] AS t WHERE t.[{id}] = s.[{id}]);
                    """;
        }
    }

    private static async Task ExecuteNonQuery(SqlConnection connection, SqlTransaction transaction, string sql)
    {
        await using var command = new SqlCommand(sql, connection, transaction)
        {
            CommandType = CommandType.Text,
        };
        await command.ExecuteNonQueryAsync();
    }
}
