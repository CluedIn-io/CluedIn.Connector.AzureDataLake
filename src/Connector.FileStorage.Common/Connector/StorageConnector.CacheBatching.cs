using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
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
//
// Only the writes of a stream that uses soft delete are batched: a hard delete removes the row, which is not something
// the set based statements are needed for, so those are always written with the single entity path.
public abstract partial class StorageConnectorBase
{
    // A single entity is written with the single entity path, which has simpler statements than the set based ones
    private const int MinimumEntitiesForSetBasedWrite = 2;

    // SqlClient allows 2100 parameters in a command, and a VALUES clause 1000 rows
    private const int MaximumParametersPerCommand = 2000;
    private const int MaximumRowsPerCommand = 1000;

    private enum CacheWriteKind
    {
        Upsert,
        SoftDelete,
    }

    // A class and not a record: requests are told apart by reference, as two writes of the same entity are equal in value
    private sealed class CacheWriteRequest(string connectionString, string tableName, SyncItem item)
    {
        public string ConnectionString { get; } = connectionString;

        public string TableName { get; } = tableName;

        public SyncItem Item { get; } = item;

        public Guid EntityId => Item.EntityId;
    }

    private sealed record SetBasedWriteGroup(
        CacheWriteKind Kind,
        bool IsChanged,
        IReadOnlyList<string> Columns,
        IReadOnlyList<SqlDbType> ColumnTypes,
        List<CacheWriteRequest> Requests,
        List<object[]> StageValues);

    private OperationBatcher<CacheWriteRequest, bool> GetCacheWriteBatcher(Guid streamId)
    {
        return _cacheWriteBatchers.GetOrAdd(streamId, _ => new Lazy<OperationBatcher<CacheWriteRequest, bool>>(CreateCacheWriteBatcher)).Value;
    }

    private OperationBatcher<CacheWriteRequest, bool> CreateCacheWriteBatcher()
    {
        return new OperationBatcher<CacheWriteRequest, bool>(
            batchSize: Math.Max(1, _cacheBatchSize),
            inputProcessingThreadsCount: 1,
            outputProcessingThreadsCount: Math.Max(1, _cacheBatchOutputThreads),
            processOutputBatchFunc: WriteCacheBatch,
            batchTimeout: _cacheBatchTimeoutMilliseconds > 0 ? TimeSpan.FromMilliseconds(_cacheBatchTimeoutMilliseconds) : null,
            shouldCollectBatchSizeMetrics: CollectCacheBatchSizeMetrics);
    }

    /// <summary>
    /// Whether the batchers count the sizes of the batches they process. Off, as it is only of interest to a measurement.
    /// Read when a stream's batcher is created.
    /// </summary>
    protected virtual bool CollectCacheBatchSizeMetrics => false;

    /// <summary>
    /// The sizes of the batches processed so far for all streams: the number at index n - 1 is how many batches had n entities.
    /// Empty unless <see cref="CollectCacheBatchSizeMetrics"/> is on.
    /// </summary>
    protected IReadOnlyList<int> GetCacheBatchSizeHistogram()
    {
        var histogram = new List<int>();
        foreach (var batcher in _cacheWriteBatchers.Values.Where(x => x.IsValueCreated).Select(x => x.Value))
        {
            foreach (var inputThread in batcher.GetBatchSizeStats())
            {
                var index = 0;
                foreach (var count in inputThread)
                {
                    if (index == histogram.Count)
                    {
                        histogram.Add(0);
                    }

                    histogram[index++] += count;
                }
            }
        }

        return histogram;
    }

    private async Task<IList<OperationResult<bool>>> WriteCacheBatch(IList<CacheWriteRequest> requests)
    {
        // The batcher's own tasks never run inside a caller's transaction, and the writes below manage theirs.
        using var suppressAmbientTransaction = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);

        // A batcher belongs to one stream, so all its requests are for the same cache table on the same connection
        return await WriteCacheTableRequests(requests, requests[0].ConnectionString, requests[0].TableName);
    }

    private async Task<IList<OperationResult<bool>>> WriteCacheTableRequests(
        IList<CacheWriteRequest> requests,
        string connectionString,
        string tableName)
    {
        var results = new Dictionary<CacheWriteRequest, OperationResult<bool>>(requests.Count);

        // When an entity occurs more than once and none of them removes it, only the one with the highest persist version
        // has an effect (it is only written over a lower persist version), so the others, adds included, are skipped.
        // The first of equal persist versions is the one, as it is the one that is written first without batching.
        var superseded = new HashSet<CacheWriteRequest>();
        foreach (var sameEntity in requests
            .GroupBy(request => request.EntityId)
            .Where(group => group.Count() > 1 && !group.Any(request => request.Item.ChangeType == VersionChangeType.Removed)))
        {
            var newest = sameEntity.MaxBy(request => request.Item.PersistVersion ?? int.MinValue);
            superseded.UnionWith(sameEntity.Where(request => request != newest));
        }

        // Only the first occurrence of an entity is written set based. Later occurrences are written afterwards,
        // one by one and in order, so the last write of an entity still wins, as it does without batching.
        var seenEntityIds = new HashSet<Guid>();
        var firstOccurrences = new List<CacheWriteRequest>();
        var laterOccurrences = new List<CacheWriteRequest>();
        foreach (var request in requests)
        {
            if (superseded.Contains(request))
            {
                results[request] = new OperationResult<bool>(true, null);
            }
            else
            {
                (seenEntityIds.Add(request.EntityId) ? firstOccurrences : laterOccurrences).Add(request);
            }
        }

        var writeOneByOne = new List<CacheWriteRequest>();
        var setBasedGroups = new List<SetBasedWriteGroup>();
        foreach (var group in GroupForSetBasedWrite(firstOccurrences))
        {
            if (group.Requests.Count < MinimumEntitiesForSetBasedWrite)
            {
                writeOneByOne.AddRange(group.Requests);
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
                var notWrittenRequests = notWritten.ToHashSet();
                foreach (var request in setBasedGroups.SelectMany(group => group.Requests).Where(request => !notWrittenRequests.Contains(request)))
                {
                    results[request] = new OperationResult<bool>(true, null);
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
                        setBasedGroups.Sum(group => group.Requests.Count),
                        tableName);
                }

                writeOneByOne.AddRange(setBasedGroups.SelectMany(group => group.Requests));
            }
        }

        foreach (var request in writeOneByOne.Concat(laterOccurrences))
        {
            try
            {
                await WriteToCacheTableUnbatched(request.ConnectionString, request.TableName, useSoftDelete: true, request.Item);
                results[request] = new OperationResult<bool>(true, null);
            }
            catch (Exception ex)
            {
                results[request] = new OperationResult<bool>(false, ex);
            }
        }

        return requests.Select(request => results[request]).ToList();
    }

    /// <summary>
    /// Groups the requests that one set of statements can write: the same kind of write, with the same columns, of the types
    /// the single entity path would send them as (a column of the stage has one type, and the server converts it into the
    /// cache table's column like it does a parameter of that type).
    /// </summary>
    private static IEnumerable<SetBasedWriteGroup> GroupForSetBasedWrite(List<CacheWriteRequest> requests)
    {
        var groups = new Dictionary<(CacheWriteKind Kind, bool IsChanged, string Signature), SetBasedWriteGroup>();
        foreach (var request in requests)
        {
            var item = request.Item;
            var kind = item.ChangeType == VersionChangeType.Removed ? CacheWriteKind.SoftDelete : CacheWriteKind.Upsert;
            var isChanged = kind == CacheWriteKind.Upsert && item.ChangeType == VersionChangeType.Changed;

            // A soft delete only touches the id, change type, timestamp and epoch
            var columns = kind == CacheWriteKind.Upsert
                ? GetPropertyKeysWithoutId(item)
                : [StorageConfigurationConstants.ChangeTypeKey, StorageConfigurationConstants.TimestampKey, StorageConfigurationConstants.EpochKey];

            // The values of the stage columns after the id, as the single entity path would send them
            var stageValues = columns.Select(column => GetStageValue(item, kind, column)).ToArray();
            var columnTypes = columns
                .Select((column, index) => CacheTableParameterHelper.GetSqlDbType(stageValues[index], GetStageColumnType(item, kind, column)))
                .ToList();

            var key = (kind, isChanged, string.Join('\u001f', columns.Zip(columnTypes, (column, type) => $"{column}:{type}")));
            if (!groups.TryGetValue(key, out var group))
            {
                group = new SetBasedWriteGroup(kind, isChanged, columns, columnTypes, new List<CacheWriteRequest>(), new List<object[]>());
                groups.Add(key, group);
            }

            group.Requests.Add(request);
            group.StageValues.Add(stageValues);
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
    /// Writes the groups in one transaction and returns the requests it did not write: the ones that already exist
    /// and are not changed by the statements (e.g. an older version), which the single entity path judges.
    /// Throws, after rolling back, when anything fails.
    /// </summary>
    private static async Task<List<CacheWriteRequest>> WriteSetBased(
        string connectionString,
        string tableName,
        List<SetBasedWriteGroup> groups)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();

        var notWritten = new List<CacheWriteRequest>();
        foreach (var group in groups)
        {
            notWritten.AddRange(await WriteSetBasedGroup(connection, transaction, tableName, group));
        }

        await transaction.CommitAsync();
        return notWritten;
    }

    private static async Task<List<CacheWriteRequest>> WriteSetBasedGroup(
        SqlConnection connection,
        SqlTransaction transaction,
        string tableName,
        SetBasedWriteGroup group)
    {
        var rowsPerCommand = Math.Clamp(MaximumParametersPerCommand / (group.Columns.Count + 1), 1, MaximumRowsPerCommand);

        var handledEntityIds = new HashSet<Guid>();
        for (var firstRow = 0; firstRow < group.Requests.Count; firstRow += rowsPerCommand)
        {
            var rowCount = Math.Min(rowsPerCommand, group.Requests.Count - firstRow);

            await using var command = new SqlCommand { Connection = connection, Transaction = transaction, CommandType = CommandType.Text };
            var rows = GetRows(command.Parameters, group, firstRow, rowCount);
            command.CommandText = GetSetBasedWriteSql(group, tableName, rows);

            // The statements return the ids of the entities they wrote, one result for each of them
            await using var reader = await command.ExecuteReaderAsync();
            do
            {
                while (await reader.ReadAsync())
                {
                    handledEntityIds.Add(reader.GetGuid(0));
                }
            }
            while (await reader.NextResultAsync());
        }

        return group.Requests.Where(request => !handledEntityIds.Contains(request.EntityId)).ToList();
    }

    /// <summary>
    /// The rows of the entities as a table of the statements. The values are parameters of the types the single entity path
    /// sends them as, so the server converts them into the cache table's columns like it does for that path.
    /// </summary>
    private static string GetRows(SqlParameterCollection parameters, SetBasedWriteGroup group, int firstRow, int rowCount)
    {
        var sql = new StringBuilder("(VALUES ");
        for (var row = 0; row < rowCount; row++)
        {
            var source = firstRow + row;
            sql.Append(row == 0 ? "(" : ", (");
            sql.Append($"@i{row}");
            parameters.Add(new SqlParameter($"@i{row}", SqlDbType.UniqueIdentifier) { Value = group.Requests[source].EntityId });

            for (var column = 0; column < group.Columns.Count; column++)
            {
                sql.Append($", @v{row}_{column}");
                parameters.Add(CacheTableParameterHelper.CreateParameter($"@v{row}_{column}", group.StageValues[source][column], group.ColumnTypes[column]));
            }

            sql.Append(')');
        }

        sql.Append($") AS s ({string.Join(", ", group.Columns.Prepend(StorageConfigurationConstants.IdKey).Select(column => $"[{column}]"))})");
        return sql.ToString();
    }

    private static string GetSetBasedWriteSql(SetBasedWriteGroup group, string tableName, string rows)
    {
        const string id = StorageConfigurationConstants.IdKey;
        var output = $"OUTPUT inserted.[{id}]";

        switch (group.Kind)
        {
            case CacheWriteKind.SoftDelete:
                return $"""
                    UPDATE t SET
                        {string.Join(",\n    ", group.Columns.Select(column => $"t.[{column}] = s.[{column}]"))}
                    {output}
                    FROM {rows} INNER LOOP JOIN [{tableName}] AS t ON t.[{id}] = s.[{id}];
                    """;

            default:
                // Same conditions as the single entity upsert with soft delete: an entity is only updated by a newer
                // persist version, and a removed one is only brought back by something other than a change.
                var versionCondition = $"WHERE t.[{StorageConfigurationConstants.PersistVersionKey}] < s.[{StorageConfigurationConstants.PersistVersionKey}]";
                if (group.IsChanged)
                {
                    var changeType = $"t.[{StorageConfigurationConstants.ChangeTypeKey}]";
                    versionCondition += $" AND ({changeType} != '{nameof(VersionChangeType.Removed)}' OR {changeType} IS NULL)";
                }

                var allColumns = group.Columns.Prepend(id).ToList();
                return $"""
                    UPDATE t SET
                        {string.Join(",\n    ", group.Columns.Select(column => $"t.[{column}] = s.[{column}]"))}
                    {output}
                    FROM {rows} INNER LOOP JOIN [{tableName}] AS t ON t.[{id}] = s.[{id}]
                    {versionCondition};

                    INSERT INTO [{tableName}] ({string.Join(", ", allColumns.Select(column => $"[{column}]"))})
                    {output}
                    SELECT {string.Join(", ", allColumns.Select(column => $"s.[{column}]"))}
                    FROM {rows}
                    WHERE NOT EXISTS (SELECT 1 FROM [{tableName}] AS t WHERE t.[{id}] = s.[{id}]);
                    """;
        }
    }
}
