using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Connector.FileStorage.Common.Connector.SqlDataWriter;
using CluedIn.Connector.Snowflake.Connector.Snowpipe;
using CluedIn.Core;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.Snowflake.Connector.SqlDataWriter;

// Streams delta rows read from the SQL Server cache table straight into the Snowflake
// transient table via the Snowpipe Streaming REST API - it never writes bytes to
// outputStream (that stream only exists to satisfy the base export pipeline's
// file-writing contract; see SnowflakeStorageClient). Rows are batched rather than sent
// one HTTP call at a time, since many small round trips are expensive against Snowflake.
internal class SnowflakeSnowpipeSqlDataWriter : SqlDataWriterBase
{
    private const int BatchSize = 1000;

    public SnowflakeSnowpipeSqlDataWriter()
    {
    }

    public override async Task<long> WriteOutputAsync(
        ExecutionContext context,
        IStorageConfiguration configuration,
        Stream outputStream,
        ICollection<string> fieldNames,
        bool isInitialExport,
        SqlDataReader reader)
    {
        if (configuration is not SnowflakeConnectorConfiguration snowflakeConfiguration)
        {
            throw new ArgumentException($"Configuration must be of type {nameof(SnowflakeConnectorConfiguration)}.", nameof(configuration));
        }

        var apiClient = new SnowflakeApiClient(SnowflakeConnectionSettings.FromConfiguration(snowflakeConfiguration));
        var channelName = $"cluedin_{Guid.NewGuid():N}";
        var totalProcessed = 0L;

        try
        {
            var channel = await apiClient.OpenChannelAsync(snowflakeConfiguration.PipeName, channelName);
            var batch = new List<IReadOnlyDictionary<string, object>>(BatchSize);

            while (await reader.ReadAsync())
            {
                if (ShouldSkip(configuration, isInitialExport, reader))
                {
                    continue;
                }

                var rowData = new Dictionary<string, object>();
                foreach (var field in fieldNames)
                {
                    rowData[field] = GetValue(field, reader, configuration);
                }

                rowData.TryGetValue(StorageConfigurationConstants.IdKey, out var entityId);
                rowData.TryGetValue(StorageConfigurationConstants.ChangeTypeKey, out var changeType);
                rowData.TryGetValue(StorageConfigurationConstants.PersistVersionKey, out var persistVersion);

                var shapedRow = new Dictionary<string, object>
                {
                    [TransientTableColumns.EntityId] = entityId?.ToString(),
                    [TransientTableColumns.ChangeType] = changeType?.ToString() ?? "Added",
                    [TransientTableColumns.PersistVersion] = persistVersion,
                    [TransientTableColumns.RowData] = rowData,
                };

                batch.Add(shapedRow);
                totalProcessed++;

                if (batch.Count >= BatchSize)
                {
                    channel = await apiClient.AppendRowsAsync(snowflakeConfiguration.PipeName, channelName, channel.ContinuationToken, batch);
                    batch.Clear();
                }

                if (totalProcessed % LoggingThreshold == 0)
                {
                    context.Log.LogDebug("Streamed {Total} rows to Snowflake transient table {TransientTable}.", totalProcessed, snowflakeConfiguration.TransientTableName);
                }
            }

            if (batch.Count > 0)
            {
                await apiClient.AppendRowsAsync(snowflakeConfiguration.PipeName, channelName, channel.ContinuationToken, batch);
            }
        }
        finally
        {
            await apiClient.CloseChannelAsync(snowflakeConfiguration.PipeName, channelName);
            (apiClient as IDisposable)?.Dispose();
        }

        return totalProcessed;
    }
}
