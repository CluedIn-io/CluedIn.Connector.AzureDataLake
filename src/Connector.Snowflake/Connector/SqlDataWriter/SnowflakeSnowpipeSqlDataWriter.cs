using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.Json;
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
//
// Each row is shaped as one column per CluedIn property (see SnowflakeSqlBuilder, which
// this writer must stay in lockstep with for column naming), not a single VARIANT blob -
// matching how the old CluedIn.Connector.Snowflake repo's table shape works.
internal class SnowflakeSnowpipeSqlDataWriter : SqlDataWriterBase
{
    private const int BatchSize = 1000;
    private static readonly TimeSpan CommitTimeout = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan CommitPollInterval = TimeSpan.FromSeconds(2);

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

                var shapedRow = new Dictionary<string, object>();
                foreach (var field in fieldNames)
                {
                    var value = GetValue(field, reader, configuration);
                    shapedRow[SnowflakeSqlBuilder.SanitizeColumnName(field)] = ConvertValueForColumn(field, value);
                }

                batch.Add(shapedRow);
                totalProcessed++;

                if (batch.Count >= BatchSize)
                {
                    channel = await apiClient.AppendRowsAsync(snowflakeConfiguration.PipeName, channelName, channel.ContinuationToken, totalProcessed.ToString(CultureInfo.InvariantCulture), batch);
                    batch.Clear();
                }

                if (totalProcessed % LoggingThreshold == 0)
                {
                    context.Log.LogDebug("Streamed {Total} rows to Snowflake transient table {TransientTable}.", totalProcessed, snowflakeConfiguration.TransientTableName);
                }
            }

            if (batch.Count > 0)
            {
                await apiClient.AppendRowsAsync(snowflakeConfiguration.PipeName, channelName, channel.ContinuationToken, totalProcessed.ToString(CultureInfo.InvariantCulture), batch);
            }

            // Appending only buffers rows - verified live that closing the channel right
            // after an append does not wait for (or force) that data to commit, and the
            // buffered rows are simply lost, leaving the transient table empty for the
            // MERGE step that follows. Wait for Snowflake to actually commit everything
            // this run appended before the finally block below closes the channel.
            if (totalProcessed > 0)
            {
                await WaitForCommitAsync(context, apiClient, snowflakeConfiguration, channelName, totalProcessed.ToString(CultureInfo.InvariantCulture));
            }
        }
        finally
        {
            await apiClient.CloseChannelAsync(snowflakeConfiguration.PipeName, channelName);
            (apiClient as IDisposable)?.Dispose();
        }

        return totalProcessed;
    }

    private static async Task WaitForCommitAsync(
        ExecutionContext context,
        ISnowflakeApiClient apiClient,
        SnowflakeConnectorConfiguration configuration,
        string channelName,
        string expectedOffsetToken)
    {
        var deadline = DateTimeOffset.UtcNow.Add(CommitTimeout);
        while (true)
        {
            var status = await apiClient.GetChannelStatusAsync(configuration.PipeName, channelName);
            if (status.RowsErrorCount > 0)
            {
                throw new SnowflakeApiException(
                    $"Snowpipe Streaming reported {status.RowsErrorCount} row error(s) on channel '{channelName}': {status.LastErrorMessage}",
                    System.Net.HttpStatusCode.OK,
                    string.Empty);
            }

            if (status.LastCommittedOffsetToken == expectedOffsetToken)
            {
                return;
            }

            if (DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"Snowpipe Streaming channel '{channelName}' on pipe '{configuration.PipeName}' did not commit offset '{expectedOffsetToken}' within {CommitTimeout}. Last committed offset was '{status.LastCommittedOffsetToken}'.");
            }

            context.Log.LogDebug(
                "Waiting for Snowflake to commit offset {ExpectedOffsetToken} on channel {ChannelName} (currently at {LastCommittedOffsetToken}).",
                expectedOffsetToken,
                channelName,
                status.LastCommittedOffsetToken);
            await Task.Delay(CommitPollInterval);
        }
    }

    // Every transient/target column is VARCHAR except PersistVersion (NUMBER, so the
    // MERGE's ROW_NUMBER() ... ORDER BY dedupes correctly) - see
    // SnowflakeSqlBuilder.GetColumnDefinition. Complex values (Codes, edges, etc.) are
    // JSON-serialized to a string, matching the old connector's
    // JsonConvert.SerializeObject(value) approach for its uniformly-VARCHAR columns.
    //
    // Numeric/temporal values are formatted with InvariantCulture (plain ToString() uses
    // the running thread's culture, so e.g. a decimal could come out "3,14" instead of
    // "3.14" depending on server locale) and, for DateTime/DateTimeOffset, the round-trip
    // "O" format (the culture-dependent default ToString() both varies by locale and loses
    // precision/offset information a plain export shouldn't lose).
    private static object ConvertValueForColumn(string fieldName, object value)
    {
        if (value == null)
        {
            return null;
        }

        if (fieldName == StorageConfigurationConstants.PersistVersionKey)
        {
            return value;
        }

        return value switch
        {
            string => value,
            bool or Guid => value.ToString(),
            DateTime dateTime => dateTime.ToString("O", CultureInfo.InvariantCulture),
            DateTimeOffset dateTimeOffset => dateTimeOffset.ToString("O", CultureInfo.InvariantCulture),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => JsonSerializer.Serialize(value),
        };
    }
}
