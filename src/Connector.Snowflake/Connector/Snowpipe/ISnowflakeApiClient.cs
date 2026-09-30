using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace CluedIn.Connector.Snowflake.Connector.Snowpipe;

internal interface ISnowflakeApiClient
{
    // timeoutSeconds: max seconds Snowflake lets the statement run before cancelling it
    // (not just how long this call waits for a response) - the 60s default matches
    // SnowflakeApiClient.DefaultStatementTimeoutSeconds and suits DDL/metadata calls; a
    // statement that can legitimately run longer (e.g. a large MERGE) needs a bigger value.
    // async: true returns the statement handle immediately (?async=true) instead of
    // blocking synchronously for up to 45s trying to complete inline first.
    Task<SnowflakeStatementResult> ExecuteStatementAsync(
        string sql,
        SnowflakeStatementScope scope = SnowflakeStatementScope.All,
        int timeoutSeconds = 60,
        bool async = false,
        CancellationToken cancellationToken = default);

    Task<SnowflakeChannelHandle> OpenChannelAsync(string pipeName, string channelName, CancellationToken cancellationToken = default);

    Task<SnowflakeChannelHandle> AppendRowsAsync(
        string pipeName,
        string channelName,
        string continuationToken,
        string offsetToken,
        IReadOnlyList<IReadOnlyDictionary<string, object>> rows,
        CancellationToken cancellationToken = default);

    Task CloseChannelAsync(string pipeName, string channelName, CancellationToken cancellationToken = default);

    // Appending rows only buffers them - verified live that closing a channel right after
    // an append does not wait for (or force) that data to actually commit, and the buffered
    // rows are simply lost. Callers must poll this until LastCommittedOffsetToken reaches
    // the offset token of their last append before closing the channel.
    Task<SnowflakeChannelStatus> GetChannelStatusAsync(string pipeName, string channelName, CancellationToken cancellationToken = default);
}

internal record SnowflakeStatementResult(bool Success, IReadOnlyList<string> ColumnNames, IReadOnlyList<IReadOnlyList<string>> Rows);

internal record SnowflakeChannelHandle(string ChannelName, string ContinuationToken);

internal record SnowflakeChannelStatus(string LastCommittedOffsetToken, long RowsInserted, long RowsErrorCount, string LastErrorMessage);

// Controls which of the connection's Database/Schema/Warehouse are sent as session context
// on a statement request. Used to isolate permission checks to a single object at a time
// (see SnowflakeConnector.VerifyDataLakeConnection) - Snowflake fails to establish session
// context for an object the role lacks access to, before the statement itself even runs,
// so a check that only supplies one object at a time gets an error that names that object
// specifically rather than one that could be any of the three.
internal enum SnowflakeStatementScope
{
    All,
    None,
    WarehouseOnly,
    DatabaseOnly,
    DatabaseAndSchema,
}
