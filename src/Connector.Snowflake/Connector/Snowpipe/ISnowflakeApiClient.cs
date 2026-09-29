using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace CluedIn.Connector.Snowflake.Connector.Snowpipe;

internal interface ISnowflakeApiClient
{
    Task<SnowflakeStatementResult> ExecuteStatementAsync(string sql, SnowflakeStatementScope scope = SnowflakeStatementScope.All, CancellationToken cancellationToken = default);

    Task<SnowflakeChannelHandle> OpenChannelAsync(string pipeName, string channelName, CancellationToken cancellationToken = default);

    Task<SnowflakeChannelHandle> AppendRowsAsync(
        string pipeName,
        string channelName,
        string continuationToken,
        IReadOnlyList<IReadOnlyDictionary<string, object>> rows,
        CancellationToken cancellationToken = default);

    Task CloseChannelAsync(string pipeName, string channelName, CancellationToken cancellationToken = default);
}

internal record SnowflakeStatementResult(bool Success, IReadOnlyList<string> ColumnNames, IReadOnlyList<IReadOnlyList<string>> Rows);

internal record SnowflakeChannelHandle(string ChannelName, string ContinuationToken);

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
