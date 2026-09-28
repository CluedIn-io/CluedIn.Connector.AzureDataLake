using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace CluedIn.Connector.Snowflake.Connector.Snowpipe;

internal interface ISnowflakeApiClient
{
    Task<SnowflakeStatementResult> ExecuteStatementAsync(string sql, CancellationToken cancellationToken = default);

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
