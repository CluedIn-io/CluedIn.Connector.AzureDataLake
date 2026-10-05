using System;
using System.Collections.Generic;
using System.Linq;
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

// SHOW <objects> LIKE '<pattern>' is the only filter Snowflake's SHOW command syntax
// supports (there's no WHERE-based exact match) - so a "does this exist" check always goes
// through a wildcard-capable match, not a genuine exact-name lookup. Escaping the pattern's
// '_'/'%'/'\' doesn't fully close that gap either: Snowflake's string-literal parser can
// strip an escaping backslash before LIKE itself ever evaluates the pattern. So any caller
// that needs to know "is the object I asked about actually here" (not "is something
// LIKE-matching it here") must additionally check the returned "name" column for a true
// match, rather than trusting that any returned row is the one it asked about.
internal static class SnowflakeStatementResultExtensions
{
    public static bool HasExactNameMatch(this SnowflakeStatementResult result, string expectedName)
    {
        return result.TryGetExactNameMatch(expectedName, out _);
    }

    public static bool TryGetExactNameMatch(this SnowflakeStatementResult result, string expectedName, out IReadOnlyList<string> row)
    {
        var nameIndex = result.ColumnNames
            .Select((name, index) => (name, index))
            .Where(pair => string.Equals(pair.name, "name", StringComparison.OrdinalIgnoreCase))
            .Select(pair => (int?)pair.index)
            .FirstOrDefault();

        if (nameIndex == null)
        {
            row = null;
            return false;
        }

        // Case-insensitive: Snowflake's default (unquoted) identifier folding is
        // upper-case, but callers here compare against names sourced from user-entered
        // configuration (warehouse/database/schema/role), which isn't guaranteed to already
        // be upper-case even though the stored object name is.
        row = result.Rows.FirstOrDefault(r => string.Equals(r.ElementAtOrDefault(nameIndex.Value), expectedName, StringComparison.OrdinalIgnoreCase));
        return row != null;
    }
}

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
