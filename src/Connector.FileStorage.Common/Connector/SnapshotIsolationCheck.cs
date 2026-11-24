using System;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FileStorage.Common.Connector;

/// <summary>
/// Reports at startup when the stream cache database does not allow snapshot isolation, which the export needs
/// to read the cache table without blocking the writes of entities into it.
/// </summary>
internal static class SnapshotIsolationCheck
{
    public static async Task LogIfNotEnabledAsync(ILogger log, string connectorName, string streamCacheConnectionString)
    {
        try
        {
            await using var connection = new SqlConnection(streamCacheConnectionString);
            await connection.OpenAsync();
            if (!await SnapshotSqlDataReader.IsSnapshotIsolationEnabledAsync(connection))
            {
                log.LogError(
                    "{ConnectorName}: snapshot isolation is not enabled on the stream cache database. " +
                    "An export then reads the cache table without it, which can block the writes of entities until the export ends. " +
                    "Enable it with: ALTER DATABASE [{DatabaseName}] SET ALLOW_SNAPSHOT_ISOLATION ON",
                    connectorName,
                    connection.Database);
            }
        }
        catch (Exception ex)
        {
            // runs in the background at startup, an unreachable database is reported by the code that needs it
            log.LogWarning(ex, "{ConnectorName}: unable to check whether snapshot isolation is enabled on the stream cache database.", connectorName);
        }
    }
}
