using System;
using System.Text;

namespace CluedIn.Connector.FileStorage.Common.Tests.Integration;

/// <summary>
/// The SQL Server databases the integration tests of the stream cache run against.
/// Each connection string is in an environment variable, base64 encoded.
/// </summary>
internal static class StreamCacheTestConnection
{
    /// <summary>The database with ALLOW_SNAPSHOT_ISOLATION on, like the one the connector is deployed with.</summary>
    public static string ConnectionString => Get("INTEGRATIONTEST_STREAMCACHE");

    /// <summary>The database with ALLOW_SNAPSHOT_ISOLATION off.</summary>
    public static string ConnectionStringWithoutSnapshotIsolation => Get("INTEGRATIONTEST_STREAMCACHE_NOSNAPSHOT");

    private static string Get(string variableName)
    {
        var encoded = Environment.GetEnvironmentVariable(variableName);
        if (string.IsNullOrWhiteSpace(encoded))
        {
            throw new InvalidOperationException($"{variableName} environment variable is not set.");
        }

        return Encoding.UTF8.GetString(Convert.FromBase64String(encoded));
    }
}
