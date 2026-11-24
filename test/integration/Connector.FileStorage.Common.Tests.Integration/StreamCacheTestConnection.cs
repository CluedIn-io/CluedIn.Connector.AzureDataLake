using System;
using System.Text;

namespace CluedIn.Connector.FileStorage.Common.Tests.Integration;

/// <summary>
/// The SQL Server database the integration tests of the stream cache run against,
/// INTEGRATIONTEST_STREAMCACHE is its connection string, base64 encoded.
/// </summary>
internal static class StreamCacheTestConnection
{
    public static string ConnectionString
    {
        get
        {
            var encoded = Environment.GetEnvironmentVariable("INTEGRATIONTEST_STREAMCACHE");
            if (string.IsNullOrWhiteSpace(encoded))
            {
                throw new InvalidOperationException("INTEGRATIONTEST_STREAMCACHE environment variable is not set.");
            }

            return Encoding.UTF8.GetString(Convert.FromBase64String(encoded));
        }
    }
}
