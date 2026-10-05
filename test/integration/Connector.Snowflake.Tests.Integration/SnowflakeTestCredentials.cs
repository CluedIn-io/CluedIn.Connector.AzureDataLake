using System;
using System.Text;

using CluedIn.Connector.Snowflake.Connector.Snowpipe;

namespace CluedIn.Connector.Snowflake.Tests.Integration;

// Reads live Snowflake credentials from environment variables, following the same pattern
// Connector.AmazonS3.Tests.Integration uses for its S3 credentials (see
// AmazonS3ConnectorTests's class doc comment) - nothing is ever hardcoded here, and there
// are no defaults: SNOWFLAKE_ACCOUNT/DATABASE/SCHEMA/WAREHOUSE, SNOWFLAKE_USER, and
// SNOWFLAKE_PRIVATE_KEY (PEM, key-pair auth) must all be supplied via environment
// variables. SNOWFLAKE_ROLE and SNOWFLAKE_PRIVATE_KEY_PASSPHRASE are optional (a blank
// role uses the user's default role - see SnowflakeConfigurationConstants). Tests skip
// when any required variable is absent, rather than failing.
internal static class SnowflakeTestCredentials
{
    // Generated once per test run rather than read from an env var - matching how
    // AzureDataLakeConnectorTests/AzureDataLakeStorageClientTests name their scratch
    // file systems/directories ($"xunit-{DateTime.Now.Ticks}") and
    // OneLakeConnectorTests names its scratch table (Guid.NewGuid().ToString("N")).
    public static string TargetTable { get; } = $"XUNIT_{DateTime.Now.Ticks}_{Guid.NewGuid():N}".ToUpperInvariant();

    public static bool IsAvailable =>
        !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("SNOWFLAKE_ACCOUNT")) &&
        !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("SNOWFLAKE_USER")) &&
        !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("SNOWFLAKE_PRIVATE_KEY")) &&
        !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("SNOWFLAKE_DATABASE")) &&
        !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("SNOWFLAKE_SCHEMA")) &&
        !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("SNOWFLAKE_WAREHOUSE"));

    public static string SkipReason =>
        "SNOWFLAKE_ACCOUNT/USER/PRIVATE_KEY/DATABASE/SCHEMA/WAREHOUSE environment variables are not all set - see docs/snowflake-connector-plan.md.";

    public static SnowflakeConnectionSettings Load()
    {
        return new SnowflakeConnectionSettings(
            GetRequired("SNOWFLAKE_ACCOUNT"),
            GetRequired("SNOWFLAKE_USER"),
            GetPrivateKeyPem(),
            Environment.GetEnvironmentVariable("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
            GetRequired("SNOWFLAKE_DATABASE"),
            GetRequired("SNOWFLAKE_SCHEMA"),
            GetRequired("SNOWFLAKE_WAREHOUSE"),
            Environment.GetEnvironmentVariable("SNOWFLAKE_ROLE"));
    }

    private static string GetRequired(string environmentVariableName)
    {
        var value = Environment.GetEnvironmentVariable(environmentVariableName);
        if (!string.IsNullOrWhiteSpace(value))
        {
            return value;
        }

        throw new InvalidOperationException($"Environment variable '{environmentVariableName}' is required for Snowflake integration tests.");
    }

    // Accepts either a raw PEM (with real newlines), a PEM with literal "\n" escapes (a
    // common way to pass multi-line secrets through single-line env vars/CI variable
    // groups), or a base64-encoded PEM.
    private static string GetPrivateKeyPem()
    {
        var raw = GetRequired("SNOWFLAKE_PRIVATE_KEY");
        if (raw.Contains("BEGIN", StringComparison.OrdinalIgnoreCase))
        {
            return raw.Replace("\\n", "\n");
        }

        return Encoding.UTF8.GetString(Convert.FromBase64String(raw));
    }
}
