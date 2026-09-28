using System;
using System.Text;

using CluedIn.Connector.Snowflake.Connector.Snowpipe;

namespace CluedIn.Connector.Snowflake.Tests.Integration;

// Reads live Snowflake credentials from environment variables, following the same pattern
// Connector.AmazonS3.Tests.Integration uses for its S3 credentials (see
// AmazonS3ConnectorTests's class doc comment) - nothing is ever hardcoded here.
//
// Known values for the test account referenced in docs/snowflake-connector-plan.md:
//   SNOWFLAKE_ACCOUNT=qs30799.ap-southeast-1 (the bare "qs30799" locator 404s - this
//   account's deployment needs the region suffix in the host, confirmed against the live
//   account), SNOWFLAKE_DATABASE=SNOWFLAKE_LEARNING_DB, SNOWFLAKE_SCHEMA=TESTSCHEMA,
//   SNOWFLAKE_TABLE=MYTESTTABLE, SNOWFLAKE_WAREHOUSE=COMPUTE_WH, SNOWFLAKE_ROLE=ACCOUNTADMIN
// SNOWFLAKE_USER and SNOWFLAKE_PRIVATE_KEY (PEM, key-pair auth) are account-specific
// secrets and must be supplied separately - tests skip when they are absent.
internal static class SnowflakeTestCredentials
{
    public static bool IsAvailable =>
        !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("SNOWFLAKE_USER")) &&
        !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("SNOWFLAKE_PRIVATE_KEY"));

    public static string SkipReason =>
        "SNOWFLAKE_USER/SNOWFLAKE_PRIVATE_KEY environment variables are not set - see docs/snowflake-connector-plan.md.";

    public static SnowflakeConnectionSettings Load()
    {
        return new SnowflakeConnectionSettings(
            GetRequired("SNOWFLAKE_ACCOUNT", "qs30799.ap-southeast-1"),
            GetRequired("SNOWFLAKE_USER"),
            GetPrivateKeyPem(),
            Environment.GetEnvironmentVariable("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
            GetRequired("SNOWFLAKE_DATABASE", "SNOWFLAKE_LEARNING_DB"),
            GetRequired("SNOWFLAKE_SCHEMA", "TESTSCHEMA"),
            GetRequired("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            Environment.GetEnvironmentVariable("SNOWFLAKE_ROLE") ?? "ACCOUNTADMIN");
    }

    public static string TargetTable => GetRequired("SNOWFLAKE_TABLE", "MYTESTTABLE");

    private static string GetRequired(string environmentVariableName, string defaultValue = null)
    {
        var value = Environment.GetEnvironmentVariable(environmentVariableName);
        if (!string.IsNullOrWhiteSpace(value))
        {
            return value;
        }

        if (defaultValue != null)
        {
            return defaultValue;
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
