using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using CluedIn.Connector.Snowflake.Connector.Snowpipe;

namespace CluedIn.Connector.Snowflake.Cleanup;

/// <summary>
/// Deletes the pipes and tables that the Snowflake integration tests leave in their schema (everything named XUNIT_...).
/// Connects with the variables the integration tests use: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY (and the optional
/// SNOWFLAKE_PRIVATE_KEY_PASSPHRASE), SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE and the optional SNOWFLAKE_ROLE.
/// --dry-run only lists what would be dropped.
/// </summary>
internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var dryRun = args.Contains("--dry-run", StringComparer.OrdinalIgnoreCase);

        SnowflakeConnectionSettings settings;
        try
        {
            settings = LoadSettings();
        }
        catch (InvalidOperationException ex)
        {
            Console.Error.WriteLine(ex.Message);
            return 1;
        }

        using var client = new SnowflakeApiClient(settings);
        var cleanup = new SnowflakeTestObjectsCleanup(client, Console.Out);

        var failures = await cleanup.RunAsync(settings.Database, settings.Schema, dryRun);
        if (failures > 0)
        {
            Console.Error.WriteLine($"{failures} object(s) could not be dropped.");
            return 1;
        }

        return 0;
    }

    private static SnowflakeConnectionSettings LoadSettings()
    {
        return new SnowflakeConnectionSettings(
            GetRequired("SNOWFLAKE_ACCOUNT"),
            GetRequired("SNOWFLAKE_USER"),
            GetPrivateKeyPem(),
            GetOptional("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
            GetRequired("SNOWFLAKE_DATABASE"),
            GetRequired("SNOWFLAKE_SCHEMA"),
            GetRequired("SNOWFLAKE_WAREHOUSE"),
            GetOptional("SNOWFLAKE_ROLE"));
    }

    private static string GetRequired(string name)
    {
        return GetOptional(name) ?? throw new InvalidOperationException($"{name} is required.");
    }

    // A variable that is not defined in the pipeline is left as it is written in the step ($(NAME)), which is not a value
    private static string GetOptional(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(value) || (value.StartsWith("$(", StringComparison.Ordinal) && value.EndsWith(')')))
        {
            return null;
        }

        return value;
    }

    // Accepts a PEM, a PEM with literal "\n" escapes, or a base64 encoded PEM, like the integration tests do
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
