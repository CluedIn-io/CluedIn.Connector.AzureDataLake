using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Connector.Snowflake.Connector;
using CluedIn.Connector.Snowflake.Connector.Snowpipe;

namespace CluedIn.Connector.Snowflake.Cleanup;

/// <summary>
/// Deletes the pipes and tables that the Snowflake integration tests leave in their schema. The tests name everything they create
/// XUNIT_ (see SnowflakeTestCredentials.TargetTable and the tables and pipes derived from it), and only that is deleted.
/// </summary>
internal sealed class SnowflakeTestObjectsCleanup
{
    internal const string TestObjectPrefix = "XUNIT_";

    // A listing returns up to 10,000 objects, so list again until none are left
    private const int MaximumPasses = 20;

    private readonly ISnowflakeApiClient _client;
    private readonly TextWriter _output;

    public SnowflakeTestObjectsCleanup(ISnowflakeApiClient client, TextWriter output)
    {
        _client = client;
        _output = output;
    }

    /// <returns>How many objects could not be deleted.</returns>
    public async Task<int> RunAsync(string database, string schema, bool dryRun)
    {
        var failures = 0;

        // The pipes read from the tables, so they go first
        failures += await DeleteAsync("pipe", database, schema, dryRun, SnowflakeSqlBuilder.DropPipeIfExists);
        failures += await DeleteAsync("table", database, schema, dryRun, SnowflakeSqlBuilder.DropTableIfExists);

        return failures;
    }

    /// <summary>
    /// The names of the objects of a SHOW result that the tests created. The LIKE of a SHOW is case insensitive and has wildcards, so
    /// the names are checked again here.
    /// </summary>
    internal static IReadOnlyList<string> GetTestObjectNames(SnowflakeStatementResult result)
    {
        var nameIndex = result.ColumnNames
            .Select((name, index) => (name, index))
            .Where(pair => string.Equals(pair.name, "name", StringComparison.OrdinalIgnoreCase))
            .Select(pair => (int?)pair.index)
            .FirstOrDefault();
        if (nameIndex == null)
        {
            throw new InvalidOperationException("The result of the SHOW statement has no 'name' column.");
        }

        return result.Rows
            .Select(row => row.ElementAtOrDefault(nameIndex.Value))
            .Where(name => name != null && name.StartsWith(TestObjectPrefix, StringComparison.Ordinal))
            .ToList();
    }

    private async Task<int> DeleteAsync(
        string objectType,
        string database,
        string schema,
        bool dryRun,
        Func<string, string, string, string> getDropStatement)
    {
        var failures = 0;
        var deleted = 0;
        var attempted = new HashSet<string>(StringComparer.Ordinal);

        for (var pass = 1; pass <= MaximumPasses; pass++)
        {
            var show = $"SHOW {objectType}s LIKE '{TestObjectPrefix.TrimEnd('_')}%' IN SCHEMA \"{SnowflakeSqlBuilder.EscapeIdentifier(database)}\".\"{SnowflakeSqlBuilder.EscapeIdentifier(schema)}\"";
            var names = GetTestObjectNames(await _client.ExecuteStatementAsync(show, SnowflakeStatementScope.None));

            // One that could not be deleted is not tried again, or this would not end
            var pending = names.Where(name => !attempted.Contains(name)).ToList();
            _output.WriteLine($"Pass {pass}: {pending.Count} {objectType}(s) start with '{TestObjectPrefix}' in {database}.{schema}.");
            if (pending.Count == 0)
            {
                break;
            }

            if (dryRun)
            {
                foreach (var name in pending.Take(20))
                {
                    _output.WriteLine($"  would drop {objectType} {name}");
                }

                break;
            }

            foreach (var name in pending)
            {
                attempted.Add(name);
                try
                {
                    await _client.ExecuteStatementAsync(getDropStatement(database, schema, name), SnowflakeStatementScope.None);
                    deleted++;
                }
                catch (Exception ex)
                {
                    failures++;
                    _output.WriteLine($"  unable to drop {objectType} {name}: {ex.Message}");
                }
            }
        }

        _output.WriteLine($"Dropped {deleted} {objectType}(s).");
        return failures;
    }
}
