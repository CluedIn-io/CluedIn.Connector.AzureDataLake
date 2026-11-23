using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CluedIn.Connector.Snowflake.Cleanup;
using CluedIn.Connector.Snowflake.Connector.Snowpipe;

using Xunit;

namespace CluedIn.Connector.Snowflake.Tests.Unit;

public class SnowflakeTestObjectsCleanupTests
{
    [Fact]
    public void GetTestObjectNames_ReturnsOnlyTheNamesThatStartWithTheTestPrefix()
    {
        var result = CreateShowResult("XUNIT_1", "XUNIT_PATTERN_B_2", "XUNITX", "XUNIT", "xunit_lower", "OTHER_XUNIT_3", "PRODUCTION_TABLE");

        var names = SnowflakeTestObjectsCleanup.GetTestObjectNames(result);

        Assert.Equal(new[] { "XUNIT_1", "XUNIT_PATTERN_B_2" }, names);
    }

    [Fact]
    public void GetTestObjectNames_UsesTheNameColumn()
    {
        var result = new SnowflakeStatementResult(
            true,
            ["created_on", "name", "comment"],
            [["XUNIT_NOT_THE_NAME", "PRODUCTION_TABLE", "XUNIT_NOT_THE_NAME"], ["x", "XUNIT_REAL", "y"]]);

        Assert.Equal(new[] { "XUNIT_REAL" }, SnowflakeTestObjectsCleanup.GetTestObjectNames(result));
    }

    [Fact]
    public void GetTestObjectNames_WithoutANameColumn_Throws()
    {
        var result = new SnowflakeStatementResult(true, ["created_on"], [["XUNIT_1"]]);

        Assert.Throws<InvalidOperationException>(() => SnowflakeTestObjectsCleanup.GetTestObjectNames(result));
    }

    [Fact]
    public async Task RunAsync_DropsThePipesBeforeTheTablesAndOnlyTheTestOnes()
    {
        var client = new FakeClient();
        client.Pipes.AddRange(["XUNIT_1_PIPE", "KEEP_PIPE"]);
        client.Tables.AddRange(["XUNIT_1", "XUNIT_1_TRANSIENT", "KEEP_TABLE"]);

        var failures = await new SnowflakeTestObjectsCleanup(client, new StringWriter()).RunAsync("DB", "SCH", dryRun: false);

        Assert.Equal(0, failures);
        Assert.Equal(
            new[]
            {
                "DROP PIPE IF EXISTS \"DB\".\"SCH\".\"XUNIT_1_PIPE\"",
                "DROP TABLE IF EXISTS \"DB\".\"SCH\".\"XUNIT_1\"",
                "DROP TABLE IF EXISTS \"DB\".\"SCH\".\"XUNIT_1_TRANSIENT\"",
            },
            client.Drops);
        Assert.Equal(new[] { "KEEP_PIPE" }, client.Pipes);
        Assert.Equal(new[] { "KEEP_TABLE" }, client.Tables);
    }

    [Fact]
    public async Task RunAsync_ListsThePrefixInTheSchemaWithoutASessionContext()
    {
        var client = new FakeClient();

        await new SnowflakeTestObjectsCleanup(client, new StringWriter()).RunAsync("DB", "SCH", dryRun: false);

        Assert.Contains("SHOW pipes LIKE 'XUNIT%' IN SCHEMA \"DB\".\"SCH\"", client.Statements);
        Assert.Contains("SHOW tables LIKE 'XUNIT%' IN SCHEMA \"DB\".\"SCH\"", client.Statements);
        Assert.All(client.Scopes, scope => Assert.Equal(SnowflakeStatementScope.None, scope));
    }

    [Fact]
    public async Task RunAsync_WithADryRun_DropsNothing()
    {
        var client = new FakeClient();
        client.Pipes.Add("XUNIT_1_PIPE");
        client.Tables.Add("XUNIT_1");
        var output = new StringWriter();

        var failures = await new SnowflakeTestObjectsCleanup(client, output).RunAsync("DB", "SCH", dryRun: true);

        Assert.Equal(0, failures);
        Assert.Empty(client.Drops);
        Assert.Contains("would drop table XUNIT_1", output.ToString());
        Assert.Contains("would drop pipe XUNIT_1_PIPE", output.ToString());
    }

    [Fact]
    public async Task RunAsync_KeepsListingUntilNoneAreLeft()
    {
        // The fake lists 2 at a time, like a listing that is limited
        var client = new FakeClient { PageSize = 2 };
        client.Tables.AddRange(["XUNIT_1", "XUNIT_2", "XUNIT_3", "XUNIT_4", "XUNIT_5"]);

        var failures = await new SnowflakeTestObjectsCleanup(client, new StringWriter()).RunAsync("DB", "SCH", dryRun: false);

        Assert.Equal(0, failures);
        Assert.Empty(client.Tables);
        Assert.Equal(5, client.Drops.Count);
    }

    [Fact]
    public async Task RunAsync_WhenADropFails_DropsTheOthersReportsTheFailureAndDoesNotTryItAgain()
    {
        var client = new FakeClient();
        client.Tables.AddRange(["XUNIT_1", "XUNIT_LOCKED", "XUNIT_3"]);
        client.FailOn = "XUNIT_LOCKED";

        var failures = await new SnowflakeTestObjectsCleanup(client, new StringWriter()).RunAsync("DB", "SCH", dryRun: false);

        Assert.Equal(1, failures);
        Assert.Equal(new[] { "XUNIT_LOCKED" }, client.Tables);
        Assert.Equal(1, client.Drops.Count(drop => drop.Contains("XUNIT_LOCKED")));
    }

    private static SnowflakeStatementResult CreateShowResult(params string[] names)
    {
        return new SnowflakeStatementResult(true, ["created_on", "name"], names.Select(name => (IReadOnlyList<string>)["2026-11-23", name]).ToList());
    }

    private sealed class FakeClient : ISnowflakeApiClient
    {
        public List<string> Pipes { get; } = new();

        public List<string> Tables { get; } = new();

        public List<string> Drops { get; } = new();

        public List<string> Statements { get; } = new();

        public List<SnowflakeStatementScope> Scopes { get; } = new();

        public int PageSize { get; set; } = int.MaxValue;

        public string FailOn { get; set; }

        public Task<SnowflakeStatementResult> ExecuteStatementAsync(
            string sql,
            SnowflakeStatementScope scope = SnowflakeStatementScope.All,
            int timeoutSeconds = 60,
            bool async = false,
            CancellationToken cancellationToken = default)
        {
            Statements.Add(sql);
            Scopes.Add(scope);

            if (sql.StartsWith("SHOW pipes", StringComparison.Ordinal))
            {
                return Task.FromResult(CreateShowResult(Pipes.Take(PageSize).ToArray()));
            }

            if (sql.StartsWith("SHOW tables", StringComparison.Ordinal))
            {
                return Task.FromResult(CreateShowResult(Tables.Take(PageSize).ToArray()));
            }

            Drops.Add(sql);
            var name = sql[(sql.LastIndexOf('.') + 2)..^1];
            if (name == FailOn)
            {
                throw new InvalidOperationException("object is locked");
            }

            (sql.StartsWith("DROP PIPE", StringComparison.Ordinal) ? Pipes : Tables).Remove(name);
            return Task.FromResult(new SnowflakeStatementResult(true, [], []));
        }

        public Task<SnowflakeChannelHandle> OpenChannelAsync(string pipeName, string channelName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<SnowflakeChannelHandle> AppendRowsAsync(string pipeName, string channelName, string continuationToken, string offsetToken, IReadOnlyList<IReadOnlyDictionary<string, object>> rows, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task CloseChannelAsync(string pipeName, string channelName, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<SnowflakeChannelStatus> GetChannelStatusAsync(string pipeName, string channelName, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
