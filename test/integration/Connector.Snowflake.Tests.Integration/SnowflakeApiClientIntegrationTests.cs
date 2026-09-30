using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.Snowflake.Connector.Snowpipe;

using Xunit;

namespace CluedIn.Connector.Snowflake.Tests.Integration;

/// <summary>
/// Integration tests for SnowflakeApiClient against a real Snowflake account.
/// Requires environment variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY,
/// SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE (all required, no defaults),
/// SNOWFLAKE_ROLE (optional - a blank role uses the user's default role). Tests skip when
/// any required variable is not set, rather than failing - see SnowflakeTestCredentials.
/// </summary>
public class SnowflakeApiClientIntegrationTests : IAsyncLifetime
{
    private SnowflakeApiClient _client;
    private string _transientTableName;

    public ValueTask InitializeAsync()
    {
        if (!SnowflakeTestCredentials.IsAvailable)
        {
            return ValueTask.CompletedTask;
        }

        _client = new SnowflakeApiClient(SnowflakeTestCredentials.Load());
        _transientTableName = $"CLUEDIN_INTEGRATION_TEST_{Guid.NewGuid():N}".ToUpperInvariant();
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_client == null)
        {
            return;
        }

        try
        {
            await _client.ExecuteStatementAsync($"DROP TABLE IF EXISTS \"{_transientTableName}\"");
        }
        catch
        {
            // best-effort cleanup
        }

        _client.Dispose();
    }

    [Fact]
    public async Task ExecuteStatementAsync_SelectOne_ReturnsOneRow()
    {
        if (!SnowflakeTestCredentials.IsAvailable)
        {
            Assert.Skip(SnowflakeTestCredentials.SkipReason);
            return;
        }

        var result = await _client.ExecuteStatementAsync("SELECT 1 AS ONE");

        Assert.True(result.Success);
        var row = Assert.Single(result.Rows);
        Assert.Equal("1", Assert.Single(row));
    }

    [Fact]
    public async Task ExecuteStatementAsync_CreateInsertAndQueryTransientTable_RoundTrips()
    {
        if (!SnowflakeTestCredentials.IsAvailable)
        {
            Assert.Skip(SnowflakeTestCredentials.SkipReason);
            return;
        }

        await _client.ExecuteStatementAsync($"""
            CREATE TRANSIENT TABLE "{_transientTableName}" (ID VARCHAR, DATA VARIANT)
            """);
        var insertSql = $"INSERT INTO \"{_transientTableName}\" (ID, DATA) SELECT 'entity-1', PARSE_JSON('{{\"name\":\"test\"}}')";
        await _client.ExecuteStatementAsync(insertSql);

        var result = await _client.ExecuteStatementAsync($"""
            SELECT ID FROM "{_transientTableName}"
            """);

        var row = Assert.Single(result.Rows);
        Assert.Equal("entity-1", Assert.Single(row));
    }

    [Fact]
    public async Task StreamingChannel_OpenAppendAndClose_LandsRowsInTransientTable()
    {
        if (!SnowflakeTestCredentials.IsAvailable)
        {
            Assert.Skip(SnowflakeTestCredentials.SkipReason);
            return;
        }

        var pipeName = $"{_transientTableName}_PIPE";
        await _client.ExecuteStatementAsync($"""
            CREATE TRANSIENT TABLE "{_transientTableName}" (ENTITY_ID VARCHAR, CHANGE_TYPE VARCHAR, PERSIST_VERSION NUMBER, ROW_DATA VARIANT)
            """);
        await _client.ExecuteStatementAsync($"""
            CREATE PIPE "{pipeName}"
            AS COPY INTO "{_transientTableName}"
            FROM TABLE (DATA_SOURCE(TYPE => 'STREAMING'))
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            """);

        var channelName = $"integration_test_{Guid.NewGuid():N}";
        var channel = await _client.OpenChannelAsync(pipeName, channelName);

        try
        {
            var rows = new List<IReadOnlyDictionary<string, object>>
            {
                new Dictionary<string, object>
                {
                    ["ENTITY_ID"] = "entity-1",
                    ["CHANGE_TYPE"] = "Added",
                    ["PERSIST_VERSION"] = 1,
                    ["ROW_DATA"] = new Dictionary<string, object> { ["name"] = "test" },
                },
            };

            // The offset token is what actually makes Snowflake commit the batch -
            // verified live that an append without one is buffered but never committed
            // (rows never become queryable, even after 60+ seconds).
            await _client.AppendRowsAsync(pipeName, channelName, channel.ContinuationToken, "1", rows);

            // Also verified live: closing the channel immediately after append - the real
            // pattern this test used to follow - does NOT wait for that append to commit
            // either, and the buffered row is simply lost. GetChannelStatusAsync must be
            // polled until the offset is actually committed before it's safe to close.
            await AssertCommitsWithinTimeout();
        }
        finally
        {
            await _client.CloseChannelAsync(pipeName, channelName);
        }

        await AssertRowLandsWithinTimeout();

        await _client.ExecuteStatementAsync($"""DROP PIPE IF EXISTS "{pipeName}" """);

        async Task AssertCommitsWithinTimeout()
        {
            var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
            while (DateTimeOffset.UtcNow < deadline)
            {
                var status = await _client.GetChannelStatusAsync(pipeName, channelName);
                Assert.Equal(0, status.RowsErrorCount);
                if (status.LastCommittedOffsetToken == "1")
                {
                    return;
                }

                await Task.Delay(TimeSpan.FromSeconds(2));
            }

            Assert.Fail($"Channel '{channelName}' did not commit offset '1' within 30 seconds.");
        }

        async Task AssertRowLandsWithinTimeout()
        {
            var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
            while (DateTimeOffset.UtcNow < deadline)
            {
                var result = await _client.ExecuteStatementAsync($"""SELECT ENTITY_ID FROM "{_transientTableName}" """);
                if (result.Rows.Count > 0)
                {
                    Assert.Equal("entity-1", Assert.Single(result.Rows[0]));
                    return;
                }

                await Task.Delay(TimeSpan.FromSeconds(2));
            }

            Assert.Fail($"Streamed row did not land in \"{_transientTableName}\" within 10 seconds of its offset committing.");
        }
    }
}
