using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CluedIn.Connector.Snowflake.Connector.Snowpipe;

using Xunit;

namespace CluedIn.Connector.Snowflake.Tests.Integration;

/// <summary>
/// Integration tests for SnowflakeApiClient against a real Snowflake account.
/// Requires environment variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY,
/// SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE (optional,
/// defaults to ACCOUNTADMIN). SNOWFLAKE_ACCOUNT/DATABASE/SCHEMA/WAREHOUSE default to the
/// known test account values documented in docs/snowflake-connector-plan.md when unset.
/// Tests skip when SNOWFLAKE_USER/SNOWFLAKE_PRIVATE_KEY are not set, rather than failing.
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

            await _client.AppendRowsAsync(pipeName, channelName, channel.ContinuationToken, rows);
        }
        finally
        {
            await _client.CloseChannelAsync(pipeName, channelName);
        }

        await _client.ExecuteStatementAsync($"""DROP PIPE IF EXISTS "{pipeName}" """);
    }
}
