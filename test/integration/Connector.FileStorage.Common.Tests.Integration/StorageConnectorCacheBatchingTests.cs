using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Processing;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Integration;

/// <summary>
/// Stores entities through a real connector into a real stream cache table, with and without batching,
/// and checks that batching does not change what ends up in the table.
/// Needs INTEGRATIONTEST_STREAMCACHE (a base64 encoded SQL Server connection string).
/// </summary>
public class StorageConnectorCacheBatchingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StoreData_WithBatching_ProducesTheSameTableAsWithoutBatching(bool softDelete)
    {
        await using var unbatched = CacheWriteHarness.Create(batching: false, softDelete);
        await using var batched = CacheWriteHarness.Create(batching: true, softDelete);

        var ids = Enumerable.Range(0, 60).Select(_ => Guid.NewGuid()).ToList();
        foreach (var harness in new[] { unbatched, batched })
        {
            await RunScenarioAsync(harness, ids);
        }

        var expected = await unbatched.ReadRowsAsync();
        var actual = await batched.ReadRowsAsync();
        AssertSameRows(expected, actual);
        Assert.NotEmpty(actual);
    }

    [Fact]
    public async Task StoreData_WithBatching_WritesConcurrentEntitiesWithSetBasedStatements()
    {
        // waits for the entities to come together, as how many of them are there to write together otherwise depends on timing
        await using var harness = CacheWriteHarness.Create(batching: true, softDelete: true, batchTimeoutMilliseconds: 100);

        // the first wave creates the table; the second one is the one that can be written set based
        await StoreAllAsync(harness, Enumerable.Range(0, 5).Select(_ => CacheWriteHarness.CreateEntity(Guid.NewGuid())));
        await StoreAllAsync(harness, Enumerable.Range(0, 100).Select(_ => CacheWriteHarness.CreateEntity(Guid.NewGuid())));

        Assert.Equal(105, await harness.CountRowsAsync());
        Assert.True(await CountStatementExecutionsAsync(harness, "INNER LOOP JOIN") > 0, "Expected the set based statements to have run.");
    }

    [Fact]
    public async Task StoreData_WithBatching_WhenABatchNeedsMoreThanOneCommand_WritesEveryEntityOnce()
    {
        // waits for the entities to come together, so that they are one batch that does not fit in one command
        await using var harness = CacheWriteHarness.Create(batching: true, softDelete: true, batchSize: 1000, batchTimeoutMilliseconds: 100);
        await StoreAllAsync(harness, Enumerable.Range(0, 3).Select(_ => CacheWriteHarness.CreateEntity(Guid.NewGuid())));

        var ids = Enumerable.Range(0, 700).Select(_ => Guid.NewGuid()).ToList();
        var results = await StoreAllAsync(harness, ids.Select((id, index) => CacheWriteHarness.CreateEntity(id, name: $"name{index}")));

        Assert.All(results, result => Assert.Equal(SaveResultState.Success, result.State));
        var rows = await harness.ReadRowsAsync();
        Assert.Equal(703, rows.Count);
        for (var index = 0; index < ids.Count; index++)
        {
            Assert.Equal($"name{index}", (string)rows.Single(row => (Guid)row["Id"] == ids[index])["Name"]);
        }
    }

    [Fact]
    public async Task StoreData_WithBatching_WhenTableDoesNotExist_CreatesItAndWritesEveryEntity()
    {
        await using var harness = CacheWriteHarness.Create(batching: true, softDelete: true);

        var results = await StoreAllAsync(harness, Enumerable.Range(0, 30).Select(_ => CacheWriteHarness.CreateEntity(Guid.NewGuid())));

        Assert.All(results, result => Assert.Equal(SaveResultState.Success, result.State));
        Assert.Equal(30, await harness.CountRowsAsync());
    }

    [Fact]
    public async Task StoreData_WithBatching_DoesNotOverwriteWithAnOlderPersistVersion()
    {
        await using var harness = CacheWriteHarness.Create(batching: true, softDelete: true);
        var ids = Enumerable.Range(0, 20).Select(_ => Guid.NewGuid()).ToList();
        await StoreAllAsync(harness, ids.Select(id => CacheWriteHarness.CreateEntity(id, VersionChangeType.Added, persistVersion: 5, name: "v5")));

        // half of them is newer, half is older than what is stored
        var results = await StoreAllAsync(
            harness,
            ids.Select((id, index) => CacheWriteHarness.CreateEntity(id, VersionChangeType.Changed, persistVersion: index % 2 == 0 ? 7 : 3, name: index % 2 == 0 ? "v7" : "v3")));

        Assert.All(results, result => Assert.Equal(SaveResultState.Success, result.State));
        var rows = await harness.ReadRowsAsync();
        Assert.Equal(10, rows.Count(row => (string)row["Name"] == "v7"));
        Assert.Equal(10, rows.Count(row => (string)row["Name"] == "v5"));
        Assert.DoesNotContain(rows, row => (string)row["Name"] == "v3");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StoreData_WithBatching_WhenAnEntityOccursTwiceInABatch_TheLaterWriteWins(bool softDelete)
    {
        await using var harness = CacheWriteHarness.Create(batching: true, softDelete);
        var repeated = Guid.NewGuid();
        await StoreAllAsync(harness, Enumerable.Range(0, 5).Select(_ => CacheWriteHarness.CreateEntity(Guid.NewGuid())));

        await StoreAllAsync(
            harness,
            Enumerable.Range(0, 20).Select(_ => CacheWriteHarness.CreateEntity(Guid.NewGuid()))
                .Prepend(CacheWriteHarness.CreateEntity(repeated, VersionChangeType.Added, persistVersion: 1, name: "first"))
                .Append(CacheWriteHarness.CreateEntity(repeated, VersionChangeType.Changed, persistVersion: 2, name: "second")));

        var rows = await harness.ReadRowsAsync();
        Assert.Equal(26, rows.Count);
        Assert.Equal("second", (string)rows.Single(row => (Guid)row["Id"] == repeated)["Name"]);
    }

    [Fact]
    public async Task StoreData_WithBatching_WithoutSoftDelete_DoesNotBatch()
    {
        await using var harness = CacheWriteHarness.Create(batching: true, softDelete: false);

        await StoreAllAsync(harness, Enumerable.Range(0, 5).Select(_ => CacheWriteHarness.CreateEntity(Guid.NewGuid())));
        await StoreAllAsync(harness, Enumerable.Range(0, 100).Select(_ => CacheWriteHarness.CreateEntity(Guid.NewGuid())));

        Assert.Equal(105, await harness.CountRowsAsync());
        Assert.Equal(0, await CountStatementExecutionsAsync(harness, "INNER LOOP JOIN"));
    }

    [Fact]
    public async Task StoreData_WithBatching_WhenOneEntityCannotBeWritten_OnlyThatEntityIsRequeued()
    {
        await using var harness = CacheWriteHarness.Create(batching: true, softDelete: false);
        var existing = Enumerable.Range(0, 10).Select(_ => Guid.NewGuid()).ToList();
        await StoreAllAsync(harness, existing.Select(id => CacheWriteHarness.CreateEntity(id)));

        // a hard delete of something that is not in the table fails for that entity only
        var missing = Guid.NewGuid();
        var results = await StoreAllAsync(
            harness,
            existing.Append(missing).Select(id => CacheWriteHarness.CreateEntity(id, VersionChangeType.Removed, persistVersion: 2)));

        Assert.Equal(10, results.Count(result => result.State == SaveResultState.Success));
        Assert.Equal(SaveResultState.ReQueue, results.Last().State);
        Assert.Equal(0, await harness.CountRowsAsync());
    }

    [Fact]
    public async Task StoreData_WithBatching_WhenSomeEntitiesFail_TheOthersAreStillWritten()
    {
        await using var harness = CacheWriteHarness.Create(batching: true, softDelete: true);
        await StoreAllAsync(harness, Enumerable.Range(0, 5).Select(_ => CacheWriteHarness.CreateEntity(Guid.NewGuid())));

        // two entities have a column the table does not have, which fails them, and the set based write of the batch they are in (when they are in the same one),
        // without failing the others
        var entities = Enumerable.Range(0, 20).Select(_ => CacheWriteHarness.CreateEntity(Guid.NewGuid())).ToList();
        entities.Insert(5, CacheWriteHarness.CreateEntity(Guid.NewGuid(), extraColumn: "extra.column"));
        entities.Insert(15, CacheWriteHarness.CreateEntity(Guid.NewGuid(), extraColumn: "extra.column"));
        var results = await StoreAllAsync(harness, entities);

        Assert.Equal(20, results.Count(result => result.State == SaveResultState.Success));
        Assert.Equal(SaveResultState.ReQueue, results[5].State);
        Assert.Equal(SaveResultState.ReQueue, results[15].State);
        Assert.Equal(25, await harness.CountRowsAsync());
    }

    private static async Task RunScenarioAsync(CacheWriteHarness harness, List<Guid> ids)
    {
        // adds (the first wave creates the table)
        await StoreAllAsync(harness, ids.Take(5).Select(id => Entity(id, VersionChangeType.Added, 1)));
        await StoreAllAsync(harness, ids.Skip(5).Select(id => Entity(id, VersionChangeType.Added, 1)));

        // changes: newer versions, and older ones that must not win
        await StoreAllAsync(harness, ids.Take(40).Select((id, index) => Entity(id, VersionChangeType.Changed, index % 4 == 0 ? 0 : 2)));

        // removals of some, and a change of one of those afterwards
        await StoreAllAsync(harness, ids.Skip(10).Take(15).Select(id => Entity(id, VersionChangeType.Removed, 3)));
        await StoreAllAsync(harness, ids.Skip(10).Take(5).Select(id => Entity(id, VersionChangeType.Changed, 4)));

        static ConnectorEntityData Entity(Guid id, VersionChangeType changeType, int persistVersion)
        {
            // values that a type conversion could change: sub millisecond ticks, a fraction a DECIMAL(18,0) rounds
            var index = id.GetHashCode() & 0xFF;
            return CacheWriteHarness.CreateEntity(
                id,
                changeType,
                persistVersion,
                name: $"name {persistVersion} {changeType}",
                age: persistVersion * 10,
                dateTime: new DateTime(2021, 3, 4, 5, 6, 7, DateTimeKind.Utc).AddTicks(1234567 + index),
                number: persistVersion + 0.5m);
        }
    }

    private static async Task<List<SaveResult>> StoreAllAsync(CacheWriteHarness harness, IEnumerable<ConnectorEntityData> entities)
    {
        // started one after the other, without waiting, so that they are stored concurrently
        var tasks = entities.Select(entity => harness.StoreAsync(entity)).ToList();
        return (await Task.WhenAll(tasks)).ToList();
    }

    private static void AssertSameRows(List<Dictionary<string, object>> expected, List<Dictionary<string, object>> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Keys.OrderBy(key => key), actual[i].Keys.OrderBy(key => key));
            foreach (var (column, expectedValue) in expected[i])
            {
                Assert.True(
                    Equals(expectedValue, actual[i][column]),
                    $"Row {expected[i]["Id"]}, column {column}: expected '{expectedValue}' ({expectedValue?.GetType().Name}) but was '{actual[i][column]}' ({actual[i][column]?.GetType().Name})");
            }
        }
    }

    private static async Task<int> CountStatementExecutionsAsync(CacheWriteHarness harness, string textFragment)
    {
        await using var connection = new SqlConnection(harness.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand("""
            SELECT ISNULL(SUM(stats.execution_count), 0)
            FROM sys.dm_exec_query_stats AS stats
            CROSS APPLY sys.dm_exec_sql_text(stats.sql_handle) AS sqlText
            WHERE sqlText.text LIKE '%' + @fragment + '%' AND sqlText.text LIKE '%' + @table + '%'
            """, connection);
        command.Parameters.AddWithValue("@fragment", textFragment);
        command.Parameters.AddWithValue("@table", harness.TableName);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }
}
