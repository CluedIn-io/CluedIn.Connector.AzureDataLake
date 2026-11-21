using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Processing;

using Microsoft.Data.SqlClient;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Integration;

/// <summary>
/// Stores competing writes of the same entities at the same time through a real connector into a real stream cache table,
/// with and without batching, and checks that the table ends up as the rules say, whatever order the writes were applied in.
/// Needs INTEGRATIONTEST_STREAMCACHE (a base64 encoded SQL Server connection string).
/// </summary>
/// <remarks>
/// What a race can do is lose a write, let an older persist version win, tear a row (columns of different writes), or fail.
/// A failure is not what the test is about as long as the pipeline's answer to it works: a write that comes back as a
/// ReQueue is stored again, as the pipeline does, and the test only fails when a write is still not stored after
/// <see cref="MaximumAttempts"/> attempts. How often that was needed, and why, is written to the test output.
/// A single run is not proof: the interleavings are down to timing, so every scenario is repeated in rounds.
/// </remarks>
public class StorageConnectorCacheRaceTests
{
    private const int MaximumAttempts = 10;
    private const int WritesInFlight = 64;
    private const int Rounds = 3;
    private const int EntitiesPerRound = 50;
    private const int HighestVersion = 5;

    // A small batch size, so that the writes of an entity are spread over many batches that are written at the same time
    private const int BatchSize = 10;

    private readonly ITestOutputHelper _output;

    public StorageConnectorCacheRaceTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Theory]
    [InlineData(false, 10)]
    [InlineData(true, 10)]
    [InlineData(true, 1)]
    public async Task StoreData_WhenTheSameNewEntityIsWrittenConcurrently_TheHighestPersistVersionWins(bool batching, int outputThreads)
    {
        await using var harness = CacheWriteHarness.Create(batching, softDelete: true, BatchSize, outputThreads: outputThreads);

        // the table exists, so that it is the entities that race and not the creation of the table
        await StoreWithRetryAsync(harness, [CacheWriteHarness.CreateEntity(Guid.NewGuid())]);

        var stats = new RaceStats();
        var allIds = new List<Guid>();
        for (var round = 0; round < Rounds; round++)
        {
            // none of them is in the table yet: whichever write gets there first inserts, the others have to update (or not)
            var ids = NewIds(EntitiesPerRound);
            allIds.AddRange(ids);
            await StoreWithRetryAsync(
                harness,
                ids.SelectMany(id => Enumerable.Range(1, HighestVersion).Select(version => Write(id, version, version == 1 ? VersionChangeType.Added : VersionChangeType.Changed))),
                stats,
                seed: round);
        }

        Report(harness, stats);
        await AssertRowsAsync(harness, allIds, expectedVersion: HighestVersion, expectedRowCount: allIds.Count + 1);
    }

    [Theory]
    [InlineData(false, 10)]
    [InlineData(true, 10)]
    [InlineData(true, 1)]
    public async Task StoreData_WhenTheSameExistingEntityIsWrittenConcurrently_TheHighestPersistVersionWins(bool batching, int outputThreads)
    {
        await using var harness = CacheWriteHarness.Create(batching, softDelete: true, BatchSize, outputThreads: outputThreads);

        var stats = new RaceStats();
        var allIds = new List<Guid>();
        for (var round = 0; round < Rounds; round++)
        {
            var ids = NewIds(EntitiesPerRound);
            allIds.AddRange(ids);
            await StoreWithRetryAsync(harness, ids.Select(id => Write(id, 1, VersionChangeType.Added)), stats: null, seed: round);

            // all of them update an existing row, with versions that come in no particular order
            await StoreWithRetryAsync(
                harness,
                ids.SelectMany(id => Enumerable.Range(2, HighestVersion - 1).Select(version => Write(id, version, VersionChangeType.Changed))),
                stats,
                seed: round);
        }

        Report(harness, stats);
        await AssertRowsAsync(harness, allIds, expectedVersion: HighestVersion, expectedRowCount: allIds.Count);
    }

    [Theory]
    [InlineData(false, 10)]
    [InlineData(true, 10)]
    [InlineData(true, 1)]
    public async Task StoreData_WhenAnEntityIsChangedAndRemovedConcurrently_ItEndsRemoved(bool batching, int outputThreads)
    {
        await using var harness = CacheWriteHarness.Create(batching, softDelete: true, BatchSize, outputThreads: outputThreads);

        var stats = new RaceStats();
        var allIds = new List<Guid>();
        for (var round = 0; round < Rounds; round++)
        {
            var ids = NewIds(EntitiesPerRound);
            allIds.AddRange(ids);
            await StoreWithRetryAsync(harness, ids.Select(id => Write(id, 1, VersionChangeType.Added)), stats: null, seed: round);

            // a change that arrives after the removal is ignored, and one that arrives before is overwritten by it
            await StoreWithRetryAsync(
                harness,
                ids.SelectMany(id => Enumerable.Range(2, 3).Select(version => Write(id, version, VersionChangeType.Changed)).Append(Write(id, 3, VersionChangeType.Removed))),
                stats,
                seed: round);
        }

        Report(harness, stats);
        var rows = await harness.ReadRowsAsync();
        Assert.Equal(allIds.Count, rows.Count);
        foreach (var row in rows)
        {
            Assert.Equal(nameof(VersionChangeType.Removed), (string)row["__ChangeType__"]);

            // which of the changes made it in before the removal is down to the timing, but a row is of one of them
            AssertRowIsOneWrite(row);
        }
    }

    [Theory]
    [InlineData(false, 10)]
    [InlineData(true, 10)]
    public async Task StoreData_WhenTheTableDoesNotExistAndEntitiesAreWrittenConcurrently_EveryEntityIsStored(bool batching, int outputThreads)
    {
        var stats = new RaceStats();
        for (var round = 0; round < Rounds; round++)
        {
            // a stream of its own for every round, as it is the creation of the table that is raced
            await using var harness = CacheWriteHarness.Create(batching, softDelete: true, BatchSize, outputThreads: outputThreads);
            var ids = NewIds(EntitiesPerRound);

            await StoreWithRetryAsync(
                harness,
                ids.SelectMany(id => Enumerable.Range(1, 2).Select(version => Write(id, version, version == 1 ? VersionChangeType.Added : VersionChangeType.Changed))),
                stats,
                seed: round);

            Report(harness, stats);
            await AssertRowsAsync(harness, ids, expectedVersion: 2, expectedRowCount: ids.Count);
        }
    }

    private static List<Guid> NewIds(int count) => Enumerable.Range(0, count).Select(_ => Guid.NewGuid()).ToList();

    // An entity of which every column says which write it is of: a row that has columns of different writes was torn
    private static ConnectorEntityData Write(Guid id, int version, VersionChangeType changeType) =>
        CacheWriteHarness.CreateEntity(id, changeType, persistVersion: version, name: $"v{version}", age: version * 10, number: version + 0.5m);

    /// <summary>
    /// Stores the entities all at once, in an order that is shuffled (the same for the same seed), with a limited number in
    /// flight like the pipeline has. A ReQueue is answered by storing the entity again.
    /// </summary>
    private static async Task StoreWithRetryAsync(CacheWriteHarness harness, IEnumerable<ConnectorEntityData> entities, RaceStats stats = null, int seed = 0)
    {
        var random = new Random(seed);
        var shuffled = entities.OrderBy(_ => random.Next()).ToList();

        using var inFlight = new SemaphoreSlim(WritesInFlight);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = shuffled.Select(entity => Task.Run(async () =>
        {
            // every write waits for the same moment to start
            await start.Task;
            await inFlight.WaitAsync();
            try
            {
                for (var attempt = 1; ; attempt++)
                {
                    var result = await harness.StoreAsync(entity);
                    if (result.State == SaveResultState.Success)
                    {
                        return;
                    }

                    if (result.State != SaveResultState.ReQueue || attempt == MaximumAttempts)
                    {
                        var cause = harness.Logger.Entries.LastOrDefault(entry => entry.Exception is not null).Exception;
                        throw new InvalidOperationException(
                            $"Storing {entity.EntityId} (persist version {entity.PersistInfo.PersistVersion}, {entity.ChangeType}) resulted in {result.State} after {attempt} attempts. Last logged exception: {cause}");
                    }

                    stats?.Requeued();
                }
            }
            finally
            {
                inFlight.Release();
            }
        })).ToList();

        start.SetResult();
        await Task.WhenAll(tasks);
    }

    private async Task AssertRowsAsync(CacheWriteHarness harness, List<Guid> ids, int expectedVersion, int expectedRowCount)
    {
        var rows = await harness.ReadRowsAsync();
        Assert.Equal(expectedRowCount, rows.Count);

        var rowsById = rows.ToDictionary(row => (Guid)row["Id"]);
        foreach (var id in ids)
        {
            Assert.True(rowsById.TryGetValue(id, out var row), $"Entity {id} is not in the table.");
            Assert.True(
                Convert.ToInt32(row["PersistVersion"]) == expectedVersion,
                $"Entity {id} has persist version {row["PersistVersion"]} ({row["Name"]}) while {expectedVersion} was written to it as well.");
            AssertRowIsOneWrite(row);
        }
    }

    private static void AssertRowIsOneWrite(Dictionary<string, object> row)
    {
        var version = Convert.ToInt32(row["PersistVersion"]);
        Assert.True(
            (string)row["Name"] == $"v{version}"
                && Convert.ToInt32(row["user.age"]) == version * 10
                && Convert.ToDecimal(row["user.number"]) == version + 0.5m,
            $"Entity {row["Id"]} has columns of more than one write: PersistVersion {version}, Name {row["Name"]}, user.age {row["user.age"]}, user.number {row["user.number"]}.");
    }

    private void Report(CacheWriteHarness harness, RaceStats stats)
    {
        // what went wrong on the way (and was answered by writing again) by SQL Server error number: 1205 is a deadlock, 2627 a duplicate key
        var failures = harness.Logger.Entries
            .Where(entry => entry.Exception is not null)
            .Select(entry => FindSqlException(entry.Exception)?.Number.ToString() ?? entry.Exception.GetType().Name)
            .GroupBy(cause => cause)
            .Select(group => $"{group.Key} x{group.Count()}");
        _output.WriteLine($"ReQueues answered by storing again: {stats.Requeues}. Logged exceptions: {(failures.Any() ? string.Join(", ", failures) : "none")}.");
    }

    private static SqlException FindSqlException(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is SqlException sqlException)
            {
                return sqlException;
            }
        }

        return null;
    }

    private sealed class RaceStats
    {
        private int _requeues;

        public int Requeues => _requeues;

        public void Requeued() => Interlocked.Increment(ref _requeues);
    }
}
