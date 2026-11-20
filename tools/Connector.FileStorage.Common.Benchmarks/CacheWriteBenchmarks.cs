using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using BenchmarkDotNet.Attributes;

using CluedIn.Connector.FileStorage.Common.Tests.Integration;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Processing;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.FileStorage.Common.Benchmarks;

public enum WriteMode
{
    /// <summary>Batching is off: every entity is written on a connection and transaction of its own.</summary>
    Single,

    /// <summary>Batching is on and writes what is there, without waiting for more.</summary>
    Batched,

    /// <summary>Batching is on and a batch waits 100 ms for more entities (the shortest wait there is).</summary>
    BatchedWaiting,
}

/// <summary>
/// Stores entities through a real connector into a real stream cache table: one write per entity (Single) against the
/// entities that are stored at the same time being written together (Batched).
/// </summary>
/// <remarks>
/// The entities are written with a limited number of calls in flight at the same time (Concurrency), which is what
/// decides how many of them there are to write together. With one call at a time every batch is of one entity, which is
/// what the cost of batching is, and where it has nothing to win.
/// Every iteration writes to a table of its own, that exists before the measurement starts.
/// </remarks>
[MemoryDiagnoser]
public class CacheWriteBenchmarks
{
    private CacheWriteHarness _harness;
    private List<ConnectorEntityData> _entities;

    /// <summary>How the entities are written: one by one, or the entities that are stored at the same time together.</summary>
    [Params(WriteMode.Single, WriteMode.Batched, WriteMode.BatchedWaiting)]
    public WriteMode Mode { get; set; }

    [Params(false, true)]
    public bool SoftDelete { get; set; }

    [Params(1, 16, 64)]
    public int Concurrency { get; set; }

    [Params(500)]
    public int Entities { get; set; }

    [IterationSetup(Target = nameof(Insert))]
    public void SetupInsert()
    {
        _harness = CacheWriteHarness.Create(Mode != WriteMode.Single, SoftDelete, batchTimeoutMilliseconds: Mode == WriteMode.BatchedWaiting ? 100 : 0);

        // the table exists before the measurement, as it does when entities are stored to a stream that has run before
        WriteAllAsync(new[] { CacheWriteHarness.CreateEntity(Guid.NewGuid()) }).GetAwaiter().GetResult();
        _entities = Enumerable.Range(0, Entities)
            .Select(_ => CacheWriteHarness.CreateEntity(Guid.NewGuid(), VersionChangeType.Added, persistVersion: 1))
            .ToList();
        ClearConnectionPool();
    }

    [IterationSetup(Target = nameof(Update))]
    public void SetupUpdate()
    {
        _harness = CacheWriteHarness.Create(Mode != WriteMode.Single, SoftDelete, batchTimeoutMilliseconds: Mode == WriteMode.BatchedWaiting ? 100 : 0);

        var ids = Enumerable.Range(0, Entities).Select(_ => Guid.NewGuid()).ToList();
        WriteAllAsync(ids.Select(id => CacheWriteHarness.CreateEntity(id, VersionChangeType.Added, persistVersion: 1)).ToList()).GetAwaiter().GetResult();
        _entities = ids
            .Select(id => CacheWriteHarness.CreateEntity(id, VersionChangeType.Changed, persistVersion: 2, name: "changed"))
            .ToList();
        ClearConnectionPool();
    }

    [IterationCleanup]
    public void Cleanup()
    {
        _harness.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _harness = null;
        _entities = null;
    }

    /// <summary>Entities the cache table has not got yet.</summary>
    [Benchmark]
    public Task Insert() => MeasureAsync(nameof(Insert));

    /// <summary>Entities the cache table has, with a newer persist version.</summary>
    [Benchmark]
    public Task Update() => MeasureAsync(nameof(Update));

    // The connections the setup used are not what the measurement is about, so every measurement starts without any
    private static void ClearConnectionPool() => SqlConnection.ClearAllPools();

    private async Task MeasureAsync(string method)
    {
        await using var monitor = await ConnectionMonitor.StartAsync(_harness.ConnectionString);
        await WriteAllAsync(_entities);
        ConnectionUsages.Add(ConnectionUsages.CaseKey(method, Mode, SoftDelete, Concurrency), await monitor.StopAsync());
    }

    private async Task WriteAllAsync(IReadOnlyCollection<ConnectorEntityData> entities)
    {
        using var inFlight = new SemaphoreSlim(Concurrency);
        var tasks = entities.Select(async entity =>
        {
            await inFlight.WaitAsync();
            try
            {
                var result = await _harness.StoreAsync(entity);
                if (result.State != SaveResultState.Success)
                {
                    throw new InvalidOperationException($"Storing {entity.EntityId} resulted in {result.State}.");
                }
            }
            finally
            {
                inFlight.Release();
            }
        }).ToList();

        await Task.WhenAll(tasks);
    }
}
