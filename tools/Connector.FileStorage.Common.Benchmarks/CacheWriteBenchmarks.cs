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

    /// <summary>Batching is on and a batch waits 50 ms for more entities (which the batcher rounds up to its 100 ms slice).</summary>
    Batched50ms,

    /// <summary>Batching is on and a batch waits 100 ms for more entities (the shortest wait there is).</summary>
    Batched100ms,
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
    // What the pipeline does with a ReQueue is to store the entity again, which is what this does too (and counts)
    private const int MaximumAttempts = 10;

    private CacheWriteHarness _harness;
    private int _requeues;
    private List<ConnectorEntityData> _entities;

    /// <summary>How the entities are written: one by one, or the entities that are stored at the same time together.</summary>
    /// <remarks>BENCH_MODES (comma separated) picks the modes to run, all of them by default.</remarks>
    [ParamsSource(nameof(Modes))]
    public WriteMode Mode { get; set; }

    [Params(true)]
    public bool SoftDelete { get; set; }

    /// <summary>Whether the batcher counts the sizes of its batches, to see what counting costs.</summary>
    /// <remarks>BENCH_STATS (comma separated) picks the values to run, both by default.</remarks>
    [ParamsSource(nameof(CollectStatsValues))]
    public bool CollectStats { get; set; }

    /// <summary>The threads that write batches at the same time (the connector's default is 10). Without batching there are none.</summary>
    /// <remarks>BENCH_THREADS (comma separated) picks the numbers to run, 10 by default.</remarks>
    [ParamsSource(nameof(OutputThreadsValues))]
    public int OutputThreads { get; set; }

    /// <remarks>BENCH_CONCURRENCY (comma separated) picks the numbers to run, 80 by default.</remarks>
    [ParamsSource(nameof(Concurrencies))]
    public int Concurrency { get; set; }

    [Params(5000)]
    public int Entities { get; set; }

    public static IEnumerable<WriteMode> Modes => FromEnvironment("BENCH_MODES", Enum.Parse<WriteMode>, Enum.GetValues<WriteMode>());

    public static IEnumerable<bool> CollectStatsValues => FromEnvironment("BENCH_STATS", bool.Parse, [true, false]);

    public static IEnumerable<int> OutputThreadsValues => FromEnvironment("BENCH_THREADS", int.Parse, [10]);

    public static IEnumerable<int> Concurrencies => FromEnvironment("BENCH_CONCURRENCY", int.Parse, [80]);

    private static IEnumerable<T> FromEnvironment<T>(string name, Func<string, T> parse, IEnumerable<T> defaults)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value)
            ? defaults
            : value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(parse).ToArray();
    }

    private int BatchTimeoutMilliseconds => Mode switch
    {
        WriteMode.Batched50ms => 50,
        WriteMode.Batched100ms => 100,
        _ => 0,
    };

    private CacheWriteHarness CreateHarness() =>
        CacheWriteHarness.Create(Mode != WriteMode.Single, SoftDelete, batchTimeoutMilliseconds: BatchTimeoutMilliseconds, outputThreads: OutputThreads, collectBatchSizeStats: CollectStats);

    [IterationSetup(Target = nameof(Insert))]
    public void SetupInsert()
    {
        _harness = CreateHarness();

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
        _harness = CreateHarness();

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

        // The setup writes through the same connector, so what the measurement cut is what came after
        var batchesBefore = _harness.GetBatchSizeHistogram();
        _requeues = 0;
        await WriteAllAsync(_entities);
        var batches = BatchSizeSummary.From(BatchSizeSummary.Since(batchesBefore, _harness.GetBatchSizeHistogram()));
        ConnectionUsages.Add(ConnectionUsages.CaseKey(method, Mode, SoftDelete, Concurrency, CollectStats, OutputThreads), await monitor.StopAsync() with { Batches = batches, Requeues = _requeues });
    }

    private async Task WriteAllAsync(IReadOnlyCollection<ConnectorEntityData> entities)
    {
        using var inFlight = new SemaphoreSlim(Concurrency);
        var tasks = entities.Select(async entity =>
        {
            await inFlight.WaitAsync();
            try
            {
                for (var attempt = 1; ; attempt++)
                {
                    var result = await _harness.StoreAsync(entity);
                    if (result.State == SaveResultState.Success)
                    {
                        break;
                    }

                    if (result.State != SaveResultState.ReQueue || attempt == MaximumAttempts)
                    {
                        var cause = _harness.Logger.Entries.LastOrDefault(entry => entry.Exception is not null).Exception;
                        throw new InvalidOperationException($"Storing {entity.EntityId} resulted in {result.State} after {attempt} attempts. Last logged exception: {cause}");
                    }

                    Interlocked.Increment(ref _requeues);
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
