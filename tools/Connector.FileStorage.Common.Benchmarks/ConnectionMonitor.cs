using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.FileStorage.Common.Benchmarks;

/// <summary>
/// What a measurement cost SQL Server in connections: the most this process had open at the same time,
/// and how many it logged in (pooled connections that are reused do not log in again), and the sizes of the batches
/// that were processed meanwhile (none without batching), and how many times an entity had to be stored again after a ReQueue.
/// </summary>
internal readonly record struct ConnectionUsage(int PeakConnections, int NewLogins, BatchSizeSummary? Batches = null, int Requeues = 0, int Deadlocks = 0, int BatchFallbacks = 0);

/// <summary>
/// The sizes of the batches the OperationBatcher processed during a measurement.
/// </summary>
internal readonly record struct BatchSizeSummary(int Batches, double Mean, int Median, int Percentile95, int Max)
{
    /// <param name="histogram">The number at index n - 1 is how many batches had n entities.</param>
    public static BatchSizeSummary? From(IReadOnlyList<int> histogram)
    {
        var batches = histogram.Sum();
        if (batches == 0)
        {
            return null;
        }

        int SizeAtOrBelow(double fraction)
        {
            var needed = (int)Math.Ceiling(batches * fraction);
            var cumulative = 0;
            for (var i = 0; i < histogram.Count; i++)
            {
                cumulative += histogram[i];
                if (cumulative >= needed)
                {
                    return i + 1;
                }
            }

            return histogram.Count;
        }

        var entities = histogram.Select((count, index) => (long)count * (index + 1)).Sum();
        var max = histogram.Select((count, index) => count > 0 ? index + 1 : 0).Max();
        return new BatchSizeSummary(batches, (double)entities / batches, SizeAtOrBelow(0.5), SizeAtOrBelow(0.95), max);
    }

    /// <summary>The histogram of what was cut after <paramref name="before"/> was read.</summary>
    public static IReadOnlyList<int> Since(IReadOnlyList<int> before, IReadOnlyList<int> after) =>
        after.Select((count, index) => count - (index < before.Count ? before[index] : 0)).ToArray();
}

/// <summary>
/// Counts the connections of this process to SQL Server while a measurement runs, from a connection of its own.
/// </summary>
internal sealed class ConnectionMonitor : IAsyncDisposable
{
    private static readonly TimeSpan _samplingInterval = TimeSpan.FromMilliseconds(5);

    private const string SessionsSql = """
        SELECT COUNT(*) FROM sys.dm_exec_sessions
        WHERE is_user_process = 1 AND host_process_id = @processId AND session_id <> @@SPID
        """;

    private const string LoginsSql = """
        SELECT cntr_value FROM sys.dm_os_performance_counters
        WHERE counter_name = 'Logins/sec' AND object_name LIKE '%General Statistics%'
        """;

    // The counter is cumulative since the server started, whatever its name says, and of the whole server
    private const string DeadlocksSql = """
        SELECT cntr_value FROM sys.dm_os_performance_counters
        WHERE counter_name = 'Number of Deadlocks/sec' AND instance_name = '_Total'
        """;

    private readonly SqlConnection _connection;
    private readonly CancellationTokenSource _stop = new();
    private Task _sampling;
    private long _loginsAtStart;
    private long _deadlocksAtStart;
    private int _peakConnections;

    private ConnectionMonitor(SqlConnection connection)
    {
        _connection = connection;
    }

    public static async Task<ConnectionMonitor> StartAsync(string connectionString)
    {
        // Not pooled, so that the monitor is not one of the connections it counts (it is left out of the count by session too)
        var builder = new SqlConnectionStringBuilder(connectionString) { Pooling = false };
        var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync();

        var monitor = new ConnectionMonitor(connection);
        monitor._loginsAtStart = await monitor.ReadLoginsAsync();
        monitor._deadlocksAtStart = await monitor.ReadDeadlocksAsync();
        monitor._peakConnections = await monitor.ReadConnectionsAsync();
        monitor._sampling = Task.Run(monitor.SampleAsync);
        return monitor;
    }

    public async Task<ConnectionUsage> StopAsync()
    {
        await _stop.CancelAsync();
        await _sampling;

        var peak = Math.Max(_peakConnections, await ReadConnectionsAsync());
        var logins = await ReadLoginsAsync() - _loginsAtStart;
        var deadlocks = await ReadDeadlocksAsync() - _deadlocksAtStart;
        return new ConnectionUsage(peak, (int)logins, Deadlocks: (int)deadlocks);
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        await _connection.DisposeAsync();
        _stop.Dispose();
    }

    private async Task SampleAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            var connections = await ReadConnectionsAsync();
            if (connections > _peakConnections)
            {
                _peakConnections = connections;
            }

            try
            {
                await Task.Delay(_samplingInterval, _stop.Token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task<int> ReadConnectionsAsync()
    {
        await using var command = new SqlCommand(SessionsSql, _connection);
        command.Parameters.AddWithValue("@processId", Environment.ProcessId);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    private async Task<long> ReadDeadlocksAsync()
    {
        await using var command = new SqlCommand(DeadlocksSql, _connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    private async Task<long> ReadLoginsAsync()
    {
        await using var command = new SqlCommand(LoginsSql, _connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }
}

/// <summary>
/// Keeps the connection usage of each measured iteration, per benchmark case, for the summary columns.
/// </summary>
internal static class ConnectionUsages
{
    private static readonly ConcurrentDictionary<string, List<ConnectionUsage>> _usages = new();

    public static void Add(string caseKey, ConnectionUsage usage)
    {
        var usages = _usages.GetOrAdd(caseKey, _ => new List<ConnectionUsage>());
        lock (usages)
        {
            usages.Add(usage);
        }
    }

    /// <summary>The usage of the measured iterations: the first one of a case is its warmup.</summary>
    public static IReadOnlyCollection<ConnectionUsage> Get(string caseKey)
    {
        if (!_usages.TryGetValue(caseKey, out var usages))
        {
            return [];
        }

        lock (usages)
        {
            return usages.Skip(1).ToArray();
        }
    }

    public static string CaseKey(string method, WriteMode mode, bool softDelete, int concurrency, bool collectStats, int outputThreads) =>
        $"{method}|{mode}|{softDelete}|{concurrency}|{collectStats}|{outputThreads}";

    public static string CaseKey(BenchmarkCase benchmarkCase)
    {
        string Parameter(string name) => benchmarkCase.Parameters.Items.Single(item => item.Name == name).Value.ToString();

        return $"{benchmarkCase.Descriptor.WorkloadMethod.Name}|{Parameter(nameof(CacheWriteBenchmarks.Mode))}|{Parameter(nameof(CacheWriteBenchmarks.SoftDelete))}|{Parameter(nameof(CacheWriteBenchmarks.Concurrency))}|{Parameter(nameof(CacheWriteBenchmarks.CollectStats))}|{Parameter(nameof(CacheWriteBenchmarks.OutputThreads))}";
    }
}

/// <summary>
/// A summary column of the connection usage, averaged over the measured iterations.
/// </summary>
internal sealed class UsageColumn : IColumn
{
    private readonly Func<ConnectionUsage, double?> _select;

    public UsageColumn(string id, string columnName, string legend, Func<ConnectionUsage, double?> select)
    {
        Id = id;
        ColumnName = columnName;
        Legend = legend;
        _select = select;
    }

    public string Id { get; }

    public string ColumnName { get; }

    public string Legend { get; }

    public bool AlwaysShow => true;

    public ColumnCategory Category => ColumnCategory.Custom;

    public int PriorityInCategory => 0;

    public bool IsNumeric => true;

    public UnitType UnitType => UnitType.Dimensionless;

    public bool IsAvailable(Summary summary) => true;

    public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase) => GetValue(summary, benchmarkCase, SummaryStyle.Default);

    public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
    {
        var values = ConnectionUsages.Get(ConnectionUsages.CaseKey(benchmarkCase)).Select(_select).OfType<double>().ToList();
        return values.Count == 0 ? "-" : values.Average().ToString("0.#");
    }
}
