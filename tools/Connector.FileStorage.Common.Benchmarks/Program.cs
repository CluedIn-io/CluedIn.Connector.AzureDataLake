using System;
using System.Linq;

using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;

using CluedIn.Connector.FileStorage.Common.Tests.Integration;

namespace CluedIn.Connector.FileStorage.Common.Benchmarks;

internal static class Program
{
    public static int Main(string[] args)
    {
        if (string.IsNullOrWhiteSpace(CacheWriteHarness.StreamCacheConnectionString))
        {
            Console.Error.WriteLine("INTEGRATIONTEST_STREAMCACHE is not set. It is a base64 encoded connection string of the SQL Server to write the stream cache tables to.");
            return 1;
        }

        // In process: the benchmarks wait for SQL Server much more than they run code, and building a project
        // per benchmark would need the repository's package sources and the CluedIn version again.
        // One invocation per iteration, because every iteration starts with a table of its own.
        var job = Job.Default
            .WithToolchain(InProcessNoEmitToolchain.Instance)
            .WithStrategy(RunStrategy.Monitoring)
            .WithLaunchCount(1)
            .WithWarmupCount(1)
            .WithIterationCount(5)
            .WithInvocationCount(1)
            .WithUnrollFactor(1);
        // Waiting for more entities with one call at a time means every entity waits out the whole timeout (~110 ms each,
        // 55 s for an iteration), which says all there is to say about it without running it six times
        var config = DefaultConfig.Instance
            .AddJob(job.AsDefault())
            .AddFilter(new SimpleFilter(benchmarkCase =>
                !(benchmarkCase.Parameters.Items.Any(item => item.Name == nameof(CacheWriteBenchmarks.Mode) && Equals(item.Value, WriteMode.BatchedWaiting))
                    && benchmarkCase.Parameters.Items.Any(item => item.Name == nameof(CacheWriteBenchmarks.Concurrency) && Equals(item.Value, 1)))))
            .AddColumn(
                new ConnectionsColumn("PeakConnections", "Peak connections", "Most connections to SQL Server this process had open at the same time (mean of the iterations)", usage => usage.PeakConnections),
                new ConnectionsColumn("NewLogins", "New logins", "Connections logged in to SQL Server during the iteration, as pooled ones are reused (mean of the iterations)", usage => usage.NewLogins));

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
        return 0;
    }
}
