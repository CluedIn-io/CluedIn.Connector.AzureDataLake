using System;

using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
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
        var config = DefaultConfig.Instance
            .AddJob(job.AsDefault())
            .AddColumn(
                new UsageColumn("PeakConnections", "Peak connections", "Most connections to SQL Server this process had open at the same time (mean of the iterations)", usage => usage.PeakConnections),
                new UsageColumn("NewLogins", "New logins", "Connections logged in to SQL Server during the iteration, as pooled ones are reused (mean of the iterations)", usage => usage.NewLogins),
                new UsageColumn("Batches", "Batches", "Batches the OperationBatcher processed during the iteration, '-' without batching (mean of the iterations)", usage => usage.Batches?.Batches),
                new UsageColumn("BatchMean", "Mean batch", "Mean number of entities in a batch (mean of the iterations)", usage => usage.Batches?.Mean),
                new UsageColumn("BatchP50", "Batch p50", "Median number of entities in a batch (mean of the iterations)", usage => usage.Batches?.Median),
                new UsageColumn("BatchP95", "Batch p95", "95th percentile of the number of entities in a batch (mean of the iterations)", usage => usage.Batches?.Percentile95),
                new UsageColumn("BatchMax", "Batch max", "Largest number of entities in a batch (mean of the iterations)", usage => usage.Batches?.Max));

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
        return 0;
    }
}
