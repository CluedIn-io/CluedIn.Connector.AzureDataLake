using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using CluedIn.Core.Connectors;
using CluedIn.Core.Processing;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

using Xunit;

namespace CluedIn.Connector.FileStorage.Common.Tests.Integration;

/// <summary>
/// A write of StoreData that fails with a SQL Server error that can go away on its own (here: the SQL Server cannot be reached)
/// is retried, with a delay, before it comes back as a ReQueue. A failure that retrying cannot fix (here: the database is not
/// there) comes back as a ReQueue at once. Needs INTEGRATIONTEST_STREAMCACHE, a base64 encoded SQL Server connection string.
/// </summary>
/// <remarks>
/// The connection strings turn off the connection retries of SqlClient (which retries a login to a missing database on its own,
/// 10 seconds apart), so that it is the retries of the connector that are tested.
/// The retries are set with environment variables that the connector reads when it is created, and those are for the whole
/// process, so these tests run on their own: no other test runs while they do.
/// </remarks>
[Collection(StoreRetryEnvironmentCollection.Name)]
public sealed class StorageConnectorStoreRetryTests : IDisposable
{
    private const string RetryCountVariableName = "FILESTORAGE_STORE_RETRY_COUNT";
    private const string RetryDelaySecondsVariableName = "FILESTORAGE_STORE_RETRY_DELAY_SECONDS";

    private readonly string _retryCountBefore = Environment.GetEnvironmentVariable(RetryCountVariableName);
    private readonly string _retryDelayBefore = Environment.GetEnvironmentVariable(RetryDelaySecondsVariableName);

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(RetryCountVariableName, _retryCountBefore);
        Environment.SetEnvironmentVariable(RetryDelaySecondsVariableName, _retryDelayBefore);
    }

    [Fact]
    public async Task StoreData_WhenTheSqlServerCannotBeReached_RetriesTheCountWithTheDelayAndThenReturnsReQueue()
    {
        Environment.SetEnvironmentVariable(RetryCountVariableName, "2");
        Environment.SetEnvironmentVariable(RetryDelaySecondsVariableName, "0.2");
        await using var harness = CacheWriteHarness.Create(batching: false, softDelete: true, connectionString: GetConnectionString($"127.0.0.1,{GetFreePort()}"));

        var stopwatch = Stopwatch.StartNew();
        var result = await harness.StoreAsync(CacheWriteHarness.CreateEntity(Guid.NewGuid()));
        stopwatch.Stop();

        Assert.Equal(SaveResultState.ReQueue, result.State);
        Assert.NotEmpty(result.Exceptions);
        Assert.Equal(2, CountRetries(harness));

        // 2 retries, each after a delay of 200 ms
        Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(380), $"The write took {stopwatch.Elapsed}, so the retries did not wait for the delay.");
    }

    [Fact]
    public async Task StoreData_WhenTheSqlServerCanBeReachedAtTheRetry_Succeeds()
    {
        Environment.SetEnvironmentVariable(RetryCountVariableName, "5");
        Environment.SetEnvironmentVariable(RetryDelaySecondsVariableName, "1");
        var proxyPort = GetFreePort();
        await using var harness = CacheWriteHarness.Create(batching: false, softDelete: true, connectionString: GetConnectionString($"127.0.0.1,{proxyPort}"));

        // The first attempt fails, as nothing listens on the port yet. The SQL Server is there while the write waits to retry.
        var storing = harness.StoreAsync(CacheWriteHarness.CreateEntity(Guid.NewGuid()));
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (CountRetries(harness) == 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(50));
        }

        Assert.False(storing.IsCompleted, "The write was expected to be waiting to retry.");
        using var proxy = TcpProxy.Start(proxyPort, GetRealEndPoint());

        var result = await storing;

        Assert.Equal(SaveResultState.Success, result.State);
        Assert.True(CountRetries(harness) >= 1);
    }

    [Fact]
    public async Task StoreData_WhenTheDatabaseIsNotThere_DoesNotRetry()
    {
        Environment.SetEnvironmentVariable(RetryCountVariableName, "5");
        Environment.SetEnvironmentVariable(RetryDelaySecondsVariableName, "30");
        var connectionString = new SqlConnectionStringBuilder(CacheWriteHarness.StreamCacheConnectionString)
        {
            InitialCatalog = $"StoreRetryMissing_{Guid.NewGuid():N}",
            Pooling = false,
            ConnectRetryCount = 0,
        }.ConnectionString;
        await using var harness = CacheWriteHarness.Create(batching: false, softDelete: true, connectionString: connectionString);

        var stopwatch = Stopwatch.StartNew();
        var result = await harness.StoreAsync(CacheWriteHarness.CreateEntity(Guid.NewGuid()));
        stopwatch.Stop();

        Assert.Equal(SaveResultState.ReQueue, result.State);
        Assert.Equal(0, CountRetries(harness));
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(20), $"The write took {stopwatch.Elapsed}, so it waited for a delay.");
    }

    [Fact]
    public async Task StoreData_WithoutRetries_ReturnsReQueueAtOnce()
    {
        Environment.SetEnvironmentVariable(RetryCountVariableName, "0");
        Environment.SetEnvironmentVariable(RetryDelaySecondsVariableName, "30");
        await using var harness = CacheWriteHarness.Create(batching: false, softDelete: true, connectionString: GetConnectionString($"127.0.0.1,{GetFreePort()}"));

        var stopwatch = Stopwatch.StartNew();
        var result = await harness.StoreAsync(CacheWriteHarness.CreateEntity(Guid.NewGuid()));
        stopwatch.Stop();

        Assert.Equal(SaveResultState.ReQueue, result.State);
        Assert.Equal(0, CountRetries(harness));
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(20), $"The write took {stopwatch.Elapsed}, so it waited for a delay.");
    }

    private static int CountRetries(CacheWriteHarness harness)
    {
        return harness.Logger.Entries.Count(x => x.Level == LogLevel.Warning && x.Message.StartsWith("Failed to store the entity", StringComparison.Ordinal));
    }

    private static string GetConnectionString(string dataSource)
    {
        return new SqlConnectionStringBuilder(CacheWriteHarness.StreamCacheConnectionString)
        {
            DataSource = dataSource,
            Pooling = false,
            ConnectRetryCount = 0,
            ConnectTimeout = 2,
        }.ConnectionString;
    }

    private static IPEndPoint GetRealEndPoint()
    {
        // "host,port" (or "tcp:host,port"), as in the connection string of the integration tests
        var dataSource = new SqlConnectionStringBuilder(CacheWriteHarness.StreamCacheConnectionString).DataSource;
        var parts = dataSource.Replace("tcp:", string.Empty, StringComparison.OrdinalIgnoreCase).Split(',');
        var host = parts[0];
        var address = host is "localhost" or "." or "(local)"
            ? IPAddress.Loopback
            : Dns.GetHostAddresses(host).First(x => x.AddressFamily == AddressFamily.InterNetwork);
        return new IPEndPoint(address, parts.Length > 1 ? int.Parse(parts[1]) : 1433);
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    /// <summary>
    /// Forwards what is sent to a port on this machine to another end point, so that the SQL Server can be made reachable
    /// on an address that was refused before.
    /// </summary>
    private sealed class TcpProxy : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly IPEndPoint _target;
        private readonly CancellationTokenSource _stop = new();

        private TcpProxy(int port, IPEndPoint target)
        {
            _target = target;
            _listener = new TcpListener(IPAddress.Loopback, port);
        }

        public static TcpProxy Start(int port, IPEndPoint target)
        {
            var proxy = new TcpProxy(port, target);
            proxy._listener.Start();
            _ = proxy.AcceptAsync();
            return proxy;
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
        }

        private async Task AcceptAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    _ = ForwardAsync(client);
                }
            }
            catch (Exception) when (_stop.IsCancellationRequested)
            {
            }
        }

        private async Task ForwardAsync(TcpClient client)
        {
            using (client)
            using (var server = new TcpClient())
            {
                try
                {
                    await server.ConnectAsync(_target.Address, _target.Port, _stop.Token);
                    var clientStream = client.GetStream();
                    var serverStream = server.GetStream();
                    await Task.WhenAny(
                        clientStream.CopyToAsync(serverStream, _stop.Token),
                        serverStream.CopyToAsync(clientStream, _stop.Token));
                }
                catch (Exception)
                {
                    // the connection ended
                }
            }
        }
    }
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class StoreRetryEnvironmentCollection
{
    public const string Name = "Store retry environment variables";
}
