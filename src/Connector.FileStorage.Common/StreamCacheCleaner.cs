using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Streams;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FileStorage.Common;

/// <summary>
/// Periodically removes the archived tables of streams from the stream cache database, except for the most recent one of each stream.
/// </summary>
internal class StreamCacheCleaner
{
    private const string LockName = "StreamCacheCleanup";
    private const int LockTimeoutInMilliseconds = 100;
    private static readonly TimeSpan _initialDelay = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan _interval = TimeSpan.FromDays(1);
    private static int _hasStarted;

    private readonly ILogger _logger;
    private readonly ApplicationContext _applicationContext;

    public StreamCacheCleaner(ILogger logger, ApplicationContext applicationContext)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
    }

    public async Task RunAsync()
    {
        // The stream cache is shared by all the connectors, cleaning it up once per process is enough.
        if (Interlocked.Exchange(ref _hasStarted, 1) == 1)
        {
            return;
        }

        await Task.Delay(_initialDelay);
        while (true)
        {
            try
            {
                await CleanUpAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error cleaning up the stream cache tables. Next run in {Interval}.", _interval);
            }

            await Task.Delay(_interval);
        }
    }

    private async Task CleanUpAsync()
    {
        var connectionStrings = _applicationContext.System.ConnectionStrings;
        var connectionStringKey = StorageConfigurationConstants.StreamCacheConnectionStringKey;
        if (!connectionStrings.ConnectionStringExists(connectionStringKey))
        {
            _logger.LogDebug("Skipping clean up of stream cache tables because connection string {ConnectionStringKey} is not configured.", connectionStringKey);
            return;
        }

        var connectionString = connectionStrings.GetConnectionString(connectionStringKey);
        await using var lockConnection = new SqlConnection(connectionString);
        await lockConnection.OpenAsync();
        if (!await DistributedLockHelper.TryAcquireExclusiveSessionLock(lockConnection, LockName, LockTimeoutInMilliseconds))
        {
            _logger.LogDebug("Skipping clean up of stream cache tables because it is already running.");
            return;
        }

        try
        {
            var remover = new StreamCacheTableRemover(_logger, connectionString);

            // The tables are read before the streams, so that the stream of a table that has just been created is not seen as removed.
            var tableNames = await remover.GetTableNamesAsync();
            var streamIds = await GetStreamIdsAsync();
            foreach (var streamTables in StreamCacheTables.FromTableNames(tableNames))
            {
                await CleanUpStreamAsync(remover, streamTables, streamIds.Contains(streamTables.StreamId));
            }
        }
        finally
        {
            await DistributedLockHelper.ReleaseSessionLock(lockConnection, LockName);
        }
    }

    internal async Task CleanUpStreamAsync(StreamCacheTableRemover remover, StreamCacheTables streamTables, bool streamExists)
    {
        var removedTableNames = await remover.RemoveObsoleteTablesAsync(streamTables);

        if (!streamExists)
        {
            _logger.LogWarning(
                "Stream '{StreamId}' no longer exists but it still has stream cache tables. Please delete these tables, and their history tables, manually: {TableNames}.",
                streamTables.StreamId,
                string.Join(", ", streamTables.TableNames.Except(removedTableNames)));
        }
    }

    private async Task<HashSet<Guid>> GetStreamIdsAsync()
    {
        var streamIds = new HashSet<Guid>();
        var streamRepository = _applicationContext.Container.Resolve<IStreamRepository>();
        var orgDataStore = _applicationContext.System.Organization.DataStores.GetDataStore<OrganizationProfile>();
        var organizationProfiles = await orgDataStore.SelectAsync(_applicationContext.System.CreateExecutionContext(), _ => true);
        foreach (var organizationProfile in organizationProfiles)
        {
            var executionContext = _applicationContext.CreateExecutionContext(organizationProfile.Id);
            var streams = await streamRepository.GetAllStreamsEx(executionContext);
            streamIds.UnionWith(streams.Select(stream => stream.Id));
        }

        return streamIds;
    }
}
