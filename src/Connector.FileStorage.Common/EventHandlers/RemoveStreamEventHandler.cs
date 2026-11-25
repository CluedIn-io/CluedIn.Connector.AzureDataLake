using System;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;
using CluedIn.Core.Events.Types;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FileStorage.Common.EventHandlers;

internal class RemoveStreamEventHandler : UpdateStreamScheduleBase, IDisposable
{
    private readonly IDisposable _subscription;
    private bool _disposedValue;

    public RemoveStreamEventHandler(
        ApplicationContext applicationContext,
        IStorageConfigurationConstants constants,
        IStorageFactory storageFactory,
        ITimeProvider timeProvider,
        Type exportEntitiesJobType,
        IScheduledJobQueue jobQueue)
        : base(
            applicationContext,
            constants,
            storageFactory,
            timeProvider,
            exportEntitiesJobType,
            jobQueue)
    {
        _subscription = ApplicationContext.System.Events.SubscribeAsync<RemoveStreamEvent>(ProcessEventAsync);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                _subscription.Dispose();
            }

            _disposedValue = true;
        }
    }

    private async Task ProcessEventAsync(RemoveStreamEvent eventData)
    {
        if (!eventData.TryGetResourceInfo("AccountId", "StreamId", out var organizationId, out var streamId))
        {
            return;
        }

        RemoveJobFromQueue(organizationId, streamId);
        await RemoveCacheTablesAsync(organizationId, streamId);
    }

    private async Task RemoveCacheTablesAsync(
        Guid organizationId,
        Guid streamId)
    {
        var logger = ApplicationContext.Container.Resolve<ILogger<RemoveStreamEventHandler>>();
        var connectionStrings = ApplicationContext.System.ConnectionStrings;
        var connectionStringKey = StorageConfigurationConstants.StreamCacheConnectionStringKey;
        if (!connectionStrings.ConnectionStringExists(connectionStringKey))
        {
            logger.LogWarning("Unable to remove cache tables for stream {StreamId} in organization {OrganizationId} because connection string {ConnectionStringKey} is not configured.", streamId, organizationId, connectionStringKey);
            return;
        }

        var connectionString = connectionStrings.GetConnectionString(connectionStringKey);
        await new StreamCacheTableRemover(logger, connectionString).RemoveAllTablesAsync(streamId);
    }

    private void RemoveJobFromQueue(
        Guid organizationId,
        Guid streamId)
    {
        var executionContext = ApplicationContext.CreateExecutionContext(organizationId);
        if (JobQueue.TryRemove(streamId.ToString(), out _))
        {
            executionContext.Log.LogWarning("Removed job {StreamId} from queue.", streamId);
        }
        else
        {
            executionContext.Log.LogWarning("Failed to remove job {StreamId} from queue.", streamId);
        }
    }

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}
