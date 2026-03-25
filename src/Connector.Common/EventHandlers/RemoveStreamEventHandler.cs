using System;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Events;
using CluedIn.Core.Events.Types;

using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.DataLake.Common.EventHandlers;

internal class RemoveStreamEventHandler : UpdateStreamScheduleBase, IDisposable
{
    private readonly IDisposable _subscription;
    private bool _disposedValue;

    public RemoveStreamEventHandler(
        ApplicationContext applicationContext,
        IDataLakeConstants constants,
        IDataLakeJobDataFactory jobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider,
        Type exportEntitiesJobType,
        IScheduledJobQueue jobQueue)
        : base(
            applicationContext,
            constants,
            jobDataFactory,
            dateTimeOffsetProvider,
            exportEntitiesJobType,
            jobQueue)
    {
        _subscription = ApplicationContext.System.Events.Local.Subscribe<RemoveStreamEvent>(ProcessEvent);
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

    private void ProcessEvent(RemoveStreamEvent eventData)
    {
        if (!eventData.TryGetResourceInfo("AccountId", "StreamId", out var organizationId, out var streamId))
        {
            return;
        }

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
