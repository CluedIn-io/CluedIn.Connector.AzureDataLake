using System;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Events.Types;

namespace CluedIn.Connector.FileStorage.Common.EventHandlers;

internal class ChangeStreamStateEventHandler : UpdateStreamScheduleBase, IDisposable
{
    private readonly IDisposable _subscription;
    private bool _disposedValue;

    public ChangeStreamStateEventHandler(
        ApplicationContext applicationContext,
        IStorageConfigurationConstants constants,
        IStorageFactory storageFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider,
        Type exportEntitiesJobType,
        IScheduledJobQueue jobQueue)
        : base(
            applicationContext,
            constants,
            storageFactory,
            dateTimeOffsetProvider,
            exportEntitiesJobType,
            jobQueue)
    {
        _subscription = ApplicationContext.System.Events.SubscribeAsync<ChangeStreamStateEvent>(ProcessEventAsync);
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

    private async Task ProcessEventAsync(ChangeStreamStateEvent eventData)
    {
        await UpdateStreamScheduleFromStreamEvent(eventData);
    }

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}
