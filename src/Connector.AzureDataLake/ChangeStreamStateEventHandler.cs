using System;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Events.Types;
using CluedIn.Core.Streams;

using Newtonsoft.Json.Linq;

namespace CluedIn.Connector.AzureDataLake;

internal class ChangeStreamStateEventHandler : UpdateStreamScheduleBase, IDisposable
{
    private readonly IDisposable _subscription;
    private bool _disposedValue;

    public ChangeStreamStateEventHandler(ApplicationContext applicationContext)
        : base(applicationContext)
    {
        _subscription = ApplicationContext.System.Events.Local.Subscribe<ChangeStreamStateEvent>(ProcessEvent);
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


    private void ProcessEvent(ChangeStreamStateEvent eventData)
    {
        ProcessEventAsync(eventData).GetAwaiter().GetResult();
    }
    private async Task ProcessEventAsync(ChangeStreamStateEvent eventData)
    {
        if (!(eventData.EventData?.StartsWith("{") ?? false))
        {
            return;
        }

        var eventObject = JsonUtility.Deserialize<JObject>(eventData.EventData);
        if (!eventObject.TryGetValue("AccountId", out var accountIdToken))
        {
            return;
        }

        if (!eventObject.TryGetValue("StreamId", out var streamIdToken))
        {
            return;
        }

        var accountId = accountIdToken.Value<string>();
        var streamIdString = streamIdToken.Value<string>();
        var streamId = new Guid(streamIdString);

        var organizationId = new Guid(accountId);
        var streamRepository = ApplicationContext.Container.Resolve<IStreamRepository>();

        var stream = await streamRepository.GetStream(streamId);
        if (stream == null)
        {
            return;
        }

        var executionContext = ApplicationContext.CreateExecutionContext(organizationId);
        await UpdateStreamSchedule(executionContext, stream);
    }

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}
