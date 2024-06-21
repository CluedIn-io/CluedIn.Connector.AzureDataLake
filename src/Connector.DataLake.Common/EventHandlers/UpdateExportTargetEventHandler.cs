using System;
using System.Threading.Tasks;
using CluedIn.Core;
using CluedIn.Core.Events.Types;
using CluedIn.Core.Streams;
using Newtonsoft.Json.Linq;

namespace CluedIn.Connector.DataLake.Common.EventHandlers;

internal class UpdateExportTargetEventHandler : UpdateStreamScheduleBase, IDisposable
{
    private readonly IDisposable _subscription;
    private bool _disposedValue;

    public UpdateExportTargetEventHandler(
        ApplicationContext applicationContext,
        Type exportEntitiesJobType)
        : base(applicationContext, exportEntitiesJobType)
    {
        _subscription = ApplicationContext.System.Events.Local.Subscribe<UpdateExportTargetEvent>(ProcessEvent);
    }


    private void ProcessEvent(UpdateExportTargetEvent eventData)
    {
        ProcessEventAsync(eventData).GetAwaiter().GetResult();
    }

    private async Task ProcessEventAsync(UpdateExportTargetEvent eventData)
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

        if (!eventObject.TryGetValue("Id", out var idToken))
        {
            return;
        }

        var accountId = accountIdToken.Value<string>();
        var providerDefinitionIdString = idToken.Value<string>();
        var providerDefinitionId = new Guid(providerDefinitionIdString);

        var organizationId = new Guid(accountId);
        var streamRepository = ApplicationContext.Container.Resolve<IStreamRepository>();

        var streamsCount = await streamRepository.GetOrganizationStreamsCount(organizationId);
        var streamsPerPage = 100;
        var totalPages = (streamsCount + streamsPerPage - 1) / streamsPerPage;

        var executionContext = ApplicationContext.CreateExecutionContext(organizationId);
        for (var i = 0; i < totalPages; ++i)
        {
            var streams = await streamRepository.GetOrganizationStreams(organizationId, i, streamsPerPage, filterConnectorProviderDefinitionId: providerDefinitionId);
            foreach (var stream in streams)
            {
                await UpdateStreamSchedule(executionContext, stream);
            }
        }
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

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}
