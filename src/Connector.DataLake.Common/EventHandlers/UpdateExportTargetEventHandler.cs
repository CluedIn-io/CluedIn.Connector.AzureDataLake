using System;
using System.Threading.Tasks;

using CluedIn.Core;
using CluedIn.Core.Events;
using CluedIn.Core.Events.Types;
using CluedIn.Core.Streams;

using Newtonsoft.Json.Linq;

namespace CluedIn.Connector.DataLake.Common.EventHandlers;

internal class UpdateExportTargetEventHandler : UpdateStreamScheduleBase, IDisposable
{
    private const int StreamsPerPage = 100;
    private readonly IDisposable _subscription;
    private bool _disposedValue;

    public UpdateExportTargetEventHandler(
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
        _subscription = ApplicationContext.System.Events.Local.Subscribe<UpdateExportTargetEvent>(ProcessEvent);
    }


    private void ProcessEvent(UpdateExportTargetEvent eventData)
    {
        ProcessEventAsync(eventData).GetAwaiter().GetResult();
    }

    private async Task ProcessEventAsync(UpdateExportTargetEvent eventData)
    {
        if (!eventData.TryGetResourceInfo("AccountId", "Id", out var organizationId, out var providerDefinitionId))
        {
            return;
        }

        var streamRepository = ApplicationContext.Container.Resolve<IStreamRepository>();
        var executionContext = ApplicationContext.CreateExecutionContext(organizationId);
        var streamsCount = await streamRepository.GetOrganizationStreamsCount(executionContext);
        var streamsPerPage = StreamsPerPage;
        var totalPages = (streamsCount + streamsPerPage - 1) / streamsPerPage;

        for (var i = 0; i < totalPages; ++i)
        {
            var streams = await streamRepository.GetOrganizationStreams(executionContext, i, streamsPerPage, filterConnectorProviderDefinitionId: providerDefinitionId);
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
