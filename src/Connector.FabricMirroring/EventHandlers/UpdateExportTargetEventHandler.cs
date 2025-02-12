using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Azure.Identity;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.FabricMirroring.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Events;
using CluedIn.Core.Events.Types;

using Microsoft.Fabric.Api;
using Microsoft.Fabric.Api.Core.Models;
using Microsoft.Fabric.Api.MirroredDatabase.Models;

using Newtonsoft.Json.Linq;

namespace CluedIn.Connector.FabricMirroring.EventHandlers;

internal class UpdateExportTargetEventHandler : UpdateMirroredDatabaseBase, IDisposable
{
    private readonly IDisposable _subscription;
    private bool _disposedValue;

    public UpdateExportTargetEventHandler(
        ApplicationContext applicationContext,
        FabricMirroringClient client,
        IDataLakeConstants constants,
        IDataLakeJobDataFactory jobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(applicationContext, constants, jobDataFactory, dateTimeOffsetProvider)
    {
        _subscription = applicationContext.System.Events.Local.Subscribe<UpdateExportTargetEvent>(ProcessEvent);
    }

    private void ProcessEvent(UpdateExportTargetEvent eventData)
    {
        ProcessEventAsync(eventData).GetAwaiter().GetResult();
    }

    private async Task ProcessEventAsync(UpdateExportTargetEvent eventData)
    {
        await UpdateOrCreateMirroredDatabaseAsync(eventData);
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
