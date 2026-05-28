using System;
using System.Threading.Tasks;

using CluedIn.Connector.FileStorage.Common;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Events;
using CluedIn.Core.Events.Types;

using Microsoft.Extensions.Logging;
using CluedIn.Connector.FileStorage.Common.EventHandlers;
using CluedIn.Connector.FabricOpenMirroring.Connector;

namespace CluedIn.Connector.FabricOpenMirroring.EventHandlers;

internal class ExportTargetEventHandler : IDisposable
{
    private readonly IDisposable _registerExportTargetSubscription;
    private readonly IDisposable _updateExportTargetSubscription;
    private readonly ILogger<ExportTargetEventHandler> _logger;
    private readonly ApplicationContext _applicationContext;
    private readonly IStorageConfigurationConstants _configurationConstants;
    private readonly IStorageFactory _storageFactory;
    private bool _disposedValue;

    public ExportTargetEventHandler(
        ILogger<ExportTargetEventHandler> logger,
        ApplicationContext applicationContext,
        IStorageConfigurationConstants constants,
        IStorageFactory jobDataFactory)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
        _configurationConstants = constants ?? throw new ArgumentNullException(nameof(constants));
        _storageFactory = jobDataFactory ?? throw new ArgumentNullException(nameof(jobDataFactory));

        _registerExportTargetSubscription = applicationContext.System.Events.Local.SubscribeAsync<RegisterExportTargetEvent>(ProcessEventAsync);
        _updateExportTargetSubscription = applicationContext.System.Events.Local.SubscribeAsync<UpdateExportTargetEvent>(ProcessEventAsync);
    }

    private async Task ProcessEventAsync<TEvent>(TEvent eventData)
        where TEvent : RemoteEvent
    {
        await UpdateOrCreateMirroredDatabaseAsync(eventData);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                _registerExportTargetSubscription.Dispose();
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

    private async Task UpdateOrCreateMirroredDatabaseAsync(RemoteEvent eventData)
    {
        if (!eventData.TryGetResourceInfo("AccountId", "Id", out var organizationId, out var providerDefinitionId))
        {
            _logger.LogDebug("Unable to find OrganizationId or ProviderDefinitionId using keys 'Id' and 'AccountId'.");
            return;
        }

        var executionContext = _applicationContext.CreateExecutionContext(organizationId);

        var providerDefinitionStore = executionContext.Organization.DataStores.GetDataStore<ProviderDefinition>();
        var providerDefinition = await providerDefinitionStore.GetByIdAsync(executionContext, providerDefinitionId);

        if (providerDefinition == null)
        {
            executionContext.Log.LogWarning("Unable to find provider definition with id '{ProviderDefinitionId}'.", providerDefinitionId);
            return;
        }

        if (providerDefinition.ProviderId != _configurationConstants.ProviderId)
        {
            executionContext.Log.LogDebug("Skipping creating of mirrored database for '{ProviderDefinitionId}' because ProviderId is not '{ProviderId}'.",
                providerDefinitionId,
                _configurationConstants.ProviderId);
            return;
        }

        var configuration = await _storageFactory.CreateStorageConfiguration(executionContext, providerDefinitionId) as OpenMirroringConnectorConfiguration;
        if (configuration == null)
        {
            throw new ApplicationException($"Failed to get job data for ProviderDefinitionId {providerDefinitionId}.");
        }

        var client = await _storageFactory.CreateStorageClient(executionContext, configuration) as OpenMirroringStorageClient;

        await client.UpdateOrCreateMirroredDatabaseAsync(providerDefinition.IsEnabled);
    }
}
