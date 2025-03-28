using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using Azure.Identity;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.EventHandlers;
using CluedIn.Connector.FabricMirroring.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Events;
using CluedIn.Core.Events.Types;

using Microsoft.Fabric.Api;
using Microsoft.Fabric.Api.Core.Models;
using Microsoft.Fabric.Api.MirroredDatabase.Models;

namespace CluedIn.Connector.FabricMirroring.EventHandlers;

internal abstract class UpdateMirroredDatabaseBase
{
    private readonly ApplicationContext _applicationContext;
    private readonly IDataLakeConstants _constants;
    private readonly IDataLakeJobDataFactory _jobDataFactory;
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;

    protected UpdateMirroredDatabaseBase(
        ApplicationContext applicationContext,
        IDataLakeConstants constants,
        IDataLakeJobDataFactory jobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
    {
        _applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
        _constants = constants ?? throw new ArgumentNullException(nameof(constants));
        _jobDataFactory = jobDataFactory ?? throw new ArgumentNullException(nameof(jobDataFactory));
        _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
    }

    private FabricClient FabricClient { get; set; }


    protected virtual async Task<MirroredDatabase> UpdateOrCreateMirroredDatabaseAsync(RemoteEvent eventData)
    {
        if (!eventData.TryGetResourceInfo("AccountId", "Id", out var organizationId, out var providerDefinitionId))
        {
            return null;
        }

        var executionContext = _applicationContext.CreateExecutionContext(organizationId);

        var jobData = await _jobDataFactory.GetConfiguration(executionContext, providerDefinitionId, string.Empty) as FabricMirroringConnectorJobData;
        if (jobData == null)
        {
            throw new ApplicationException($"Failed to get job data for ProviderDefinitionId {providerDefinitionId}.");
        }
        var sharedKeyCredential = new ClientSecretCredential(jobData.TenantId, jobData.ClientId, jobData.ClientSecret);

        FabricClient = new FabricClient(sharedKeyCredential);

        var workspace = await GetWorkspaceAsync(jobData.WorkspaceName);
        if (workspace == null)
        {
            throw new ApplicationException($"Failed to find workspace using {jobData.WorkspaceName}.");
        }

        var mirrorDatabaseName = jobData.MirroredDatabaseName;
        var mirroredDatabase = await GetMirroredDatabaseAsync(workspace.Id, jobData.MirroredDatabaseName);
        if (mirroredDatabase == null)
        {
            if (!jobData.ShouldCreateMirroredDatabase)
            {
                throw new ApplicationException($"Mirrored database is not found using workspace {jobData.WorkspaceName} and mirrored database name {jobData.MirroredDatabaseName}.");
            }
            var result = await FabricClient.MirroredDatabase.Items.CreateMirroredDatabaseAsync(
                workspace.Id,
                new CreateMirroredDatabaseRequest(
                    mirrorDatabaseName,
                    new MirroredDatabaseDefinition(
                        new List<MirroredDatabaseDefinitionPart>
                        {
                            new MirroredDatabaseDefinitionPart
                            {
                                Path = "mirroring.json",
                                Payload = Convert.ToBase64String(
                                    Encoding.UTF8.GetBytes($$"""
                                    {
                                        "properties": {
                                            "source": {
                                                "type": "GenericMirror",
                                                "typeProperties": {}
                                            },
                                            "target": {
                                                "type": "MountedRelationalDatabase",
                                                "typeProperties": {
                                                    "format": "Delta"
                                                }
                                            }
                                        }
                                    }
                                    """)
                                ),
                                PayloadType = PayloadType.InlineBase64,
                            }
                        }
                    )
                )
            );

            var status = await PollForCompletion(workspace.Id, result.Value.Id.Value);
            if (status != SqlEndpointProvisioningStatus.Success)
            {
                throw new ApplicationException($"Failed to provision sql endpoint using workspace {jobData.WorkspaceName} and mirrored database name {jobData.MirroredDatabaseName}.");
            }

        }

        mirroredDatabase = await GetMirroredDatabaseAsync(workspace.Id, jobData.MirroredDatabaseName);

        var store = executionContext.Organization.DataStores.GetDataStore<ProviderDefinition>();
        var definition = await store.GetByIdAsync(executionContext, providerDefinitionId);
        if (definition.IsEnabled)
        {
            await StartMirroringAsync(mirroredDatabase);
        }
        else
        {
            await StopMirroringAsync(mirroredDatabase);
        }

        return mirroredDatabase;
    }

    private async Task StartMirroringAsync(MirroredDatabase mirroredDatabase)
    {
        await FabricClient.MirroredDatabase.Mirroring.StartMirroringAsync(mirroredDatabase.WorkspaceId.Value, mirroredDatabase.Id.Value);
    }
    private async Task StopMirroringAsync(MirroredDatabase mirroredDatabase)
    {
        await FabricClient.MirroredDatabase.Mirroring.StopMirroringAsync(mirroredDatabase.WorkspaceId.Value, mirroredDatabase.Id.Value);
    }

    protected async Task<FabricMirroringConnectorJobData> GetJobData(RemoteEvent eventData)
    {
        if (!eventData.TryGetResourceInfo("AccountId", "Id", out var organizationId, out var providerDefinitionId))
        {
            return null;
        }
        var executionContext = _applicationContext.CreateExecutionContext(organizationId);

        return await _jobDataFactory.GetConfiguration(executionContext, providerDefinitionId, string.Empty) as FabricMirroringConnectorJobData;
    }

    protected async Task<SqlEndpointProvisioningStatus?> PollForCompletion(Guid workspaceId, Guid mirroredDatabaseId)
    {
        var start = _dateTimeOffsetProvider.GetCurrentUtcTime();
        while (true)
        {
            var result = await FabricClient.MirroredDatabase.Items.GetMirroredDatabaseAsync(workspaceId, mirroredDatabaseId);
            var status = result.Value?.Properties?.SqlEndpointProperties?.ProvisioningStatus;

            if (status != null && status != SqlEndpointProvisioningStatus.InProgress)
            {
                return status.Value;
            }
            var now = _dateTimeOffsetProvider.GetCurrentUtcTime();
            if (now - start > TimeSpan.FromMinutes(10))
            {
                return null;
            }

            await Task.Delay(5000);
        }

    }

    protected async Task<MirroredDatabase?> GetMirroredDatabaseAsync(Guid workspaceId, string mirroredDatabaseName)
    {
        await foreach (var mirroredDatabase in FabricClient.MirroredDatabase.Items.ListMirroredDatabasesAsync(workspaceId))
        {
            if (mirroredDatabase.DisplayName == mirroredDatabaseName)
            {
                return mirroredDatabase;
            }
        }

        return null;
    }

    protected async Task StartMirroringAsync(Guid workspaceId, Guid mirroredDatabaseId)
    {
        await FabricClient.MirroredDatabase.Mirroring.StartMirroringAsync(workspaceId, mirroredDatabaseId);
    }

    protected async Task<Workspace?> GetWorkspaceAsync(string workspaceNamke)
    {
        await foreach (var workspace in FabricClient.Core.Workspaces.ListWorkspacesAsync())
        {
            if (workspace.DisplayName == workspaceNamke)
            {
                return workspace;
            }
        }

        return null;
    }
}
internal class ExportTargetEventHandler : UpdateMirroredDatabaseBase, IDisposable
{
    private readonly IDisposable _registerExportTargetSubscription;
    private readonly IDisposable _updateExportTargetSubscription;
    private bool _disposedValue;

    public ExportTargetEventHandler(
        ApplicationContext applicationContext,
        FabricMirroringClient client,
        IDataLakeConstants constants,
        IDataLakeJobDataFactory jobDataFactory,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(applicationContext, constants, jobDataFactory, dateTimeOffsetProvider)
    {
        _registerExportTargetSubscription = applicationContext.System.Events.Local.Subscribe<RegisterExportTargetEvent>(ProcessEvent);
        _updateExportTargetSubscription = applicationContext.System.Events.Local.Subscribe<UpdateExportTargetEvent>(ProcessEvent);
    }

    private void ProcessEvent<TEvent>(TEvent eventData)
        where TEvent : RemoteEvent
    {
        ProcessEventAsync(eventData).GetAwaiter().GetResult();
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
}
