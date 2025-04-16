using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Azure.Identity;
using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;

using Microsoft.Fabric.Api.MirroredDatabase.Models;

using Microsoft.Fabric.Api;
using Microsoft.Extensions.Logging;
using Microsoft.Fabric.Api.Core.Models;
using System.Text;

namespace CluedIn.Connector.FabricOpenMirroring.Connector;

public class OpenMirroringClient : DataLakeClient, IOpenMirrorringDataLakeClient
{
    private readonly ILogger<OpenMirroringClient> _logger;
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;

    private static readonly TimeSpan CreationTimeOut = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan DelayBetweenCreationPolls = TimeSpan.FromSeconds(5);

    public OpenMirroringClient(
        ILogger<OpenMirroringClient> logger,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
    }

    protected override DataLakeServiceClient GetDataLakeServiceClient(IDataLakeJobData configuration)
    {
        var casted = CastJobData<OpenMirroringConnectorJobData>(configuration);
        var accountName = "onelake";

        var sharedKeyCredential = new ClientSecretCredential(casted.TenantId, casted.ClientId, casted.ClientSecret);

        var dfsUri = $"https://{accountName}.dfs.fabric.microsoft.com";

        var dataLakeServiceClient = new DataLakeServiceClient(
            new Uri(dfsUri),
            sharedKeyCredential);
        return dataLakeServiceClient;
    }

    protected override string GetDirectory(IDataLakeJobData configuration)
    {
        var casted = CastJobData<OpenMirroringConnectorJobData>(configuration);
        return  $"{casted.MirroredDatabaseName}.MountedRelationalDatabase/Files/LandingZone";
    }

    protected override string GetFileSystemName(IDataLakeJobData configuration)
    {
        var casted = CastJobData<OpenMirroringConnectorJobData>(configuration);
        return casted.WorkspaceName;
    }

    public virtual async Task<MirroredDatabase> UpdateOrCreateMirroredDatabaseAsync(IDataLakeJobData dataLakeJobData, bool isEnabled)
    {
        var jobData = CastJobData<OpenMirroringConnectorJobData>(dataLakeJobData);

        var sharedKeyCredential = new ClientSecretCredential(jobData.TenantId, jobData.ClientId, jobData.ClientSecret);

        var fabricClient = new FabricClient(sharedKeyCredential);

        var workspace = await GetWorkspaceAsync(fabricClient, jobData.WorkspaceName);
        if (workspace == null)
        {
            throw new ApplicationException($"Failed to find workspace using {jobData.WorkspaceName}.");
        }

        var mirrorDatabaseName = jobData.MirroredDatabaseName;
        var mirroredDatabase = await GetMirroredDatabaseAsync(fabricClient, workspace.Id, jobData.MirroredDatabaseName);
        if (mirroredDatabase == null)
        {
            await CreateMirroredDatabase(fabricClient, jobData, workspace, mirrorDatabaseName);
            mirroredDatabase = await GetMirroredDatabaseAsync(fabricClient, workspace.Id, jobData.MirroredDatabaseName);
        }

        if (isEnabled)
        {
            await fabricClient.MirroredDatabase.Mirroring.StartMirroringAsync(mirroredDatabase.WorkspaceId.Value, mirroredDatabase.Id.Value);
        }
        else
        {
            await fabricClient.MirroredDatabase.Mirroring.StopMirroringAsync(mirroredDatabase.WorkspaceId.Value, mirroredDatabase.Id.Value);
        }

        return mirroredDatabase;
    }

    private async Task CreateMirroredDatabase(
        FabricClient fabricClient,
        OpenMirroringConnectorJobData jobData,
        Workspace workspace,
        string mirrorDatabaseName)
    {
        if (!jobData.ShouldCreateMirroredDatabase)
        {
            throw new ApplicationException($"Mirrored database is not found using workspace {jobData.WorkspaceName} and mirrored database name {jobData.MirroredDatabaseName}.");
        }

        var result = await fabricClient.MirroredDatabase.Items.CreateMirroredDatabaseAsync(
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

        var status = await PollForCompletionAsync(fabricClient, workspace.Id, result.Value.Id.Value);
        if (status != SqlEndpointProvisioningStatus.Success)
        {
            throw new ApplicationException($"Failed to provision sql endpoint using workspace {jobData.WorkspaceName} and mirrored database name {jobData.MirroredDatabaseName}.");
        }
    }

    private async Task<SqlEndpointProvisioningStatus?> PollForCompletionAsync(FabricClient fabricClient, Guid workspaceId, Guid mirroredDatabaseId)
    {
        var start = _dateTimeOffsetProvider.GetCurrentUtcTime();
        while (true)
        {
            var result = await fabricClient.MirroredDatabase.Items.GetMirroredDatabaseAsync(workspaceId, mirroredDatabaseId);
            var status = result.Value?.Properties?.SqlEndpointProperties?.ProvisioningStatus;

            if (status != null && status != SqlEndpointProvisioningStatus.InProgress)
            {
                return status.Value;
            }
            var now = _dateTimeOffsetProvider.GetCurrentUtcTime();
            if (now - start > CreationTimeOut)
            {
                return null;
            }

            await Task.Delay(DelayBetweenCreationPolls);
        }

    }

    private async Task<MirroredDatabase?> GetMirroredDatabaseAsync(FabricClient fabricClient, Guid workspaceId, string mirroredDatabaseName)
    {
        await foreach (var mirroredDatabase in fabricClient.MirroredDatabase.Items.ListMirroredDatabasesAsync(workspaceId))
        {
            if (mirroredDatabase.DisplayName.Equals(mirroredDatabaseName, StringComparison.OrdinalIgnoreCase))
            {
                return mirroredDatabase;
            }
        }

        return null;
    }

    private async Task<Workspace?> GetWorkspaceAsync(FabricClient fabricClient, string workspaceName)
    {
        await foreach (var workspace in fabricClient.Core.Workspaces.ListWorkspacesAsync())
        {
            if (workspace.DisplayName.Equals(workspaceName, StringComparison.OrdinalIgnoreCase))
            {
                return workspace;
            }
        }

        return null;
    }
}
