using System;
using System.Threading.Tasks;

using Azure.Identity;
using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;
using System.Text;
using System.Net.Http;
using System.Text.Json.Serialization;
using System.Text.Json;
using Microsoft.Fabric.Api;
using Microsoft.Fabric.Api.Core.Models;
using Microsoft.Fabric.Api.MirroredDatabase.Models;

namespace CluedIn.Connector.FabricOpenMirroring.Connector;

public class OpenMirroringClient : DataLakeClient
{
    private readonly ILogger<OpenMirroringClient> _logger;
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;

    private static readonly TimeSpan CreationTimeOut = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan DelayBetweenCreationPolls = TimeSpan.FromSeconds(5);
    private static readonly JsonSerializerOptions SerializerOptions = new JsonSerializerOptions()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters =
        {
            new JsonStringEnumConverter(),
        },
    };

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

    public virtual async Task UpdateOrCreateMirroredDatabaseAsync(IDataLakeJobData dataLakeJobData, bool isEnabled)
    {
        var jobData = CastJobData<OpenMirroringConnectorJobData>(dataLakeJobData);
        if (!jobData.ShouldCreateMirroredDatabase)
        {
            _logger.LogDebug("Skipping creation of mirrored database because {Setting} is disabled.", nameof(jobData.ShouldCreateMirroredDatabase));
            return;
        }

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
            await StartMirroringAsync(fabricClient, mirroredDatabase.WorkspaceId.Value, mirroredDatabase.Id.Value);
        }
        else
        {
            await StopMirroringAsync(fabricClient, mirroredDatabase.WorkspaceId.Value, mirroredDatabase.Id.Value);
        }
    }

    private async Task StopMirroringAsync(FabricClient fabricClient, Guid workspaceId, Guid mirroredDatabaseId)
    {
        _logger.LogDebug("Begin stop mirroring of Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseId, workspaceId);
        await fabricClient.MirroredDatabase.Mirroring.StopMirroringAsync(workspaceId, mirroredDatabaseId);

        _logger.LogDebug("End stop mirroring of Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseId, workspaceId);
    }

    private async Task StartMirroringAsync(FabricClient fabricClient, Guid workspaceId, Guid mirroredDatabaseId)
    {
        _logger.LogDebug("Begin start mirroring of Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseId, workspaceId);
        await fabricClient.MirroredDatabase.Mirroring.StartMirroringAsync(workspaceId, mirroredDatabaseId);

        _logger.LogDebug("End start mirroring of Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseId, workspaceId);
    }

    private async Task CreateMirroredDatabase(
        FabricClient fabricClient,
        OpenMirroringConnectorJobData jobData,
        Workspace workspace,
        string mirroredDatabaseName)
    {
        if (!jobData.ShouldCreateMirroredDatabase)
        {
            throw new ApplicationException($"Mirrored database is not found using workspace {jobData.WorkspaceName} and mirrored database name {jobData.MirroredDatabaseName}.");
        }
        _logger.LogDebug("Begin creating Mirrored Database {MirroredDatabaseName} in Workspace {WorkspaceId}.", mirroredDatabaseName, workspace.Id);
        var payload = Convert.ToBase64String(
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
        );

        var result = await fabricClient.MirroredDatabase.Items.CreateMirroredDatabaseAsync(
            workspace.Id,
            new CreateMirroredDatabaseRequest(mirroredDatabaseName,
            new MirroredDatabaseDefinition([
                new MirroredDatabaseDefinitionPart()
                {
                    Path = "mirroring.json",
                    Payload = payload,
                    PayloadType = PayloadType.InlineBase64,
                }
            ])));
        _logger.LogDebug("End creating Mirrored Database {MirroredDatabaseName} in Workspace {WorkspaceId}. Mirrored Database Id is '{MirroredDatabaseId}'.", mirroredDatabaseName, workspace.Id, result?.Value?.Id);

        if (result?.Value?.Id == null)
        {
            throw new ApplicationException($"Mirrored Database Id is not found for '{mirroredDatabaseName}' in workspace {workspace.Id}.");
        }

        _logger.LogDebug("Begin polling completion status for  Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseName, workspace.Id);
        var status = await PollForCompletionAsync(fabricClient, workspace.Id, result.Value.Id.Value);
        if (status != SqlEndpointProvisioningStatus.Success)
        {
            throw new ApplicationException($"Failed to provision sql endpoint using workspace {jobData.WorkspaceName} and mirrored database name {jobData.MirroredDatabaseName}.");
        }
        _logger.LogDebug("End polling completion status for  Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseName, workspace.Id);
    }

    private async Task<SqlEndpointProvisioningStatus?> PollForCompletionAsync(FabricClient fabricClient, Guid workspaceId, Guid mirroredDatabaseId)
    {
        var start = _dateTimeOffsetProvider.GetCurrentUtcTime();
        while (true)
        {
            var result = await fabricClient.MirroredDatabase.Items.GetMirroredDatabaseAsync(workspaceId, mirroredDatabaseId);
            var status = result?.Value?.Properties?.SqlEndpointProperties?.ProvisioningStatus;

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

    private async Task EnsureSuccess(string url, HttpResponseMessage response)
    {
        if (!response.IsSuccessStatusCode)
        {
            var responseContent = await response.Content.ReadAsStringAsync();
            _logger.LogError("Error making call to '{Url}'. Response was '{ResponseContent}'.", url, responseContent);
            response.EnsureSuccessStatusCode();
        }
    }

    private async Task<Workspace?> GetWorkspaceAsync(FabricClient fabricClient, string workspaceName)
    {
        _logger.LogDebug("Begin getting workspace from name {WorkspaceName}.", workspaceName);
        await foreach (var workspace in fabricClient.Core.Workspaces.ListWorkspacesAsync())
        {
            if (workspace.DisplayName.Equals(workspaceName, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogDebug("End getting workspace from name {WorkspaceName}. Workspace Id {WorkspaceId}.", workspaceName, workspace.Id);
                return workspace;
            }
        }

        _logger.LogDebug("Fail getting workspace from name {WorkspaceName}.", workspaceName);
        return null;
    }
}
