using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Azure.Identity;
using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Core;

using Microsoft.Extensions.Logging;
using System.Text;
using System.Net.Http;
using System.Net.Http.Json;
using Azure.Core;
using System.Net.Http.Headers;
using System.Text.Json.Serialization;
using System.Text.Json;

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

    public virtual async Task UpdateOrCreateMirroredDatabaseAsync(IDataLakeJobData dataLakeJobData, bool isEnabled)
    {
        var jobData = CastJobData<OpenMirroringConnectorJobData>(dataLakeJobData);
        if (!jobData.ShouldCreateMirroredDatabase)
        {
            _logger.LogDebug("Skipping creation of mirrored database because {Setting} is disabled.", nameof(jobData.ShouldCreateMirroredDatabase));
            return;
        }

        var sharedKeyCredential = new ClientSecretCredential(jobData.TenantId, jobData.ClientId, jobData.ClientSecret);
        var tokenResult = await sharedKeyCredential.GetTokenAsync(
        new TokenRequestContext(new string[]
        {
            "https://api.fabric.microsoft.com/.default"
        }));
        var token = tokenResult.Token;

        var httpClient = new HttpClient();

        var workspace = await GetWorkspaceAsync(httpClient, token, jobData.WorkspaceName);
        if (workspace == null)
        {
            throw new ApplicationException($"Failed to find workspace using {jobData.WorkspaceName}.");
        }

        var mirrorDatabaseName = jobData.MirroredDatabaseName;
        var mirroredDatabase = await GetMirroredDatabaseAsync(httpClient, token, workspace.Id, jobData.MirroredDatabaseName);
        if (mirroredDatabase == null)
        {
            await CreateMirroredDatabase(httpClient, token, jobData, workspace, mirrorDatabaseName);
            mirroredDatabase = await GetMirroredDatabaseAsync(httpClient, token, workspace.Id, jobData.MirroredDatabaseName);
        }

        if (isEnabled)
        {
            await StartMirroringAsync(httpClient, token, mirroredDatabase.WorkspaceId.Value, mirroredDatabase.Id.Value);
        }
        else
        {
            await StopMirroringAsync(httpClient, token, mirroredDatabase.WorkspaceId.Value, mirroredDatabase.Id.Value);
        }
    }

    private async Task StopMirroringAsync(HttpClient httpClient, string token, Guid workspaceId, Guid mirroredDatabaseId)
    {
        _logger.LogDebug("Begin stop mirroring of Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseId, workspaceId);
        var url = $"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/mirroredDatabases/{mirroredDatabaseId}/startMirroring";
        var request = new HttpRequestMessage();
        request.Method = HttpMethod.Post;
        request.RequestUri = new Uri(url);
        request.Headers.Add("Authorization", $"Bearer {token}");
        var response = await httpClient.SendAsync(request);

        await EnsureSuccess(url, response);

        _logger.LogDebug("End stop mirroring of Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseId, workspaceId);
    }

    private async Task StartMirroringAsync(HttpClient httpClient, string token, Guid workspaceId, Guid mirroredDatabaseId)
    {
        _logger.LogDebug("Begin start mirroring of Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseId, workspaceId);
        var url = $"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/mirroredDatabases/{mirroredDatabaseId}/startMirroring";
        var request = new HttpRequestMessage();
        request.Method = HttpMethod.Post;
        request.RequestUri = new Uri(url);
        request.Headers.Add("Authorization", $"Bearer {token}");
        var response = await httpClient.SendAsync(request);

        await EnsureSuccess(url, response);

        _logger.LogDebug("End start mirroring of Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseId, workspaceId);
    }

    private async Task CreateMirroredDatabase(
        HttpClient httpClient,
        string token,
        OpenMirroringConnectorJobData jobData,
        Workspace workspace,
        string mirroredDatabaseName)
    {
        if (!jobData.ShouldCreateMirroredDatabase)
        {
            throw new ApplicationException($"Mirrored database is not found using workspace {jobData.WorkspaceName} and mirrored database name {jobData.MirroredDatabaseName}.");
        }
        _logger.LogDebug("Begin creating Mirrored Database {MirroredDatabaseName} in Workspace {WorkspaceId}.", mirroredDatabaseName, workspace.Id);
        var url = $"https://api.fabric.microsoft.com/v1/workspaces/{workspace.Id}/mirroredDatabases";
        var request = new HttpRequestMessage();
        request.Method = HttpMethod.Post;
        request.RequestUri = new Uri(url);
        request.Headers.Add("Authorization", $"Bearer {token}");
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
        request.Content = new StringContent($$"""
            {
                "displayName": "{{mirroredDatabaseName}}",
                "definition": {
                    "parts": [
                      {
                          "path": "mirroring.json",
                          "payload": "{{payload}}",
                          "payloadType": "InlineBase64"
                      }
                    ]
                }
            }
            """);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        var response = await httpClient.SendAsync(request);

        await EnsureSuccess(url, response);
        var result = await response.Content.ReadFromJsonAsync<MirroredDatabase>(options: SerializerOptions);
        _logger.LogDebug("End creating Mirrored Database {MirroredDatabaseName} in Workspace {WorkspaceId}. Mirrored Database Id is '{MirroredDatabaseId}'.", mirroredDatabaseName, workspace.Id, result?.Id);

        if (result?.Id == null)
        {
            throw new ApplicationException($"Mirrored Database Id is not found for '{mirroredDatabaseName}' in workspace {workspace.Id}.");
        }

        _logger.LogDebug("Begin polling completion status for  Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseName, workspace.Id);
        var status = await PollForCompletionAsync(httpClient, token, workspace.Id, result.Id.Value);
        if (status != SqlEndpointProvisioningStatus.Success)
        {
            throw new ApplicationException($"Failed to provision sql endpoint using workspace {jobData.WorkspaceName} and mirrored database name {jobData.MirroredDatabaseName}.");
        }
        _logger.LogDebug("End polling completion status for  Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseName, workspace.Id);
    }

    private async Task<SqlEndpointProvisioningStatus?> PollForCompletionAsync(HttpClient httpClient, string token, Guid workspaceId, Guid mirroredDatabaseId)
    {
        var start = _dateTimeOffsetProvider.GetCurrentUtcTime();
        while (true)
        {
            var result = await GetMirroredDatabaseAsync(httpClient, token, workspaceId, mirroredDatabaseId);
            var status = result?.Properties?.SqlEndpointProperties?.ProvisioningStatus;

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

    private async Task<MirroredDatabase> GetMirroredDatabaseAsync(HttpClient httpClient, string token, Guid workspaceId, Guid mirroredDatabaseId)
    {
        var url = $"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/mirroredDatabases/{mirroredDatabaseId}";
        var request = new HttpRequestMessage();
        request.Method = HttpMethod.Get;
        request.RequestUri = new Uri(url);
        request.Headers.Add("Authorization", $"Bearer {token}");
        var response = await httpClient.SendAsync(request);
        await EnsureSuccess(url, response);
        var content = await response.Content.ReadFromJsonAsync<MirroredDatabase>(options: SerializerOptions);
        return content;
    }

    private async Task<MirroredDatabase?> GetMirroredDatabaseAsync(HttpClient httpClient, string token, Guid workspaceId, string mirroredDatabaseName)
    {
        await foreach (var mirroredDatabase in ListMirroredDatabasesAsync())
        {
            if (mirroredDatabase.DisplayName.Equals(mirroredDatabaseName, StringComparison.OrdinalIgnoreCase))
            {
                return mirroredDatabase;
            }
        }

        return null;
        async IAsyncEnumerable<MirroredDatabase> ListMirroredDatabasesAsync()
        {
            var url = $"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/mirroredDatabases";
            do
            {
                var request = new HttpRequestMessage();
                request.Method = HttpMethod.Get;
                request.RequestUri = new Uri(url);
                request.Headers.Add("Authorization", $"Bearer {token}");
                var response = await httpClient.SendAsync(request);
                await EnsureSuccess(url, response);
                var content = await response.Content.ReadFromJsonAsync<ListMirroredDatabasesResponse>(options: SerializerOptions);

                foreach (var mirroredDatabase in content.Value)
                {
                    yield return mirroredDatabase;
                }

                url = content.ContinuationUri;
            }
            while (!string.IsNullOrWhiteSpace(url));
        }
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

    private async Task<Workspace?> GetWorkspaceAsync(HttpClient httpClient, string token, string workspaceName)
    {
        _logger.LogDebug("Begin getting workspace from name {WorkspaceName}.", workspaceName);
        await foreach (var workspace in ListWorkspacesAsync())
        {
            if (workspace.DisplayName.Equals(workspaceName, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogDebug("End getting workspace from name {WorkspaceName}. Workspace Id {WorkspaceId}.", workspaceName, workspace.Id);
                return workspace;
            }
        }

        _logger.LogDebug("Fail getting workspace from name {WorkspaceName}.", workspaceName);
        return null;

        async IAsyncEnumerable<Workspace> ListWorkspacesAsync()
        {
            var url = "https://api.fabric.microsoft.com/v1/workspaces";
            do
            {
                var request = new HttpRequestMessage();
                request.Method = HttpMethod.Get;
                request.RequestUri = new Uri(url);
                request.Headers.Add("Authorization", $"Bearer {token}");
                var response = await httpClient.SendAsync(request);
                await EnsureSuccess(url, response);
                var content = await response.Content.ReadFromJsonAsync<ListWorkspacesResponse>();

                foreach (var workspace in content.Value)
                {
                    yield return workspace;
                }

                url = content.ContinuationUri;
            }
            while (!string.IsNullOrWhiteSpace(url));
        }
    }

    private record Workspace(Guid Id, string DisplayName, string Description, string Type, Guid CapacityId);
    private enum SqlEndpointProvisioningStatus
    {
        Failed,
        InProgress,
        Success,
    }
    private record SqlEndpointProperties(string ConnectionString, Guid? Id, SqlEndpointProvisioningStatus? ProvisioningStatus);
    private record MirroredDatabaseProperties(string OneLakeTablesPath, SqlEndpointProperties SqlEndpointProperties);
    private record MirroredDatabase(Guid? Id, string DisplayName, Guid? WorkspaceId, MirroredDatabaseProperties? Properties);
    private record ListMirroredDatabasesResponse(List<MirroredDatabase> Value, string ContinuationToken, string ContinuationUri);
    private record ListWorkspacesResponse(List<Workspace> Value, string ContinuationToken, string ContinuationUri);
}
