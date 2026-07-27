using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using System.Transactions;

using Azure.Core;
using Azure.Identity;

using CluedIn.Connector.DataLake.Common.Connector;
using CluedIn.Connector.FileStorage.Common.Connector;
using CluedIn.Core;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.FabricOpenMirroring.Connector;

internal class OpenMirroringStorageClient : DataLakeStorageClient
{
    private const int CreateMirroredDatabaseLockTimeoutInMilliseconds = 100;
    private readonly ApplicationContext _applicationContext;
    private readonly ILogger<OpenMirroringStorageClient> _logger;
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;
    private readonly OpenMirroringConnectorConfiguration _configuration;
private static readonly TimeSpan TotalCreationTimeOut = CreationPollTimeOut.Add(TimeSpan.FromMinutes(5)); // Buffer for create/list + start/stop mirroring
    private static readonly JsonSerializerOptions SerializerOptions = new JsonSerializerOptions()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters =
        {
            new JsonStringEnumConverter(),
        },
    };

    public OpenMirroringStorageClient(
        ILogger<OpenMirroringStorageClient> logger,
        OpenMirroringConnectorConfiguration configuration,
        ApplicationContext applicationContext,
        IDateTimeOffsetProvider dateTimeOffsetProvider):
        base(logger, configuration)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
        _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    public async Task<bool> HasValidWorkspaceAsync()
    {
        var fileSystemClient = await GetFileSystemClientAsync(createIfNotExists: false);
        return fileSystemClient != null;
    }

    protected override async Task<string> GetStorageUrlAsync()
    {
        if (_configuration.UseWorkspaceLevelPrivateLink)
        {
            var workspaceId = await GetWorkspaceIdAsync();
            if (workspaceId == null)
            {
                throw new InvalidOperationException($"Failed to obtain workspace id from workspace name {_configuration.WorkspaceName}");
            }

            var url = $"https://{GetWorkspaceSpecificPrefix(workspaceId.Value)}.dfs.fabric.microsoft.com";
            _logger.LogDebug("Using workspace level private link url {Url} for workspace {WorkspaceName}", url, _configuration.WorkspaceName);
            return url;
        }

        var accountName = "onelake";
        return $"https://{accountName}.dfs.fabric.microsoft.com";
    }

    private string GetWorkspaceSpecificPrefix(Guid workspaceId)
    {
        var workspaceIdString = workspaceId.ToString("N");
        return $"{workspaceIdString}.z{workspaceIdString[..2]}";
    }

    private string GetApiUrl(Guid workspaceId)
    {
        if (_configuration.UseWorkspaceLevelPrivateLink)
        {
            var url = $"https://{GetWorkspaceSpecificPrefix(workspaceId)}.w.api.fabric.microsoft.com";
            _logger.LogDebug("Using workspace level private link url {Url} for workspace {WorkspaceName}", url, _configuration.WorkspaceName);
            return url;
        }

        return "https://api.fabric.microsoft.com";
    }

    internal async Task<Guid?> GetWorkspaceIdAsync()
    {
        return await _applicationContext.System.Cache.GetItemAsync(
            $"OpenMirroringWorkspaceId_{_configuration.TenantId}_{_configuration.ClientId}_{_configuration.WorkspaceName}",
            GetWorkspaceIdFromServiceAsync,
            cachePolicy: policy => policy.WithAbsoluteExpiration(_dateTimeOffsetProvider.GetCurrentUtcTime().AddSeconds(30))
        );

        async Task<Guid?> GetWorkspaceIdFromServiceAsync()
        {
            var token = await GetToken();
            using var httpClient = new HttpClient();
            var workspace = await GetWorkspaceAsync(httpClient, token);
            return workspace?.Id;
        }

    }

    private async Task<string> GetToken()
    {
        var sharedKeyCredential = new ClientSecretCredential(_configuration.TenantId, _configuration.ClientId, _configuration.ClientSecret);
        var tokenResult = await sharedKeyCredential.GetTokenAsync(
            new TokenRequestContext(
            [
                "https://api.fabric.microsoft.com/.default"
            ]));
        var token = tokenResult.Token;
        return token;
    }

    public virtual async Task<UpdateOrCreateMirroredDatabaseResult> UpdateOrCreateMirroredDatabaseAsync(Guid providerDefinitionId, bool isEnabled)
    {
        if (!_configuration.ShouldCreateMirroredDatabase)
        {
            _logger.LogDebug("Skipping creation of mirrored database '{MirroredDatabaseName}' because {Setting} is disabled.", _configuration.MirroredDatabaseName, nameof(_configuration.ShouldCreateMirroredDatabase));
            return new UpdateOrCreateMirroredDatabaseResult(IsSkipped: true, IsCreated: false, IsEnabled: null);

        }

        using var transactionScope = new TransactionScope(
                TransactionScopeOption.Required,
                TotalCreationTimeOut,
                TransactionScopeAsyncFlowOption.Enabled);
        await using var connection = new SqlConnection(_configuration.StreamCacheConnectionString);
        await connection.OpenAsync();


        if (!await DistributedLockHelper.TryAcquireExclusiveLock(connection, $"CreateMirroredDatabase_{providerDefinitionId}", CreateMirroredDatabaseLockTimeoutInMilliseconds))
        {
            _logger.LogInformation("Unable to acquire lock to create mirrored database for ProviderDefinition '{ProviderDefinitionId}'. Skipping creation.", providerDefinitionId);
            return new UpdateOrCreateMirroredDatabaseResult(IsSkipped: true, IsCreated: false, IsEnabled: null);
        }

        _logger.LogInformation("Begin create mirrored database '{MirroredDatabaseName}' for ProviderDefinition '{ProviderDefinitionId}'.", _configuration.MirroredDatabaseName, providerDefinitionId);

        var token = await GetToken();

        using var httpClient = new HttpClient();

        var workspace = await GetWorkspaceAsync(httpClient, token);
        if (workspace == null)
        {
            throw new ApplicationException($"Failed to find workspace using {_configuration.WorkspaceName}.");
        }

        try
        {
            await CreateAndSetState();
            _logger.LogInformation("End create mirrored database '{MirroredDatabaseName}' for ProviderDefinition '{ProviderDefinitionId}'.", _configuration.MirroredDatabaseName, providerDefinitionId);
            return new UpdateOrCreateMirroredDatabaseResult(IsSkipped: false, IsCreated: true, IsEnabled: isEnabled);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to create or set state for mirrored database '{MirroredDatabaseName}'.", _configuration.MirroredDatabaseName);
            throw;
        }

        async Task CreateAndSetState()
        {
            var mirroredDatabase = await GetMirroredDatabaseAsync(httpClient, token, workspace.Id);
            if (mirroredDatabase == null)
            {
                await CreateMirroredDatabase(httpClient, token, workspace);
                mirroredDatabase = await GetMirroredDatabaseAsync(httpClient, token, workspace.Id);
                if (mirroredDatabase == null)
                {
                    throw new ApplicationException($"Mirrored database '{_configuration.MirroredDatabaseName}' was not found after creation in workspace '{workspace.Id}'.");
                }
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
    }

    private async Task StopMirroringAsync(HttpClient httpClient, string token, Guid workspaceId, Guid mirroredDatabaseId)
    {
        _logger.LogDebug("Begin stop mirroring of Mirrored Database {MirroredDatabaseId} in Workspace {WorkspaceId}.", mirroredDatabaseId, workspaceId);
        var url = $"{GetApiUrl(workspaceId)}/v1/workspaces/{workspaceId}/mirroredDatabases/{mirroredDatabaseId}/stopMirroring";
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
        var url = $"{GetApiUrl(workspaceId)}/v1/workspaces/{workspaceId}/mirroredDatabases/{mirroredDatabaseId}/startMirroring";
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
        Workspace workspace)
    {
        if (!_configuration.ShouldCreateMirroredDatabase)
        {
            throw new ApplicationException($"Mirrored database is not found using workspace {_configuration.WorkspaceName} and mirrored database name {_configuration.MirroredDatabaseName}.");
        }

        var mirroredDatabaseName = _configuration.MirroredDatabaseName;
        _logger.LogDebug("Begin creating Mirrored Database {MirroredDatabaseName} in Workspace {WorkspaceId}.", mirroredDatabaseName, workspace.Id);
        var url = $"{GetApiUrl(workspace.Id)}/v1/workspaces/{workspace.Id}/mirroredDatabases";
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
            throw new ApplicationException($"Failed to provision sql endpoint using workspace {_configuration.WorkspaceName} and mirrored database name {_configuration.MirroredDatabaseName}.");
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
            if (now - start > CreationPollTimeOut)
            {
                return null;
            }

            await Task.Delay(DelayBetweenCreationPolls);
        }

    }

    private async Task<MirroredDatabase> GetMirroredDatabaseAsync(HttpClient httpClient, string token, Guid workspaceId, Guid mirroredDatabaseId)
    {
        var url = $"{GetApiUrl(workspaceId)}/v1/workspaces/{workspaceId}/mirroredDatabases/{mirroredDatabaseId}";
        var request = new HttpRequestMessage();
        request.Method = HttpMethod.Get;
        request.RequestUri = new Uri(url);
        request.Headers.Add("Authorization", $"Bearer {token}");
        var response = await httpClient.SendAsync(request);
        await EnsureSuccess(url, response);
        var content = await response.Content.ReadFromJsonAsync<MirroredDatabase>(options: SerializerOptions);
        return content;
    }

    private async Task<MirroredDatabase?> GetMirroredDatabaseAsync(HttpClient httpClient, string token, Guid workspaceId)
    {
        var mirroredDatabaseName = _configuration.MirroredDatabaseName;
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
            var url = $"{GetApiUrl(workspaceId)}/v1/workspaces/{workspaceId}/mirroredDatabases";
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

    private async Task<Workspace?> GetWorkspaceAsync(HttpClient httpClient, string token)
    {
        var workspaceName = _configuration.WorkspaceName;
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

internal record UpdateOrCreateMirroredDatabaseResult(bool IsSkipped, bool IsCreated, bool? IsEnabled);
