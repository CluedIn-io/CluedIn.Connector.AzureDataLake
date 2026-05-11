using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Threading.Tasks;

using Azure.Core;
using Azure.Identity;

using CluedIn.Connector.DataLake.Common.Connector;
using Microsoft.Extensions.Logging;
using CluedIn.Core;

namespace CluedIn.Connector.OneLake.Connector;

internal class OneLakeStorageClient : DataLakeStorageClient
{
    private readonly OneLakeConnectorConfiguration _configuration;
    private readonly ApplicationContext _applicationContext;
    private readonly IDateTimeOffsetProvider _dateTimeOffsetProvider;
    public ILogger<OneLakeStorageClient> Logger { get; }

    public OneLakeStorageClient(
        ILogger<OneLakeStorageClient> logger,
        OneLakeConnectorConfiguration configuration,
        ApplicationContext applicationContext,
        IDateTimeOffsetProvider dateTimeOffsetProvider)
        : base(logger, configuration)
    {
        _applicationContext = applicationContext ?? throw new ArgumentNullException(nameof(applicationContext));
        _dateTimeOffsetProvider = dateTimeOffsetProvider ?? throw new ArgumentNullException(nameof(dateTimeOffsetProvider));
        Logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
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
            Logger.LogDebug("Using workspace level private link url {Url} for workspace {WorkspaceName}", url, _configuration.WorkspaceName);
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
            Logger.LogDebug("Using workspace level private link url {Url} for workspace {WorkspaceName}", url, _configuration.WorkspaceName);
            return url;
        }

        return "https://api.fabric.microsoft.com";
    }

    internal async Task<Guid?> GetWorkspaceIdAsync()
    {
        return await _applicationContext.System.Cache.GetItemAsync(
            $"OneLakeWorkspaceId_{_configuration.TenantId}_{_configuration.ClientId}_{_configuration.WorkspaceName}",
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

    internal async Task LoadToTableAsync(string sourceFileName, string targetTableName)
    {
        if (!_configuration.ShouldLoadToTable)
        {
            return;
        }

        var token = await GetToken();

        using var httpClient = new HttpClient();

        var workspace = await GetWorkspaceAsync(httpClient, token);
        if (workspace == null)
        {
            throw new ApplicationException($"Workspace {_configuration.WorkspaceName}is not found.");
        }

        var lakehouse = await GetLakehouseAsync(httpClient, token, workspace.Id);
        if (lakehouse == null)
        {
            throw new ApplicationException($"Lakehouse {_configuration.ItemName} is not found in workspace {workspace.Id}.");
        }

        var filePath = $"{_configuration.ItemFolder}/{sourceFileName}";
        await LoadTableAsync(httpClient, token, workspace.Id, lakehouse.Id.Value, targetTableName, filePath);
    }

    private async Task LoadTableAsync(HttpClient httpClient, string token, Guid workspaceId, Guid lakehouseId, string tableName, string filePath)
    {
        Logger.LogDebug("Begin loading data from file {File} to table {TableName}.", filePath, tableName);
        var request = new HttpRequestMessage();
        request.Method = HttpMethod.Post;
        request.RequestUri = new Uri($"{GetApiUrl(workspaceId)}/v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}/tables/{tableName}/load");
        request.Headers.Add("Authorization", $"Bearer {token}");
        request.Content = new StringContent($$"""
            {
                "pathType": "File",
                "relativePath": "{{filePath}}",
                "mode": "Overwrite"
            }
            """);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        var response = await httpClient.SendAsync(request);

        if (!response.IsSuccessStatusCode)
        {
            var responseContent = await response.Content.ReadAsStringAsync();
            Logger.LogError("Failed to load data from file {File} to table {TableName}. {Error}.", filePath, tableName, responseContent);
            response.EnsureSuccessStatusCode();
        }
        Logger.LogDebug("End loading data from file {File} to table {TableName}.", filePath, tableName);
    }

    private async Task<Lakehouse?> GetLakehouseAsync(HttpClient httpClient, string token, Guid workspaceId)
    {
        var lakehouseName = _configuration.ItemName;
        Logger.LogDebug("Begin getting lakehouse from name {LakehouseName}.", lakehouseName);
        await foreach (var lakehouse in ListLakehousesAsync(workspaceId))
        {
            if (lakehouse.DisplayName.Equals(lakehouseName, StringComparison.OrdinalIgnoreCase))
            {
                Logger.LogDebug("End getting lakehouse from name {LakehouseName}. Lakehouse Id {LakehouseId}.", lakehouseName, lakehouse.Id);
                return lakehouse;
            }
        }

        Logger.LogDebug("Fail getting lakehouse from name {LakehouseName}.", lakehouseName);
        return null;

        async IAsyncEnumerable<Lakehouse> ListLakehousesAsync(Guid workspaceId)
        {
            var url = $"{GetApiUrl(workspaceId)}/v1/workspaces/{workspaceId}/lakehouses";
            do
            {
                var request = new HttpRequestMessage();
                request.Method = HttpMethod.Get;
                request.RequestUri = new Uri(url);
                request.Headers.Add("Authorization", $"Bearer {token}");
                var response = await httpClient.SendAsync(request);
                var content = await response.Content.ReadFromJsonAsync<GetLakehouseResponse>();

                foreach (var lakehouse in content.Value)
                {
                    yield return lakehouse;
                }

                url = content.ContinuationUri;
            }
            while (!string.IsNullOrWhiteSpace(url));
        }
    }

    private async Task<Workspace?> GetWorkspaceAsync(HttpClient httpClient, string token)
    {
        var workspaceName = _configuration.WorkspaceName;
        Logger.LogDebug("Begin getting workspace from name {WorkspaceName}.", workspaceName);
        await foreach (var workspace in ListWorkspacesAsync())
        {
            if (workspace.DisplayName.Equals(workspaceName, StringComparison.OrdinalIgnoreCase))
            {
                Logger.LogDebug("End getting workspace from name {WorkspaceName}. Workspace Id {WorkspaceId}.", workspaceName, workspace.Id);
                return workspace;
            }
        }

        Logger.LogDebug("Fail getting workspace from name {WorkspaceName}.", workspaceName);
        return null;

        async IAsyncEnumerable<Workspace> ListWorkspacesAsync()
        {
            // Use the public URL because we do not have the workspace id yet
            var url = "https://api.fabric.microsoft.com/v1/workspaces";
            do
            {
                var request = new HttpRequestMessage();
                request.Method = HttpMethod.Get;
                request.RequestUri = new Uri(url);
                request.Headers.Add("Authorization", $"Bearer {token}");
                var response = await httpClient.SendAsync(request);
                var content = await response.Content.ReadFromJsonAsync<GetWorkspaceResponse>();

                foreach (var workspace in content.Value)
                {
                    yield return workspace;
                }

                url = content.ContinuationUri;
            }
            while (!string.IsNullOrWhiteSpace(url));
        }
    }

    private record Lakehouse(Guid? Id, string DisplayName, Guid WorkspaceId);
    private record Workspace(Guid Id, string DisplayName, string Description, string Type, Guid CapacityId);
    private record GetWorkspaceResponse(List<Workspace> Value, string ContinuationToken, string ContinuationUri);
    private record GetLakehouseResponse(List<Lakehouse> Value, string ContinuationToken, string ContinuationUri);
}
