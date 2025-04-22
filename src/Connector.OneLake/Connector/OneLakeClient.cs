using System;
using System.IO;
using System.Threading.Tasks;

using Azure.Identity;
using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;


using Azure.Core;
using System.Net.Http;
using System.Net.Http.Json;
using System.Collections.Generic;
using System.Security.Policy;
using System.Net.Http.Headers;
using Castle.Core.Logging;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.OneLake.Connector;

public class OneLakeClient : DataLakeClient
{
    public ILogger<OneLakeClient> Logger { get; }

    public OneLakeClient(ILogger<OneLakeClient> logger)
    {
        Logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override DataLakeServiceClient GetDataLakeServiceClient(IDataLakeJobData configuration)
    {
        var casted = CastJobData<OneLakeConnectorJobData>(configuration);
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
        var casted = CastJobData<OneLakeConnectorJobData>(configuration);
        return  $"{casted.ItemName}.{casted.ItemType}/{casted.ItemFolder}/"; //"jlalakehouse.Lakehouse/Files/";
    }

    protected override string GetFileSystemName(IDataLakeJobData configuration)
    {
        var casted = CastJobData<OneLakeConnectorJobData>(configuration);
        return casted.WorkspaceName;
    }

    internal async Task LoadToTableAsync(string sourceFileName, string targetTableName, IDataLakeJobData configuration)
    {
        var casted = CastJobData<OneLakeConnectorJobData>(configuration);
        if (!casted.ShouldLoadToTable)
        {
            return;
        }

        var sharedKeyCredential = new ClientSecretCredential(casted.TenantId, casted.ClientId, casted.ClientSecret);
        var tokenResult = await sharedKeyCredential.GetTokenAsync(
        new TokenRequestContext(new string[]
        {
            "https://api.fabric.microsoft.com/.default"
        }));
        var token = tokenResult.Token;

        var httpClient = new HttpClient();

        var workspace = await GetWorkspaceAsync(httpClient, token, casted.WorkspaceName);
        if (workspace == null)
        {
            throw new ApplicationException($"Workspace {casted.WorkspaceName}is not found.");
        }

        var lakehouse = await GetLakehouseAsync(httpClient, token, workspace.Id, casted.ItemName);
        if (lakehouse == null)
        {
            throw new ApplicationException($"Lakehouse {casted.ItemName} is not found in workspace {workspace.Id}.");
        }

        var filePath = $"{casted.ItemFolder}/{sourceFileName}";
        await LoadTableAsync(httpClient, token, workspace.Id, lakehouse.Id.Value, targetTableName, filePath);
    }

    private async Task LoadTableAsync(HttpClient httpClient, string token, Guid workspaceId, Guid lakehouseId, string tableName, string filePath)
    {
        Logger.LogError("Begin loading data from file {File} to table {TableName}.", filePath, tableName);
        var request = new HttpRequestMessage();
        request.Method = HttpMethod.Post;
        request.RequestUri = new Uri($"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}/tables/{tableName}/load");
        request.Headers.Add("Authorization", $"Bearer {token}");
        request.Content = new StringContent($$"""
            {
                "pathType": "File",
                "relativePath": "{{filePath}}",
                "fileExtension": "{{Path.GetExtension(filePath)[1..]}}",
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
        Logger.LogError("End loading data from file {File} to table {TableName}.", filePath, tableName);
    }

    private async Task<Lakehouse?> GetLakehouseAsync(HttpClient httpClient, string token, Guid workspaceId, string lakehouseName)
    {
        await foreach (var lakehouse in ListLakehousesAsync(workspaceId))
        {
            if (lakehouse.DisplayName.Equals(lakehouseName, StringComparison.OrdinalIgnoreCase))
            {
                return lakehouse;
            }
        }

        return null;

        async IAsyncEnumerable<Lakehouse> ListLakehousesAsync(Guid workspaceId)
        {
            var url = $"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/lakehouses";
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

    private async Task<Workspace?> GetWorkspaceAsync(HttpClient httpClient, string token, string workspaceName)
    {
        await foreach (var workspace in ListWorkspacesAsync())
        {
            if (workspace.DisplayName.Equals(workspaceName, StringComparison.OrdinalIgnoreCase))
            {
                return workspace;
            }
        }

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

    //internal HttpMessage CreateLoadTableRequest(Guid workspaceId, Guid lakehouseId, string tableName, LoadTableRequest loadTableRequest)
    //{
    //    var message = _pipeline.CreateMessage();
    //    var request = message.Request;
    //    request.Method = RequestMethod.Post;
    //    var uri = new RawRequestUriBuilder();
    //    uri.Reset(_endpoint);
    //    uri.AppendPath("/workspaces/", false);
    //    uri.AppendPath(workspaceId, true);
    //    uri.AppendPath("/lakehouses/", false);
    //    uri.AppendPath(lakehouseId, true);
    //    uri.AppendPath("/tables/", false);
    //    uri.AppendPath(tableName, true);
    //    uri.AppendPath("/load", false);
    //    request.Uri = uri;
    //    request.Headers.Add("Accept", "application/json");
    //    request.Headers.Add("Content-Type", "application/json");
    //    var content = new Utf8JsonRequestContent();
    //    content.JsonWriter.WriteObjectValue<LoadTableRequest>(loadTableRequest);
    //    request.Content = content;
    //    return message;
    //}
}
