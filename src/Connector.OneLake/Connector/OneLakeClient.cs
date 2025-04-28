using System;
using System.Threading.Tasks;

using Azure.Identity;
using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;

using Microsoft.Extensions.Logging;
using Microsoft.Fabric.Api.Core.Models;
using Microsoft.Fabric.Api;
using Microsoft.Fabric.Api.Lakehouse.Models;

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

        var fabricClient = new FabricClient(sharedKeyCredential);

        var workspace = await GetWorkspaceAsync(fabricClient, casted.WorkspaceName);
        if (workspace == null)
        {
            throw new ApplicationException($"Workspace {casted.WorkspaceName}is not found.");
        }

        var lakehouse = await GetLakehouseAsync(fabricClient, workspace.Id, casted.ItemName);
        if (lakehouse == null)
        {
            throw new ApplicationException($"Lakehouse {casted.ItemName} is not found in workspace {workspace.Id}.");
        }

        var filePath = $"{casted.ItemFolder}/{sourceFileName}";
        await LoadTableAsync(fabricClient, workspace.Id, lakehouse.Id.Value, targetTableName, filePath);
    }

    private async Task LoadTableAsync(FabricClient fabricClient, Guid workspaceId, Guid lakehouseId, string tableName, string filePath)
    {
        Logger.LogDebug("Begin loading data from file {File} to table {TableName}.", filePath, tableName);
        await fabricClient.Lakehouse.Tables.LoadTableAsync(workspaceId, lakehouseId, tableName, new LoadTableRequest(filePath, PathType.File));
        Logger.LogDebug("End loading data from file {File} to table {TableName}.", filePath, tableName);
    }

    private async Task<Lakehouse?> GetLakehouseAsync(FabricClient fabricClient, Guid workspaceId, string lakehouseName)
    {
        Logger.LogDebug("Begin getting lakehouse from name {LakehouseName}.", lakehouseName);
        await foreach (var lakehouse in fabricClient.Lakehouse.Items.ListLakehousesAsync(workspaceId))
        {
            if (lakehouse.DisplayName.Equals(lakehouseName, StringComparison.OrdinalIgnoreCase))
            {
                Logger.LogDebug("End getting lakehouse from name {LakehouseName}. Lakehouse Id {LakehouseId}.", lakehouseName, lakehouse.Id);
                return lakehouse;
            }
        }

        Logger.LogDebug("Fail getting lakehouse from name {LakehouseName}.", lakehouseName);
        return null;
    }

    private async Task<Workspace?> GetWorkspaceAsync(FabricClient fabricClient, string workspaceName)
    {
        Logger.LogDebug("Begin getting workspace from name {WorkspaceName}.", workspaceName);
        await foreach (var workspace in fabricClient.Core.Workspaces.ListWorkspacesAsync())
        {
            if (workspace.DisplayName.Equals(workspaceName, StringComparison.OrdinalIgnoreCase))
            {
                Logger.LogDebug("End getting workspace from name {WorkspaceName}. Workspace Id {WorkspaceId}.", workspaceName, workspace.Id);
                return workspace;
            }
        }

        Logger.LogDebug("Fail getting workspace from name {WorkspaceName}.", workspaceName);
        return null;
    }
}
