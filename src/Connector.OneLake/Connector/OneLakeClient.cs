using System;
using System.IO;
using System.Threading.Tasks;

using Azure.Identity;
using Azure.Storage.Files.DataLake;

using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;

using Microsoft.Fabric.Api;
using Microsoft.Fabric.Api.Core.Models;
using Microsoft.Fabric.Api.Lakehouse.Models;

using ParquetModel = Microsoft.Fabric.Api.Lakehouse.Models.Parquet;
using CsvModel = Microsoft.Fabric.Api.Lakehouse.Models.Csv;

namespace CluedIn.Connector.OneLake.Connector;

public class OneLakeClient : DataLakeClient
{
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

        var loadTableRequest = new LoadTableRequest($"{casted.ItemFolder}/{sourceFileName}", PathType.File)
        {
            Mode = ModeType.Overwrite,
            FileExtension = Path.GetExtension(sourceFileName)[1..],
            FormatOptions = GetFileFormatOptions(configuration),
        };

        await fabricClient.Lakehouse.Tables.LoadTableAsync(workspace.Id, lakehouse.Id.Value, targetTableName, loadTableRequest);
    }

    private FileFormatOptions GetFileFormatOptions(IDataLakeJobData configuration)
    {
        if (configuration.OutputFormat.Equals(DataLakeConstants.OutputFormats.Parquet, StringComparison.OrdinalIgnoreCase))
        {
            return new ParquetModel();
        }

        if (configuration.OutputFormat.Equals(DataLakeConstants.OutputFormats.Csv, StringComparison.OrdinalIgnoreCase))
        {
            return new CsvModel();
        }

        throw new NotSupportedException($"File format {configuration.OutputFormat} is not supported.");
    }

    private async Task<Lakehouse?> GetLakehouseAsync(FabricClient fabricClient, Guid workspaceId, string lakehouseName)
    {
        await foreach (var lakehouse in fabricClient.Lakehouse.Items.ListLakehousesAsync(workspaceId))
        {
            if (lakehouse.DisplayName.Equals(lakehouseName, StringComparison.OrdinalIgnoreCase))
            {
                return lakehouse;
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
