using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Files.DataLake;
using CluedIn.Connector.DataLake.Common;
using CluedIn.Connector.DataLake.Common.Connector;

namespace CluedIn.Connector.AzureDatabricks.Connector;

public class AzureDatabricksClient : DataLakeClient, IExternalStorageClient
{
    protected override DataLakeServiceClient GetDataLakeServiceClient(IDataLakeJobData configuration)
    {
        var casted = CastJobData<AzureDatabricksConnectorJobData>(configuration);
        var accountName = "onelake";

        var sharedKeyCredential = new ClientSecretCredential(casted.TenantId, casted.ClientId, casted.ClientSecret);

        var dfsUri = $"https://{accountName}.dfs.fabric.microsoft.com";

        var dataLakeServiceClient = new DataLakeServiceClient(
            new Uri(dfsUri),
            sharedKeyCredential);
        return dataLakeServiceClient;
    }

    // IExternalStorageClient implementation
    public async Task EnsureDirectoryExists(object configuration, string subDirectory = null)
    {
        await EnsureDataLakeDirectoryExist((IDataLakeJobData)configuration, subDirectory);
    }

    public async Task SaveData(object configuration, string content, string fileName, string contentType)
    {
        await SaveData((IDataLakeJobData)configuration, content, fileName, contentType);
    }

    public async Task DeleteDirectory(object configuration, string subDirectory)
    {
        await DeleteDirectory((IDataLakeJobData)configuration, subDirectory);
    }

    public async Task DeleteFile(object configuration, string fileName)
    {
        await DeleteFile((IDataLakeJobData)configuration, fileName);
    }

    public async Task<bool> FileExists(object configuration, string fileName, string subDirectory = null)
    {
        return await FileInPathExists((IDataLakeJobData)configuration, fileName, subDirectory);
    }

    public async Task<bool> DirectoryExists(object configuration, string subDirectory = null)
    {
        return await DirectoryExists((IDataLakeJobData)configuration, subDirectory);
    }

    public async Task<IEnumerable<string>> ListFiles(object configuration, string subDirectory = null)
    {
        var containers = await GetFilesInDirectory((IDataLakeJobData)configuration, subDirectory);
        var result = new List<string>();
        if (containers != null)
        {
            foreach (var c in containers)
            {
                result.Add(c.Name);
            }
        }
        return result;
    }
}
