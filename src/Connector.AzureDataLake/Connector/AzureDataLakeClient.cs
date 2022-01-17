using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using System;
using System.IO;
using System.Threading.Tasks;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public class AzureDataLakeClient : IAzureDataLakeClient
    {
        public async Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(AzureDataLakeConnectorJobData configuration)
        {
            var dataLakeFileSystemClient = await EnsureDataLakeFileSystemClientAsync(configuration);
            var directoryClient = dataLakeFileSystemClient.GetDirectoryClient(configuration.DirectoryName);
            if (!await directoryClient.ExistsAsync())
            {
                directoryClient = await dataLakeFileSystemClient.CreateDirectoryAsync(configuration.DirectoryName);
            }

            return directoryClient;
        }

        public async Task SaveData(AzureDataLakeConnectorJobData configuration, string content)
        {
            var directoryClient = await EnsureDataLakeDirectoryExist(configuration);
            var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH-mm-ss");
            var fileName = $"{configuration.ContainerName}.{timestamp}.json";

            var dataLakeFileClient = directoryClient.GetFileClient(fileName);
            await using var memoryStream = new MemoryStream();
            await using var streamWriter = new StreamWriter(memoryStream);
            await streamWriter.WriteAsync(content);
            memoryStream.Position = 0;

            var options = new DataLakeFileUploadOptions
            {
                HttpHeaders = new PathHttpHeaders { ContentType = "application/json" }
            };

            await dataLakeFileClient.UploadAsync(memoryStream, options);
        }        

        private DataLakeServiceClient GetDataLakeServiceClient(AzureDataLakeConnectorJobData configuration)
        {
            return new DataLakeServiceClient(
                new Uri($"https://{configuration.AccountName}.dfs.core.windows.net"),
                new StorageSharedKeyCredential(configuration.AccountName, configuration.AccountKey));
        }

        private async Task<DataLakeFileSystemClient> EnsureDataLakeFileSystemClientAsync(
            AzureDataLakeConnectorJobData configuration)
        {
            var dataLakeServiceClient = GetDataLakeServiceClient(configuration);
            var dataLakeFileSystemClient = dataLakeServiceClient.GetFileSystemClient(configuration.FileSystemName);
            if (!await dataLakeFileSystemClient.ExistsAsync())
            {
                dataLakeFileSystemClient = await dataLakeServiceClient.CreateFileSystemAsync(configuration.FileSystemName);
            }

            return dataLakeFileSystemClient;
        }        
    }
}
