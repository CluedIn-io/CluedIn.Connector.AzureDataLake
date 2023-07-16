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

        public async Task SaveData(AzureDataLakeConnectorJobData configuration, string content, string fileName)
        {
            var directoryClient = await EnsureDataLakeDirectoryExist(configuration);

            var dataLakeFileClient = directoryClient.GetFileClient(fileName);
            var options = new DataLakeFileUploadOptions
            {
                HttpHeaders = new PathHttpHeaders { ContentType = "application/json" }
            };

            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
            var response = await dataLakeFileClient.UploadAsync(stream, options);
            
            if (response?.Value == null)
            {
                throw new Exception($"{nameof(DataLakeFileClient)}.{nameof(DataLakeFileClient.UploadAsync)} did not return a valid path");
            }
        }

        public async Task DeleteFile(AzureDataLakeConnectorJobData configuration, string fileName)
        {
            var directoryClient = await EnsureDataLakeDirectoryExist(configuration);
            var dataLakeFileClient = directoryClient.GetFileClient(fileName);

            var response = await dataLakeFileClient.DeleteAsync();

            if (response.Status != 200)
            {
                throw new Exception($"{nameof(DataLakeFileClient)}.{nameof(DataLakeFileClient.DeleteAsync)} returned {response.Status}");
            }
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
