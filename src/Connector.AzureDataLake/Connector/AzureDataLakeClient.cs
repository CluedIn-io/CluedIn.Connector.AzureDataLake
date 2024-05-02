using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using Azure;

namespace CluedIn.Connector.AzureDataLake.Connector
{
    public class AzureDataLakeClient : IAzureDataLakeClient
    {
        private readonly Dictionary<int, TaskCompletionSource<DataLakeDirectoryClient>> _getDirectoryClientTaskCompletionSource;
        private readonly object _getFileSystemClientObject;

        public AzureDataLakeClient()
        {
            _getDirectoryClientTaskCompletionSource = new();
            _getFileSystemClientObject = new();
        }

        public async Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(AzureDataLakeConnectorJobData configuration)
        {
            var key = configuration.GetHashCode();
            var isExecute = false;
            TaskCompletionSource<DataLakeDirectoryClient> getDirectoryClientTask = null;
            lock (_getFileSystemClientObject)
            {
                _getDirectoryClientTaskCompletionSource.TryGetValue(key, out getDirectoryClientTask);
                if (getDirectoryClientTask == null)
                {
                    getDirectoryClientTask = new TaskCompletionSource<DataLakeDirectoryClient>();
                    _getDirectoryClientTaskCompletionSource.Add(key, getDirectoryClientTask);
                    isExecute = true;
                }
            }

            if (isExecute)
            {
                try
                {
                    var dataLakeFileSystemClient = await EnsureDataLakeFileSystemClientAsync(configuration);
                    var directoryClient = dataLakeFileSystemClient.GetDirectoryClient(configuration.DirectoryName);
                    if (!await directoryClient.ExistsAsync())
                    {
                        directoryClient = await dataLakeFileSystemClient.CreateDirectoryAsync(configuration.DirectoryName);
                    }

                    getDirectoryClientTask.SetResult(directoryClient);

                    return directoryClient;
                }
                catch (Exception ex)
                {
                    getDirectoryClientTask.SetException(ex);

                    throw;
                }
                finally
                {
                    _getDirectoryClientTaskCompletionSource.Remove(key);
                }
            }
            else
                return await getDirectoryClientTask.Task;
        }

        public async Task SaveData(AzureDataLakeConnectorJobData configuration, string content, string fileName)
        {
            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));

            await SaveData(configuration, stream, fileName);
        }

        public async Task SaveData(AzureDataLakeConnectorJobData configuration, Stream content, string fileName)
        {
            var directoryClient = await EnsureDataLakeDirectoryExist(configuration);

            var dataLakeFileClient = directoryClient.GetFileClient(fileName);

            Response<PathInfo> response;
            if (Path.GetExtension(fileName) == ".json")
            {
                if (await dataLakeFileClient.ExistsAsync())
                    await dataLakeFileClient.DeleteAsync();

                var options = new DataLakeFileUploadOptions()
                {
                    HttpHeaders = new PathHttpHeaders { ContentType = "application/json" }
                };

                response = await dataLakeFileClient.UploadAsync(content, options);
            }
            else
                response = await dataLakeFileClient.UploadAsync(content, overwrite: true);

            if (response?.Value == null)
            {
                throw new Exception($"{nameof(DataLakeFileClient)}.{nameof(DataLakeFileClient.UploadAsync)} did not return a valid path");
            }
        }

        //public async Task SaveParquetData(AzureDataLakeConnectorJobData configuration, ParquetSchema schema,
        //    IReadOnlyCollection<Dictionary<string, object>> data, string fileName)
        //{
        //    var directoryClient = await EnsureDataLakeDirectoryExist(configuration);

        //    var dataLakeFileClient = directoryClient.GetFileClient(fileName);

        //    using var stream = new MemoryStream();
        //    await ParquetSerializer.SerializeAsync(schema, data, stream);

        //    stream.Position = 0;
        //    var response = await dataLakeFileClient.UploadAsync(stream, overwrite: true);

        //    if (response?.Value == null)
        //    {
        //        throw new Exception($"{nameof(DataLakeFileClient)}.{nameof(DataLakeFileClient.UploadAsync)} did not return a valid path");
        //    }
        //}

        //public async Task SaveParquetData(AzureDataLakeConnectorJobData configuration, Table table, string fileName)
        //{
        //    var directoryClient = await EnsureDataLakeDirectoryExist(configuration);

        //    var dataLakeFileClient = directoryClient.GetFileClient(fileName);

        //    using (var stream = new MemoryStream())
        //    {
        //        using (var writer = await ParquetWriter.CreateAsync(table.Schema, stream))
        //        {
        //            await writer.WriteAsync(table);
        //        }
        //        stream.Position = 0;
        //        var response = await dataLakeFileClient.UploadAsync(stream, overwrite: true);

        //        if (response?.Value == null)
        //        {
        //            throw new Exception($"{nameof(DataLakeFileClient)}.{nameof(DataLakeFileClient.UploadAsync)} did not return a valid path");
        //        }
        //    }
        //}

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

        private async Task<DataLakeFileSystemClient> EnsureDataLakeFileSystemClientAsync(AzureDataLakeConnectorJobData configuration)
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
