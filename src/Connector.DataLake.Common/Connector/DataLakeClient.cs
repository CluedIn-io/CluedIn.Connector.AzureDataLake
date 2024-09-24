using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using CluedIn.Core.Connectors;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace CluedIn.Connector.DataLake.Common.Connector
{
    public abstract class DataLakeClient : IDataLakeClient
    {
        public async Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration)
        {
            var fileSystemClient = await GetFileSystemClientAsync(configuration);
            var directory = GetDirectory(configuration);
            var directoryClient = fileSystemClient.GetDirectoryClient(directory);
            if (!await directoryClient.ExistsAsync())
            {
                directoryClient = await fileSystemClient.CreateDirectoryAsync(directory);
            }

            return directoryClient;
        }

        public async Task SaveData(IDataLakeJobData configuration, string content, string fileName, string contentType)
        {
            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
            var directoryClient = await EnsureDataLakeDirectoryExist(configuration);

            var dataLakeFileClient = directoryClient.GetFileClient(fileName);
            var options = new DataLakeFileUploadOptions
            {
                HttpHeaders = new PathHttpHeaders { ContentType = contentType }
            };

            var response = await dataLakeFileClient.UploadAsync(stream, options);

            if (response?.Value == null)
            {
                throw new Exception($"{nameof(DataLakeFileClient)}.{nameof(DataLakeFileClient.UploadAsync)} did not return a valid path");
            }
        }

        public async Task DeleteFile(IDataLakeJobData configuration, string fileName)
        {
            var directoryClient = await EnsureDataLakeDirectoryExist(configuration);
            var dataLakeFileClient = directoryClient.GetFileClient(fileName);

            var response = await dataLakeFileClient.DeleteAsync();

            if (response.Status != 200)
            {
                throw new Exception($"{nameof(DataLakeFileClient)}.{nameof(DataLakeFileClient.DeleteAsync)} returned {response.Status}");
            }
        }

        protected abstract string GetDirectory(IDataLakeJobData configuration);
        protected abstract string GetFileSystemName(IDataLakeJobData configuration);
        protected abstract DataLakeServiceClient GetDataLakeServiceClient(IDataLakeJobData configuration);

        protected static TJobData CastJobData<TJobData>(IDataLakeJobData jobData) where TJobData : class, IDataLakeJobData
        {
            if (jobData is not TJobData castedJobData)
            {
                throw new ApplicationException($"Provided job data is not of expected type '{typeof(TJobData)}'. It is '{jobData.GetType()}'.");
            }

            return castedJobData;
        }

        public async Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName)
        {
            var serviceClient = GetDataLakeServiceClient(configuration);
            var fileSystemName = GetFileSystemName(configuration);
            var fileSystemClient = serviceClient.GetFileSystemClient(fileSystemName);

            if (!await fileSystemClient.ExistsAsync())
            {
                return false;
            }

            var directory = GetDirectory(configuration);
            var directoryClient = fileSystemClient.GetDirectoryClient(directory);
            if (!await directoryClient.ExistsAsync())
            {
                return false;
            }

            var dataLakeFileClient = directoryClient.GetFileClient(fileName);
            return await dataLakeFileClient.ExistsAsync();
        }

        private async Task<DataLakeFileSystemClient> GetFileSystemClientAsync(
            IDataLakeJobData configuration)
        {
            var dataLakeServiceClient = GetDataLakeServiceClient(configuration);
            var fileSystemName = GetFileSystemName(configuration);
            var dataLakeFileSystemClient = dataLakeServiceClient.GetFileSystemClient(fileSystemName);
            if (!await dataLakeFileSystemClient.ExistsAsync())
            {
                dataLakeFileSystemClient = await dataLakeServiceClient.CreateFileSystemAsync(fileSystemName);
            }

            return dataLakeFileSystemClient;
        }

        public async Task<IEnumerable<IConnectorContainer>> GetFilesInDirectory(IDataLakeJobData configuration)
        {
            var serviceClient = GetDataLakeServiceClient(configuration);
            var fileSystemName = GetFileSystemName(configuration);
            var fileSystemClient = serviceClient.GetFileSystemClient(fileSystemName);

            if (!await fileSystemClient.ExistsAsync())
            {
                return null;
            }

            var directory = GetDirectory(configuration);
            var directoryClient = fileSystemClient.GetDirectoryClient(directory);
            if (!await directoryClient.ExistsAsync())
            {
                return null;
            }

            var files = directoryClient.GetPathsAsync().GetAsyncEnumerator();
            await files.MoveNextAsync();
            var item = files.Current;

            var result = new List<IConnectorContainer>();
            while (item != null)
            {
                if (!item.IsDirectory.GetValueOrDefault())
                {
                    result.Add(new DataLakeContainer
                    {
                        Name = Path.GetFileName(item.Name),
                        FullyQualifiedName = directoryClient.Uri.ToString() + "/" + Path.GetFileName(item.Name)
                    });
                }

                if (!await files.MoveNextAsync())
                {
                    break;
                }

                item = files.Current;
            }

            return result;
        }
    }
}
