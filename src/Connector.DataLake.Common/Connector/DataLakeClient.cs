using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

using System;
using System.IO;
using System.Threading.Tasks;

namespace CluedIn.Connector.DataLake.Common.Connector
{
    public abstract class DataLakeClient : IDataLakeClient
    {
        public Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration)
        {
            return EnsureDataLakeDirectoryExist(configuration, string.Empty);
        }

        public async Task<DataLakeDirectoryClient> EnsureDataLakeDirectoryExist(IDataLakeJobData configuration, string subDirectory)
        {
            var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: true);
            var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: true);

            return directoryClient;
        }

        public async Task DeleteDirectory(IDataLakeJobData configuration, string subDirectory)
        {
            var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: true);
            var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: true);

            await directoryClient.DeleteIfExistsAsync();
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

        public Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName)
        {
            return FileInPathExists(configuration, fileName, string.Empty);
        }

        public async Task<bool> FileInPathExists(IDataLakeJobData configuration, string fileName, string subDirectory)
        {
            var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: false);
            if (!await fileSystemClient.ExistsAsync())
            {
                return false;
            }

            var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: false);
            if (!await directoryClient.ExistsAsync())
            {
                return false;
            }

            var dataLakeFileClient = directoryClient.GetFileClient(fileName);
            return await dataLakeFileClient.ExistsAsync();
        }

        public Task<bool> DirectoryExists(IDataLakeJobData configuration)
        {
            return DirectoryExists(configuration, string.Empty);
        }

        public async Task<bool> DirectoryExists(IDataLakeJobData configuration, string subDirectory)
        {
            var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: false);
            if (!await fileSystemClient.ExistsAsync())
            {
                return false;
            }

            var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: false);
            return await directoryClient.ExistsAsync();
        }

        public Task<PathProperties> GetFilePathProperties(IDataLakeJobData configuration, string fileName)
        {
            return GetFilePathProperties(configuration, fileName, string.Empty);
        }

        public async Task<PathProperties> GetFilePathProperties(IDataLakeJobData configuration, string fileName, string subDirectory)
        {
            var fileSystemClient = await GetFileSystemClientAsync(configuration, ensureExists: false);

            if (!await fileSystemClient.ExistsAsync())
            {
                return null;
            }

            var directoryClient = await GetDirectoryClientAsync(configuration, fileSystemClient, subDirectory, ensureExists: false);
            if (!await directoryClient.ExistsAsync())
            {
                return null;
            }

            var dataLakeFileClient = directoryClient.GetFileClient(fileName);
            if (!await dataLakeFileClient.ExistsAsync())
            {
                return null;
            }

            return await dataLakeFileClient.GetPropertiesAsync();
        }

        private async Task<DataLakeDirectoryClient> GetDirectoryClientAsync(
            IDataLakeJobData configuration,
            DataLakeFileSystemClient fileSystemClient,
            string subDirectory,
            bool ensureExists)
        {
            var directory = GetDirectory(configuration);
            var directoryClient = fileSystemClient.GetDirectoryClient(directory);
            if (string.IsNullOrWhiteSpace(subDirectory))
            {
                return directoryClient;
            }

            directoryClient = directoryClient.GetSubDirectoryClient(subDirectory);

            if (ensureExists && !await directoryClient.ExistsAsync())
            {
                directoryClient = await fileSystemClient.CreateDirectoryAsync(directoryClient.Path);
            }

            return directoryClient;
        }

        private async Task<DataLakeFileSystemClient> GetFileSystemClientAsync(
            IDataLakeJobData configuration,
            bool ensureExists)
        {
            var dataLakeServiceClient = GetDataLakeServiceClient(configuration);
            var fileSystemName = GetFileSystemName(configuration);
            var dataLakeFileSystemClient = dataLakeServiceClient.GetFileSystemClient(fileSystemName);
            if (ensureExists && !await dataLakeFileSystemClient.ExistsAsync())
            {
                dataLakeFileSystemClient = await dataLakeServiceClient.CreateFileSystemAsync(fileSystemName);
            }

            return dataLakeFileSystemClient;
        }
    }
}
